"""
Module for reading data from JDBC sources.

Classes
-------
JdbcReader
    Reader for JDBC tables.
"""

from typing import Any, Dict, Optional

from koheesio.models import Field, SecretStr
from koheesio.spark.readers import Reader


class JdbcReader(Reader):
    """
    Reader for JDBC tables.

    Wrapper around Spark's jdbc read format

    Notes
    -----
    * Query has precedence over dbtable. If query and dbtable both are filled in, dbtable will be ignored!
    * Extra options to the spark reader can be passed through the `options` input. Refer to Spark documentation
        for details: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    * Consider using `fetchsize` as one of the options, as it is greatly increases the performance of the reader
    * Consider using `numPartitions`, `partitionColumn`, `lowerBound`, `upperBound` together with real or synthetic
        partitioning column as it will improve the reader performance

    When implementing a JDBC reader, the `get_options()` method should be implemented. The method should return a dict
    of options required for the specific JDBC driver. The `get_options()` method can be overridden in the child class.
    Additionally, the `driver` parameter should be set to the name of the JDBC driver. Be aware that the driver jar
    needs to be included in the Spark session; this class does not (and can not) take care of that!

    Example
    -------
    > Note: jars should be added to the Spark session manually. This class does not take care of that.

    This example depends on the jar for MS SQL:
     `https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.2.1.jre8/mssql-jdbc-9.2.1.jre8.jar`

    ```python
    from koheesio.spark.readers.jdbc import JdbcReader

    jdbc_mssql = JdbcReader(
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
        url="jdbc:sqlserver://10.xxx.xxx.xxx:1433;databaseName=YOUR_DATABASE",
        user="YOUR_USERNAME",
        password="***",
        dbtable="schemaname.tablename",
        options={"fetchsize": 100},
    )
    df = jdbc_mssql.read()
    ```
    """

    format: str = Field(default="jdbc", description="The type of format to load. Defaults to 'jdbc'.")
    driver: str = Field(
        default=...,
        description="Driver name. Be aware that the driver jar needs to be passed to the task",
    )
    url: str = Field(
        default=...,
        description="URL for the JDBC driver. Note, in some environments you need to use the IP Address instead of "
        "the hostname of the server.",
    )
    user: str = Field(default=..., description="User to authenticate to the server")
    password: SecretStr = Field(default=..., description="Password belonging to the username")
    dbtable: Optional[str] = Field(default=None, description="Database table name, also include schema name")
    query: Optional[str] = Field(default=None, description="Query")
    options: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Extra options to pass to spark reader")

    def get_options(self):
        """
        Dictionary of options required for the specific JDBC driver.

        Note: override this method if driver requires custom names, e.g. Snowflake: `sfUrl`, `sfUser`, etc.
        """
        return {
            "driver": self.driver,
            "url": self.url,
            "user": self.user,
            "password": self.password,
            **self.options,
        }

    def execute(self):
        """Wrapper around Spark's jdbc read format"""

        # Can't have both dbtable and query empty
        if not self.dbtable and not self.query:
            raise ValueError("Please do not leave dbtable and query both empty!")

        if self.query and self.dbtable:
            self.log.info("Both 'query' and 'dbtable' are filled in, 'dbtable' will be ignored!")

        options = self.get_options()

        if pw := self.password:
            options["password"] = pw.get_secret_value()

        if query := self.query:
            options["query"] = query
            self.log.info(f"Executing query: {self.query}")
        else:
            options["dbtable"] = self.dbtable

        self.output.df = self.spark.read.format(self.format).options(**options).load()
