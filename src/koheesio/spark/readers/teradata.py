"""
Teradata reader.
"""

from typing import Any, Dict, Optional

from koheesio.models import Field
from koheesio.spark.readers.jdbc import JdbcReader


class TeradataReader(JdbcReader):
    """
    Wrapper around JdbcReader for Teradata.

    Notes
    -----
    * Consider using synthetic partitioning column when using partitioned read:
        `MOD(HASHBUCKET(HASHROW(<TABLE>.<COLUMN>)), <NUM_PARTITIONS>)`
    * Relevant jars should be added to the Spark session manually. This class does not take care of that.

    See Also
    --------
    * Refer to [JdbcReader](jdbc.md#koheesio.spark.readers.jdbc.JdbcReader) for the list of all available
        parameters.
    * Refer to Teradata docs for the list of all available connection string parameters:
        https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABJIHBJ

    Example
    -------
    This example depends on the Teradata `terajdbc4` JAR. e.g. terajdbc4-17.20.00.15. Keep in mind that older versions
    of `terajdbc4` drivers also require `tdgssconfig` JAR.

    ```python
    from koheesio.spark.readers.teradata import TeradataReader

    td = TeradataReader(
        url="jdbc:teradata://<domain_or_ip>/logmech=ldap,charset=utf8,database=<db>,type=fastexport, maybenull=on",
        user="YOUR_USERNAME",
        password="***",
        dbtable="schema_name.table_name",
    )
    ```

    Parameters
    ----------
    url : str
        JDBC connection string. Refer to Teradata docs for the list of all available connection string parameters.
        Example: `jdbc:teradata://<domain_or_ip>/logmech=ldap,charset=utf8,database=<db>,type=fastexport, maybenull=on`
    user : str
        Username
    password : SecretStr
        Password
    dbtable : str
        Database table name, also include schema name
    options : Optional[Dict[str, Any]], optional, default={"fetchsize": 2000, "numPartitions": 10}
        Extra options to pass to the Teradata JDBC driver. Refer to Teradata docs for the list of all available
        connection string parameters.
    query : Optional[str], optional, default=None
        Query
    format : str
        The type of format to load. Defaults to 'jdbc'. Should not be changed.
    driver : str
        Driver name. Be aware that the driver jar needs to be passed to the task. Should not be changed.
    """

    driver: str = Field(
        "com.teradata.jdbc.TeraDriver",
        description="Make sure that the necessary JARs are available in the cluster: terajdbc4-x.x.x.x",
    )
    params: Optional[Dict[str, Any]] = Field(
        {"fetchsize": 2000, "numPartitions": 10},
        description="Extra options to pass to the Teradata JDBC driver",
        alias="options",
    )
