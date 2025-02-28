"""
HANA reader.
"""

from typing import Any, Dict, Optional

from koheesio.models import Field
from koheesio.spark.readers.jdbc import JdbcReader


class HanaReader(JdbcReader):
    """
    Wrapper around JdbcReader for SAP HANA

    Notes
    -----
    * Refer to [JdbcReader](jdbc.md#koheesio.spark.readers.jdbc.JdbcReader) for the list of all available parameters.
    * Refer to SAP HANA Client Interface Programming Reference docs for the list of all available connection string
        parameters:
        https://help.sap.com/docs/SAP_HANA_CLIENT/f1b440ded6144a54ada97ff95dac7adf/109397c2206a4ab2a5386d494f4cf75e.html

    Example
    -------
    > Note: jars should be added to the Spark session manually. This class does not take care of that.

    This example depends on the SAP HANA `ngdbc` JAR. e.g. ngdbc-2.5.49.

    ```python
    from koheesio.spark.readers.hana import HanaReader

    jdbc_hana = HanaReader(
        url="jdbc:sap://<domain_or_ip>:<port>/?<options>",
        user="YOUR_USERNAME",
        password="***",
        dbtable="schema_name.table_name",
    )
    df = jdbc_hana.read()
    ```

    Parameters
    ----------
    url : str
        JDBC connection string. Refer to SAP HANA docs for the list of all available connection string parameters.
        Example: jdbc:sap://<server>:<port>[/?<options>]
    user : str
    password : SecretStr
    dbtable : str
        Database table name, also include schema name
    params : Optional[Dict[str, Any]], default={"fetchsize": 2000, "numPartitions": 10}
        Extra options/params to pass to the SAP HANA JDBC driver. Refer to SAP HANA docs for the list of all available
        connection string parameters. Also can be passed as arbitrary keyword arguments to the HanaReader class.
        Default values: {"fetchsize": 2000, "numPartitions": 10}
    query : Optional[str]
        Query
    format : str
        The type of format to load. Defaults to 'jdbc'. Should not be changed.
    driver : str
        Driver name. Be aware that the driver jar needs to be passed to the task. Should not be changed.
    """

    driver: str = Field(
        default="com.sap.db.jdbc.Driver",
        description="Make sure that the necessary JARs are available in the cluster: ngdbc-2-x.x.x.x",
    )
    params: Optional[Dict[str, Any]] = Field(
        default={"fetchsize": 2000, "numPartitions": 10},
        description="Extra options to pass to the SAP HANA JDBC driver",
        alias="options",
    )
