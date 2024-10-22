"""
Snowflake steps and tasks for Koheesio

Every class in this module is a subclass of `Step` or `Task` and is used to perform operations on Snowflake.

Notes
-----
Every Step in this module is based on [SnowflakeBaseModel](./snowflake.md#koheesio.spark.snowflake.SnowflakeBaseModel).
The following parameters are available for every Step.

Parameters
----------
url : str
    Hostname for the Snowflake account, e.g. <account>.snowflakecomputing.com.
    Alias for `sfURL`.
user : str
    Login name for the Snowflake user.
    Alias for `sfUser`.
password : SecretStr
    Password for the Snowflake user.
    Alias for `sfPassword`.
database : str
    The database to use for the session after connecting.
    Alias for `sfDatabase`.
sfSchema : str
    The schema to use for the session after connecting.
    Alias for `schema` ("schema" is a reserved name in Pydantic, so we use `sfSchema` as main name instead).
role : str
    The default security role to use for the session after connecting.
    Alias for `sfRole`.
warehouse : str
    The default virtual warehouse to use for the session after connecting.
    Alias for `sfWarehouse`.
authenticator : Optional[str], optional, default=None
    Authenticator for the Snowflake user. Example: "okta.com".
options : Optional[Dict[str, Any]], optional, default={"sfCompress": "on", "continue_on_error": "off"}
    Extra options to pass to the Snowflake connector.
format : str, optional, default="snowflake"
    The default `snowflake` format can be used natively in Databricks, use `net.snowflake.spark.snowflake` in other
    environments and make sure to install required JARs.
"""

from koheesio.integrations.spark.snowflake import *
from koheesio.logger import warn

warn(
    "The koheesio.spark.snowflake module is deprecated. Please use the koheesio.integrations.spark.snowflake classes instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "AddColumn",
    "CreateOrReplaceTableFromDataFrame",
    "DbTableQuery",
    "GetTableSchema",
    "GrantPrivilegesOnFullyQualifiedObject",
    "GrantPrivilegesOnObject",
    "GrantPrivilegesOnTable",
    "GrantPrivilegesOnView",
    "Query",
    "RunQuery",
    "SnowflakeBaseModel",
    "SnowflakeReader",
    "SnowflakeStep",
    "SnowflakeTableStep",
    "SnowflakeTransformation",
    "SnowflakeWriter",
    "SyncTableAndDataFrameSchema",
    "SynchronizeDeltaToSnowflakeTask",
    "TableExists",
]