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

import json
from typing import Any, Dict, List, Optional, Set, Union
from abc import ABC
from copy import deepcopy
from textwrap import dedent

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f
from pyspark.sql import types as t

from koheesio import Step, StepOutput
from koheesio.logger import LoggingFactory, warn
from koheesio.models import (
    BaseModel,
    ExtraParamsMixin,
    Field,
    SecretStr,
    conlist,
    field_validator,
    model_validator,
)
from koheesio.spark import SparkStep
from koheesio.spark.delta import DeltaTableStep
from koheesio.spark.readers.delta import DeltaTableReader, DeltaTableStreamReader
from koheesio.spark.readers.jdbc import JdbcReader
from koheesio.spark.transformations import Transformation
from koheesio.spark.writers import BatchOutputMode, Writer
from koheesio.spark.writers.stream import (
    ForEachBatchStreamWriter,
    writer_to_foreachbatch,
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

# pylint: disable=inconsistent-mro, too-many-lines
# Turning off inconsistent-mro because we are using ABCs and Pydantic models and Tasks together in the same class
# Turning off too-many-lines because we are defining a lot of classes in this file


class SnowflakeBaseModel(BaseModel, ExtraParamsMixin, ABC):
    """
    BaseModel for setting up Snowflake Driver options.

    Notes
    -----
    * Snowflake is supported natively in Databricks 4.2 and newer:
        https://docs.snowflake.com/en/user-guide/spark-connector-databricks
    * Refer to Snowflake docs for the installation instructions for non-Databricks environments:
        https://docs.snowflake.com/en/user-guide/spark-connector-install
    * Refer to Snowflake docs for connection options:
        https://docs.snowflake.com/en/user-guide/spark-connector-use#setting-configuration-options-for-the-connector

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

    url: str = Field(
        default=...,
        alias="sfURL",
        description="Hostname for the Snowflake account, e.g. <account>.snowflakecomputing.com",
        examples=["example.snowflakecomputing.com"],
    )
    user: str = Field(default=..., alias="sfUser", description="Login name for the Snowflake user")
    password: SecretStr = Field(default=..., alias="sfPassword", description="Password for the Snowflake user")
    authenticator: Optional[str] = Field(
        default=None,
        description="Authenticator for the Snowflake user",
        examples=["okta.com"],
    )
    database: str = Field(
        default=..., alias="sfDatabase", description="The database to use for the session after connecting"
    )
    sfSchema: str = Field(default=..., alias="schema", description="The schema to use for the session after connecting")
    role: str = Field(
        default=..., alias="sfRole", description="The default security role to use for the session after connecting"
    )
    warehouse: str = Field(
        default=...,
        alias="sfWarehouse",
        description="The default virtual warehouse to use for the session after connecting",
    )
    options: Optional[Dict[str, Any]] = Field(
        default={"sfCompress": "on", "continue_on_error": "off"},
        description="Extra options to pass to the Snowflake connector",
    )
    format: str = Field(
        default="snowflake",
        description="The default `snowflake` format can be used natively in Databricks, use "
        "`net.snowflake.spark.snowflake` in other environments and make sure to install required JARs.",
    )

    def get_options(self):
        """Get the sfOptions as a dictionary."""
        return {
            key: value
            for key, value in {
                "sfURL": self.url,
                "sfUser": self.user,
                "sfPassword": self.password.get_secret_value(),
                "authenticator": self.authenticator,
                "sfDatabase": self.database,
                "sfSchema": self.sfSchema,
                "sfRole": self.role,
                "sfWarehouse": self.warehouse,
                **self.options,
            }.items()
            if value is not None
        }


class SnowflakeStep(SnowflakeBaseModel, SparkStep, ABC):
    """Expands the SnowflakeBaseModel so that it can be used as a Step"""


class SnowflakeTableStep(SnowflakeStep, ABC):
    """Expands the SnowflakeStep, adding a 'table' parameter"""

    table: str = Field(default=..., description="The name of the table")

    def get_options(self):
        options = super().get_options()
        options["table"] = self.table
        return options


class SnowflakeReader(SnowflakeBaseModel, JdbcReader):
    """
    Wrapper around JdbcReader for Snowflake.

    Example
    -------
    ```python
    sr = SnowflakeReader(
        url="foo.snowflakecomputing.com",
        user="YOUR_USERNAME",
        password="***",
        database="db",
        schema="schema",
    )
    df = sr.read()
    ```

    Notes
    -----
    * Snowflake is supported natively in Databricks 4.2 and newer:
        https://docs.snowflake.com/en/user-guide/spark-connector-databricks
    * Refer to Snowflake docs for the installation instructions for non-Databricks environments:
        https://docs.snowflake.com/en/user-guide/spark-connector-install
    * Refer to Snowflake docs for connection options:
        https://docs.snowflake.com/en/user-guide/spark-connector-use#setting-configuration-options-for-the-connector
    """

    driver: Optional[str] = None  # overriding `driver` property of JdbcReader, because it is not required by Snowflake


class SnowflakeTransformation(SnowflakeBaseModel, Transformation, ABC):
    """Adds Snowflake parameters to the Transformation class"""


class RunQuery(SnowflakeStep):
    """
    Run a query on Snowflake that does not return a result, e.g. create table statement

    This is a wrapper around 'net.snowflake.spark.snowflake.Utils.runQuery' on the JVM

    Example
    -------
    ```python
    RunQuery(
        database="MY_DB",
        schema="MY_SCHEMA",
        warehouse="MY_WH",
        user="account",
        password="***",
        role="APPLICATION.SNOWFLAKE.ADMIN",
        query="CREATE TABLE test (col1 string)",
    ).execute()
    ```
    """

    query: str = Field(default=..., description="The query to run", alias="sql")

    @field_validator("query")
    def validate_query(cls, query):
        """Replace escape characters"""
        return query.replace("\\n", "\n").replace("\\t", "\t").strip()

    def get_options(self):
        # Executing the RunQuery without `host` option in Databricks throws:
        # An error occurred while calling z:net.snowflake.spark.snowflake.Utils.runQuery.
        # : java.util.NoSuchElementException: key not found: host
        options = super().get_options()
        options["host"] = options["sfURL"]
        return options

    def execute(self) -> None:
        if not self.query:
            self.log.warning("Empty string given as query input, skipping execution")
            return
        # noinspection PyProtectedMember
        self.spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(self.get_options(), self.query)


class Query(SnowflakeReader):
    """
    Query data from Snowflake and return the result as a DataFrame

    Example
    -------
    ```python
    Query(
        database="MY_DB",
        schema_="MY_SCHEMA",
        warehouse="MY_WH",
        user="gid.account@nike.com",
        password=Secret("super-secret-password"),
        role="APPLICATION.SNOWFLAKE.ADMIN",
        query="SELECT * FROM MY_TABLE",
    ).execute().df
    ```
    """

    query: str = Field(default=..., description="The query to run")

    @field_validator("query")
    def validate_query(cls, query):
        """Replace escape characters"""
        query = query.replace("\\n", "\n").replace("\\t", "\t").strip()
        return query

    def get_options(self):
        """add query to options"""
        options = super().get_options()
        options["query"] = self.query
        return options


class DbTableQuery(SnowflakeReader):
    """
    Read table from Snowflake using the `dbtable` option instead of `query`

    Example
    -------
    ```python
    DbTableQuery(
        database="MY_DB",
        schema_="MY_SCHEMA",
        warehouse="MY_WH",
        user="user",
        password=Secret("super-secret-password"),
        role="APPLICATION.SNOWFLAKE.ADMIN",
        table="db.schema.table",
    ).execute().df
    ```
    """

    dbtable: str = Field(default=..., alias="table", description="The name of the table")


class TableExists(SnowflakeTableStep):
    """
    Check if the table exists in Snowflake by using INFORMATION_SCHEMA.

    Example
    -------
    ```python
    k = TableExists(
        url="foo.snowflakecomputing.com",
        user="YOUR_USERNAME",
        password="***",
        database="db",
        schema="schema",
        table="table",
    )
    ```
    """

    class Output(StepOutput):
        """Output class for TableExists"""

        exists: bool = Field(default=..., description="Whether or not the table exists")

    def execute(self):
        query = (
            dedent(
                # Force upper case, due to case-sensitivity of where clause
                f"""
            SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG     = '{self.database}'
              AND TABLE_SCHEMA      = '{self.sfSchema}'
              AND TABLE_TYPE        = 'BASE TABLE'
              AND upper(TABLE_NAME) = '{self.table.upper()}'
            """  # nosec B608: hardcoded_sql_expressions
            )
            .upper()
            .strip()
        )

        self.log.debug(f"Query that was executed to check if the table exists:\n{query}")

        df = Query(**self.get_options(), query=query).read()

        exists = df.count() > 0
        self.log.info(f"Table {self.table} {'exists' if exists else 'does not exist'}")
        self.output.exists = exists


def map_spark_type(spark_type: t.DataType):
    """
    Translates Spark DataFrame Schema type to SnowFlake type

    | Basic Types       | Snowflake Type |
    |-------------------|----------------|
    | StringType        | STRING         |
    | NullType          | STRING         |
    | BooleanType       | BOOLEAN        |

    | Numeric Types     | Snowflake Type |
    |-------------------|----------------|
    | LongType          | BIGINT         |
    | IntegerType       | INT            |
    | ShortType         | SMALLINT       |
    | DoubleType        | DOUBLE         |
    | FloatType         | FLOAT          |
    | NumericType       | FLOAT          |
    | ByteType          | BINARY         |

    | Date / Time Types | Snowflake Type |
    |-------------------|----------------|
    | DateType          | DATE           |
    | TimestampType     | TIMESTAMP      |

    | Advanced Types    | Snowflake Type |
    |-------------------|----------------|
    | DecimalType       | DECIMAL        |
    | MapType           | VARIANT        |
    | ArrayType         | VARIANT        |
    | StructType        | VARIANT        |

    References
    ----------
    - Spark SQL DataTypes: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    - Snowflake DataTypes: https://docs.snowflake.com/en/sql-reference/data-types.html

    Parameters
    ----------
    spark_type : pyspark.sql.types.DataType
        DataType taken out of the StructField

    Returns
    -------
    str
        The Snowflake data type
    """
    # StructField means that the entire Field was passed, we need to extract just the dataType before continuing
    if isinstance(spark_type, t.StructField):
        spark_type = spark_type.dataType

    # Check if the type is DayTimeIntervalType
    if isinstance(spark_type, t.DayTimeIntervalType):
        warn(
            "DayTimeIntervalType is being converted to STRING. "
            "Consider converting to a more supported date/time/timestamp type in Snowflake."
        )

    # fmt: off
    # noinspection PyUnresolvedReferences
    data_type_map = {
        # Basic Types
        t.StringType: "STRING",
        t.NullType: "STRING",
        t.BooleanType: "BOOLEAN",

        # Numeric Types
        t.LongType: "BIGINT",
        t.IntegerType: "INT",
        t.ShortType: "SMALLINT",
        t.DoubleType: "DOUBLE",
        t.FloatType: "FLOAT",
        t.NumericType: "FLOAT",
        t.ByteType: "BINARY",
        t.BinaryType: "VARBINARY",

        # Date / Time Types
        t.DateType: "DATE",
        t.TimestampType: "TIMESTAMP",
        t.DayTimeIntervalType: "STRING",

        # Advanced Types
        t.DecimalType:
            f"DECIMAL({spark_type.precision},{spark_type.scale})"  # pylint: disable=no-member
            if isinstance(spark_type, t.DecimalType) else "DECIMAL(38,0)",
        t.MapType: "VARIANT",
        t.ArrayType: "VARIANT",
        t.StructType: "VARIANT",
    }
    return data_type_map.get(type(spark_type), 'STRING')
    # fmt: on


class CreateOrReplaceTableFromDataFrame(SnowflakeTransformation):
    """
    Create (or Replace) a Snowflake table which has the same schema as a Spark DataFrame

    Can be used as any Transformation. The DataFrame is however left unchanged, and only used for determining the
    schema of the Snowflake Table that is to be created (or replaced).

    Example
    -------
    ```python
    CreateOrReplaceTableFromDataFrame(
        database="MY_DB",
        schema="MY_SCHEMA",
        warehouse="MY_WH",
        user="gid.account@nike.com",
        password="super-secret-password",
        role="APPLICATION.SNOWFLAKE.ADMIN",
        table="MY_TABLE",
        df=df,
    ).execute()
    ```

    Or, as a Transformation:
    ```python
    CreateOrReplaceTableFromDataFrame(
        ...
        table="MY_TABLE",
    ).transform(df)
    ```

    """

    table: str = Field(default=..., alias="table_name", description="The name of the (new) table")

    class Output(SnowflakeTransformation.Output):
        """Output class for CreateOrReplaceTableFromDataFrame"""

        input_schema: t.StructType = Field(default=..., description="The original schema from the input DataFrame")
        snowflake_schema: str = Field(
            default=..., description="Derived Snowflake table schema based on the input DataFrame"
        )
        query: str = Field(default=..., description="Query that was executed to create the table")

    def execute(self):
        self.output.df = self.df

        input_schema = self.df.schema
        self.output.input_schema = input_schema

        snowflake_schema = ", ".join([f"{c.name} {map_spark_type(c.dataType)}" for c in input_schema])
        self.output.snowflake_schema = snowflake_schema

        table_name = f"{self.database}.{self.sfSchema}.{self.table}"
        query = f"CREATE OR REPLACE TABLE {table_name} ({snowflake_schema})"
        self.output.query = query

        RunQuery(**self.get_options(), query=query).execute()


class GrantPrivilegesOnObject(SnowflakeStep):
    """
    A wrapper on Snowflake GRANT privileges

    With this Step, you can grant Snowflake privileges to a set of roles on a table, a view, or an object

    See Also
    --------
    https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html

    Parameters
    ----------
    warehouse : str
        The name of the warehouse. Alias for `sfWarehouse`
    user : str
        The username. Alias for `sfUser`
    password : SecretStr
        The password. Alias for `sfPassword`
    role : str
        The role name
    object : str
        The name of the object to grant privileges on
    type : str
        The type of object to grant privileges on, e.g. TABLE, VIEW
    privileges : Union[conlist(str, min_length=1), str]
        The Privilege/Permission or list of Privileges/Permissions to grant on the given object.
    roles : Union[conlist(str, min_length=1), str]
        The Role or list of Roles to grant the privileges to

    Example
    -------
    ```python
    GrantPermissionsOnTable(
        object="MY_TABLE",
        type="TABLE",
        warehouse="MY_WH",
        user="gid.account@nike.com",
        password=Secret("super-secret-password"),
        role="APPLICATION.SNOWFLAKE.ADMIN",
        permissions=["SELECT", "INSERT"],
    ).execute()
    ```

    In this example, the `APPLICATION.SNOWFLAKE.ADMIN` role will be granted `SELECT` and `INSERT` privileges on
    the `MY_TABLE` table using the `MY_WH` warehouse.
    """

    object: str = Field(default=..., description="The name of the object to grant privileges on")
    type: str = Field(default=..., description="The type of object to grant privileges on, e.g. TABLE, VIEW")

    privileges: Union[conlist(str, min_length=1), str] = Field(
        default=...,
        alias="permissions",
        description="The Privilege/Permission or list of Privileges/Permissions to grant on the given object. "
        "See https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html",
    )
    roles: Union[conlist(str, min_length=1), str] = Field(
        default=...,
        alias="role",
        validation_alias="roles",
        description="The Role or list of Roles to grant the privileges to",
    )

    class Output(SnowflakeStep.Output):
        """Output class for GrantPrivilegesOnObject"""

        query: conlist(str, min_length=1) = Field(
            default=..., description="Query that was executed to grant privileges", validate_default=False
        )

    @model_validator(mode="before")
    def set_roles_privileges(cls, values):
        """Coerce roles and privileges to be lists if they are not already."""
        roles_value = values.get("roles") or values.get("role")
        privileges_value = values.get("privileges")

        if not (roles_value and privileges_value):
            raise ValueError("You have to specify roles AND privileges when using 'GrantPrivilegesOnObject'.")

        # coerce values to be lists
        values["roles"] = [roles_value] if isinstance(roles_value, str) else roles_value
        values["role"] = values["roles"][0]  # hack to keep the validator happy
        values["privileges"] = [privileges_value] if isinstance(privileges_value, str) else privileges_value

        return values

    @model_validator(mode="after")
    def validate_object_and_object_type(self):
        """Validate that the object and type are set."""
        object_value = self.object
        if not object_value:
            raise ValueError("You must provide an `object`, this should be the name of the object. ")

        object_type = self.type
        if not object_type:
            raise ValueError(
                "You must provide a `type`, e.g. TABLE, VIEW, DATABASE. "
                "See https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html"
            )

        return self

    def get_query(self, role: str):
        """Build the GRANT query

        Parameters
        ----------
        role: str
            The role name

        Returns
        -------
        query : str
            The Query that performs the grant
        """
        query = f"GRANT {','.join(self.privileges)} ON {self.type} {self.object} TO ROLE {role}".upper()
        return query

    def execute(self):
        self.output.query = []
        roles = self.roles

        for role in roles:
            query = self.get_query(role)
            self.output.query.append(query)
            RunQuery(**self.get_options(), query=query).execute()


class GrantPrivilegesOnFullyQualifiedObject(GrantPrivilegesOnObject):
    """Grant Snowflake privileges to a set of roles on a fully qualified object, i.e. `database.schema.object_name`

    This class is a subclass of `GrantPrivilegesOnObject` and is used to grant privileges on a fully qualified object.
    The advantage of using this class is that it sets the object name to be fully qualified, i.e.
    `database.schema.object_name`.

    Meaning, you can set the `database`, `schema` and `object` separately and the object name will be set to be fully
    qualified, i.e. `database.schema.object_name`.

    Example
    -------
    ```python
    GrantPrivilegesOnFullyQualifiedObject(
        database="MY_DB",
        schema="MY_SCHEMA",
        warehouse="MY_WH",
        ...
        object="MY_TABLE",
        type="TABLE",
        ...
    )
    ```

    In this example, the object name will be set to be fully qualified, i.e. `MY_DB.MY_SCHEMA.MY_TABLE`.
    If you were to use `GrantPrivilegesOnObject` instead, you would have to set the object name to be fully qualified
    yourself.
    """

    @model_validator(mode="after")
    def set_object_name(self):
        """Set the object name to be fully qualified, i.e. database.schema.object_name"""
        # database, schema, obj_name
        db = self.database
        schema = self.model_dump()["sfSchema"]  # since "schema" is a reserved name
        obj_name = self.object

        self.object = f"{db}.{schema}.{obj_name}"

        return self


class GrantPrivilegesOnTable(GrantPrivilegesOnFullyQualifiedObject):
    """Grant Snowflake privileges to a set of roles on a table"""

    type: str = "TABLE"
    object: str = Field(
        default=...,
        alias="table",
        description="The name of the Table to grant Privileges on. This should be just the name of the table; so "
        "without Database and Schema, use sfDatabase/database and sfSchema/schema to set those instead.",
    )


class GrantPrivilegesOnView(GrantPrivilegesOnFullyQualifiedObject):
    """Grant Snowflake privileges to a set of roles on a view"""

    type: str = "VIEW"
    object: str = Field(
        default=...,
        alias="view",
        description="The name of the View to grant Privileges on. This should be just the name of the view; so "
        "without Database and Schema, use sfDatabase/database and sfSchema/schema to set those instead.",
    )


class GetTableSchema(SnowflakeStep):
    """
    Get the schema from a Snowflake table as a Spark Schema

    Notes
    -----
    * This Step will execute a `SELECT * FROM <table> LIMIT 1` query to get the schema of the table.
    * The schema will be stored in the `table_schema` attribute of the output.
    * `table_schema` is used as the attribute name to avoid conflicts with the `schema` attribute of Pydantic's
        BaseModel.

    Example
    -------
    ```python
    schema = (
        GetTableSchema(
            database="MY_DB",
            schema_="MY_SCHEMA",
            warehouse="MY_WH",
            user="gid.account@nike.com",
            password="super-secret-password",
            role="APPLICATION.SNOWFLAKE.ADMIN",
            table="MY_TABLE",
        )
        .execute()
        .table_schema
    )
    ```
    """

    table: str = Field(default=..., description="The Snowflake table name")

    class Output(StepOutput):
        """Output class for GetTableSchema"""

        table_schema: t.StructType = Field(default=..., serialization_alias="schema", description="The Spark Schema")

    def execute(self) -> Output:
        query = f"SELECT * FROM {self.table} LIMIT 1"  # nosec B608: hardcoded_sql_expressions
        df = Query(**self.get_options(), query=query).execute().df
        self.output.table_schema = df.schema


class AddColumn(SnowflakeStep):
    """
    Add an empty column to a Snowflake table with given name and DataType

    Example
    -------
    ```python
    AddColumn(
        database="MY_DB",
        schema_="MY_SCHEMA",
        warehouse="MY_WH",
        user="gid.account@nike.com",
        password=Secret("super-secret-password"),
        role="APPLICATION.SNOWFLAKE.ADMIN",
        table="MY_TABLE",
        col="MY_COL",
        dataType=StringType(),
    ).execute()
    ```
    """

    table: str = Field(default=..., description="The name of the Snowflake table")
    column: str = Field(default=..., description="The name of the new column")
    type: f.DataType = Field(default=..., description="The DataType represented as a Spark DataType")

    class Output(SnowflakeStep.Output):
        """Output class for AddColumn"""

        query: str = Field(default=..., description="Query that was executed to add the column")

    def execute(self):
        query = f"ALTER TABLE {self.table} ADD COLUMN {self.column} {map_spark_type(self.type)}".upper()
        self.output.query = query
        RunQuery(**self.get_options(), query=query).execute()


class SyncTableAndDataFrameSchema(SnowflakeStep, SnowflakeTransformation):
    """
    Sync the schema's of a Snowflake table and a DataFrame. This will add NULL columns for the columns that are not in
    both and perform type casts where needed.

    The Snowflake table will take priority in case of type conflicts.
    """

    df: DataFrame = Field(default=..., description="The Spark DataFrame")
    table: str = Field(default=..., description="The table name")
    dry_run: Optional[bool] = Field(default=False, description="Only show schema differences, do not apply changes")

    class Output(SparkStep.Output):
        """Output class for SyncTableAndDataFrameSchema"""

        original_df_schema: t.StructType = Field(default=..., description="Original DataFrame schema")
        original_sf_schema: t.StructType = Field(default=..., description="Original Snowflake schema")
        new_df_schema: t.StructType = Field(default=..., description="New DataFrame schema")
        new_sf_schema: t.StructType = Field(default=..., description="New Snowflake schema")
        sf_table_altered: bool = Field(
            default=False, description="Flag to indicate whether Snowflake schema has been altered"
        )

    def execute(self):
        self.log.warning("Snowflake table will always take a priority in case of data type conflicts!")

        # spark side
        df_schema = self.df.schema
        self.output.original_df_schema = deepcopy(df_schema)  # using deepcopy to avoid storing in place changes
        df_cols = [c.name.lower() for c in df_schema]

        # snowflake side
        sf_schema = GetTableSchema(**self.get_options(), table=self.table).execute().table_schema
        self.output.original_sf_schema = sf_schema
        sf_cols = [c.name.lower() for c in sf_schema]

        if self.dry_run:
            # Display differences between Spark DataFrame and Snowflake schemas
            # and provide dummy values that are expected as class outputs.
            self.log.warning(f"Columns to be added to Snowflake table: {set(df_cols) - set(sf_cols)}")
            self.log.warning(f"Columns to be added to Spark DataFrame: {set(sf_cols) - set(df_cols)}")

            self.output.new_df_schema = t.StructType()
            self.output.new_sf_schema = t.StructType()
            self.output.df = self.df
            self.output.sf_table_altered = False

        else:
            # Add columns to SnowFlake table that exist in DataFrame
            for df_column in df_schema:
                if df_column.name.lower() not in sf_cols:
                    AddColumn(
                        **self.get_options(),
                        table=self.table,
                        column=df_column.name,
                        type=df_column.dataType,
                    ).execute()
                    self.output.sf_table_altered = True

            if self.output.sf_table_altered:
                sf_schema = GetTableSchema(**self.get_options(), table=self.table).execute().table_schema
                sf_cols = [c.name.lower() for c in sf_schema]

            self.output.new_sf_schema = sf_schema

            # Add NULL columns to the DataFrame if they exist in SnowFlake but not in the df
            df = self.df
            for sf_col in self.output.original_sf_schema:
                sf_col_name = sf_col.name.lower()
                if sf_col_name not in df_cols:
                    sf_col_type = sf_col.dataType
                    df = df.withColumn(sf_col_name, f.lit(None).cast(sf_col_type))

            # Put DataFrame columns in the same order as the Snowflake table
            df = df.select(*sf_cols)

            self.output.df = df
            self.output.new_df_schema = df.schema


class SnowflakeWriter(SnowflakeBaseModel, Writer):
    """Class for writing to Snowflake

    See Also
    --------
    - [koheesio.steps.writers.Writer](writers/index.md#koheesio.spark.writers.Writer)
    - [koheesio.steps.writers.BatchOutputMode](writers/index.md#koheesio.spark.writers.BatchOutputMode)
    - [koheesio.steps.writers.StreamingOutputMode](writers/index.md#koheesio.spark.writers.StreamingOutputMode)
    """

    table: str = Field(default=..., description="Target table name")
    insert_type: Optional[BatchOutputMode] = Field(
        BatchOutputMode.APPEND, alias="mode", description="The insertion type, append or overwrite"
    )

    def execute(self):
        """Write to Snowflake"""
        self.log.debug(f"writing to {self.table} with mode {self.insert_type}")
        self.df.write.format(self.format).options(**self.get_options()).option("dbtable", self.table).mode(
            self.insert_type
        ).save()


class TagSnowflakeQuery(Step, ExtraParamsMixin):
    """
    Provides Snowflake query tag pre-action that can be used to easily find queries through SF history search
    and further group them for debugging and cost tracking purposes.

    Takes in query tag attributes as kwargs and additional Snowflake options dict that can optionally contain
    other set of pre-actions to be applied to a query, in that case existing pre-action aren't dropped, query tag
    pre-action will be added to them.

    Passed Snowflake options dictionary is not modified in-place, instead anew dictionary containing updated pre-actions
    is returned.

    Notes
    -----
    See this article for explanation: https://select.dev/posts/snowflake-query-tags

    Arbitrary tags can be applied, such as team, dataset names, business capability, etc.

    Example
    -------
    ```python
    query_tag = AddQueryTag(
        options={"preactions": ...},
        task_name="cleanse_task",
        pipeline_name="ingestion-pipeline",
        etl_date="2022-01-01",
        pipeline_execution_time="2022-01-01T00:00:00",
        task_execution_time="2022-01-01T01:00:00",
        environment="dev",
        trace_id="e0fdec43-a045-46e5-9705-acd4f3f96045",
        span_id="cb89abea-1c12-471f-8b12-546d2d66f6cb",
        ),
    ).execute().options
    ```
    """

    options: Dict = Field(
        default_factory=dict, description="Additional Snowflake options, optionally containing additional preactions"
    )

    class Output(StepOutput):
        """Output class for AddQueryTag"""

        options: Dict = Field(default=..., description="Copy of provided SF options, with added query tag preaction")

    def execute(self):
        """Add query tag preaction to Snowflake options"""
        tag_json = json.dumps(self.extra_params, indent=4, sort_keys=True)
        tag_preaction = f"ALTER SESSION SET QUERY_TAG = '{tag_json}';"
        preactions = self.options.get("preactions", "")
        preactions = f"{preactions}\n{tag_preaction}".strip()
        updated_options = dict(self.options)
        updated_options["preactions"] = preactions
        self.output.options = updated_options


class SynchronizeDeltaToSnowflakeTask(SnowflakeStep):
    """
    Synchronize a Delta table to a Snowflake table

    * Overwrite - only in batch mode
    * Append - supports batch and streaming mode
    * Merge - only in streaming mode

    Example
    -------
    ```python
    SynchronizeDeltaToSnowflakeTask(
        url="acme.snowflakecomputing.com",
        user="admin",
        role="ADMIN",
        warehouse="SF_WAREHOUSE",
        database="SF_DATABASE",
        schema="SF_SCHEMA",
        source_table=DeltaTableStep(...),
        target_table="my_sf_table",
        key_columns=[
            "id",
        ],
        streaming=False,
    ).run()
    ```
    """

    source_table: DeltaTableStep = Field(default=..., description="Source delta table to synchronize")
    target_table: str = Field(default=..., description="Target table in snowflake to synchronize to")
    synchronisation_mode: BatchOutputMode = Field(
        default=BatchOutputMode.MERGE,
        description="Determines if synchronisation will 'overwrite' any existing table, 'append' new rows or "
        "'merge' with existing rows.",
    )
    checkpoint_location: Optional[str] = Field(default=None, description="Checkpoint location to use")
    schema_tracking_location: Optional[str] = Field(
        default=None,
        description="Schema tracking location to use. "
        "Info: https://docs.delta.io/latest/delta-streaming.html#-schema-tracking",
    )
    staging_table_name: Optional[str] = Field(
        default=None, alias="staging_table", description="Optional snowflake staging name", validate_default=False
    )
    key_columns: Optional[List[str]] = Field(
        default_factory=list,
        description="Key columns on which merge statements will be MERGE statement will be applied.",
    )
    streaming: Optional[bool] = Field(
        default=False,
        description="Should synchronisation happen in streaming or in batch mode. Streaming is supported in 'APPEND' "
        "and 'MERGE' mode. Batch is supported in 'OVERWRITE' and 'APPEND' mode.",
    )
    persist_staging: Optional[bool] = Field(
        default=False,
        description="In case of debugging, set `persist_staging` to True to retain the staging table for inspection "
        "after synchronization.",
    )

    enable_deletion: Optional[bool] = Field(
        default=False,
        description="In case of merge synchronisation_mode add deletion statement in merge query.",
    )

    writer_: Optional[Union[ForEachBatchStreamWriter, SnowflakeWriter]] = None

    @field_validator("staging_table_name")
    def _validate_staging_table(cls, staging_table_name):
        """Validate the staging table name and return it if it's valid."""
        if "." in staging_table_name:
            raise ValueError(
                "Custom staging table must not contain '.', it is located in the same Schema as the target table."
            )
        return staging_table_name

    @model_validator(mode="before")
    def _checkpoint_location_check(cls, values: Dict):
        """Give a warning if checkpoint location is given but not expected and vice versa"""
        streaming = values.get("streaming")
        checkpoint_location = values.get("checkpoint_location")
        log = LoggingFactory.get_logger(cls.__name__)

        if streaming is False and checkpoint_location is not None:
            log.warning("checkpoint_location is provided but will be ignored in batch mode")
        if streaming is True and checkpoint_location is None:
            log.warning("checkpoint_location is not provided in streaming mode")
        return values

    @model_validator(mode="before")
    def _synch_mode_check(cls, values: Dict):
        """Validate requirements for various synchronisation modes"""
        streaming = values.get("streaming")
        synchronisation_mode = values.get("synchronisation_mode")
        key_columns = values.get("key_columns")

        allowed_output_modes = [BatchOutputMode.OVERWRITE, BatchOutputMode.MERGE, BatchOutputMode.APPEND]

        if synchronisation_mode not in allowed_output_modes:
            raise ValueError(
                f"Synchronisation mode should be one of {', '.join([m.value for m in allowed_output_modes])}"
            )
        if synchronisation_mode == BatchOutputMode.OVERWRITE and streaming is True:
            raise ValueError("Synchronisation mode can't be 'OVERWRITE' with streaming enabled")
        if synchronisation_mode == BatchOutputMode.MERGE and streaming is False:
            raise ValueError("Synchronisation mode can't be 'MERGE' with streaming disabled")
        if synchronisation_mode == BatchOutputMode.MERGE and len(key_columns) < 1:
            raise ValueError("MERGE synchronisation mode requires a list of PK columns in `key_columns`.")

        return values

    @property
    def non_key_columns(self) -> List[str]:
        """Columns of source table that aren't part of the (composite) primary key"""
        lowercase_key_columns: Set[str] = {c.lower() for c in self.key_columns}
        source_table_columns = self.source_table.columns
        non_key_columns: List[str] = [c for c in source_table_columns if c.lower() not in lowercase_key_columns]
        return non_key_columns

    @property
    def staging_table(self):
        """Intermediate table on snowflake where staging results are stored"""
        if stg_tbl_name := self.staging_table_name:
            return stg_tbl_name

        return f"{self.source_table.table}_stg"

    @property
    def reader(self):
        """
        DeltaTable reader

        Returns:
        --------
            DeltaTableReader the will yield source delta table
        """
        # Wrap in lambda functions to mimic lazy evaluation.
        # This ensures the Task doesn't fail if a config isn't provided for a reader/writer that isn't used anyway
        map_mode_reader = {
            BatchOutputMode.OVERWRITE: lambda: DeltaTableReader(
                table=self.source_table, streaming=False, schema_tracking_location=self.schema_tracking_location
            ),
            BatchOutputMode.APPEND: lambda: DeltaTableReader(
                table=self.source_table,
                streaming=self.streaming,
                schema_tracking_location=self.schema_tracking_location,
            ),
            BatchOutputMode.MERGE: lambda: DeltaTableStreamReader(
                table=self.source_table, read_change_feed=True, schema_tracking_location=self.schema_tracking_location
            ),
        }
        return map_mode_reader[self.synchronisation_mode]()

    def _get_writer(self) -> Union[SnowflakeWriter, ForEachBatchStreamWriter]:
        """
        Writer to persist to snowflake

        Depending on configured options, this returns an SnowflakeWriter or ForEachBatchStreamWriter:
        - OVERWRITE/APPEND mode yields SnowflakeWriter
        - MERGE mode yields ForEachBatchStreamWriter

        Returns
        -------
        ForEachBatchStreamWriter | SnowflakeWriter
            The right writer for the configured options and mode
        """
        # Wrap in lambda functions to mimic lazy evaluation.
        # This ensures the Task doesn't fail if a config isn't provided for a reader/writer that isn't used anyway
        map_mode_writer = {
            (BatchOutputMode.OVERWRITE, False): lambda: SnowflakeWriter(
                table=self.target_table, insert_type=BatchOutputMode.OVERWRITE, **self.get_options()
            ),
            (BatchOutputMode.APPEND, False): lambda: SnowflakeWriter(
                table=self.target_table, insert_type=BatchOutputMode.APPEND, **self.get_options()
            ),
            (BatchOutputMode.APPEND, True): lambda: ForEachBatchStreamWriter(
                checkpointLocation=self.checkpoint_location,
                batch_function=writer_to_foreachbatch(
                    SnowflakeWriter(table=self.target_table, insert_type=BatchOutputMode.APPEND, **self.get_options())
                ),
            ),
            (BatchOutputMode.MERGE, True): lambda: ForEachBatchStreamWriter(
                checkpointLocation=self.checkpoint_location,
                batch_function=self._merge_batch_write_fn(
                    key_columns=self.key_columns,
                    non_key_columns=self.non_key_columns,
                    staging_table=self.staging_table,
                ),
            ),
        }
        return map_mode_writer[(self.synchronisation_mode, self.streaming)]()

    @property
    def writer(self) -> Union[ForEachBatchStreamWriter, SnowflakeWriter]:
        """
        Writer to persist to snowflake

        Depending on configured options, this returns an SnowflakeWriter or ForEachBatchStreamWriter:
        - OVERWRITE/APPEND mode yields SnowflakeWriter
        - MERGE mode yields ForEachBatchStreamWriter

        Returns
        -------
            Union[ForEachBatchStreamWriter, SnowflakeWriter]
        """
        # Cache 'writer' object in memory to ensure same object is used everywhere, this ensures access to underlying
        # member objects such as active streaming queries (if any).
        if not self.writer_:
            self.writer_ = self._get_writer()
        return self.writer_

    def truncate_table(self, snowflake_table):
        """Truncate a given snowflake table"""
        truncate_query = f"""TRUNCATE TABLE IF EXISTS {snowflake_table}"""
        query_executor = RunQuery(
            **self.get_options(),
            query=truncate_query,
        )
        query_executor.execute()

    def drop_table(self, snowflake_table):
        """Drop a given snowflake table"""
        self.log.warning(f"Dropping table {snowflake_table} from snowflake")
        drop_table_query = f"""DROP TABLE IF EXISTS {snowflake_table}"""
        query_executor = RunQuery(**self.get_options(), query=drop_table_query)
        query_executor.execute()

    def _merge_batch_write_fn(self, key_columns, non_key_columns, staging_table):
        """Build a batch write function for merge mode"""

        # pylint: disable=unused-argument
        def inner(dataframe: DataFrame, batchId: int):
            self._build_staging_table(dataframe, key_columns, non_key_columns, staging_table)
            self._merge_staging_table_into_target()

        # pylint: enable=unused-argument
        return inner

    @staticmethod
    def _compute_latest_changes_per_pk(
        dataframe: DataFrame, key_columns: List[str], non_key_columns: List[str]
    ) -> DataFrame:
        """Compute the latest changes per primary key"""
        windowSpec = Window.partitionBy(*key_columns).orderBy(f.col("_commit_version").desc())
        ranked_df = (
            dataframe.filter("_change_type != 'update_preimage'")
            .withColumn("rank", f.rank().over(windowSpec))
            .filter("rank = 1")
            .select(*key_columns, *non_key_columns, "_change_type")  # discard unused columns
            .distinct()
        )
        return ranked_df

    def _build_staging_table(self, dataframe, key_columns, non_key_columns, staging_table):
        """Build snowflake staging table"""
        ranked_df = self._compute_latest_changes_per_pk(dataframe, key_columns, non_key_columns)
        batch_writer = SnowflakeWriter(
            table=staging_table, df=ranked_df, insert_type=BatchOutputMode.APPEND, **self.get_options()
        )
        batch_writer.execute()

    def _merge_staging_table_into_target(self) -> None:
        """
        Merge snowflake staging table into final snowflake table
        """
        merge_query = self._build_sf_merge_query(
            target_table=self.target_table,
            stage_table=self.staging_table,
            pk_columns=self.key_columns,
            non_pk_columns=self.non_key_columns,
            enable_deletion=self.enable_deletion,
        )

        query_executor = RunQuery(
            **self.get_options(),
            query=merge_query,
        )
        query_executor.execute()

    @staticmethod
    def _build_sf_merge_query(
        target_table: str, stage_table: str, pk_columns: List[str], non_pk_columns, enable_deletion: bool = False
    ):
        """Build a CDF merge query string

        Parameters
        ----------
        target_table: Table
            Destination table to merge into
        stage_table: Table
            Temporary table containing updates to be executed
        pk_columns: List[str]
            Column names used to uniquely identify each row
        non_pk_columns: List[str]
            Non-key columns that may need to be inserted/updated
        enable_deletion: bool
            DELETE actions are synced. If set to False (default) then sync is non-destructive

        Returns
        -------
        str
            Query to be executed on the target database
        """
        all_fields = [*pk_columns, *non_pk_columns]
        key_join_string = " AND ".join(f"target.{k} = temp.{k}" for k in pk_columns)
        columns_string = ", ".join(all_fields)
        assignment_string = ", ".join(f"{k} = temp.{k}" for k in non_pk_columns)
        values_string = ", ".join(f"temp.{k}" for k in all_fields)

        query = f"""
        MERGE INTO {target_table} target
        USING {stage_table} temp ON {key_join_string}
        WHEN MATCHED AND temp._change_type = 'update_postimage' THEN UPDATE SET {assignment_string}
        WHEN NOT MATCHED AND temp._change_type != 'delete' THEN INSERT ({columns_string}) VALUES ({values_string})
        """  # nosec B608: hardcoded_sql_expressions
        if enable_deletion:
            query += "WHEN MATCHED AND temp._change_type = 'delete' THEN DELETE"

        return query

    def extract(self) -> DataFrame:
        """
        Extract source table
        """
        if self.synchronisation_mode == BatchOutputMode.MERGE:
            if not self.source_table.is_cdf_active:
                raise RuntimeError(
                    f"Source table {self.source_table.table_name} does not have CDF enabled. "
                    f"Set TBLPROPERTIES ('delta.enableChangeDataFeed' = true) to enable. "
                    f"Current properties = {self.source_table_properties}"
                )

        df = self.reader.read()
        self.output.source_df = df
        return df

    def load(self, df) -> DataFrame:
        """Load source table into snowflake"""
        if self.synchronisation_mode == BatchOutputMode.MERGE:
            self.log.info(f"Truncating staging table {self.staging_table}")
            self.truncate_table(self.staging_table)
        self.writer.write(df)
        self.output.target_df = df
        return df

    def execute(self) -> None:
        # extract
        df = self.extract()
        self.output.source_df = df

        # synchronize
        self.output.target_df = df
        self.load(df)
        if not self.persist_staging:
            # If it's a streaming job, await for termination before dropping staging table
            if self.streaming:
                self.writer.await_termination()
            self.drop_table(self.staging_table)

    def run(self):
        """alias of execute"""
        return self.execute()
