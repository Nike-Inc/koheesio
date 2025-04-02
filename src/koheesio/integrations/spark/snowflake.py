# noinspection PyUnresolvedReferences
"""
Snowflake steps and tasks for Koheesio

Every class in this module is a subclass of `Step` or `Task` and is used to perform operations on Snowflake.

Notes
-----
Every Step in this module is based on [SnowflakeBaseModel](./snowflake.md#koheesio.integrations.snowflake.SnowflakeBaseModel).
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

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Union
from abc import ABC
from copy import deepcopy
import json
from textwrap import dedent

from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql import types as t

from koheesio import Step, StepOutput
from koheesio.integrations.snowflake import *
from koheesio.logger import LoggingFactory, warn
from koheesio.models import ExtraParamsMixin, Field, field_validator, model_validator
from koheesio.spark import DataFrame, DataType, SparkStep
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
    "TagSnowflakeQuery",
    "map_spark_type",
]

# pylint: disable=inconsistent-mro, too-many-lines
# Turning off inconsistent-mro because we are using ABCs and Pydantic models and Tasks together in the same class
# Turning off too-many-lines because we are defining a lot of classes in this file


def map_spark_type(spark_type: t.DataType) -> str:
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


class SnowflakeSparkStep(SparkStep, SnowflakeBaseModel, ABC):
    """Expands the SnowflakeBaseModel so that it can be used as a SparkStep"""


class SnowflakeReader(SnowflakeBaseModel, JdbcReader, SparkStep):
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

    format: str = Field(default="snowflake", description="The format to use when writing to Snowflake")
    # overriding `driver` property of JdbcReader, because it is not required by Snowflake
    driver: Optional[str] = None  # type: ignore

    def execute(self) -> SparkStep.Output:
        """Read from Snowflake"""
        super().execute()


class SnowflakeTransformation(SnowflakeBaseModel, Transformation, ABC):
    """Adds Snowflake parameters to the Transformation class"""


class RunQuery(SnowflakeSparkStep):
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

    @model_validator(mode="after")
    def validate_spark_and_deprecate(self) -> RunQuery:
        """If we do not have a spark session with a JVM, we can not use spark to run the query"""
        warn(
            "The RunQuery class is deprecated and will be removed in a future release. "
            "Please use the Python connector for Snowflake instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if not hasattr(self.spark, "_jvm"):
            raise RuntimeError(
                "Your Spark session does not have a JVM and cannot run Snowflake query using RunQuery implementation. "
                "Please update your code to use python connector for Snowflake."
            )
        return self

    @field_validator("query")
    def validate_query(cls, query: str) -> str:
        """Replace escape characters, strip whitespace, ensure it is not empty"""
        query = query.replace("\\n", "\n").replace("\\t", "\t").strip()
        if not query:
            raise ValueError("Query cannot be empty")
        return query

    def execute(self) -> RunQuery.Output:
        # Executing the RunQuery without `host` option raises the following error:
        #   An error occurred while calling z:net.snowflake.spark.snowflake.Utils.runQuery.
        #   : java.util.NoSuchElementException: key not found: host
        options = self.get_options()
        options["host"] = self.url
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
        user="gid.account@abc.com",
        password=Secret("super-secret-password"),
        role="APPLICATION.SNOWFLAKE.ADMIN",
        query="SELECT * FROM MY_TABLE",
    ).execute().df
    ```
    """

    query: str = Field(default=..., description="The query to run")

    @field_validator("query")
    def validate_query(cls, query: str) -> str:
        """Replace escape characters"""
        query = query.replace("\\n", "\n").replace("\\t", "\t").strip()
        return query

    def get_options(self, by_alias: bool = True, include: Set[str] = None) -> Dict[str, Any]:
        """add query to options"""
        options = super().get_options(by_alias)
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

    def execute(self) -> Output:
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
        self.log.info(
            f"Table '{self.database}.{self.sfSchema}.{self.table}' {'exists' if exists else 'does not exist'}"
        )
        self.output.exists = exists


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
        user="gid.account@abc.com",
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

    account: str = Field(default=..., description="The Snowflake account")
    table: str = Field(default=..., alias="table_name", description="The name of the (new) table")

    class Output(SnowflakeTransformation.Output):
        """Output class for CreateOrReplaceTableFromDataFrame"""

        input_schema: t.StructType = Field(default=..., description="The original schema from the input DataFrame")
        snowflake_schema: str = Field(
            default=..., description="Derived Snowflake table schema based on the input DataFrame"
        )
        query: str = Field(default=..., description="Query that was executed to create the table")

    def execute(self) -> Output:
        self.output.df = self.df

        input_schema = self.df.schema
        self.output.input_schema = input_schema

        snowflake_schema = ", ".join([f"{c.name} {map_spark_type(c.dataType)}" for c in input_schema])
        self.output.snowflake_schema = snowflake_schema

        table_name = f"{self.database}.{self.sfSchema}.{self.table}"
        query = f"CREATE OR REPLACE TABLE {table_name} ({snowflake_schema})"
        self.output.query = query

        SnowflakeRunQueryPython(**self.get_options(), query=query).execute()


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
            user="gid.account@abc.com",
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
        user="gid.account@abc.com",
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
    type: DataType = Field(default=..., description="The DataType represented as a Spark DataType")  # type: ignore
    account: str = Field(default=..., description="The Snowflake account")

    class Output(SnowflakeStep.Output):
        """Output class for AddColumn"""

        query: str = Field(default=..., description="Query that was executed to add the column")

    def execute(self) -> Output:
        query = f"ALTER TABLE {self.table} ADD COLUMN {self.column} {map_spark_type(self.type)}".upper()
        self.output.query = query
        SnowflakeRunQueryPython(**self.get_options(), query=query).execute()


class SyncTableAndDataFrameSchema(SnowflakeStep, SnowflakeTransformation):
    """
    Sync the schema's of a Snowflake table and a DataFrame. This will add NULL columns for the columns that are not in
    both and perform type casts where needed.

    The Snowflake table will take priority in case of type conflicts.
    """

    df: DataFrame = Field(default=..., description="The Spark DataFrame")
    table: str = Field(default=..., description="The table name")
    dry_run: bool = Field(default=False, description="Only show schema differences, do not apply changes")

    class Output(SparkStep.Output):
        """Output class for SyncTableAndDataFrameSchema"""

        original_df_schema: t.StructType = Field(default=..., description="Original DataFrame schema")
        original_sf_schema: t.StructType = Field(default=..., description="Original Snowflake schema")
        new_df_schema: t.StructType = Field(default=..., description="New DataFrame schema")
        new_sf_schema: t.StructType = Field(default=..., description="New Snowflake schema")
        sf_table_altered: bool = Field(
            default=False, description="Flag to indicate whether Snowflake schema has been altered"
        )

    def execute(self) -> Output:
        self.log.warning("Snowflake table will always take a priority in case of data type conflicts!")

        # spark side
        df_schema = self.df.schema
        self.output.original_df_schema = deepcopy(df_schema)  # using deepcopy to avoid storing in place changes
        df_cols = {c.name.lower() for c in df_schema}

        # snowflake side
        _options = {**self.get_options(), "table": self.table}
        sf_schema = GetTableSchema(**_options).execute().table_schema
        self.output.original_sf_schema = sf_schema
        sf_cols = {c.name.lower() for c in sf_schema}

        if self.dry_run:
            # Display differences between Spark DataFrame and Snowflake schemas
            # and provide dummy values that are expected as class outputs.
            _sf_diff = df_cols - sf_cols
            self.log.warning(f"Columns to be added to Snowflake table: {set(df_cols) - set(sf_cols)}")
            _df_diff = sf_cols - df_cols
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
                sf_cols = {c.name.lower() for c in sf_schema}

            self.output.new_sf_schema = sf_schema

            # Add NULL columns to the DataFrame if they exist in SnowFlake but not in the df
            df = self.df
            for sf_col in self.output.original_sf_schema:
                sf_col_name = sf_col.name.lower()
                if sf_col_name not in df_cols:
                    sf_col_type = sf_col.dataType
                    df = df.withColumn(sf_col_name, f.lit(None).cast(sf_col_type))  # type: ignore

            # Put DataFrame columns in the same order as the Snowflake table
            df = df.select(*sf_cols)

            self.output.df = df
            self.output.new_df_schema = df.schema


class SnowflakeWriter(SnowflakeBaseModel, Writer):
    """Class for writing to Snowflake

    See Also
    --------
    - [koheesio.spark.writers.Writer](writers/index.md#koheesio.spark.writers.Writer)
    - [koheesio.spark.writers.BatchOutputMode](writers/index.md#koheesio.spark.writers.BatchOutputMode)
    - [koheesio.spark.writers.StreamingOutputMode](writers/index.md#koheesio.spark.writers.StreamingOutputMode)
    """

    table: str = Field(default=..., description="Target table name")
    insert_type: Optional[BatchOutputMode] = Field(
        BatchOutputMode.APPEND, alias="mode", description="The insertion type, append or overwrite"
    )
    format: str = Field("snowflake", description="The format to use when writing to Snowflake")

    def execute(self) -> SnowflakeWriter.Output:
        """Write to Snowflake"""
        self.log.debug(f"writing to {self.table} with mode {self.insert_type}")
        self.df.write.format(self.format).options(**self.get_options()).option("dbtable", self.table).mode(
            self.insert_type
        ).save()


class SynchronizeDeltaToSnowflakeTask(SnowflakeSparkStep):
    """
    Synchronize a Delta table to a Snowflake table

    * Overwrite - only in batch mode
    * Append - supports batch and streaming mode
    * Merge - only in streaming mode

    Example
    -------
    ```python
    SynchronizeDeltaToSnowflakeTask(
        account="acme",
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
    streaming: bool = Field(
        default=False,
        description="Should synchronisation happen in streaming or in batch mode. Streaming is supported in 'APPEND' "
        "and 'MERGE' mode. Batch is supported in 'OVERWRITE' and 'APPEND' mode.",
    )
    persist_staging: bool = Field(
        default=False,
        description="In case of debugging, set `persist_staging` to True to retain the staging table for inspection "
        "after synchronization.",
    )
    enable_deletion: bool = Field(
        default=False,
        description="In case of merge synchronisation_mode add deletion statement in merge query.",
    )
    account: Optional[str] = Field(
        default=None,
        description="The Snowflake account to connect to. "
        "If not provided, the `truncate_table` and `drop_table` methods will fail.",
    )

    writer_: Optional[Union[ForEachBatchStreamWriter, SnowflakeWriter]] = None

    @field_validator("staging_table_name")
    def _validate_staging_table(cls, staging_table_name: str) -> str:
        """Validate the staging table name and return it if it's valid."""
        if "." in staging_table_name:
            raise ValueError(
                "Custom staging table must not contain '.', it is located in the same Schema as the target table."
            )
        return staging_table_name

    @model_validator(mode="before")
    def _checkpoint_location_check(cls, values: Dict) -> Dict:
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
    def _synch_mode_check(cls, values: Dict) -> Dict:
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
        if synchronisation_mode == BatchOutputMode.MERGE and len(key_columns) < 1:  # type: ignore
            raise ValueError("MERGE synchronisation mode requires a list of PK columns in `key_columns`.")

        return values

    @property
    def non_key_columns(self) -> List[str]:
        """Columns of source table that aren't part of the (composite) primary key"""
        lowercase_key_columns: Set[str] = {c.lower() for c in self.key_columns}  # type: ignore
        source_table_columns = self.source_table.columns
        non_key_columns: List[str] = [c for c in source_table_columns if c.lower() not in lowercase_key_columns]  # type: ignore
        return non_key_columns

    @property
    def staging_table(self) -> str:
        """Intermediate table on snowflake where staging results are stored"""
        if stg_tbl_name := self.staging_table_name:
            return stg_tbl_name

        return f"{self.source_table.table}_stg"

    @property
    def reader(self) -> Union[DeltaTableReader, DeltaTableStreamReader]:
        """
        DeltaTable reader

        Returns:
        --------
        DeltaTableReader
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
                    key_columns=self.key_columns,  # type: ignore
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

    def truncate_table(self, snowflake_table: str) -> None:
        """Truncate a given snowflake table"""
        truncate_query = f"""TRUNCATE TABLE IF EXISTS {snowflake_table}"""  # nosec B608: hardcoded_sql_expressions
        query_executor = SnowflakeRunQueryPython(
            **self.get_options(),
            query=truncate_query,
        )
        query_executor.execute()

    def drop_table(self, snowflake_table: str) -> None:
        """Drop a given snowflake table"""
        self.log.warning(f"Dropping table {snowflake_table} from snowflake")
        drop_table_query = f"""DROP TABLE IF EXISTS {snowflake_table}"""  # nosec B608: hardcoded_sql_expressions
        query_executor = SnowflakeRunQueryPython(**self.get_options(), query=drop_table_query)
        query_executor.execute()

    def _merge_batch_write_fn(self, key_columns: List[str], non_key_columns: List[str], staging_table: str) -> Callable:
        """Build a batch write function for merge mode"""

        # pylint: disable=unused-argument
        # noinspection PyUnusedLocal,PyPep8Naming
        def inner(dataframe: DataFrame, batchId: int):  # type: ignore
            self._build_staging_table(dataframe, key_columns, non_key_columns, staging_table)
            self._merge_staging_table_into_target()

        # pylint: enable=unused-argument
        return inner

    @staticmethod
    def _compute_latest_changes_per_pk(
        dataframe: DataFrame, key_columns: List[str], non_key_columns: List[str]
    ) -> DataFrame:
        """Compute the latest changes per primary key"""
        window_spec = Window.partitionBy(*key_columns).orderBy(f.col("_commit_version").desc())
        ranked_df = (
            dataframe.filter("_change_type != 'update_preimage'")
            .withColumn("rank", f.rank().over(window_spec))  # type: ignore
            .filter("rank = 1")
            .select(*key_columns, *non_key_columns, "_change_type")  # discard unused columns
            .distinct()
        )
        return ranked_df

    def _build_staging_table(
        self, dataframe: DataFrame, key_columns: List[str], non_key_columns: List[str], staging_table: str
    ) -> None:
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
            pk_columns=[*(self.key_columns or [])],
            non_pk_columns=self.non_key_columns,
            enable_deletion=self.enable_deletion,
        )  # type: ignore

        query_executor = SnowflakeRunQueryPython(
            **self.get_options(),
            query=merge_query,
        )
        query_executor.execute()

    @staticmethod
    def _build_sf_merge_query(
        target_table: str,
        stage_table: str,
        pk_columns: List[str],
        non_pk_columns: List[str],
        enable_deletion: bool = False,
    ) -> str:
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

        query = dedent(
            f"""
            MERGE INTO {target_table} target
            USING {stage_table} temp ON {key_join_string}
            WHEN MATCHED AND (temp._change_type = 'update_postimage' OR temp._change_type = 'insert')
                THEN UPDATE SET {assignment_string}
            WHEN NOT MATCHED AND temp._change_type != 'delete'
                THEN INSERT ({columns_string})
                VALUES ({values_string})
            {"WHEN MATCHED AND temp._change_type = 'delete' THEN DELETE" if enable_deletion else ""}"""
        ).strip()  # nosec B608: hardcoded_sql_expressions

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
                    f"Current properties = {self.source_table.get_persisted_properties()}"
                )

        df = self.reader.read()
        self.output.source_df = df
        return df

    def load(self, df: DataFrame) -> DataFrame:
        """Load source table into snowflake"""
        if self.synchronisation_mode == BatchOutputMode.MERGE:
            self.log.info(f"Truncating staging table {self.staging_table}")  # type: ignore
            self.truncate_table(self.staging_table)
        self.writer.write(df)
        self.output.target_df = df
        return df

    def execute(self) -> SynchronizeDeltaToSnowflakeTask.Output:
        # extract
        df = self.extract()
        self.output.source_df = df

        # synchronize
        self.output.target_df = df
        self.load(df)
        if not self.persist_staging:
            # If it's a streaming job, await for termination before dropping staging table
            if self.streaming:
                self.writer.await_termination()  # type: ignore
            self.drop_table(self.staging_table)


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
    #### Using `options` parameter
    ```python
    query_tag = (
        AddQueryTag(
            options={"preactions": "ALTER SESSION"},
            task_name="cleanse_task",
            pipeline_name="ingestion-pipeline",
            etl_date="2022-01-01",
            pipeline_execution_time="2022-01-01T00:00:00",
            task_execution_time="2022-01-01T01:00:00",
            environment="dev",
            trace_id="acd4f3f96045",
            span_id="546d2d66f6cb",
        )
        .execute()
        .options
    )
    ```
    In this example, the query tag pre-action will be added to the Snowflake options.

    #### Using `preactions` parameter
    Instead of using `options` parameter, you can also use `preactions` parameter to provide existing preactions.
    ```python
    query_tag = AddQueryTag(
        preactions="ALTER SESSION"
        ...
    ).execute().options
    ```

    The result will be the same as in the previous example.

    #### Using `get_options` method
    The shorthand method `get_options` can be used to get the `options` dictionary.
    ```python
    query_tag = AddQueryTag(...).get_options()
    ```
    """

    options: Dict = Field(
        default_factory=dict, description="Additional Snowflake options, optionally containing additional preactions"
    )

    preactions: Optional[str] = Field(default="", description="Existing preactions from Snowflake options")

    class Output(StepOutput):
        """Output class for AddQueryTag"""

        options: Dict = Field(default=..., description="Snowflake options dictionary with added query tag preaction")

    def execute(self) -> Output:
        """Add query tag preaction to Snowflake options"""
        tag_json = json.dumps(self.extra_params, indent=4, sort_keys=True)
        tag_preaction = f"ALTER SESSION SET QUERY_TAG = '{tag_json}';"
        preactions = self.options.get("preactions", self.preactions)
        # update options with new preactions
        self.output.options = {**self.options, "preactions": f"{preactions}\n{tag_preaction}".strip()}

    def get_options(self) -> Dict:
        """shorthand method to get the options dictionary

        Functionally equivalent to running `execute().options`

        Returns
        -------
        Dict
            Snowflake options dictionary with added query tag preaction
        """
        return self.execute().options
