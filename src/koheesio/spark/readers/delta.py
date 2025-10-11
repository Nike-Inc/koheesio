"""Read data from a Delta table and return a DataFrame or DataStream

Classes
-------
DeltaTableReader
    Reads data from a Delta table and returns a DataFrame
DeltaTableStreamReader
    Reads data from a Delta table and returns a DataStream
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from pydantic import PrivateAttr

from pyspark.sql import DataFrameReader
from pyspark.sql import functions as f

from koheesio.logger import LoggingFactory
from koheesio.models import Field, ListOfColumns, field_validator, model_validator
from koheesio.spark import Column, DataStreamReader
from koheesio.spark.delta import DeltaTableStep
from koheesio.spark.readers import Reader
from koheesio.utils import get_random_string

__all__ = ["DeltaTableReader", "DeltaTableStreamReader"]
STREAMING_SCHEMA_WARNING = (
    "\nImportant!\n"
    "Although you can start the streaming source from a specified version or timestamp, the schema of the streaming "
    "source is always the latest schema of the Delta table. You must ensure there is no incompatible schema change to "
    "the Delta table after the specified version or timestamp. Otherwise, the streaming source may return incorrect "
    "results when reading the data with an incorrect schema."
)

STREAMING_ONLY_OPTIONS = [
    "ignore_deletes",
    "ignore_changes",
    "starting_version",
    "starting_timestamp",
    "schema_tracking_location",
]


class DeltaTableReader(Reader):
    """Reads data from a Delta table and returns a DataFrame
    Delta Table can be read in batch or streaming mode
    It also supports reading change data feed (CDF) in both batch mode and streaming mode

    Parameters
    ----------
    table : Union[DeltaTableStep, str]
        The table to read
    filter_cond : Optional[Union[Column, str]]
        Filter condition to apply to the dataframe. Filters can be provided by using Column or string expressions.
        For example: `f.col('state') == 'Ohio'`, `state = 'Ohio'` or  `(col('col1') > 3) & (col('col2') < 9)`
    columns : Optional[ListOfColumns]
        Columns to select from the table. One or many columns can be provided as strings.
        For example: `['col1', 'col2']`, `['col1']` or `'col1'`
    streaming : Optional[bool]
        Whether to read the table as a Stream or not
    read_change_feed : bool
        readChangeFeed: Change Data Feed (CDF) feature allows Delta tables to track row-level changes between versions
        of a Delta table. When enabled on a Delta table, the runtime records 'change events' for all the data written
        into the table. This includes the row data along with metadata indicating whether the specified row was
        inserted, deleted, or updated. See: https://docs.databricks.com/delta/delta-change-data-feed.html
    starting_version : str
        startingVersion: The Delta Lake version to start from. All table changes starting from this version (inclusive)
        will be read by the streaming source. You can obtain the commit versions from the version column of the
        DESCRIBE HISTORY command output.
    starting_timestamp : str
        startingTimestamp: The timestamp to start from. All table changes committed at or after the timestamp
        (inclusive) will be read by the streaming source. Either provide a timestamp string
        (e.g. 2019-01-01T00:00:00.000Z) or a date string (e.g. 2019-01-01)
    ignore_deletes : bool
        ignoreDeletes: Ignore transactions that delete data at partition boundaries. Note: Only supported for
        streaming tables.
        For more info see https://docs.databricks.com/structured-streaming/delta-lake.html#ignore-updates-and-deletes
    ignore_changes : bool
        ignoreChanges: re-process updates if files had to be rewritten in the source table due to a data changing
        operation such as UPDATE, MERGE INTO, DELETE (within partitions), or OVERWRITE. Unchanged rows may still be
        emitted, therefore your downstream consumers should be able to handle duplicates. Deletes are not propagated
        downstream. ignoreChanges subsumes ignoreDeletes. Therefore, if you use ignoreChanges, your stream will not be
        disrupted by either deletions or updates to the source table.

    """

    table: Union[DeltaTableStep, str] = Field(default=..., description="The table to read")
    filter_cond: Optional[Union[Column, str]] = Field(
        default=None,
        alias="filterCondition",
        description="Filter condition to apply to the dataframe. Filters can be provided by using Column or string "
        "expressions For example: `f.col('state') == 'Ohio'`, `state = 'Ohio'` or  "
        "`(col('col1') > 3) & (col('col2') < 9)`",
    )
    columns: Optional[ListOfColumns] = Field(
        default=None,
        description="Columns to select from the table. One or many columns can be provided as strings. For example: "
        "`['col1', 'col2']`, `['col1']` or `'col1'` ",
    )

    # sql: Optional[str, Path]  # TODO: add support for SQL file, or advanced SQL expression
    streaming: Optional[bool] = Field(default=False, description="Whether to read the table as a Stream or not")
    read_change_feed: bool = Field(
        default=False,
        alias="readChangeFeed",
        description="Change Data Feed (CDF) feature allows Delta tables to track row-level changes between versions of "
        "a Delta table. When enabled on a Delta table, the runtime records 'change events' for all the data written "
        "into the table. This includes the row data along with metadata indicating whether the specified row was "
        "inserted, deleted, or updated. See: https://docs.databricks.com/delta/delta-change-data-feed.html",
    )

    # Specify initial position, one of:
    starting_version: Optional[str] = Field(
        default=None,
        alias="startingVersion",
        description="startingVersion: The Delta Lake version to start from. All table changes starting from this "
        "version (inclusive) will be read by the streaming source. You can obtain the commit versions from the version "
        "column of the DESCRIBE HISTORY command output." + STREAMING_SCHEMA_WARNING,
    )
    starting_timestamp: Optional[str] = Field(
        default=None,
        alias="startingTimestamp",
        description="startingTimestamp: The timestamp to start from. All table changes committed at or after the "
        "timestamp (inclusive) will be read by the streaming source. "
        "Either provide a timestamp string (e.g. 2019-01-01T00:00:00.000Z) or a date string (e.g. 2019-01-01)"
        + STREAMING_SCHEMA_WARNING,
    )

    # Streaming only options
    # ---
    # Ignore updates and deletes, one of:
    ignore_deletes: bool = Field(
        default=False,
        alias="ignoreDeletes",
        description="ignoreDeletes: Ignore transactions that delete data at partition boundaries. "
        "Note: Only supported for streaming tables. "
        "For more info see https://docs.databricks.com/structured-streaming/delta-lake.html#ignore-updates-and-deletes",
    )
    ignore_changes: bool = Field(
        default=False,
        alias="ignoreChanges",
        description="ignoreChanges: re-process updates if files had to be rewritten in the source table due to a data "
        "changing operation such as UPDATE, MERGE INTO, DELETE (within partitions), or OVERWRITE. Unchanged rows may "
        "still be emitted, therefore your downstream consumers should be able to handle duplicates. Deletes are not "
        "propagated downstream. ignoreChanges subsumes ignoreDeletes. Therefore if you use ignoreChanges, your stream "
        "will not be disrupted by either deletions or updates to the source table.",
    )
    skip_change_commits: bool = Field(
        default=False,
        alias="skipChangeCommits",
        description="skipChangeCommits: Skip processing of change commits. "
        "Note: Only supported for streaming tables. (not supported in Open Source Delta Implementation). "
        "Prefer using skipChangeCommits over ignoreDeletes and ignoreChanges starting DBR12.1 and above. "
        "For more info see https://docs.databricks.com/structured-streaming/delta-lake.html#skip-change-commits",
    )

    schema_tracking_location: Optional[str] = Field(
        default=None,
        alias="schemaTrackingLocation",
        description="schemaTrackingLocation: Track the location of source schema. "
        "Note: Recommend to enable Delta reader version: 3 and writer version: 7 for this option. "
        "For more info see https://docs.delta.io/latest/delta-column-mapping.html" + STREAMING_SCHEMA_WARNING,
    )

    # private attrs
    __temp_view_name__: Optional[str] = None
    __reader: Optional[Union[DataStreamReader, DataFrameReader]] = PrivateAttr(default=None)

    @property
    def temp_view_name(self) -> str:
        """Get the temporary view name for the dataframe for SQL queries"""
        return self.__temp_view_name__

    @field_validator("table")
    def _validate_table_name(cls, tbl: Union[DeltaTableStep, str]) -> DeltaTableStep:
        """Validate the table name provided as a string or a DeltaTableStep instance."""
        if isinstance(tbl, str):
            return DeltaTableStep(table=tbl)
        if isinstance(tbl, DeltaTableStep):
            return tbl
        raise AttributeError(f"Table name provided cannot be processed as a Table : {tbl}")

    @model_validator(mode="after")
    def _validate_starting_version_and_timestamp(self) -> "DeltaTableReader":
        """Validate 'starting_version' and 'starting_timestamp' - Only one of each should be provided"""
        starting_version = self.starting_version
        starting_timestamp = self.starting_timestamp
        streaming = self.streaming
        read_change_feed = self.read_change_feed

        if starting_version and starting_timestamp:
            raise ValueError(
                f"Specify either a 'starting_version' or a 'starting_timestamp', not both. \n"
                f"provided values: ({starting_version=}, {starting_timestamp=})"
            )

        if not streaming and read_change_feed and all([starting_version is None, starting_timestamp is None]):
            raise ValueError(
                "Specify either a 'starting_version' or a 'starting_timestamp' "
                "when reading change data feed in a batch mode."
            )

        return self

    @model_validator(mode="after")
    def _validate_ignore_deletes_and_changes_and_skip_commits(self) -> "DeltaTableReader":
        """Validate 'ignore_deletes' and 'ignore_changes' - Only one of each should be provided"""
        ignore_deletes = self.ignore_deletes
        ignore_changes = self.ignore_changes
        skip_change_commits = self.skip_change_commits

        if ignore_deletes and ignore_changes and skip_change_commits:
            raise ValueError(
                f"Specify either a 'ignore_deletes' or a 'ignore_changes', not both. \n"
                f"provided values: ({ignore_deletes=}, {ignore_changes=}, {skip_change_commits=})"
            )

        return self

    @model_validator(mode="before")
    def _warn_on_streaming_options_without_streaming(cls, options: Dict) -> Dict:
        """throws a warning if streaming options were provided, but streaming was not set to true"""
        streaming_options = [val for opt, val in options.items() if opt in STREAMING_ONLY_OPTIONS]
        streaming_toggled_on = options.get("streaming")

        if any(streaming_options) and not streaming_toggled_on:
            log = LoggingFactory.get_logger(name=cls.__name__, inherit_from_koheesio=True)
            log.warning(
                f"Streaming options were provided, but streaming was not toggled on. Was this intended?\n{options = }"
            )

        return options

    @model_validator(mode="after")
    def set_temp_view_name(self) -> "DeltaTableReader":
        """Set a temporary view name for the dataframe for SQL queries"""
        table_name = self.table.table
        vw_name = get_random_string(prefix=f"tmp_{table_name}")
        self.__temp_view_name__ = vw_name
        return self

    @property
    def view(self) -> str:
        """Create a temporary view of the dataframe for SQL queries"""
        temp_view_name = self.temp_view_name

        if (output_df := self.output.df) is None:
            self.log.warning(
                "Attempting to createTempView without any data being present. Please run .execute() or .read() first. "
                "View creation was unsuccessful."
            )
        else:
            output_df.createOrReplaceTempView(temp_view_name)

        return temp_view_name

    def get_options(self) -> Dict[str, Any]:
        """Get the options for the DeltaTableReader based on the `streaming` attribute"""
        options = {
            # Enable Change Data Feed (CDF) feature
            "readChangeFeed": self.read_change_feed,
            # Initial position, one of:
            "startingVersion": self.starting_version,
            "startingTimestamp": self.starting_timestamp,
        }

        # Streaming only options
        if self.streaming:
            options = {
                **options,
                # Ignore updates and deletes, one of:
                "ignoreDeletes": self.ignore_deletes,
                "ignoreChanges": self.ignore_changes,
                "skipChangeCommits": self.skip_change_commits,
                "schemaTrackingLocation": self.schema_tracking_location,
            }
        # Batch only options
        else:
            pass  # there are none... for now :)

        def normalize(v: Union[str, bool]) -> str:
            """normalize values"""
            # True becomes "true", False becomes "false"
            v = str(v).lower() if isinstance(v, bool) else v
            return v

        # Any options with `value == None` are filtered out
        return {k: normalize(v) for k, v in options.items() if v is not None}

    def __get_stream_reader(self) -> DataStreamReader:
        """Returns a basic DataStreamReader (streaming mode)"""
        return self.spark.readStream.format("delta")

    def __get_batch_reader(self) -> DataFrameReader:
        """Returns a basic DataFrameReader (batch mode)"""
        return self.spark.read.format("delta")

    @property
    def reader(self) -> Union[DataStreamReader, DataFrameReader]:
        """Return the reader for the DeltaTableReader based on the `streaming` attribute"""
        if not self.__reader:
            self.__reader = self.__get_stream_reader() if self.streaming else self.__get_batch_reader()
            self.__reader = self.__reader.options(**self.get_options())

        return self.__reader

    @reader.setter
    def reader(self, value: Union[DataStreamReader, DataFrameReader]):
        self.__reader = value

    def execute(self) -> Reader.Output:
        df = self.reader.table(self.table.table_name)
        if self.filter_cond is not None:
            df = df.filter(f.expr(self.filter_cond) if isinstance(self.filter_cond, str) else self.filter_cond)  # type: ignore
        if self.columns is not None:
            df = df.select(*self.columns)
        self.output.df = df

    # def read_from_sql_file(self):
    #     """TODO: implement create_dataframe_from_sql_file"""
    # TODO: I think it should method in DeltaTable, if you want to create table based on DDL
    # TODO: If you want to use SQL query for DataFrame, it make sense to add it to parent Reader class


class DeltaTableStreamReader(DeltaTableReader):
    """Reads data from a Delta table and returns a DataStream"""

    streaming: bool = True

    # def read_from_sql_file(self):
    #     """TODO: implement create_datastream_from_sql_file"""


# TODO: add support to extra read options for DeltaTableReader
