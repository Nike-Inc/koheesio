"""
Module for creating and managing Delta tables.
"""

from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
import warnings

from py4j.protocol import Py4JJavaError  # type: ignore

from pyspark.sql.types import DataType

from koheesio.logger import LoggingFactory
from koheesio.models import Field, field_validator, model_validator
from koheesio.spark import AnalysisException, DataFrame, SparkStep
from koheesio.spark.utils import on_databricks
from koheesio.steps import Step, StepOutput

log = LoggingFactory.get_logger(name=__name__, inherit_from_koheesio=True)


class DeltaTableStep(SparkStep):
    """
    Class for creating and managing Delta tables.

    DeltaTable aims to provide a simple interface to create and manage Delta tables.
    It is a wrapper around the Spark SQL API for Delta tables.


    <!--- TODO: handle errors like this one:
    #     AnalysisException: You are trying to read a Delta table ... that does not have any columns.
    #     Write some new data with the option `mergeSchema = true` to be able to read the table.
    -->

    Example
    -------
    ```python
    from koheesio.steps import DeltaTableStep

    DeltaTableStep(
        table="my_table",
        database="my_database",
        catalog="my_catalog",
        create_if_not_exists=True,
        default_create_properties={
            "delta.randomizeFilePrefixes": "true",
            "delta.checkpoint.writeStatsAsStruct": "true",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
        },
    )
    ```

    Methods
    -------
    get_persisted_properties()
        Get persisted properties of table.
    add_property(key: str, value: str, override: bool = False)
        Alter table and set table property.
    add_properties(properties: Dict[str, str], override: bool = False)
        Alter table and add properties.
    execute()
        Nothing to execute on a Table.
    max_version_ts_of_last_execution(query_predicate: str = None) -> datetime.datetime
        Max version timestamp of last execution. If no timestamp is found, returns 1900-01-01 00:00:00.
        Note: will raise an error if column `VERSION_TIMESTAMP` does not exist.

    Properties
    ----------
    - name -> str
        Deprecated. Use `.table_name` instead.
    - table_name -> str
        Table name.
    - dataframe -> DataFrame
        Returns a DataFrame to be able to interact with this table.
    - columns -> Optional[List[str]]
        Returns all column names as a list.
    - has_change_type -> bool
        Checks if a column named `_change_type` is present in the table.
    - exists -> bool
        Check if table exists.

    Parameters
    ----------
    table : str
        Table name.
    database : str, optional, default=None
        Database or Schema name.
    catalog : str, optional, default=None
        Catalog name.
    create_if_not_exists : bool, optional, default=False
        Force table creation if it doesn't exist. Note: Default properties will be applied to the table during CREATION.
    default_create_properties : Dict[str, str], optional, default={"delta.randomizeFilePrefixes": "true", "delta.checkpoint.writeStatsAsStruct": "true", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"}
        Default table properties to be applied during CREATION if `force_creation` True.
    """

    # Order of inputs is reversed to ensure that TableName is always present
    table: str
    database: Optional[str] = Field(
        default=None,
        description="Database or Schema name.",
    )
    catalog: Optional[str] = Field(
        default=None,
        description="Catalog name. "
        "Note: Can be ignored if using a SparkCatalog that does not support catalog notation (e.g. Hive)",
    )

    # Table parameters
    create_if_not_exists: bool = Field(
        default=False,
        alias="force_creation",
        description="Force table creation if it doesn't exist."
        "Note: Default properties will be applied to the table during CREATION.",
    )
    default_create_properties: Dict[str, Union[str, bool, int]] = Field(
        default={
            "delta.randomizeFilePrefixes": "true",
            "delta.checkpoint.writeStatsAsStruct": "true",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
        },
        description="Default table properties to be applied during CREATION if `create_if_not_exists` True",
    )

    @field_validator("default_create_properties")
    def _adjust_default_properties(cls, default_create_properties: dict) -> dict:
        """Adjust default properties based on environment."""
        if on_databricks():
            default_create_properties["delta.autoOptimize.autoCompact"] = True
            default_create_properties["delta.autoOptimize.optimizeWrite"] = True

        for k, v in default_create_properties.items():
            default_create_properties[k] = str(v) if not isinstance(v, bool) else str(v).lower()

        return default_create_properties

    @model_validator(mode="after")
    def _validate_catalog_database_table(self) -> "DeltaTableStep":
        """Validate that catalog, database/schema, and table are correctly set"""
        database, catalog, table = self.database, self.catalog, self.table

        try:
            self.log.debug(f"Value of `table` input parameter: {table}")
            catalog, database, table = table.split(".")
            self.log.debug("Catalog, database and table were given")
        except ValueError as e:
            if str(e) == "not enough values to unpack (expected 3, got 1)":
                self.log.debug("Only table name was given")
            elif str(e) == "not enough values to unpack (expected 3, got 2)":
                self.log.debug("Only table name and database name were given")
                database, table = table.split(".")
            else:
                raise ValueError(f"Unable to parse values for Table: {table}") from e

        self.database, self.catalog, self.table = database, catalog, table
        return self

    def get_persisted_properties(self) -> Dict[str, str]:
        """Get persisted properties of table.

        Returns
        -------
        Dict[str, str]
            Persisted properties as a dictionary.
        """
        persisted_properties = {}
        raw_options = self.spark.sql(f"SHOW TBLPROPERTIES {self.table_name}").collect()

        for ro in raw_options:
            key, value = ro.asDict().values()
            persisted_properties[key] = value

        return persisted_properties

    @property
    def is_cdf_active(self) -> bool:
        """Check if CDF property is set and activated

        Returns
        -------
        bool
            delta.enableChangeDataFeed property is set to 'true'

        """
        props = self.get_persisted_properties()
        return props.get("delta.enableChangeDataFeed", "false") == "true"

    def add_property(self, key: str, value: Union[str, int, bool], override: bool = False) -> None:
        """Alter table and set table property.

        Parameters
        ----------
        key: str
            Property key(name).
        value: Union[str, int, bool]
            Property value.
        override: bool
            Enable override of existing value for property in table.

        """
        persisted_properties = self.get_persisted_properties()
        v_str = str(value) if not isinstance(value, bool) else str(value).lower()

        def _alter_table() -> None:
            property_pair = f"'{key}'='{v_str}'"

            try:
                # noinspection SqlNoDataSourceInspection
                self.spark.sql(f"ALTER TABLE {self.table_name} SET TBLPROPERTIES ({property_pair})")
                self.log.debug(f"Table `{self.table_name}` has been altered. Property `{property_pair}` added.")
            except Py4JJavaError as e:
                msg = f"Property `{key}` can not be applied to table `{self.table_name}`. Exception: {e}"
                self.log.warning(msg)
                warnings.warn(msg)

        if self.exists:
            if key in persisted_properties and persisted_properties[key] != v_str:
                if override:
                    self.log.debug(
                        f"Property `{key}` presents in `{self.table_name}` and has value `{persisted_properties[key]}`."
                        f"Override is enabled. The value will be changed to `{v_str}`."
                    )
                    _alter_table()
                else:
                    self.log.debug(
                        f"Skipping adding property `{key}`, because it is already set "
                        f"for table `{self.table_name}` to `{v_str}`. To override it, provide override=True"
                    )
            else:
                _alter_table()
        else:
            self.default_create_properties[key] = v_str

    def add_properties(self, properties: Dict[str, Union[str, bool, int]], override: bool = False) -> None:
        """Alter table and add properties.

        Parameters
        ----------
        properties : Dict[str, Union[str, int, bool]]
            Properties to be added to table.
        override : bool, optional, default=False
            Enable override of existing value for property in table.

        """
        for k, v in properties.items():
            v_str = str(v) if not isinstance(v, bool) else str(v).lower()
            self.add_property(key=k, value=v_str, override=override)

    def execute(self) -> None:
        """Nothing to execute on a Table"""

    @property
    def table_name(self) -> str:
        """Fully qualified table name in the form of `catalog.database.table`"""
        return ".".join([n for n in [self.catalog, self.database, self.table] if n])

    @property
    def dataframe(self) -> DataFrame:
        """Returns a DataFrame to be able to interact with this table"""
        return self.spark.table(self.table_name)

    @property
    def columns(self) -> Optional[List[str]]:
        """Returns all column names as a list.

        Example
        -------
        ```python
        DeltaTableStep(...).columns
        ```
        Would for example return `['age', 'name']` if the table has columns `age` and `name`.
        """
        return self.dataframe.columns if self.exists else None

    def get_column_type(self, column: str) -> Optional[DataType]:
        """Get the type of a specific column in the table.

        Parameters
        ----------
        column : str
            Column name.

        Returns
        -------
        Optional[DataType]
            Column type.
        """
        return self.dataframe.schema[column].dataType if self.columns and column in self.columns else None

    @property
    def has_change_type(self) -> bool:
        """Checks if a column named `_change_type` is present in the table"""
        return "_change_type" in self.columns  # type: ignore

    @property
    def exists(self) -> bool:
        """Check if table exists.
        Depending on the value of the boolean flag `create_if_not_exists` a different logging level is provided."""
        result = False

        try:
            from koheesio.spark.utils.connect import is_remote_session

            _df = self.spark.table(self.table_name)

            if is_remote_session():
                # In Spark remote session it is not enough to call just spark.table(self.table_name)
                # as it will not raise an exception, we have to make action call on table to check if it exists
                _df.take(1)

            result = True
        except AnalysisException as e:
            err_msg = str(e).lower()
            if err_msg.startswith("[table_or_view_not_found]") or err_msg.startswith("table or view not found"):
                self.log.debug(f"Table `{self.table_name}` does not exist.")
            else:
                raise e

        return result

    def describe_history(self, limit: Optional[int] = None) -> Optional[DataFrame]:
        """
        Get the latest `limit` rows from the Delta Log.
        The information is in reverse chronological order.

        Parameters
        ----------
        limit : Optional[int]
            Number of rows to return.

        Returns
        -------
        Optional[DataFrame]
            Delta Table's history as a DataFrame or None if the table does not exist.

        Examples
        -------
        ```python
        DeltaTableStep(...).describe_history()
        ```
        Would return the full history from a Delta Log.

        ```python
        DeltaTableStep(...).describe_history(limit=10)
        ```
        Would return the last 10 operations from the Delta Log.
        """
        if self.exists:
            history_df = self.spark.sql(f"DESCRIBE HISTORY {self.table_name}")
            history_df = history_df.orderBy("version", ascending=False)
            if limit:
                history_df = history_df.limit(limit)
            return history_df
        else:
            self.log.warning(f"Table `{self.table_name}` does not exist.")


class StaleDataCheckStep(Step):
    """
    Determines if the data inside the Delta table is stale based on the elapsed time since
    the last modification and, optionally, based on the current week day.

    The staleness interval is specified as a `timedelta` object.
    If `refresh_day_num` is provided, it adds an extra condition to mark the data as stale if the current day matches with the specified weekday.

    The date of the last modification of the table is taken from the Delta Log.

    Parameters
    ----------
    table : Union[DeltaTableStep, str]
        The table to check for stale data.
    interval : timedelta
        The interval to consider data stale. Users can pass a `timedelta` object or an ISO-8601 compliant string representing the interval.
        For example `P1W3DT2H30M` is equivalent to `timedelta(weeks=1, days=3, hours=2, minutes=30)`.
    refresh_day_num : int, optional
        The weekday number (0=Monday, 6=Sunday) on which
        data should be refreshed if it has not already. Enforces
        a maximum period limit of 6 days, 23 hours, 59 minutes and 59 seconds.

    Examples
    --------
    Assume now is January 31st, 2025 (Friday) 12:00:00 and the last modification dates
    in the history are shown alongside the examples.

    Example 1: Last modified on January 28th, 2025, 11:00:00 checking with a 3-day threshold:
    ```python
    is_stale = (
        StaleDataCheckStep(table=table, interval=timedelta(days=3))
        .execute()
        .is_data_stale
    )
    print(
        is_stale
    )  # True, as the last modification was 3 days and 1 hour ago which is more than 3 days.
    ```

    Example 2: Last modified on January 28th, 2025, 11:00:00 checking with a 3-day and 1-hour threshold:
    ```python
    is_stale = (
        StaleDataCheckStep(
            table=table, interval=timedelta(days=3, hours=1)
        )
        .execute()
        .is_data_stale
    )
    print(
        is_stale
    )  # True, as the last modification was 3 days and 1 hour ago which is the same as the threshold.
    ```

    Example 3: Last modified on January 28th, 2025, 11:00:00 checking with a 3-day and 2-hour threshold:
    ```python
    is_stale = (
        StaleDataCheckStep(
            table=table, interval=timedelta(days=3, hours=2)
        )
        .execute()
        .is_data_stale
    )
    print(
        is_stale
    )  # False, as the last modification was 3 days and 1 hour ago which is less than 3 days and 2 hours.
    ```

    Example 4: Same as example 3 but with the interval defined as an ISO-8601 string:
    ```python
    is_stale = (
        StaleDataCheckStep(table=table, interval="P3DT2H")
        .execute()
        .is_data_stale
    )
    print(
        is_stale
    )  # False, as the last modification was 3 days and 1 hour ago which is less than 3 days and 2 hours.
    ```

    Example 5: Last modified on January 28th, 2025, 11:00:00 checking with a 5-day threshold and refresh_day_num = 5 (Friday):
    ```python
    is_stale = (
        StaleDataCheckStep(
            table=table, interval=timedelta(days=5), refresh_day_num=5
        )
        .execute()
        .is_data_stale
    )
    print(
        is_stale
    )  # True, 3 days and 1 hour is less than 5 days but refresh_day_num is the same as the current day.
    ```

    Returns
    -------
    bool
        True if data is considered stale by exceeding the defined time limits or if the current
        day equals to `refresh_day_num`. Returns False if conditions are not met.

    Raises
    ------
    ValueError
        If the total period exceeds 7 days when `refresh_day_num` is set.
    ValidationError
        If `refresh_day_num` is not between 0 and 6.
    """

    table: Union[DeltaTableStep, str] = Field(
        ...,
        description="The table to check for stale data.",
    )
    interval: timedelta = Field(
        ...,
        description="The interval to consider data stale.",
    )
    refresh_day_num: Optional[int] = Field(
        default=None, description="The weekday number on which data should be refreshed.", ge=0, le=6
    )

    class Output(StepOutput):
        """Output class for StaleDataCheckStep."""

        is_data_stale: bool = Field(
            ..., description="Boolean flag indicating whether data in the table is stale or not"
        )

    @field_validator("table")
    def _validate_table(cls, table: Union[DeltaTableStep, str]) -> Union[DeltaTableStep, str]:
        """Validate `table` value"""
        if isinstance(table, str):
            return DeltaTableStep(table=table)
        return table

    @model_validator(mode="after")
    def _validate_refresh_day_num(self) -> "StaleDataCheckStep":
        """Validate input when `refresh_day_num` is provided."""
        if self.refresh_day_num is not None:
            max_period = timedelta(days=6, hours=23, minutes=59, seconds=59)
            if self.interval > max_period:
                raise ValueError("With refresh_day_num set, the total period must be less than 7 days.")

        return self

    def execute(self) -> Output:
        # Get the history of the Delta table
        history_df = self.table.describe_history()

        if not history_df:
            log.debug(f"No history found for `{self.table.table_name}`.")
            self.output.is_data_stale = True  # Consider data stale if the table does not exist
            return self.output

        modification_operations = [
            "WRITE",
            "MERGE",
            "DELETE",
            "UPDATE",
            "REPLACE TABLE AS SELECT",
            "CREATE TABLE AS SELECT",
            "TRUNCATE",
            "RESTORE",
        ]

        # Filter the history to data modification operations only
        history_df = history_df.filter(history_df["operation"].isin(modification_operations))

        # Get the last modification operation's timestamp
        last_modification = history_df.select("timestamp").first()

        if not last_modification:
            log.debug(f"No modification operation found in the history for `{self.table.table_name}`.")
            self.output.is_data_stale = True
            return self.output

        current_time = datetime.now()
        last_modification_timestamp = last_modification["timestamp"]

        cut_off_date = current_time - self.interval

        log.debug(f"Last modification timestamp: {last_modification_timestamp}, cut-off date: {cut_off_date}")

        is_stale_by_time = last_modification_timestamp <= cut_off_date

        if self.refresh_day_num is not None:
            current_day_of_week = current_time.weekday()
            log.debug(f"Current day of the week: {current_day_of_week}, refresh day: {self.refresh_day_num}")

            is_appropriate_day_for_refresh = current_day_of_week == self.refresh_day_num
            self.output.is_data_stale = is_stale_by_time or is_appropriate_day_for_refresh
            return self.output

        self.output.is_data_stale = is_stale_by_time
