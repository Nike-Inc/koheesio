"""
Utils for working with Delta tables.
"""
from typing import Optional, Union
from datetime import datetime, timedelta

from koheesio.logger import LoggingFactory
from koheesio.models import Field, field_validator, model_validator
from koheesio.spark.delta import DeltaTableStep
from koheesio.steps import Step, StepOutput

log = LoggingFactory.get_logger(name=__name__, inherit_from_koheesio=True)


class StaleDataCheckStep(Step):
    """
    Determines if the data inside the table is stale based on the elapsed time since 
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
    ```
    is_stale = StaleDataCheckStep(table=table, interval=timedelta(days=3)).execute().is_data_stale
    print(is_stale)  # True, as the last modification was 3 days and 1 hour ago which is more than 3 days.
    ```

    Example 2: Last modified on January 28th, 2025, 11:00:00 checking with a 3-day and 1-hour threshold:
    ```
    is_stale = StaleDataCheckStep(table=table, interval=timedelta(days=3, hours=1)).execute().is_data_stale
    print(is_stale)  # True, as the last modification was 3 days and 1 hour ago which is the same as the threshold.
    ```

    Example 3: Last modified on January 28th, 2025, 11:00:00 checking with a 3-day and 2-hour threshold:
    ```
    is_stale = StaleDataCheckStep(table=table, interval=timedelta(days=3, hours=2)).execute().is_data_stale
    print(is_stale)  # False, as the last modification was 3 days and 1 hour ago which is less than 3 days and 2 hours.
    ```

    Example 4: Same as example 3 but with the interval defined as an ISO-8601 string:
    ```
    is_stale = StaleDataCheckStep(table=table, interval="P3DT2H").execute().is_data_stale
    print(is_stale)  # False, as the last modification was 3 days and 1 hour ago which is less than 3 days and 2 hours.
    ```

    Example 5: Last modified on January 28th, 2025, 11:00:00 checking with a 5-day threshold and refresh_day_num = 5 (Friday):
    ```
    is_stale = StaleDataCheckStep(table=table, interval=timedelta(days=5), refresh_day_num=5).execute().is_data_stale
    print(is_stale)  # True, 3 days and 1 hour is less than 5 days but refresh_day_num is the same as the current day.
    ```

    Returns
    -------
    bool
        True if data is considered stale by exceeding the defined time limits or if the current
        day equals to `refresh_day_num`. Returns False if conditions are not met.
    
    Raises
    ------
    ValueError
        If `refresh_day_num` is not between 0 and 6.
        If the total period exceeds 7 days when `refresh_day_num` is set.
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
        default=None,
        description="The weekday number on which data should be refreshed.",
    )

    class Output(StepOutput):
        """Output class for StaleDataCheckStep."""

        is_data_stale: bool = Field(..., description="Boolean flag indicating whether data in the table is stale or not")

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
            if not 0 <= self.refresh_day_num <= 6:
                raise ValueError("refresh_day_num should be between 0 (Monday) and 6 (Sunday).")

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
        return self.output
