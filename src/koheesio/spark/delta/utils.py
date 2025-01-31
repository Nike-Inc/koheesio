"""
Utils for working with Delta tables.
"""
from datetime import datetime, timedelta

from koheesio.spark.delta import DeltaTableStep
from koheesio.utils.date_time import DTInterval


def is_data_stale(
    table: DeltaTableStep,
    months: int = 0,
    weeks: int = 0,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0,
    dt_interval: DTInterval = None,
    refresh_day_num: int = None,
) -> bool:
    """
    Determines if the data inside a table is stale based on the elapsed time since 
    the last modification and, optionally, based on the current week day.

    The function allows specifying limits in terms of months, weeks, days, hours, minutes, and seconds to determine 
    how old data can be before it is considered stale. If `refresh_day_num` is provided, it adds an extra condition 
    to mark data as stale if the current day matches with the specified weekday.

    The last modification date is taken from the Delta Log.

    Parameters
    ----------
    table : str
        The path to the table to check.
    months : int, default 0
        Threshold in months to determine staleness.
    weeks : int, default 0
        Threshold in weeks.
    days : int, default 0
        Threshold in days.
    hours : int, default 0
        Threshold in hours.
    minutes : int, default 0
        Threshold in minutes.
    seconds : int, default 0
        Threshold in seconds.
    dt_interval : DTInterval, optional
        An alternative to directly specifying time components.
        This should be an instance of DTInterval, which provides
        the `to_timedelta` method that converts structured time
        descriptions into a timedelta object.
    refresh_day_num : int, optional
        The weekday number (0=Monday, 6=Sunday) on which
        data should be refreshed if it has not already. Enforces
        a maximum period limit of 6 days, 23 hours, 59 minutes and 59 seconds.

    Returns
    -------
    bool
        True if data is considered stale by exceeding the defined time limits or if the current
        day equals to `refresh_day_num`. Returns False if conditions are not met.
    """

    if not any((months, weeks, days, hours, minutes, seconds)) and dt_interval is None:
        raise ValueError(
            "You must provide either time components (months, weeks, days, hours, minutes, seconds) or dt_interval."
        )

    if months > 0:  # Convert months to days
        month_days = int(months * 30.44)  # Average month length
        days += month_days

    staleness_period = (
        timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
        if any((weeks, days, hours, minutes, seconds))
        else dt_interval.to_timedelta
    )

    if refresh_day_num is not None:
        if not 0 <= refresh_day_num <= 6:
            raise ValueError("refresh_day_num should be between 0 (Monday) and 6 (Sunday).")

        max_period = timedelta(days=6, hours=23, minutes=59, seconds=59)
        if staleness_period > max_period:
            raise ValueError("With refresh_day_num set, the total period must be less than 7 days.")

    current_time = datetime.now()

    # Get the history of the Delta table
    history_df = DeltaTableStep(table=table).describe_history()

    if not history_df:
        return True  # Consider data stale if no history exists

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
        return True  # No modification operation found in the history

    last_modification_timestamp = last_modification["timestamp"]

    cut_off_date = current_time - staleness_period

    is_stale_by_time = last_modification_timestamp <= cut_off_date

    if refresh_day_num is not None:
        current_day_of_week = current_time.weekday()

        is_appropriate_day_for_refresh = current_day_of_week == refresh_day_num
        return is_stale_by_time or is_appropriate_day_for_refresh

    return is_stale_by_time
