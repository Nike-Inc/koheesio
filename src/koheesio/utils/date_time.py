"""
Utility functions related to date and time operations
"""

from typing import Optional
from datetime import timedelta
import re

from koheesio.models import BaseModel, model_validator

extract_dt_interval = re.compile(
    r"""
    (?P<years>\d+)\s+years?\s*|
    (?P<months>\d+)\s+months?\s*|
    (?P<weeks>\d+)\s+weeks?\s*|
    (?P<days>\d+)\s+days?\s*|
    (?P<hours>\d+)\s+hours?\s*|
    (?P<minutes>\d+)\s+(?:minutes?|mins?)\s*|
    (?P<seconds>\d+)\s+(?:seconds?|secs?)\s*|
    (?P<milliseconds>\d+)\s+(?:milliseconds?|millis?)\s*|
    (?P<microseconds>\d+)\s+(?:microseconds?|micros?)\s*
""",
    re.VERBOSE,
)


class DTInterval(BaseModel):
    """
    A class to define date and time intervals using human-readable strings or individual time components.

    Parameters
    ----------
    interval : str, optional
        A human-readable string specifying the duration of the interval, broken down
        into years, months, weeks, days, hours, minutes, seconds, milliseconds, and microseconds.
    years : int, optional, default 0
        Number of years in the interval.
    months : int, optional, default 0
        Number of months in the interval.
    weeks : int, optional, default 0
        Number of weeks in the interval.
    days : int, optional, default 0
        Number of days in the interval.
    hours : int, optional, default 0
        Number of hours in the interval.
    minutes : int, optional, default 0
        Number of minutes in the interval.
    seconds : int, optional, default 0
        Number of seconds in the interval.
    milliseconds : int, optional, default 0
        Number of milliseconds in the interval.
    microseconds : int, optional, default 0
        Number of microseconds in the interval.

    Examples
    --------
    Creating an instance with time components:

    ```python
    print(DTInterval(years=2, weeks=3, hours=12).to_timedelta)
    ```
    751 days, 12:00:00

    Creating an instance from a string:
    ```python
    print(
        DTInterval(
            interval="1 year 2 months 3 weeks 4 days 5 hours 100 minutes 200 seconds 300 milliseconds 400 microseconds"
        ).to_timedelta
    )
    ```
    451 days, 6:43:20.300400

    Methods
    -------
    to_timedelta()
        Converts the DTInterval instance to a timedelta object, aggregating all specified time components.
    """

    interval: Optional[str] = None
    years: Optional[int] = 0  # = days * 365.25 (average year length, rounded up)
    months: Optional[int] = 0  # = days * 30.44 (average month length, rounded up)
    weeks: Optional[int] = 0
    days: Optional[int] = 0
    hours: Optional[int] = 0
    minutes: Optional[int] = 0
    seconds: Optional[int] = 0
    milliseconds: Optional[int] = 0
    microseconds: Optional[int] = 0

    @model_validator(mode="before")
    def process_interval(cls, values: dict) -> dict:
        """Processes the input interval string and extracts the time components"""
        if interval_value := values.get("interval"):
            matches = extract_dt_interval.finditer(interval_value)

            # update values with the extracted values
            for match in matches:
                for key, value in match.groupdict().items():
                    if value:
                        values[key] = int(value)
        return values

    @model_validator(mode="after")
    def calculate_days(self) -> "DTInterval":
        """Years and months are not supported in timedelta, so we need to convert them to days"""
        if self.years or self.months:
            year_month_days = int(self.years * 365.25 + self.months * 30.44)  # average year length
            self.days += year_month_days
            self.years = 0
            self.months = 0
        return self

    @property
    def to_timedelta(self) -> timedelta:
        """Returns the object as a timedelta object"""
        return timedelta(
            days=self.days,
            seconds=self.seconds,
            microseconds=self.microseconds,
            milliseconds=self.milliseconds,
            minutes=self.minutes,
            hours=self.hours,
            weeks=self.weeks,
        )
