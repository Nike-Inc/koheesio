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
    """
    interval: Optional[str] = None
    years: int = 0  # = days * 365.25 (average year length, rounded up)
    months: int = 0  # = days * 30.44 (average month length, rounded up)
    weeks: int = 0
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0
    milliseconds: int = 0
    microseconds: int = 0

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
