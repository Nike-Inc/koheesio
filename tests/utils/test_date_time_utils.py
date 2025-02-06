from datetime import timedelta

import pytest

from koheesio.utils.date_time import DTInterval


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            # check with all values
            "5 years 3 months 2 weeks 4 days 10 hours 42 minutes 4 seconds 20 milliseconds 200 microseconds",
            timedelta(
                days=int(4 + 14 + 3 * 30.44 + 5 * 365.25),
                seconds=4,
                microseconds=200,
                milliseconds=20,
                minutes=42,
                hours=10,
            ),
        ),
        (
            # check with aliases
            "7 years 4 months 1 weeks 3 days 8 hours 15 mins 30 secs 50 millis 100 micros",
            timedelta(
                days=int(3 + 7 + 4 * 30.44 + 7 * 365.25),
                seconds=30,
                microseconds=100,
                milliseconds=50,
                minutes=15,
                hours=8,
            ),
        ),
        (
            # check with singular values
            "1 year 1 month 1 week 1 day 1 hour 1 minute 1 second 1 millisecond 1 microsecond",
            timedelta(days=395 + 8, seconds=1, microseconds=1, milliseconds=1, minutes=1, hours=1),
        ),
        (
            # check with singular alias values
            "1 min 1 sec 1 milli 1 micro",
            timedelta(seconds=1, microseconds=1, milliseconds=1, minutes=1),
        ),
        (
            # test with week value
            "1 week",
            timedelta(weeks=1),
        ),
        ("5 years", timedelta(days=int(365.25 * 5))),
    ],
)
def test_dt_interval(input, expected):
    dt_interval = DTInterval(interval=input)
    assert dt_interval.to_timedelta == expected
