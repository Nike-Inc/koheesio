import logging

import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.debug import Peek


@pytest.mark.parametrize(
    "log_level,expected",
    [
        ("DEBUG", True),
        ("INFO", False),
        ("WARNING", False),
    ],
)
def test_on_debug(monkeypatch, log_level, expected):
    LoggingFactory(level=log_level)

    peek = Peek(enabled=True)
    actual = peek._on_debug()
    assert actual == expected


@pytest.mark.parametrize(
    "input_values,log_level,expected_calls",
    [
        # description: enabled is False, irrespective of log level or enabled by logger, it should not run
        (
            dict(enabled=False, enabled_by_logger=True, truncate_output=True, vertical=True, n=10, print_schema=True),
            "INFO",
            dict(printSchema=0, show=0),
        ),
        (
            dict(enabled=False, enabled_by_logger=True, truncate_output=True, vertical=True, n=10),
            "DEBUG",
            dict(printSchema=0, show=0),
        ),
        # description: enabled is True and enabled_by_logger is False, it should run irrespective of log level
        (
            dict(enabled=True, enabled_by_logger=False, truncate_output=True, vertical=True, n=10, print_schema=True),
            "WARNING",
            dict(printSchema=1, show=1),
        ),
        (
            dict(enabled=True, enabled_by_logger=False, truncate_output=True, vertical=True, n=10, print_schema=False),
            "DEBUG",
            dict(printSchema=0, show=1),
        ),
        # description: both Flags True, it should run based on log level (DEBUG or lower)
        (
            dict(enabled=True, enabled_by_logger=True, truncate_output=True, vertical=True, n=10, print_schema=False),
            "INFO",
            dict(printSchema=0, show=0),
        ),
        (
            dict(enabled=True, enabled_by_logger=True, truncate_output=True, vertical=True, n=10, print_schema=True),
            "DEBUG",
            dict(printSchema=1, show=1),
        ),
    ],
)
def test_peek(spark, mocker, monkeypatch, input_values, log_level, expected_calls):
    input_df = spark.createDataFrame(
        [("Banana lemon orange", 1000, "USA"), ("Carrots Blueberries", 1500, "USA"), ("Beans", 1600, "USA")],
        schema="product string, amount int, country string",
    )

    # Set the expected log level
    LoggingFactory(level=log_level)

    peek = Peek(**input_values)
    peek.df = input_df

    # Mock the methods
    peek.df.printSchema = mocker.MagicMock()
    peek.df.show = mocker.MagicMock()

    peek.execute()

    # Check if the methods were called the expected number of times
    assert peek.df.printSchema.call_count == expected_calls["printSchema"]
    assert peek.df.show.call_count == expected_calls["show"]

    if expected_calls["show"] > 0:
        peek.df.show.assert_called_with(
            truncate=input_values["truncate_output"], n=input_values["n"], vertical=input_values["vertical"]
        )
