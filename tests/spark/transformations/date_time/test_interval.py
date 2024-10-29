import datetime as dt

import pytest

from pyspark.sql import types as T

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.date_time.interval import (
    DateTimeAddInterval,
    DateTimeColumn,
    DateTimeSubtractInterval,
    adjust_time,
    col,
    dt_column,
    validate_interval,
)

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name="test_date_time")


@pytest.mark.parametrize(
    "input_data,column_name,operation,interval,expected",
    [
        (
            # "2019-01-01 00:00:00",
            dt.datetime(2019, 1, 1, 0, 0, 0),
            "datetime",
            "+",
            "1 day",
            dt.datetime(2019, 1, 2, 0, 0, 0),
        ),
        (
            "2019-01-01 00:00:00",
            "datetime",
            "-",
            "1 hour",
            "2018-12-31 23:00:00",
        ),
        (
            "2019-01-01",
            "date",
            "+",
            "1 day",
            "2019-01-02 00:00:00",
        ),
        (
            # 2019-01-01
            dt.datetime(2019, 1, 1, 0, 0, 0),
            # T.StructType([T.StructField("date", T.DateType())]),
            "date",
            "+",
            "10 months",
            # 2019-11-01
            dt.datetime(2019, 11, 1, 0, 0, 0),
        ),
        # (
        #     # Unhappy example !
        #     # ---
        #     # Seemingly there is a bug in Spark where an AttributeError is raised when trying to convert the use the
        #     # time column in the createDataFrame function. The error is not raised when using a date or datetime
        #     # column.
        #     # Error raised by pyspark: `AttributeError: 'datetime.time' object has no attribute 'timetuple'`
        #     # For this reason, we are leaving this test commented out for now...
        #     # ---
        #     # 00:00:00
        #     dt.time(0, 0, 0),
        #     "time",
        #     "+",
        #     "10 minutes",
        #     # 00:10:00
        #     dt.time(0, 10, 0),
        # ),
        (
            # try with microseconds
            # 2019-01-01 00:00:00.000
            dt.datetime(2019, 1, 1, 0, 0, 0, 0),
            "datetime",
            "+",
            "10 microseconds",
            # 2019-01-01 00:00:00.010
            dt.datetime(2019, 1, 1, 0, 0, 0, 10),
        ),
        (
            # check that we handle null values correctly
            None,
            "datetime",
            "+",
            "10 microseconds",
            None,
        ),
        (
            # complicated interval
            dt.datetime(2019, 1, 1, 0, 0, 0, 0),
            "datetime",
            "-",
            "5 days 3 hours 7 minutes 30 seconds 1 millisecond",
            dt.datetime(2018, 12, 26, 20, 52, 29, 999000),
        ),
    ],
)
def test_interval(input_data, column_name, operation, interval, expected, spark):
    if input_data is None:
        # create dataframe with null value and type string
        df = spark.createDataFrame([(input_data,)], T.StructType([T.StructField(column_name, T.StringType())]))
    else:
        df = spark.createDataFrame([(input_data,)], [column_name])

    column = col(column_name)
    column = DateTimeColumn.from_column(column)

    if operation == "-":
        df_adjusted = df.withColumn("adjusted", column - interval)
    elif operation == "+":
        df_adjusted = df.withColumn("adjusted", column + interval)
    else:
        raise RuntimeError(f"Invalid operation: {operation}")

    result = df_adjusted.collect()[0]["adjusted"]
    assert result == expected


def test_interval_unhappy(spark):
    with pytest.raises(ValueError):
        validate_interval("some random sym*bol*s")
    # invalid operation
    with pytest.raises(ValueError):
        _ = adjust_time(col("some_col"), "invalid operation", "1 day")
    # invalid interval
    with pytest.raises(ValueError):
        _ = adjust_time(col("some_col"), "add", "invalid value")
    # invalid input passed to the Transformation
    with pytest.raises(ValueError):
        DateTimeAddInterval(column="date", interval="1 foo")


def test_interval_column_transformations(df_with_all_types):
    df = df_with_all_types.select("timestamp")

    result_df = df
    add_interval = DateTimeAddInterval(column="timestamp", interval="1 month", target_column="date_plus_1_month")
    result_df = add_interval.transform(result_df)
    actual_add = result_df.head()["date_plus_1_month"]

    subtract_interval = DateTimeSubtractInterval(
        column="timestamp", interval="1 month", target_column="date_minus_1_month"
    )
    result_df = subtract_interval.transform(result_df)
    actual_subtract = result_df.head()["date_minus_1_month"]

    # expected data
    expected_add = dt.datetime(2023, 2, 1, 0, 1, 1)
    expected_subtract = dt.datetime(2022, 12, 1, 0, 1, 1)

    assert actual_add == expected_add
    assert actual_subtract == expected_subtract


def test_dt_column_with_string(spark):
    """Test that the dt_column function is a drop in replacement for pyspark.sql.functions.col given a string input"""
    df = spark.createDataFrame([(1, "2022-01-01 00:00:00")], ["id", "my_column"])
    result = df.select(dt_column("my_column")).collect()[0][0]
    assert result == "2022-01-01 00:00:00"


def test_dt_column_with_invalid_input():
    with pytest.raises(TypeError):
        dt_column(123)
