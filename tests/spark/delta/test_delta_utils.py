from datetime import datetime

from freezegun import freeze_time
import pytest

from koheesio.spark.delta.utils import is_data_stale
from koheesio.utils.date_time import DTInterval


@pytest.fixture
def test_history_df(spark):
    return spark.createDataFrame(
        [
            {"timestamp": datetime(year=2024, month=12, day=30, hour=5, minute=29, second=30), "operation": "WRITE", "version": 1},
            {
                "timestamp": datetime(year=2024, month=12, day=30, hour=5, minute=28, second=30),
                "operation": "CREATE TABLE",
                "version": 0
            },
        ]
    )


@pytest.mark.parametrize(
    "months, weeks, days, hours, minutes, seconds, expected",
    [
        (1, 0, 0, 0, 0, 0, False),  # Not stale, since 1 month > 1 day and some hours ago
        (0, 1, 0, 0, 0, 0, False),  # Not stale, since 1 week > 1 day and some hours ago
        (0, 0, 2, 0, 0, 0, False),  # Not stale, since 2 days > 1 day and some hours ago
        (0, 0, 1, 7, 0, 0, False),  # Not stale, since 1 day and 7 hours > 1 day, 6 hours, 30 minutes
        (0, 0, 1, 6, 31, 0, False),  # Not stale, since 1 minute more than the timestamp age
        (0, 0, 1, 6, 30, 31, False),  # Not stale, since 1 second more than the timestamp age
        (0, 0, 1, 6, 30, 30, True),  # Exactly equal to the age, should be considered stale
        (0, 0, 1, 6, 30, 29, True),  # Stale, falls 1 second short
        (0, 0, 1, 0, 0, 0, True),  # Stale, falls several hours and minutes short
        (
            0,
            0,
            0,
            18,
            0,
            0,
            True,
        ),  # Stale, despite being more than half a day, but less than the full duration since the timestamp
    ],
)
@freeze_time("2024-12-31 12:00:00")
def test_is_data_stale__no_refresh_day_num_with_time_components(
    months, weeks, days, hours, minutes, seconds, expected, test_history_df, mocker
):

    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=test_history_df)

    assert (
        is_data_stale(
            "dummy_table", months=months, weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds
        )
        == expected
    )


@pytest.mark.parametrize(
    "dt_interval, expected",
    [
        (DTInterval(interval="2 days"), False),  # Not stale, since 2 days > 1 day and some hours ago
        (
            DTInterval(interval="1 day, 7 hours"),
            False,
        ),  # Not stale, since 1 day and 7 hours > 1 day, 6 hours, 30 minutes
        (
            DTInterval(interval="1 day, 6 hours, 31 minutes"),
            False,
        ),  # Not stale, since 1 minute more than the timestamp age
        (
            DTInterval(interval="1 day, 6 hours, 30 minutes, 31 seconds"),
            False,
        ),  # Not stale, since 1 second more than the timestamp age
        (
            DTInterval(interval="1 day, 6 hours, 30 minutes, 30 seconds"),
            True,
        ),  # Exactly equal to the age, should be considered stale
        (DTInterval(interval="1 day, 6 hours, 30 minutes, 29 seconds"), True),  # Stale, falls 1 second short
        (DTInterval(interval="1 day"), True),  # Stale, falls several hours and minutes short
        (
            DTInterval(interval="18 hours"),
            True,
        ),  # Stale, despite being more than half a day, but less than the full duration since the timestamp
    ],
)
@freeze_time("2024-12-31 12:00:00")
def test_is_data_stale__no_refresh_day_num_with_dt_interval(dt_interval, expected, test_history_df, mocker):

    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=test_history_df)

    assert is_data_stale("dummy_table", dt_interval=dt_interval) == expected


def test_is_data_stale__no_table(mocker):

    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=None)

    assert is_data_stale("dummy_table", days=1)


def test_is_data_stale__no_modification_history(mocker, spark):

    history_df = spark.createDataFrame(
        [
            {
                "timestamp": datetime(year=2024, month=12, day=30, hour=5, minute=28, second=30),
                "operation": "CREATE TABLE",
            }
        ]
    )

    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=history_df)

    assert is_data_stale("dummy_table", days=1)


@pytest.mark.parametrize(
    "interval, refresh_day_num, expected",
    [
        (
            DTInterval(interval="2 days, 6 hours, 30 minutes, 30 seconds"),
            2,
            False,
        ),  # Data is not stale and it's before the refresh day
        (
            DTInterval(interval="2 days, 6 hours, 30 minutes, 30 seconds"),
            1,
            True,
        ),  # Data is not stale but it is the refresh day
        (
            DTInterval(interval="2 days, 6 hours, 30 minutes, 30 seconds"),
            0,
            False,
        ),  # Data is not stale and it is past the refresh day
        (
            DTInterval(interval="6 hours, 30 minutes, 30 seconds"),
            2,
            True,
        ),  # Data is stale and it's before the refresh day
        (DTInterval(interval="6 hours, 30 minutes, 30 seconds"), 1, True),  # Data is stale and it is the refresh day
        (
            DTInterval(interval="6 hours, 30 minutes, 30 seconds"),
            0,
            True,
        ),  # Data is stale and it is past the refresh day
    ],
)
@freeze_time("2024-12-31 12:00:00")  # Tuesday
def test_is_data_stale__with_refresh_day(interval, refresh_day_num, expected, test_history_df, mocker):

    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=test_history_df)

    assert is_data_stale("dummy_table", dt_interval=interval, refresh_day_num=refresh_day_num) == expected


def test_is_data_stale__missing_input():
    with pytest.raises(ValueError):
        is_data_stale("dummy_table")


def test_is_data_stale__invalid_refresh_day():
    with pytest.raises(ValueError):
        is_data_stale("dummy_table", dt_interval=DTInterval(interval="1 day"), refresh_day_num=7)


def test_is_data_stale__invalid_staleness_period_with_refresh_day():
    with pytest.raises(ValueError):
        is_data_stale("dummy_table", dt_interval=DTInterval(interval="1 month"), refresh_day_num=5)
