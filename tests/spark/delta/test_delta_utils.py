from datetime import datetime, timedelta

from freezegun import freeze_time
import pytest

from koheesio.spark.delta.utils import StaleDataCheckStep


@pytest.fixture
def test_history_df(spark):
    return spark.createDataFrame(
        [
            {
                "timestamp": datetime(year=2024, month=12, day=30, hour=5, minute=29, second=30),
                "operation": "WRITE",
                "version": 1
            },
            {
                "timestamp": datetime(year=2024, month=12, day=30, hour=5, minute=28, second=30),
                "operation": "CREATE TABLE",
                "version": 0
            },
        ]
    )


@pytest.mark.parametrize(
    "interval, expected",
    [
        (timedelta(weeks=1), False),  # Not stale, since 1 week > 1 day and some hours ago
        (timedelta(days=2), False),  # Not stale, since 2 days > 1 day and some hours ago
        (timedelta(days=1, hours=7), False),  # Not stale, since 1 day and 7 hours > 1 day, 6 hours, 30 minutes
        (timedelta(days=1, hours=6, minutes=31), False),  # Not stale, since 1 minute more than the timestamp age
        (timedelta(days=1, hours=6, minutes=30, seconds=31), False),  # Not stale, since 1 second more than the timestamp age
        (timedelta(days=1, hours=6, minutes=30, seconds=30), True),  # Exactly equal to the age, should be considered stale
        (timedelta(days=1, hours=6, minutes=30, seconds=29), True),  # Stale, falls 1 second short
        (timedelta(days=1), True),  # Stale, falls several hours and minutes short
        (timedelta(hours=18), True)  # Stale, despite being more than half a day, but less than the full duration since the timestamp
    ],
)
@freeze_time("2024-12-31 12:00:00")
def test_stale_data_check_step__no_refresh_day_num_with_time_components(
    interval, expected, test_history_df, mocker
):
    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=test_history_df)
    assert (
        StaleDataCheckStep(table="dummy_table", interval=interval).execute().is_data_stale
        == expected
    )


def test_stale_data_check_step__no_table(mocker):
    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=None)
    assert StaleDataCheckStep(table="dummy_table", interval=timedelta(days=1)).execute().is_data_stale


def test_stale_data_check_step__no_modification_history(mocker, spark):
    history_df = spark.createDataFrame(
        [
            {
                "timestamp": datetime(year=2024, month=12, day=30, hour=5, minute=28, second=30),
                "operation": "CREATE TABLE",
            }
        ]
    )
    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=history_df)
    assert StaleDataCheckStep(table="dummy_table", interval=timedelta(days=1)).execute().is_data_stale


@pytest.mark.parametrize(
    "interval, refresh_day_num, expected",
    [
        (
            timedelta(days=2, hours=6, minutes=30, seconds=30),
            2,
            False,
        ),  # Data is not stale and it's before the refresh day
        (
            timedelta(days=2, hours=6, minutes=30, seconds=30),
            1,
            True,
        ),  # Data is not stale but it is the refresh day
        (
            timedelta(days=2, hours=6, minutes=30, seconds=30),
            0,
            False,
        ),  # Data is not stale and it is past the refresh day
        (
            timedelta(hours=6, minutes=30, seconds=30),
            2,
            True,
        ),  # Data is stale and it's before the refresh day
        (
            timedelta(hours=6, minutes=30, seconds=30),
            1,
            True
        ),  # Data is stale and it is the refresh day
        (
            timedelta(hours=6, minutes=30, seconds=30),
            0,
            True,
        ),  # Data is stale and it is past the refresh day
    ],
)
@freeze_time("2024-12-31 12:00:00")  # Tuesday
def test_stale_data_check_step__with_refresh_day(interval, refresh_day_num, expected, test_history_df, mocker):
    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=test_history_df)
    assert StaleDataCheckStep(table="dummy_table", interval=interval, refresh_day_num=refresh_day_num).execute().is_data_stale == expected


def test_stale_data_check_step__invalid_refresh_day():
    with pytest.raises(ValueError):
        StaleDataCheckStep(table="dummy_table", interval=timedelta(days=1), refresh_day_num=7).execute()


def test_stale_data_check_step__invalid_staleness_period_with_refresh_day():
    with pytest.raises(ValueError):
        StaleDataCheckStep(table="dummy_table", interval=timedelta(days=10), refresh_day_num=5).execute()
