from datetime import datetime, timedelta
import os
from pathlib import Path
from unittest.mock import patch

from chispa import assert_df_equality
from conftest import setup_test_data
from freezegun import freeze_time
import pytest

from pydantic import ValidationError

from pyspark.sql.types import LongType

from koheesio.logger import LoggingFactory
from koheesio.spark.delta import DeltaTableStep, StaleDataCheckStep

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name="test_delta", inherit_from_koheesio=True)


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            "test_catalog.test_schema.test_table",
            {
                "catalog": "test_catalog",
                "database": "test_schema",
                "table": "test_table",
                "table_name": "test_catalog.test_schema.test_table",
            },
        ),
        (
            "test_schema.test_table",
            {
                "catalog": None,
                "database": "test_schema",
                "table": "test_table",
                "table_name": "test_schema.test_table",
            },
        ),
        (
            {"catalog": "a_catalog", "table": "a_schema.a_table"},
            {
                "catalog": "a_catalog",
                "database": "a_schema",
                "table": "a_table",
                "table_name": "a_catalog.a_schema.a_table",
            },
        ),
        (
            "delta_table",
            {
                "catalog": None,
                "database": None,
                "table": "delta_table",
                "table_name": "delta_table",
            },
        ),
    ],
)
def test_table(value, expected):
    if isinstance(value, str):
        actual = DeltaTableStep(table=value)
        log.info(f"actual: {actual}")
    elif isinstance(value, dict):
        actual = DeltaTableStep(**value)
    else:
        raise AssertionError(f"Unable to run test with given input '{value}'")

    assert actual.catalog == expected["catalog"]
    assert actual.database == expected["database"]
    assert actual.table == expected["table"]
    assert actual.table_name == expected["table_name"]

    log.info("delta test completed")


def test_delta_table_properties(spark, setup, delta_file):
    setup_test_data(spark=spark, delta_file=Path(delta_file))
    table_name = "delta_test_table"
    dt = DeltaTableStep(
        table=table_name,
        default_create_properties={
            "delta.randomizeFilePrefixes": "true",
            "delta.checkpoint.writeStatsAsStruct": "true",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
            "not_string_property_but_bool": True,
            "not_string_property_but_int": 123,
        },
    )

    assert dt.exists is True
    assert dt.dataframe.count() == 10
    assert dt.columns == ["id"]
    assert dt.get_column_type("id") == LongType()
    assert dt.get_column_type("id").simpleString() == "bigint"
    assert dt.get_column_type("none") is None
    assert dt.get_column_type(None) is None
    assert dt.has_change_type is False

    assert dt.table_name == table_name

    properties = dt.get_persisted_properties()
    assert properties.get("delta.enableChangeDataFeed") == "true"
    dt.add_properties({"delta.enableChangeDataFeed": "false"}, override=True)
    assert dt.get_persisted_properties().get("delta.enableChangeDataFeed") == "false"

    dt.add_properties({"delta.enableChangeDataFeed": "true"})
    assert dt.get_persisted_properties().get("delta.enableChangeDataFeed") == "false"

    assert properties.get("test_property") is None
    dt.add_properties({"test_property": "delta_table_value"})
    assert dt.get_persisted_properties().get("test_property") == "delta_table_value"

    assert dt.default_create_properties.get("not_string_property_but_bool") == "true"
    assert dt.default_create_properties.get("not_string_property_but_int") == "123"

    dt.add_properties({"test_property_bool": True})
    assert dt.get_persisted_properties().get("test_property_bool") == "true"
    dt.add_properties({"test_property_int": 123})
    assert dt.get_persisted_properties().get("test_property_int") == "123"

    for v in dt.default_create_properties.values():
        assert isinstance(v, str)


@patch.dict(
    os.environ,
    {
        "DATABRICKS_RUNTIME_VERSION": "lts_11_spark_3_scala_2.12",
    },
)
def test_delta_table_properties_dbx():
    dt = DeltaTableStep(table="delta_test_table_dbx", create_if_not_exists=True)
    assert dt.default_create_properties.get("delta.autoOptimize.autoCompact") == "true"


@pytest.mark.parametrize("value,expected", [("too.many.dots.given.to.be.a.real.table", pytest.raises(ValidationError))])
def test_table_failed(value, expected):
    with expected:
        DeltaTableStep(table=value)

    dt = DeltaTableStep(table="unknown_table")
    assert dt.exists is False


@pytest.mark.parametrize(
    ["table", "create_if_not_exists", "log_level"], [("unknown", False, "DEBUG"), ("unknown", True, "INFO")]
)
def test_exists(caplog, table, create_if_not_exists, log_level):
    with caplog.at_level(log_level):
        dt = DeltaTableStep(table=table, create_if_not_exists=create_if_not_exists)
        dt.log.setLevel(log_level)
        assert dt.exists is False


@pytest.fixture
def test_describe_history_df(spark):
    data = [
        {"version": 0, "timestamp": "2024-12-30 12:00:00", "tableName": "test_table", "operation": "CREATE TABLE"},
        {"version": 1, "timestamp": "2024-12-31 05:29:30", "tableName": "test_table", "operation": "WRITE"},
        {"version": 2, "timestamp": "2025-01-01 11:12:19", "tableName": "test_table", "operation": "MERGE"},
    ]
    return spark.createDataFrame(data)


def test_describe_history__no_limit(mocker, spark, test_describe_history_df):
    mocker.patch.object(DeltaTableStep, "exists", new_callable=mocker.PropertyMock(return_value=True))
    dt = DeltaTableStep(table="test_table")
    mocker.patch.object(spark, "sql", return_value=test_describe_history_df)
    result = dt.describe_history()
    expected_df = spark.createDataFrame(
        [
            {"version": 2, "timestamp": "2025-01-01 11:12:19", "tableName": "test_table", "operation": "MERGE"},
            {"version": 1, "timestamp": "2024-12-31 05:29:30", "tableName": "test_table", "operation": "WRITE"},
            {"version": 0, "timestamp": "2024-12-30 12:00:00", "tableName": "test_table", "operation": "CREATE TABLE"},
        ]
    )
    assert_df_equality(result, expected_df, ignore_column_order=True)


def test_describe_history__with_limit(mocker, spark, test_describe_history_df):
    mocker.patch.object(DeltaTableStep, "exists", new_callable=mocker.PropertyMock(return_value=True))
    dt = DeltaTableStep(table="test_table")
    mocker.patch.object(spark, "sql", return_value=test_describe_history_df)
    result = dt.describe_history(limit=1)
    expected_df = spark.createDataFrame(
        [
            {"version": 2, "timestamp": "2025-01-01 11:12:19", "tableName": "test_table", "operation": "MERGE"},
        ]
    )
    assert_df_equality(result, expected_df, ignore_column_order=True)


def test_describe_history__no_table(mocker):
    mocker.patch.object(DeltaTableStep, "exists", new_callable=mocker.PropertyMock(return_value=False))
    dt = DeltaTableStep(table="test_table")
    result = dt.describe_history()

    assert result is None


@pytest.fixture
def test_stale_data_check_history_df(spark):
    return spark.createDataFrame(
        [
            {
                "timestamp": datetime(year=2024, month=12, day=30, hour=5, minute=29, second=30),
                "operation": "WRITE",
                "version": 1,
            },
            {
                "timestamp": datetime(year=2024, month=12, day=30, hour=5, minute=28, second=30),
                "operation": "CREATE TABLE",
                "version": 0,
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
        (
            timedelta(days=1, hours=6, minutes=30, seconds=31),
            False,
        ),  # Not stale, since 1 second more than the timestamp age
        (
            timedelta(days=1, hours=6, minutes=30, seconds=30),
            True,
        ),  # Exactly equal to the age, should be considered stale
        (timedelta(days=1, hours=6, minutes=30, seconds=29), True),  # Stale, falls 1 second short
        (timedelta(days=1), True),  # Stale, falls several hours and minutes short
        (
            timedelta(hours=18),
            True,
        ),  # Stale, despite being more than half a day, but less than the full duration since the timestamp
    ],
)
@freeze_time("2024-12-31 12:00:00")
def test_stale_data_check_step__no_refresh_day_num_with_time_components(
    interval, expected, test_stale_data_check_history_df, mocker
):
    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=test_stale_data_check_history_df)
    assert StaleDataCheckStep(table="dummy_table", interval=interval).execute().is_data_stale == expected


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
        (timedelta(hours=6, minutes=30, seconds=30), 1, True),  # Data is stale and it is the refresh day
        (
            timedelta(hours=6, minutes=30, seconds=30),
            0,
            True,
        ),  # Data is stale and it is past the refresh day
    ],
)
@freeze_time("2024-12-31 12:00:00")  # Tuesday
def test_stale_data_check_step__with_refresh_day(
    interval, refresh_day_num, expected, test_stale_data_check_history_df, mocker
):
    mocker.patch("koheesio.spark.delta.DeltaTableStep.describe_history", return_value=test_stale_data_check_history_df)
    assert (
        StaleDataCheckStep(table="dummy_table", interval=interval, refresh_day_num=refresh_day_num)
        .execute()
        .is_data_stale
        == expected
    )


def test_stale_data_check_step__invalid_refresh_day():
    with pytest.raises(ValueError):
        StaleDataCheckStep(table="dummy_table", interval=timedelta(days=1), refresh_day_num=7).execute()


def test_stale_data_check_step__invalid_staleness_period_with_refresh_day():
    with pytest.raises(ValueError):
        StaleDataCheckStep(table="dummy_table", interval=timedelta(days=10), refresh_day_num=5).execute()
