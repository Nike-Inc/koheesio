import os
from pathlib import Path
from unittest.mock import patch

import pytest
from conftest import setup_test_data

from pydantic import ValidationError

from pyspark.sql.types import LongType

from koheesio.logger import LoggingFactory
from koheesio.spark.delta import DeltaTableStep

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name="test_delta")


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
    with pytest.raises(ValidationError):
        DeltaTableStep(table=value)

    dt = DeltaTableStep(table="unknown_table")
    assert dt.exists is False
