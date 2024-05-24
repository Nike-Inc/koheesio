from typing import Any, Dict

import pytest

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.transform import Transform

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name="test_transform")


def dummy_transform_func(df: DataFrame, target_column: str, value: str):
    return df.withColumn(target_column, f.lit(value))


def no_kwargs_dummy_func(df: DataFrame):
    return df


def transform_output_test(sdf: DataFrame, expected_data: Dict[str, Any]):
    return sdf.head().asDict() == expected_data


def test_dummy_function(dummy_df):
    # testing the dummy functions unwrapped / outside Transform class
    df = dummy_transform_func(dummy_df, "hello", "world")
    assert transform_output_test(df, {"id": 0, "hello": "world"})

    df = no_kwargs_dummy_func(dummy_df)
    assert transform_output_test(df, {"id": 0})


def test_verbose_transform(dummy_df):
    # verbose style input in Transform
    log.info(f"dummy_df: {dummy_df}")
    df = Transform(df=dummy_df, func=dummy_transform_func, params={"target_column": "foo", "value": "bar"}).execute().df
    assert transform_output_test(df, {"id": 0, "foo": "bar"})


def test_short_notation_on_transform(dummy_df):
    # shortened style notation (easier to read)
    df = Transform(df=dummy_df, func=dummy_transform_func, target_column="llama", value="drama").execute().df
    assert transform_output_test(df, {"id": 0, "llama": "drama"})


def test_ignore_too_much_input(dummy_df):
    # when too much input is given, transform should ignore extra input
    df = Transform(
        dummy_transform_func,
        target_column="so long",
        # ignored input
        value="and thanks for all the fish",
        title=42,
        author="Adams",
    ).transform(dummy_df)
    assert transform_output_test(df, {"id": 0, "so long": "and thanks for all the fish"})


def test_order_of_params(dummy_df):
    # order of params input should not matter
    df = Transform(
        dummy_transform_func,
        # out of position and extraneous
        value="lorem",
        title="thing",
        weekday="Wednesday",
        target_column="ipsum",
    ).transform(dummy_df)
    assert transform_output_test(df, {"id": 0, "ipsum": "lorem"})


def test_no_kwargs_function(dummy_df):
    df = Transform(no_kwargs_dummy_func).transform(dummy_df)
    assert transform_output_test(df, {"id": 0})


def test_from_func(dummy_df):
    # noinspection PyPep8Naming
    AddFooColumn = Transform.from_func(dummy_transform_func, target_column="foo")
    df = AddFooColumn(value="bar").transform(dummy_df)
    assert transform_output_test(df, {"id": 0, "foo": "bar"})
