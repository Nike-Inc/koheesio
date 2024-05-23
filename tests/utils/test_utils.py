import os
from unittest.mock import patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from koheesio.spark.utils import on_databricks, schema_struct_to_schema_str
from koheesio.utils import get_args_for_func, get_random_string


def test_get_args_for_func():
    def func(a, b, c):
        return a + b + c

    func, args = get_args_for_func(func, {"a": 1, "b": 2, "c": 3})
    assert args == {"a": 1, "b": 2, "c": 3}


def test_import_class():
    import datetime

    from koheesio.utils import import_class

    assert import_class("datetime.datetime") == datetime.datetime


@pytest.mark.parametrize(
    "env_var_value, expected_result",
    [("lts_11_spark_3_scala_2.12", True), ("unit_test", True), (None, False)],
)
def test_on_databricks(env_var_value, expected_result):
    if env_var_value is not None:
        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": env_var_value}):
            assert on_databricks() == expected_result
    else:
        with patch.dict(os.environ, clear=True):
            assert on_databricks() == expected_result


def test_schema_struct_to_schema_str():
    struct_schema = StructType([StructField("a", StringType()), StructField("b", StringType())])
    val = schema_struct_to_schema_str(struct_schema)
    assert val == "a STRING,\nb STRING"
    assert schema_struct_to_schema_str(None) == ""


def test_get_random_string():
    assert get_random_string(10) != get_random_string(10)
    assert len(get_random_string(10)) == 10
    assert len(get_random_string(10, "abc")) == 10
    assert get_random_string(10, "abc").startswith("abc_")
