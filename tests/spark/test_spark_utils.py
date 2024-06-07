from os import environ
from unittest.mock import patch

import pytest

from pyspark.sql.types import StringType, StructField, StructType

from koheesio.spark.utils import (
    import_pandas_based_on_pyspark_version,
    on_databricks,
    schema_struct_to_schema_str,
)


def test_schema_struct_to_schema_str():
    struct_schema = StructType([StructField("a", StringType()), StructField("b", StringType())])
    val = schema_struct_to_schema_str(struct_schema)
    assert val == "a STRING,\nb STRING"
    assert schema_struct_to_schema_str(None) == ""


@pytest.mark.parametrize(
    "env_var_value, expected_result",
    [("lts_11_spark_3_scala_2.12", True), ("unit_test", True), (None, False)],
)
def test_on_databricks(env_var_value, expected_result):
    if env_var_value is not None:
        with patch.dict(environ, {"DATABRICKS_RUNTIME_VERSION": env_var_value}):
            assert on_databricks() == expected_result
    else:
        with patch.dict(environ, clear=True):
            assert on_databricks() == expected_result


@pytest.mark.parametrize(
    "spark_version, pandas_version, expected_error",
    [
        (3.3, "1.2.3", None),  # PySpark 3.3, pandas < 2, should not raise an error
        (3.4, "2.3.4", None),  # PySpark not 3.3, pandas >= 2, should not raise an error
        (3.3, "2.3.4", ImportError),  # PySpark 3.3, pandas >= 2, should raise an error
        (3.4, "1.2.3", ImportError),  # PySpark not 3.3, pandas < 2, should raise an error
    ],
)
def test_import_pandas_based_on_pyspark_version(spark_version, pandas_version, expected_error):
    with (
        patch("koheesio.spark.utils.get_spark_minor_version", return_value=spark_version),
        patch("pandas.__version__", new=pandas_version),
    ):
        if expected_error:
            with pytest.raises(expected_error):
                import_pandas_based_on_pyspark_version()
        else:
            import_pandas_based_on_pyspark_version()  # This should not raise an error
