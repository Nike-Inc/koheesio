import os
from unittest.mock import MagicMock, patch

import pytest

from pyspark.sql.types import StringType, StructField, StructType

from koheesio.spark.utils import (
    get_column_name,
    import_pandas_based_on_pyspark_version,
    on_databricks,
    schema_struct_to_schema_str,
    show_string,
)
from koheesio.spark.utils.common import (
    PysparkConnectModuleNotAvailableWarning,
    check_if_pyspark_connect_module_is_available,
    get_active_session,
    get_spark_minor_version,
)


class TestGetActiveSession:
    def test_unhappy_get_active_session_spark_connect(self):
        """Test that get_active_session raises an error when no active session is found when using spark connect."""
        with (
            # ensure that we are forcing the code to think that we are using spark connect
            patch(
                "koheesio.spark.utils.common.check_if_pyspark_connect_module_is_available",
                return_value=True,
            ),
            # make sure that spark session is not found
            patch("pyspark.sql.SparkSession.getActiveSession", return_value=None),
        ):
            session = MagicMock(SparkSession=MagicMock(getActiveSession=MagicMock(return_value=None)))
            with patch.dict("sys.modules", {"pyspark.sql.connect.session": session}):
                with pytest.raises(
                    RuntimeError,
                    match="No active Spark session found. Please create a Spark session before using module "
                    "connect_utils. Or perform local import of the module.",
                ):
                    get_active_session()

    def test_unhappy_get_active_session(self):
        """Test that get_active_session raises an error when no active session is found."""
        with (
            patch(
                "koheesio.spark.utils.common.check_if_pyspark_connect_module_is_available",
                return_value=False,
            ),
            patch("pyspark.sql.SparkSession.getActiveSession", return_value=None),
        ):
            with pytest.raises(
                RuntimeError,
                match="No active Spark session found. Please create a Spark session before using module connect_utils. "
                "Or perform local import of the module.",
            ):
                get_active_session()

    def test_get_active_session_with_spark(self, spark):
        """Test get_active_session when an active session is found"""
        session = get_active_session()
        assert session is not None


class TestCheckIfPysparkConnectIsSupported:
    def test_if_pyspark_connect_is_not_available(self, mocker):
        """Test that check_if_pyspark_connect_module_is_available returns False or raises an ImportError when pyspark connect
        is not supported irregardless of the pyspark version used."""
        mocker.patch.dict(
            "sys.modules",
            {
                "pyspark.sql.connect": None,
                "grpc": None,
            },
        )
        mocker.patch.dict(
            os.environ,
            {
                "SPARK_CONNECT_MODE_ENABLED": "0",
            },
            clear=True,
        )
        assert check_if_pyspark_connect_module_is_available() is False

    @pytest.mark.parametrize("spark_minor_version", [3.3, 3.4, 3.5])
    def test_check_if_pyspark_connect_module_is_available(self, spark_minor_version: float, mocker):
        """Test that check_if_pyspark_connect_module_is_available returns True when pyspark connect is supported."""
        # Arrange
        mocker.patch("koheesio.spark.utils.common.SPARK_MINOR_VERSION", spark_minor_version)
        mocker.patch.dict(
            "sys.modules",
            {
                "pyspark.sql.connect.column": MagicMock(Column=MagicMock()),
                "pyspark.sql.connect": MagicMock(),
                "grpc": MagicMock(),
            },
        )
        mocker.patch.dict(os.environ, {"SPARK_REMOTE": "UNIT_TEST"})

        # Act
        actual_check = check_if_pyspark_connect_module_is_available()

        # Assert
        assert os.environ["SPARK_REMOTE"] == "UNIT_TEST"
        if spark_minor_version >= 3.4:
            assert actual_check is True
        else:
            assert actual_check is False

    @pytest.mark.parametrize("spark_minor_version", [3.3, 3.4, 3.5])
    def test_pyspark_connect_set_without_partial_deps(self, spark_minor_version: float, mocker):
        """Test that check_if_pyspark_connect_is_available raises an ImportError if pyspark connect is accessed without
        the dependencies being available"""
        mocker.patch("koheesio.spark.utils.common.SPARK_MINOR_VERSION", spark_minor_version)

        with patch.dict("sys.modules", {"grpc": None}):
            # Act & Assert
            if spark_minor_version < 3.4:
                assert check_if_pyspark_connect_module_is_available() is False
            else:
                with pytest.warns(PysparkConnectModuleNotAvailableWarning):
                    assert check_if_pyspark_connect_module_is_available() is False


def test_get_spark_minor_version():
    """Test that get_spark_minor_version returns the correctly formatted version."""
    with patch("koheesio.spark.utils.common.spark_version", "9.9.42"):
        assert get_spark_minor_version() == 9.9


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
        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": env_var_value}):
            assert on_databricks() == expected_result
    else:
        with patch.dict(os.environ, clear=True):
            assert on_databricks() == expected_result


@pytest.mark.parametrize(
    "spark_version, pandas_version, expected_error",
    [
        (3.3, "1.2.3", None),  # PySpark 3.3, pandas < 2, should not raise an error
        (3.4, "2.3.4", None),  # PySpark not 3.3, pandas >= 2, should not raise an error
        (3.3, "2.3.4", ImportError),  # PySpark 3.3, pandas >= 2, should raise an error
        (
            3.4,
            "1.2.3",
            ImportError,
        ),  # PySpark not 3.3, pandas < 2, should raise an error
    ],
)
def test_import_pandas_based_on_pyspark_version(spark_version, pandas_version, expected_error):
    with (
        patch(
            "koheesio.spark.utils.common.get_spark_minor_version",
            return_value=spark_version,
        ),
        patch("pandas.__version__", new=pandas_version),
    ):
        if expected_error:
            with pytest.raises(expected_error):
                import_pandas_based_on_pyspark_version()
        else:
            import_pandas_based_on_pyspark_version()  # This should not raise an error


def test_show_string(dummy_df):
    actual = show_string(dummy_df, n=1, truncate=1, vertical=False)
    assert actual == "+---+\n| id|\n+---+\n|  0|\n+---+\n"


def test_column_name():
    from pyspark.sql.functions import col

    name = "my_column"
    column = col(name)
    assert get_column_name(column) == name
