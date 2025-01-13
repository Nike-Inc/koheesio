"""

The following fixtures are available:
- `warehouse_path`: A temporary warehouse folder that can be used with SparkSessions.
- `checkpoint_folder`: A temporary checkpoint folder that can be used with Spark streams.
- `spark_with_delta`: A Spark session fixture with Delta enabled.
- `set_env_vars`: A fixture to set environment variables for PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON.
- `spark`: A fixture that ensures PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are set to the current Python executable
    and returns a Spark session.
- `streaming_dummy_df`: A fixture that creates a streaming DataFrame from a Delta table for testing purposes.
- `mock_df`: A fixture to mock a DataFrame's methods.
- `mock_spark_reader`: SparkSession fixture that makes any call to `SparkSession.read.load()` return a custom DataFrame.
"""
from typing import Any, Generator, Optional
from dataclasses import dataclass, field
import datetime
from logging import Logger
import os
from pathlib import Path
import sys
from textwrap import dedent
from unittest import mock

from delta import configure_spark_with_delta_pip

from koheesio.models import FilePath
from koheesio.spark import DataFrame, SparkSession
from koheesio.spark.testing.utils import setup_test_data
from koheesio.utils.testing import FixtureValue, pytest, register_fixture, register_fixtures

__fixtures__ = [
    # fixtures
    "warehouse_path",
    "checkpoint_folder",
    "spark_with_delta",
    "set_env_vars",
    "spark", 
    "streaming_dummy_df",
    "mock_df",
    "mock_spark_reader",
]
__all__ = [
    "SparkContextData",
    "setup_test_data",
    "register_fixture",
    "register_fixtures",
    *__fixtures__,
]

@pytest.fixture
def warehouse_path(tmp_path_factory: pytest.TempPathFactory, random_uuid: str, logger: Logger) -> Generator[str]:
    """Fixture to create a temporary warehouse folder that can be used with Spark.

    This fixture uses pytest's built-in `tmp_path_factory` to create a temporary folder that can be used as a warehouse
    folder for Spark. The folder is created with a unique name to avoid conflicts between tests.

    This warehouse folder is automatically cleaned up after the test session is completed.

    Example
    -------
    ```python
    @pytest.fixture
    def spark_builder_with_warehouse(warehouse_path):
        '''example of a fixture that uses the warehouse_path fixture'''
        from spark.sql import SparkSession

        assert os.path.exists(warehouse_path)

        builder = (
            SparkSession.builder
            # the warehouse_path fixture is used here
            .config("spark.sql.warehouse.dir", warehouse_path)
        )
        yield builder
    ```
    """
    warehouse_folder = tmp_path_factory.mktemp("spark-warehouse" + random_uuid)
    logger.debug(f"Building test warehouse folder '{warehouse_folder}'")
    # we deliberately return the path as a string so it can be used with a spark builder directly
    yield warehouse_folder.as_posix()


@pytest.fixture
def checkpoint_folder(tmp_path_factory: pytest.TempPathFactory, random_uuid: str, logger: Logger) -> Generator[str]:
    """Fixture to create a temporary checkpoint folder that can be used with Spark streams.

    Example
    -------
    ```python
    def test_writing_to_delta_with_checkpoint(spark, checkpoint_folder):
        df = spark.range(1)
        df.writeStream.format("delta").option("checkpointLocation", checkpoint_folder).start()
    ```
    """
    fldr = tmp_path_factory.mktemp("checkpoint" + random_uuid)
    logger.debug(f"Building test checkpoint folder '{fldr}'")
    yield fldr.as_posix()


@pytest.fixture
def spark_with_delta(warehouse_path: str, random_uuid: str) -> Generator[SparkSession]:
    """Spark session fixture with Delta enabled.

    Dynamically creates a Spark session that has Delta enabled. The session is created with a unique name to avoid
    conflicts between tests. The session is automatically stopped after the test session is completed.

    The created SparkSession will either be a local session or a remote session, depending on the value of the
    environment variable `SPARK_REMOTE`. If `SPARK_REMOTE` is set to "local", the session will be a remote session.

    Default Configuration
    ---------------------
    - `spark.sql.extensions` is set to `io.delta.sql.DeltaSparkSessionExtension`
    - `spark.sql.warehouse.dir` is set to the warehouse path provided by the `warehouse_path` fixture
    - `spark.sql.catalog.spark_catalog` is set to `org.apache.spark.sql.delta.catalog.DeltaCatalog`
    - `spark.sql.session.timeZone` is set to `UTC`
    - `spark.sql.execution.arrow.pyspark.enabled` is set to `true`
    - `spark.sql.execution.arrow.pyspark.fallback.enabled` is set to `true`

    Example
    -------
    ```python
    def test_spark_with_delta(spark_with_delta):
        spark = spark_with_delta
        assert spark.version.startswith("3.")
    ```
    """
    builder = SparkSession.builder.appName("test_session" + random_uuid)

    if os.environ.get("SPARK_REMOTE") == "local":
        # SPARK_TESTING is set in environment variables
        # This triggers spark connect logic
        # ---->>>> For testing, we use 0 to use an ephemeral port to allow parallel testing.
        # ---->>>> See also SPARK-42272.
        from pyspark.version import __version__ as spark_version

        builder = configure_spark_with_delta_pip(
            spark_session_builder=builder.remote("local"),
            extra_packages=[f"org.apache.spark:spark-connect_2.12:{spark_version}"],
        )
    else:
        builder = builder.master("local[*]")
        builder = configure_spark_with_delta_pip(spark_session_builder=builder)

    builder = (
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    )

    spark_session = builder.getOrCreate()

    yield spark_session

    # stop the spark session after the test session is completed
    spark_session.stop()


@pytest.fixture
def set_env_vars() -> Generator:
    """
    Set environment variables for PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON.

    This function sets the environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the current Python
    executable. It is used to ensure that the correct Python version is used when running PySpark.
    """
    # set PYSPARK_PYTHON
    existing_pyspark_python = os.environ.get("PYSPARK_PYTHON")
    os.environ["PYSPARK_PYTHON"] = sys.executable

    # set PYSPARK_DRIVER_PYTHON
    existing_pyspark_driver_python = os.environ.get("PYSPARK_DRIVER_PYTHON")
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    yield

    # reset PYSPARK_PYTHON
    del os.environ["PYSPARK_PYTHON"]
    if existing_pyspark_python:
        os.environ["PYSPARK_PYTHON"] = existing_pyspark_python

    # reset PYSPARK_DRIVER_PYTHON
    del os.environ["PYSPARK_DRIVER_PYTHON"]
    if existing_pyspark_driver_python:
        os.environ["PYSPARK_DRIVER_PYTHON"] = existing_pyspark_driver_python


@pytest.fixture
def spark(set_env_vars: pytest.FixtureRequest, spark_with_delta: SparkSession) -> Generator[SparkSession]:
    """Ensures PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are set to the current Python executable and returns a Spark
    session.
    """
    yield spark_with_delta


@pytest.fixture
def streaming_dummy_df(spark: SparkSession, delta_file: FilePath) -> Generator[DataFrame]:
    """
    Creates a streaming DataFrame from a Delta table for testing purposes.

    This function sets up test data in a Delta table and returns a streaming DataFrame
    that reads from this table.
    """
    setup_test_data(spark=spark, delta_file=Path(delta_file))
    yield spark.readStream.table("delta_test_table")


@pytest.fixture
def mock_df(spark: SparkSession) -> Generator[mock.Mock]:
    """
    Fixture to mock a DataFrame's methods.

    Dynamically selects the right type of DataFrame to mock based on the spec of the active spark session.

    # TODO:
    #  - add examples
    #  - indicate that this is meant to be used with writer methods
    """
    # create a local DataFrame so we can get the spec of the DataFrame
    # this is required so that we create the right type of mock (i.e. `spark.sql.connect` vs `spark.sql`)
    df = spark.range(1)

    # mock the df.write method
    mock_df_write = mock.create_autospec(type(df.write))

    # mock the save method
    mock_df_write.save = mock.Mock(return_value=None)

    # mock the format, option(s), and mode methods
    mock_df_write.format.return_value = mock_df_write
    mock_df_write.options.return_value = mock_df_write
    mock_df_write.option.return_value = mock_df_write
    mock_df_write.mode.return_value = mock_df_write

    # now create a mock DataFrame with the mocked write method
    mock_df = mock.create_autospec(type(df), instance=True)
    mock_df.write = mock_df_write
    yield mock_df


@dataclass
class SparkContextData:
    """Helper class to mock the behavior of a Spark session's `read` object.

    This class is used to mock the behavior of a Spark session's `read` object. It allows the user to set the output
    DataFrame that will be returned by the `load` method. The class also provides methods to get the options that were
    passed to the `load` method.

    See `mock_spark_reader` for more details.

    Methods used for mocking
    ------------------------
    mock_options(*args, **kwargs) -> SparkSession.read:
        Mock method for the `options` and `option` methods of a Spark session's `read` object.
    
    get_options -> dict[str, Any]:
        Returns the options that were passed.
    
    load() -> DataFrame:
        Mock method for the `load` method of a Spark session's `read` object.    
    
    Methods for testing
    -------------------
    set_output_df(df: DataFrame) -> None:
        Allows the user to set the output DataFrame that will be returned by the `load` method.
    
    set_df(df: DataFrame) -> None:
        Alias for `set_output_df`.
    
    assert_option_called_with(key: str, value: Any) -> None:
        Asserts that a specific option was called with the expected value.
    """

    spark: SparkSession
    _options_dict: dict[str, Any] = field(default_factory=dict)
    _output_df: Optional[DataFrame] = None

    def mock_options(self, *args, **kwargs) -> SparkSession.read:  # type: ignore
        """Mock method for the `options` and `option` methods of a Spark session's `read` object."""
        self._options_dict.update(kwargs)
        return self.spark.read

    def set_output_df(self, df: DataFrame) -> None:
        """Allows the user to set the output DataFrame that will be returned by the `load` method."""
        self._output_df = df  # type: ignore

    set_df = set_output_df
    """Alias for `set_output_df`"""

    @property
    def get_options(self) -> dict[str, Any]:
        """Returns the options that were passed"""
        return self._options_dict

    def load(self) -> DataFrame:
        """Mock method for the `load` method of a Spark session's `read` object."""
        return self._output_df

    def assert_option_called_with(self, key: str, value: Any) -> None:
        """Asserts that a specific option was called with the expected value."""
        assert (
            self._options_dict.get(key) is not None
        ), f"No option with name {key} was called. Actual options: {self._options_dict}"
        assert (
            self._options_dict.get(key) == value
        ), f"Expected {key} to be {value}, but got {self._options_dict.get(key)}"


@pytest.fixture
def mock_spark_reader(
    spark: SparkSession, sample_df_with_strings: DataFrame, mocker: FixtureValue
) -> Generator[SparkContextData]:
    """SparkSession fixture that makes any call to `SparkSession.read.load()` return a custom DataFrame.

    Note: This fixture is meant to be used with read operations of a Spark session.

    Because of the use of `type(spark.read)`, this fixture automatically alters its behavior for either a remote or
    regular Spark session.

    By default, `mock_spark_reader` will set the output data to `sample_df_with_strings`. You can use the
    `set_output_df` or `set_df` methods to overwrite this with any DataFrame of your choosing.

    The fixture also provides a method `assert_option_called_with` to check that specific options were called.

    Example
    -------
    ### Example of setting up `mock_spark_reader` with a custom DataFrame
    ```python
    def test_with_mock_spark_reader(mock_spark_reader):
        my_custom_df = spark.range(10)

        # set the output DataFrame to `my_custom_df`
        mock_spark_reader.set_output_df(my_custom_df)
        
        # now any call to `spark.read...load()` will return `my_custom_df`
        df = spark.read.format("csv").load()
        
        # asserting that the output DataFrame is `my_custom_df` will pass
        assert df.count() == my_custom_df.count()
    ```

    ### Checking that specific options were called
    ```python
    def test_spark_called_with_right_options(mock_spark_reader):
        spark.read.format("csv").option("foo", "bar")
        mock_spark_reader.assert_option_called_with("foo", "bar")
    ```

    Parameters
    ----------
    spark : SparkSession
        The Spark session to use.
    sample_df_with_strings : DataFrame
        Default DataFrame if not specifically being overwritten
    mocker : MockerFixture

    Returns
    -------
    SparkContextData
        An instance of SparkContextData containing the SparkReader and methods to set the output DataFrame and get
        options.
    """
    context_data = SparkContextData(spark)
    context_data.set_output_df(sample_df_with_strings)

    spark_reader = type(spark.read)

    mocker.patch.object(spark_reader, "options", side_effect=context_data.mock_options)
    mocker.patch.object(spark_reader, "load", side_effect=context_data.load)

    # TODO: 
    #   - [ ] check what happens when the formatting methods (like `.csv` or `.json`) are called
    #   - [ ] check what happens if the `format` method is called
    #   - [ ] see if there are any other methods that need to be mocked or tracked
    #   - [ ] add neccessary tests
    #   - [ ] add more examples
    #   - [ ] add more documentation

    yield context_data
