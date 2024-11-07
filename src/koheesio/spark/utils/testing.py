"""Utilities for testing Spark applications.

This module contains utilities and pytest fixtures that can be used to run tests using Spark.

The following fixtures are available:
- `warehouse_path`: A temporary warehouse folder that can be used with SparkSessions.
- `checkpoint_folder`: A temporary checkpoint folder that can be used with Spark streams.
- `spark_with_delta`: A Spark session fixture with Delta enabled.
...
"""
from typing import Union
from collections import namedtuple
import datetime
from decimal import Decimal
import os
from pathlib import Path
import sys
from textwrap import dedent
from unittest import mock

from delta import configure_spark_with_delta_pip
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from koheesio.logger import LoggingFactory
from koheesio.models import FilePath
from koheesio.utils.testing import fixture, is_port_free, logger, random_uuid, register_fixtures

__all__ = [
    "fixture",
    "warehouse_path",
    "spark_with_delta",
    "spark",
    "sample_df_with_all_types",
    'sample_df_with_string_timestamp',
    'sample_df_with_timestamp',
    'sample_df_with_strings',
    'sample_df_to_partition',
    "set_env_vars",
    "setup_test_data",
    "register_fixture",
    "register_fixtures",
    # ...
    "checkpoint_folder",
    "is_port_free",
    "dummy_df",
    "dummy_spark",
    "logger",
    "mock_df",
    "random_uuid",
    "sample_df_to_partition",
    "spark",
    "streaming_dummy_df",
]


@pytest.fixture
def warehouse_path(tmp_path_factory, random_uuid, logger):
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
            SparkSession
            .builder
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
def spark_with_delta(warehouse_path, random_uuid):
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

    spark_session.stop()
    yield spark_session





@pytest.fixture
def checkpoint_folder(tmp_path_factory, random_uuid, logger):
    # TODO
    fldr = tmp_path_factory.mktemp("checkpoint" + random_uuid)
    logger.debug(f"Building test checkpoint folder '{fldr}'")
    yield fldr.as_posix()





@pytest.fixture
def set_env_vars():
    # @pytest.fixture(autouse=True, scope="session")
    # TODO
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
def spark(set_env_vars, spark_with_delta):
    """Ensures PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are set to the current Python executable and returns a Spark
    session.
    """
    yield spark_with_delta


@pytest.fixture
def dummy_df(spark):
    """
    A 1-row DataFrame with one column: id.

    ## df:
    | id |
    |----|
    | 1  |

    ## schema:
    - id: long (nullable = false)
    """
    return spark.range(1)


@pytest.fixture
def sample_df_to_partition(spark):
    """
    DataFrame with two columns: partition and value.

    ## df:
    | partition | Value |
    |-----------|-------|
    | BE        | 12    |
    | FR        | 20    |

    ## schema:
    - partition: string (nullable = true)
    - value: long (nullable = true)
    """
    data = [["BE", 12], ["FR", 20]]
    schema = ["partition", "value"]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_df_with_strings(spark):
    """
    DataFrame with two columns: id and string.
    Includes 3 rows of data, including a null value.

    ## df:
    | id | string |
    |----|--------|
    | 1  | hello  |
    | 2  | world  |
    | 3  |        |

    ## schema:
    - id: bigint (nullable = true)
    - string: string (nullable = true)
    """
    data = [[1, "hello"], [2, "world"], [3, ""]]
    schema = ["id", "string"]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_df_with_timestamp(spark):
    """
    DataFrame with two columns: a_date and a_timestamp.

    ## df:
    | id | a_date              | a_timestamp         |
    |----|---------------------|---------------------|
    | 1  | 1970-04-20 12:33:09 | 2000-07-01 01:01:00 |
    | 2  | 1980-05-21 13:34:08 | 2010-08-02 02:02:00 |
    | 3  | 1990-06-22 14:35:07 | 2020-09-03 03:03:00 |

    ## schema:
    - id: bigint (nullable = true)
    - a_date: timestamp (nullable = true)
    - a_timestamp: timestamp (nullable = true)
    """
    data = [
        (1, datetime.datetime(1970, 4, 20, 12, 33, 9), datetime.datetime(2000, 7, 1, 1, 1)),
        (2, datetime.datetime(1980, 5, 21, 13, 34, 8), datetime.datetime(2010, 8, 2, 2, 2)),
        (3, datetime.datetime(1990, 6, 22, 14, 35, 7), datetime.datetime(2020, 9, 3, 3, 3)),
    ]
    schema = ["id", "a_date", "a_timestamp"]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="class")
def sample_df_with_string_timestamp(spark):
    """
    DataFrame with two columns: id and a_string_timestamp.
    The timestamps in this data are stored as strings.

    ## df:
    | id | a_string_timestamp |
    |----|--------------------|
    | 1  | 202301010420       |
    | 2  | 202302020314       |

    ## schema:
    - id: bigint (nullable = true)
    - a_string_timestamp: string (nullable = true)
    """
    data = [(1, "202301010420"), (2, "202302020314")]
    schema = ["id", "a_string_timestamp"]
    return spark.createDataFrame(data, schema)








@pytest.fixture
def streaming_dummy_df(spark, delta_file):
    # TODO
    setup_test_data(spark=spark, delta_file=Path(delta_file))
    yield spark.readStream.table("delta_test_table")










def setup_test_data(spark: SparkSession, delta_file: FilePath, view_name: str ="delta_test_view"):
    """
    Sets up test data for the Spark session.

    Reads a given Delta file, creates a temporary view, and populates a Delta table with the view's data.

    Parameters
    ----------
    spark : SparkSession
        The Spark session to use.
    delta_file : Union[str, Path]
        The path to the Delta file to read.
    view_name : str, optional
        The name of the temporary view to create, by default "delta_test_view"
    """
    delta_file = delta_file.absolute().as_posix()
    spark.read.format("delta").load(delta_file).limit(10).createOrReplaceTempView("delta_test_view")
    spark.sql(
        dedent(
            """
            CREATE TABLE IF NOT EXISTS delta_test_table
            USING DELTA
            TBLPROPERTIES ("delta.enableChangeDataFeed" = "true")
            AS SELECT v.* FROM {view_name} v
            """
        ), view_name=view_name
    )


SparkContextData = namedtuple("SparkContextData", ["spark", "options_dict"])
"""A named tuple containing the Spark session and the options dictionary used to create the DataFrame"""


@pytest.fixture(scope="class")
def dummy_spark(spark, sample_df_with_strings) -> SparkContextData:
    # TODO:
    #  - make the sample data a parameter
    #  - Update documentation
    #  - Add better examples
    #  - Take inspiration from the Snowflake RunQuery fixture
    """SparkSession fixture that makes any call to SparkSession.read.load() return a DataFrame with strings.

    Because of the use of `type(spark.read)`, this fixture automatically alters its behavior for either a remote or
    regular Spark session.

    Example
    -------
    ```python
    def test_dummy_spark(dummy_spark, sample_df_with_strings):
        df = dummy_spark.read.load()
        assert df.count() == sample_df_with_strings.count()
    ```

    Returns
    -------
    SparkContextData
        A named tuple containing the Spark session and the options dictionary used to create the DataFrame
    """
    _options_dict = {}

    def mock_options(*args, **kwargs):
        _options_dict.update(kwargs)
        return spark.read

    spark_reader = type(spark.read)
    with mock.patch.object(spark_reader, "options", side_effect=mock_options):
        with mock.patch.object(spark_reader, "load", return_value=sample_df_with_strings):
            yield SparkContextData(spark, _options_dict)


@pytest.fixture
def mock_df(spark) -> mock.Mock:
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


def await_job_completion(spark, timeout=300, query_id=None):
    """
    Waits for a Spark streaming job to complete.

    This function checks the status of a Spark streaming job and waits until it is completed or a timeout is reached.
    If a query_id is provided, it waits for the specific streaming job with that id. Otherwise, it waits for any active
    streaming job.
    """
    logger = LoggingFactory.get_logger(name="await_job_completion", inherit_from_koheesio=True)

    start_time = datetime.datetime.now()
    spark = spark.getActiveSession()
    logger.info("Waiting for streaming job to complete")
    if query_id is not None:
        stream = spark.streams.get(query_id)
        while stream.isActive and (datetime.datetime.now() - start_time).seconds < timeout:
            spark.streams.awaitAnyTermination(20)
    else:
        while len(spark.streams.active) > 0 and (datetime.datetime.now() - start_time).seconds < timeout:
            spark.streams.awaitAnyTermination(20)
    spark.streams.resetTerminated()
    logger.info("Streaming job completed")


@pytest.fixture
def sample_df_with_all_types(spark):
    """Create a DataFrame with all supported Spark datatypes

    This DataFrame has 1 row and 14 columns, each column containing a single value of the given datatype.

    ## df:
    | BYTE | SHORT | INTEGER | LONG | FLOAT | DOUBLE | DECIMAL | STRING | BINARY | BOOLEAN | TIMESTAMP           | DATE       | ARRAY | MAP        | VOID |
    |------|-------|---------|------|-------|--------|---------|--------|--------|---------|---------------------|------------|-------|------------|------|
    | 1    | 1     | 1       | 1    | 1.0   | 1.0    | 1.0     | a      | a      | true    | 2023-01-01T00:01:01 | 2023-01-01 | ["a"] | {"a": "b"} | null |

    ## schema:
    - BYTE: byte (nullable = true)
    - SHORT: short (nullable = true)
    - INTEGER: integer (nullable = true)
    - LONG: long (nullable = true)
    - FLOAT: float (nullable = true)
    - DOUBLE: double (nullable = true)
    - DECIMAL: decimal(10,0) (nullable = true)
    - STRING: string (nullable = true)
    - BINARY: binary (nullable = true)
    - BOOLEAN: boolean (nullable = true)
    - TIMESTAMP: timestamp (nullable = true)
    - DATE: date (nullable = true)
    - ARRAY: array (nullable = true)
    - MAP: map (nullable = true)
    - VOID: void (nullable = true)

    """
    data = dict(
        BYTE=(1, "byte", ByteType()),
        SHORT=(1, "short", ShortType()),
        INTEGER=(1, "integer", IntegerType()),
        LONG=(1, "long", LongType()),
        FLOAT=(1.0, "float", FloatType()),
        DOUBLE=(1.0, "double", DoubleType()),
        DECIMAL=(Decimal(1.0), "decimal", DecimalType()),
        STRING=("a", "string", StringType()),
        BINARY=(b"a", "binary", BinaryType()),
        BOOLEAN=(True, "boolean", BooleanType()),
        # '2023-01-01T00:01:01'
        TIMESTAMP=(datetime.datetime.utcfromtimestamp(1672531261), "timestamp", TimestampType()),
        DATE=(datetime.date(2023, 1, 1), "date", DateType()),
        ARRAY=(["a"], "array", ArrayType(StringType())),
        MAP=({"a": "b"}, "map", MapType(StringType(), StringType())),
        VOID=(None, "void", NullType()),
    )
    return spark.createDataFrame(
        data=[[v[0] for v in data.values()]],
        schema=StructType([StructField(name=v[1], dataType=v[2]) for v in data.values()]),
    )



