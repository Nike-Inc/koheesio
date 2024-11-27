from typing import Any
from collections import namedtuple
import datetime
from decimal import Decimal
import os
from pathlib import Path
import socket
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
from koheesio.spark.readers.dummy import DummyReader


def is_port_free(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("localhost", port))
            return True
        except socket.error:
            return False


@pytest.fixture(scope="session")
def warehouse_path(tmp_path_factory, random_uuid, logger):
    fldr = tmp_path_factory.mktemp("spark-warehouse" + random_uuid)
    logger.debug(f"Building test warehouse folder '{fldr}'")
    yield fldr.as_posix()


@pytest.fixture(scope="function")
def checkpoint_folder(tmp_path_factory, random_uuid, logger):
    fldr = tmp_path_factory.mktemp("checkpoint" + random_uuid)
    logger.debug(f"Building test checkpoint folder '{fldr}'")
    yield fldr.as_posix()


@pytest.fixture(scope="session")
def spark(warehouse_path, random_uuid):
    """Spark session fixture with Delta enabled."""
    builder = SparkSession.builder.appName("test_session" + random_uuid)

    if os.environ.get("SPARK_REMOTE") == "local":
        # SPARK_TESTING is set in environment variables
        # This triggers spark connect logic
        # ---->>>> For testing, we use 0 to use an ephemeral port to allow parallel testing.
        # --->>>>>> See also SPARK-42272.
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

    spark_session.stop()


@pytest.fixture(autouse=True, scope="session")
def set_env_vars():
    """
    Set environment variables for PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON.

    This function sets the environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON
    to the current Python executable. It is used to ensure that the correct Python version
    is used when running PySpark.
    """
    existing_pyspark_python = os.environ.get("PYSPARK_PYTHON")
    existing_pyspark_driver_python = os.environ.get("PYSPARK_DRIVER_PYTHON")

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    yield

    if existing_pyspark_python is not None:
        os.environ["PYSPARK_PYTHON"] = existing_pyspark_python
    else:
        del os.environ["PYSPARK_PYTHON"]

    if existing_pyspark_driver_python is not None:
        os.environ["PYSPARK_DRIVER_PYTHON"] = existing_pyspark_driver_python
    else:
        del os.environ["PYSPARK_DRIVER_PYTHON"]


@pytest.fixture(scope="session", autouse=True)
def setup(spark, delta_file):
    db_name = "klettern"

    if not spark.catalog.databaseExists(db_name):
        spark.sql(f"CREATE DATABASE {db_name}")
        spark.sql(f"USE {db_name}")

    setup_test_data(spark=spark, delta_file=Path(delta_file))
    yield


@pytest.fixture(scope="class")
def dummy_df(spark):
    """
    | id |
    |----|
    | 1  |
    """
    yield DummyReader(range=1).read()


@pytest.fixture(scope="class")
def sample_df_to_partition(spark):
    """
    | partition | Value |
    |-----------|-------|
    | BE        | 12    |
    | FR        | 20    |
    """
    data = [["BE", 12], ["FR", 20]]
    schema = ["partition", "value"]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def streaming_dummy_df(spark, delta_file):
    setup_test_data(spark=spark, delta_file=Path(delta_file))
    yield spark.readStream.table("delta_test_table")


@pytest.fixture(scope="class")
def sample_df_with_strings(spark):
    """
    df:
    | id | string |
    |----|--------|
    | 1  | hello  |
    | 2  | world  |
    | 3  |        |
    """
    data = [[1, "hello"], [2, "world"], [3, ""]]
    schema = ["id", "string"]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="class")
def sample_df_with_timestamp(spark):
    """
    df:
    | id | a_date              | a_timestamp         |
    |----|---------------------|---------------------|
    | 1  | 1970-04-20 12:33:09 | 2000-07-01 01:01:00 |
    | 2  | 1980-05-21 13:34:08 | 2010-08-02 02:02:00 |
    | 3  | 1990-06-22 14:35:07 | 2020-09-03 03:03:00 |

    Schema:
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
    | id | a_string_timestamp |
    |----|--------------------|
    | 1  | 202301010420       |
    | 2  | 202302020314       |
    """
    data = [(1, "202301010420"), (2, "202302020314")]
    schema = ["id", "a_string_timestamp"]
    return spark.createDataFrame(data, schema)


def setup_test_data(spark, delta_file):
    """
    Sets up test data for the Spark session. Reads a Delta file, creates a temporary view,
    and populates a Delta table with the view's data.
    """
    delta_file = delta_file.absolute().as_posix()
    spark.read.format("delta").load(delta_file).limit(10).createOrReplaceTempView("delta_test_view")
    spark.sql(
        dedent(
            """
            CREATE TABLE IF NOT EXISTS delta_test_table
            USING DELTA
            TBLPROPERTIES ("delta.enableChangeDataFeed" = "true")
            AS SELECT v.* FROM delta_test_view v
            """
        )
    )


SparkContextData = namedtuple("SparkContextData", ["spark", "options_dict"])
"""A named tuple containing the Spark session and the options dictionary used to create the DataFrame"""


@pytest.fixture(scope="class")
def dummy_spark(spark, sample_df_with_strings) -> SparkContextData:
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


@pytest.fixture(scope="class")
def mock_df(spark) -> mock.Mock:
    """Fixture to mock a DataFrame's methods."""
    # create a local DataFrame so we can get the spec of the DataFrame
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
def df_with_all_types(spark):
    """Create a DataFrame with all supported Spark datatypes

    This DataFrame has 1 row and 14 columns, each column containing a single value of the given datatype.
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


class ScopeSecrets:
    class SecretMeta:
        def __init__(self, key: str):
            self.key = key

    def __init__(self, secrets: dict):
        self.secrets = secrets

    def get(self, scope: str, key: str) -> Any:
        return self.secrets.get(key)

    def list(self, scope: str):
        keys = [ScopeSecrets.SecretMeta(key=key) for key in self.secrets.keys()]

        return keys
