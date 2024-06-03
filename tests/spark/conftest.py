import datetime
import os
import sys
from decimal import Decimal
from pathlib import Path
from textwrap import dedent
from unittest.mock import Mock

import pytest
from delta import configure_spark_with_delta_pip

from pyspark.sql import DataFrame, SparkSession
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
    builder = (
        SparkSession.builder.appName("test_session" + random_uuid)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    )

    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
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
    | paritition | Value
    |----|----|
    | BE | 12 |
    | FR | 20 |
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
    | id | a_date              | a_timestamp
    |----|---------------------|---------------------
    | 1  | 1970-04-20 12:33:09 |
    | 2  | 1980-05-21 13:34:08 |
    | 3  | 1990-06-22 14:35:07 |

    Schema:
    - id: bigint (nullable = true)
    - date: timestamp (nullable = true)
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


@pytest.fixture(scope="class")
def dummy_spark():
    class DummySpark:
        """Mocking SparkSession"""

        def __init__(self):
            self.options_dict = {}

        def mock_method(self, *args, **kwargs):
            return self

        @property
        def mock_property(self):
            return self

        def mock_options(self, *args, **kwargs):
            self.options_dict = kwargs
            return self

        options = mock_options
        format = mock_method
        read = mock_property

        _jvm = Mock()
        _jvm.net.snowflake.spark.snowflake.Utils.runQuery.return_value = True

        @staticmethod
        def load() -> DataFrame:
            df = Mock(spec=DataFrame)
            df.count.return_value = 1
            df.schema = StructType([StructField("foo", StringType(), True)])
            return df

    return DummySpark()


def await_job_completion(timeout=300, query_id=None):
    """
    Waits for a Spark streaming job to complete.

    This function checks the status of a Spark streaming job and waits until it is completed or a timeout is reached.
    If a query_id is provided, it waits for the specific streaming job with that id. Otherwise, it waits for any active
    streaming job.
    """
    logger = LoggingFactory.get_logger(name="await_job_completion", inherit_from_koheesio=True)

    start_time = datetime.datetime.now()
    spark = SparkSession.getActiveSession()
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
