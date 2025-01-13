"""
Fixtures defined in this module:

- warehouse_path: Creates a temporary directory for the Spark warehouse.
- checkpoint_folder: Creates a temporary directory for Spark checkpoints.
- spark: Provides a Spark session with Delta enabled.

- set_env_vars: Sets environment variables for PySpark.
- setup: Sets up the test database and data.
- dummy_df: Provides a dummy DataFrame.
- sample_df_to_partition: Provides a sample DataFrame for partitioning tests.
- streaming_dummy_df: Provides a streaming DataFrame from a Delta table.
- sample_df_with_strings: Provides a sample DataFrame with string values.
- sample_df_with_timestamp: Provides a sample DataFrame with timestamp values.
- sample_df_with_string_timestamp: Provides a sample DataFrame with string timestamps.
- mock_spark_reader: Mocks SparkSession.read.load() to return a DataFrame with strings.
- mock_df: Mocks a DataFrame's methods.
- df_with_all_types: Provides a DataFrame with all supported Spark datatypes.
"""

from typing import Generator

from koheesio.spark import SparkSession
from koheesio.spark.testing import (
    await_job_completion,
)
from koheesio.spark.testing.fixtures import (
    checkpoint_folder,
    mock_df,
    mock_spark_reader,
    set_env_vars,
    spark,
    spark_with_delta,
    streaming_dummy_df,
    warehouse_path,
)
from koheesio.spark.testing.sample_data import (
    sample_df_to_partition,
    sample_df_with_all_types,
    sample_df_with_string_timestamp,
    sample_df_with_strings,
    sample_df_with_timestamp,
)
from koheesio.spark.testing.utils import setup_test_data
from koheesio.testing import fixture, register_fixtures
from koheesio.utils import is_port_free

__all__ = [
    "await_job_completion",
    "checkpoint_folder",
    "mock_df",
    "mock_spark_reader",
    "is_port_free",
    "mock_df",
    "sample_df_to_partition",
    "sample_df_with_all_types",
    "sample_df_with_strings",
    "sample_df_with_string_timestamp",
    "sample_df_with_timestamp",
    "set_env_vars",
    "setup",
    "setup_test_data",
    "spark",
    "streaming_dummy_df",
    "warehouse_path",
]

# Register necessary fixtures
set_env_vars = fixture(fixture_function=set_env_vars, scope="session", autouse=True)
warehouse_path, checkpoint_folder = register_fixtures(
    warehouse_path,  # Path to the Spark warehouse folder
    checkpoint_folder,  # Path to the Spark checkpoint folder
    scope="session"
)
spark_with_delta, spark, streaming_dummy_df = register_fixtures(
    spark_with_delta,  # Active SparkSession with Delta enabled
    spark,  # Active SparkSession to be used within the tests
    streaming_dummy_df,  # Streaming DataFrame from a Delta table
    scope="session"
)
mock_df, mock_spark_reader = register_fixtures(
    mock_df,  # Mocked DataFrame
    mock_spark_reader,  # Mocked SparkSession.read
    scope="class"
)
sample_df_to_partition, sample_df_with_all_types, sample_df_with_string_timestamp, sample_df_with_strings, sample_df_with_timestamp = register_fixtures(
    sample_df_to_partition,  # Sample DataFrame for partitioning tests
    sample_df_with_all_types,  # Sample DataFrame with all supported Spark datatypes
    sample_df_with_string_timestamp,  # Sample DataFrame with string timestamps
    sample_df_with_strings,  # Sample DataFrame with string values
    sample_df_with_timestamp,  # Sample DataFrame with timestamp values
    scope="function"
)


print("@@@@@@ This is the conftest.py file in the tests/spark directory.")

# @fixture(scope="session", autouse=True)
# def setup(spark: SparkSession, delta_file: str) -> Generator:
#     """
#     Setup our test environment with a database named 'klettern' and test data.

#     Fixtures used:
#     - spark: Active SparkSession to be used within the tests
#     - delta_file: Path to the Delta file containing the test data
#     """
#     db_name = "klettern"

#     if not spark.catalog.databaseExists(db_name):
#         spark.sql(f"CREATE DATABASE {db_name}")
#         spark.sql(f"USE {db_name}")

#     # TODO: change setup_test_data so that we don't have to pass a Path object (str_or_path)
#     setup_test_data(spark=spark, delta_file=delta_file)
#     yield
