"""Utilities for testing Spark applications.

This module contains utilities and pytest fixtures that can be used to run tests using Spark.

The following fixtures are available:
- `warehouse_path`: A temporary warehouse folder that can be used with SparkSessions.
- `checkpoint_folder`: A temporary checkpoint folder that can be used with Spark streams.
- `spark_with_delta`: A Spark session fixture with Delta enabled.
- `set_env_vars`: A fixture to set environment variables for PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON.
- `spark`: A fixture that ensures PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are set to the current Python executable
    and returns a Spark session.
- `streaming_dummy_df`: A fixture that creates a streaming DataFrame from a Delta table for testing purposes.
- `mock_df`: A fixture to mock a DataFrame's methods.

The following utility functions are available:
- `setup_test_data`: Sets up test data for the Spark session.
- `await_job_completion`: Waits for a Spark streaming job to complete.
- `is_port_free`: Checks if a port is free.
- `register_fixture`: Registers a fixture with the provided scope.
- `register_fixtures`: Registers multiple fixtures with the provided scope.
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

from koheesio.logger import LoggingFactory
from koheesio.models import FilePath
from koheesio.spark import DataFrame, SparkSession
from koheesio.utils.testing import fixture, is_port_free, logger, pytest, random_uuid, register_fixtures

__all__ = [
    "fixture",
    # fixtures
    "warehouse_path",
    "checkpoint_folder",
    "spark_with_delta",
    "set_env_vars",
    "spark", 
    "streaming_dummy_df",
    "mock_df",

    # "sample_df_with_all_types",
    # "sample_df_with_string_timestamp",
    # "sample_df_with_timestamp",
    # "sample_df_with_strings",
    # "sample_df_to_partition",
    
    "dummy_df",
    "dummy_spark",
    "setup_test_data",
    "register_fixture",
    "register_fixtures",
    # ...
    "is_port_free",

    "logger",
    "random_uuid",
    "sample_df_to_partition",
]






def await_job_completion(spark: SparkSession, timeout: int = 300, query_id: Optional[str] = None) -> None:
    """
    Waits for a Spark streaming job to complete.

    This function checks the status of a Spark streaming job and waits until it is completed or a timeout is reached.
    If a query_id is provided, it waits for the specific streaming job with that id. Otherwise, it waits for any active
    streaming job.

    # TODO: Add examples
    """
    logger = LoggingFactory.get_logger(name="await_job_completion", inherit_from_koheesio=True)

    start_time = datetime.datetime.now()
    spark = spark.getActiveSession()  # type: ignore
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
