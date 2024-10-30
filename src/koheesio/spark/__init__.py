"""
Spark step module
"""

from __future__ import annotations

from typing import Optional
from abc import ABC
import warnings

from pydantic import Field

from koheesio import Step, StepOutput
from koheesio.models import model_validator
from koheesio.spark.utils.common import (
    AnalysisException,
    Column,
    DataFrame,
    DataFrameReader,
    DataFrameWriter,
    DataStreamReader,
    DataStreamWriter,
    DataType,
    ParseException,
    SparkSession,
    StreamingQuery,
)

__all__ = [
    "SparkStep",
    "Column",
    "DataFrame",
    "ParseException",
    "SparkSession",
    "AnalysisException",
    "DataType",
    "DataFrameReader",
    "DataStreamReader",
    "DataFrameWriter",
    "DataStreamWriter",
    "StreamingQuery",
]


class SparkStep(Step, ABC):
    """Base class for a Spark step

    Extends the Step class with SparkSession support. The following:
    - Spark steps are expected to return a Spark DataFrame as output.
    - spark property is available to access the active SparkSession instance.
    - The SparkSession instance can be provided as an argument to the constructor through the `spark` parameter.
    """

    spark: Optional[SparkSession] = Field(
        default=None,
        description="The SparkSession instance. If not provided, the active SparkSession will be used.",
        validate_default=False,
    )

    class Output(StepOutput):
        """Output class for SparkStep"""

        df: Optional[DataFrame] = Field(default=None, description="The Spark DataFrame")

    @model_validator(mode="after")
    def _get_active_spark_session(self) -> SparkStep:
        """Return active SparkSession instance
        If a user provides a SparkSession instance, it will be returned. Otherwise, an active SparkSession will be
        attempted to be retrieved.
        """
        if self.spark is None:
            from koheesio.spark.utils.common import get_active_session

            self.spark = get_active_session()
        return self


def current_timestamp_utc(spark):
    warnings.warn(
        message=(
            "The current_timestamp_utc function has been moved to the koheesio.spark.functions module."
            "Import it from there instead. Current import path will be deprecated in the future."
        ),
        category=DeprecationWarning,
        stacklevel=2,
    )
    from koheesio.spark.functions import current_timestamp_utc as _current_timestamp_utc

    return _current_timestamp_utc(spark)
