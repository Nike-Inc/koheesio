"""
Spark step module
"""

from __future__ import annotations

from abc import ABC
from typing import Optional

from pydantic import Field

from koheesio import Step, StepOutput
from koheesio.models import model_validator
from koheesio.spark.utils.common import (
    AnalysisException,
    Column,
    DataFrame,
    DataStreamReader,
    DataType,
    ParseException,
    SparkSession,
)

__all__ = [
    "SparkStep",
    "Column",
    "DataFrame",
    "ParseException",
    "SparkSession",
    "AnalysisException",
    "DataType",
    "DataStreamReader",
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
