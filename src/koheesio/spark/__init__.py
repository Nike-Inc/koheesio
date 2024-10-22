"""
Spark step module
"""

from __future__ import annotations

from abc import ABC
from typing import Optional

from pydantic import Field

from koheesio import Step, StepOutput
from koheesio.spark.utils.common import (
    AnalysisException,
    Column,
    DataFrame,
    DataStreamReader,
    DataType,
    ParseException,
    SparkSession,
)


class SparkStep(Step, ABC):
    """Base class for a Spark step

    Extends the Step class with SparkSession support. The following:
    - Spark steps are expected to return a Spark DataFrame as output.
    - spark property is available to access the active SparkSession instance.
    """

    class Output(StepOutput):
        """Output class for SparkStep"""

        df: Optional[DataFrame] = Field(default=None, description="The Spark DataFrame")

    @property
    def spark(self) -> Optional[SparkSession]:
        """Get active SparkSession instance"""
        from koheesio.spark.utils.connect import get_active_session

        return get_active_session()


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
