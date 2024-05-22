"""
Spark step module
"""

from __future__ import annotations

from abc import ABC
from typing import Optional

from pydantic import Field
from pyspark.sql import DataFrame, SparkSession

from koheesio.steps.step import Step, StepOutput


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
        return SparkSession.getActiveSession()
