"""
Spark step module
"""

from __future__ import annotations

from typing import Optional
from abc import ABC

from pydantic import Field

from pyspark.sql import Column
from pyspark.sql import DataFrame as PySparkSQLDataFrame
from pyspark.sql import SparkSession as OriginalSparkSession
from pyspark.sql import functions as F

try:
    from pyspark.sql.utils import AnalysisException as SparkAnalysisException
except ImportError:
    from pyspark.errors.exceptions.base import AnalysisException as SparkAnalysisException

from koheesio import Step, StepOutput

# TODO: Move to spark/__init__.py after reorganizing the code
# Will be used for typing checks and consistency, specifically for PySpark >=3.5
DataFrame = PySparkSQLDataFrame
SparkSession = OriginalSparkSession
AnalysisException = SparkAnalysisException


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


# TODO: Move to spark/functions/__init__.py after reorganizing the code
def current_timestamp_utc(spark: SparkSession) -> Column:
    """Get the current timestamp in UTC"""
    return F.to_utc_timestamp(F.current_timestamp(), spark.conf.get("spark.sql.session.timeZone"))
