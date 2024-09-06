"""
Spark step module
"""

from __future__ import annotations

from typing import Optional, Union
from abc import ABC

from pydantic import Field

from pyspark.sql import Column
from pyspark.sql import DataFrame as _SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from pyspark.sql.utils import AnalysisException
except ImportError:
    from pyspark.errors.exceptions.base import AnalysisException

from koheesio import Step, StepOutput
from koheesio.spark.utils import get_spark_minor_version
from koheesio.logger import warn


DataFrame = _SparkDataFrame
if get_spark_minor_version() >= 3.5:
    try:
            from pyspark.sql.connect.session import DataFrame as _SparkConnectDataFrame
            DataFrame = Union[_SparkDataFrame, _SparkConnectDataFrame]
    except ImportError:
        warn(
            "Spark Connect is not available for use. If needed, please install the required package "
            "'koheesio[spark-connect]'."
        )

__all__ = ["SparkStep", "DataFrame", "current_timestamp_utc"]

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


# TODO: Move to spark/utils.py after reorganizing the code
def current_timestamp_utc(spark: SparkSession) -> Column:
    """Get the current timestamp in UTC"""
    return F.to_utc_timestamp(F.current_timestamp(), spark.conf.get("spark.sql.session.timeZone"))
