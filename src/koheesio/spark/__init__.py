"""
Spark step module
"""

from __future__ import annotations

from abc import ABC
from typing import Any, Optional, Union

from pydantic import Field
from pyspark import sql
from pyspark.sql import functions as F

try:
    from pyspark.sql.utils import AnalysisException  # type: ignore
except ImportError:
    from pyspark.errors.exceptions.base import AnalysisException

from koheesio import Step, StepOutput


class SparkStep(Step, ABC):
    """Base class for a Spark step

    Extends the Step class with SparkSession support. The following:
    - Spark steps are expected to return a Spark DataFrame as output.
    - spark property is available to access the active SparkSession instance.
    """

    class Output(StepOutput):
        """Output class for SparkStep"""

        df: Optional[Union["sql.DataFrame", Any]] = Field(  # type: ignore
            default=None, description="The Spark DataFrame"
        )

    @property
    def spark(self) -> Optional[Union["sql.SparkSession", Any]]:  # type: ignore
        """Get active SparkSession instance"""
        return sql.session.SparkSession.getActiveSession()  # type: ignore


# TODO: Move to spark/functions/__init__.py after reorganizing the code
def current_timestamp_utc(
    spark: Union["sql.SparkSession", "sql.connect.session.SparkSession"],
) -> Union["sql.Column", "sql.connect.column.Column"]:
    """Get the current timestamp in UTC"""
    return F.to_utc_timestamp(F.current_timestamp(), spark.conf.get("spark.sql.session.timeZone"))  # type: ignore
