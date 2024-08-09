"""
Spark step module
"""

from __future__ import annotations

from abc import ABC
from typing import Optional, Union

import pkg_resources
import pyspark
from pydantic import Field
from pyspark.sql import Column as SQLColumn
from pyspark.sql import DataFrame as PySparkSQLDataFrame
from pyspark.sql import SparkSession as LocalSparkSession
from pyspark.sql import functions as F

from koheesio import Step, StepOutput

if pkg_resources.get_distribution("pyspark").version > "3.5":
    from pyspark.sql.connect.column import Column as RemoteColumn
    from pyspark.sql.connect.dataframe import DataFrame as RemoteDataFrame
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession

    DataFrame = Union[PySparkSQLDataFrame, RemoteDataFrame]
    Column = Union[RemoteColumn, SQLColumn]
    SparkSession = Union[LocalSparkSession, RemoteSparkSession]
else:
    DataFrame = PySparkSQLDataFrame
    Column = SQLColumn
    SparkSession = LocalSparkSession


try:
    from pyspark.sql.utils import AnalysisException as SparkAnalysisException
except ImportError:
    from pyspark.errors.exceptions.base import AnalysisException as SparkAnalysisException


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
        return pyspark.sql.session.SparkSession.getActiveSession()


# TODO: Move to spark/functions/__init__.py after reorganizing the code
def current_timestamp_utc(spark: SparkSession) -> Column:
    """Get the current timestamp in UTC"""
    return F.to_utc_timestamp(F.current_timestamp(), spark.conf.get("spark.sql.session.timeZone"))
