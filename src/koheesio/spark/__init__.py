"""
Spark step module
"""

from __future__ import annotations

import importlib.metadata
import importlib.util
from abc import ABC
from typing import Optional, TypeAlias, Union

import pyspark
from packaging import version
from pydantic import Field
from pyspark.sql import Column as SQLColumn
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession as LocalSparkSession
from pyspark.sql import functions as F

from koheesio import Step, StepOutput


def check_if_pyspark_connect_is_supported():
    result = False
    module_name: str = "pyspark"
    if version.parse(importlib.metadata.version(module_name)) >= version.parse("3.5"):
        try:
            importlib.import_module(f"{module_name}.sql.connect")
            result = True
        except ModuleNotFoundError:
            result = False
    return result


if check_if_pyspark_connect_is_supported():
    from pyspark.sql.connect.column import Column as RemoteColumn
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession

    DataFrame: TypeAlias = Union[SparkDataFrame, ConnectDataFrame]  # type: ignore
    Column: TypeAlias = Union[SQLColumn, RemoteColumn]  # type: ignore
    SparkSession: TypeAlias = Union[LocalSparkSession, RemoteSparkSession]  # type: ignore
else:
    DataFrame: TypeAlias = SparkDataFrame  # type: ignore
    Column: TypeAlias = SQLColumn  # type: ignore
    SparkSession: TypeAlias = LocalSparkSession  # type: ignore


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

    @property
    def is_remote_spark_session(self) -> bool:
        """Check if the current SparkSession is a remote session"""
        return check_if_pyspark_connect_is_supported() and self.spark.conf.get("spark.remote")


# TODO: Move to spark/functions/__init__.py after reorganizing the code
def current_timestamp_utc(spark: SparkSession) -> Column:
    """Get the current timestamp in UTC"""
    return F.to_utc_timestamp(F.current_timestamp(), spark.conf.get("spark.sql.session.timeZone"))
