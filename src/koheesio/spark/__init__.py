"""
Spark step module
"""

from __future__ import annotations

import importlib.metadata
import importlib.util
from typing import Optional, TypeAlias, Union
from abc import ABC

from pydantic import Field

import pyspark
from pyspark.sql import Column as SQLColumn
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession as LocalSparkSession
from pyspark.sql import functions as F
from pyspark.version import __version__ as spark_version

from koheesio import Step, StepOutput


def get_spark_minor_version() -> float:
    """Returns the minor version of the spark instance.

    For example, if the spark version is 3.3.2, this function would return 3.3
    """
    return float(".".join(spark_version.split(".")[:2]))


# shorthand for the get_spark_minor_version function
SPARK_MINOR_VERSION: float = get_spark_minor_version()


def check_if_pyspark_connect_is_supported():
    result = False
    module_name: str = "pyspark"
    if SPARK_MINOR_VERSION >= 3.5:
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
    from pyspark.errors.exceptions.base import (
        AnalysisException as SparkAnalysisException,
    )


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
