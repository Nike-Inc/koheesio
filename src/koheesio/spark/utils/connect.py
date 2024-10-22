from typing import Optional, TypeAlias

from pyspark import sql
from pyspark.errors import exceptions

from koheesio.spark.utils import check_if_pyspark_connect_is_supported
from koheesio.spark.utils.common import Column, DataFrame, ParseException, SparkSession


def get_active_session() -> SparkSession:  # type: ignore
    if check_if_pyspark_connect_is_supported():
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        session: SparkSession = (
            ConnectSparkSession.getActiveSession() or sql.SparkSession.getActiveSession()  # type: ignore
        )
    else:
        session = sql.SparkSession.getActiveSession()  # type: ignore

    if not session:
        raise RuntimeError(
            "No active Spark session found. Please create a Spark session before using module connect_utils."
            " Or perform local import of the module."
        )

    return session


def is_remote_session(spark: Optional[SparkSession] = None) -> bool:
    result = False

    if (_spark := spark or get_active_session()) and check_if_pyspark_connect_is_supported():
        result = True if _spark.conf.get("spark.remote", None) else False  # type: ignore

    return result


def _get_data_frame_class() -> TypeAlias:
    return sql.connect.dataframe.DataFrame if is_remote_session() else sql.DataFrame  # type: ignore


def _get_column_class() -> TypeAlias:
    return sql.connect.column.Column if is_remote_session() else sql.column.Column  # type: ignore


def _get_spark_session_class() -> TypeAlias:
    if check_if_pyspark_connect_is_supported():
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        return ConnectSparkSession if is_remote_session() else sql.SparkSession  # type: ignore
    else:
        return sql.SparkSession  # type: ignore


def _get_parse_exception_class() -> TypeAlias:
    return exceptions.connect.ParseException if is_remote_session() else exceptions.captured.ParseException  # type: ignore


# DataFrame: TypeAlias = _get_data_frame_class() if check_if_pyspark_connect_is_supported else sql.DataFrame  # type: ignore # noqa: F811
# Column: TypeAlias = _get_column_class() if check_if_pyspark_connect_is_supported else sql.Column  # type: ignore # noqa: F811
# SparkSession: TypeAlias = _get_spark_session_class() if check_if_pyspark_connect_is_supported else sql.SparkSession  # type: ignore # noqa: F811
# ParseException: TypeAlias = (
#     _get_parse_exception_class() if check_if_pyspark_connect_is_supported else exceptions.captured.ParseException  # type: ignore
# )  # type: ignore  # noqa: F811


__all__ = [
    "DataFrame",
    "Column",
    "SparkSession",
    "ParseException",
    "get_active_session",
    "is_remote_session",
]
