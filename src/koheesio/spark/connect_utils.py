import inspect
from typing import Optional, TypeAlias, Union

from pyspark import sql
from pyspark.errors import exceptions

from koheesio.spark.utils import check_if_pyspark_connect_is_supported


def get_active_session() -> Optional[Union["sql.SparkSession", "sql.connect.session.SparkSession"]]:  # type: ignore
    if check_if_pyspark_connect_is_supported():
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        session = (
            ConnectSparkSession.getActiveSession() or sql.SparkSession.getActiveSession()  # type: ignore
        )
    else:
        session = sql.SparkSession.getActiveSession()

    return session


def is_remote_session() -> bool:
    result = False

    if get_active_session() and check_if_pyspark_connect_is_supported():
        result = True if get_active_session().conf.get("spark.remote", None) else False  # type: ignore

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


DataFrame: TypeAlias = _get_data_frame_class() if check_if_pyspark_connect_is_supported else sql.DataFrame  # type: ignore # noqa: F811
Column: TypeAlias = _get_column_class() if check_if_pyspark_connect_is_supported else sql.Column  # type: ignore # noqa: F811
SparkSession: TypeAlias = _get_spark_session_class() if check_if_pyspark_connect_is_supported else sql.SparkSession  # type: ignore # noqa: F811
ParseException: TypeAlias = (
    _get_parse_exception_class() if check_if_pyspark_connect_is_supported else exceptions.captured.ParseException  # type: ignore
)  # type: ignore  # noqa: F811


def get_column_name(col: Column) -> str:
    """Get the column name from a Column object

    Normally, the name of a Column object is not directly accessible in the regular pyspark API. This function
    extracts the name of the given column object without needing to provide it in the context of a DataFrame.

    Parameters
    ----------
    col: Column
        The Column object

    Returns
    -------
    str
        The name of the given column
    """
    # we have to distinguish between the Column object from column from local session and remote
    if hasattr(col, "_jc"):
        # In case of a 'regular' Column object, we can directly access the name attribute through the _jc attribute
        name = col._jc.toString()
    elif any(cls.__module__ == "pyspark.sql.connect.column" for cls in inspect.getmro(col.__class__)):
        name = col._expr.name()
    else:
        raise ValueError("Column object is not a valid Column object")

    return name


__all__ = [
    "DataFrame",
    "Column",
    "SparkSession",
    "ParseException",
    "get_column_name",
    "get_active_session",
    "is_remote_session",
]
