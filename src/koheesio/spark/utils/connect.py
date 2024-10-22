from typing import Optional

from koheesio.spark.utils import check_if_pyspark_connect_is_supported
from koheesio.spark.utils.common import SparkSession, get_active_session

__all__ = ["is_remote_session"]


def is_remote_session(spark: Optional[SparkSession] = None) -> bool:
    result = False

    if (_spark := spark or get_active_session()) and check_if_pyspark_connect_is_supported():
        result = True if _spark.conf.get("spark.remote", None) else False  # type: ignore

    return result
