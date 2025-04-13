from typing import Optional

from pyspark.sql import SparkSession

from koheesio.spark.utils.common import check_if_pyspark_connect_module_is_available, get_active_session

__all__ = ["is_remote_session"]


def is_remote_session(spark: Optional[SparkSession] = None) -> bool:
    result = False

    if (_spark := spark or get_active_session()) and check_if_pyspark_connect_module_is_available():
        # result = True if _spark.conf.get("spark.remote", None) else False  # type: ignore
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        result = isinstance(_spark, ConnectSparkSession)

    return result
