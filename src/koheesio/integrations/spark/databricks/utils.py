from __future__ import annotations

from pyspark.sql import SparkSession

from koheesio.spark.utils import on_databricks


def get_dbutils(spark_session: SparkSession) -> DBUtils:  # type: ignore  # noqa: F821
    if not on_databricks():
        raise RuntimeError("dbutils not available")

    from pyspark.dbutils import DBUtils  # pylint: disable=E0611,E0401 # type: ignore

    dbutils = DBUtils(spark_session)

    return dbutils
