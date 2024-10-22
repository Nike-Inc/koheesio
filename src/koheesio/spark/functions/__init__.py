from pyspark.sql import functions as F

from koheesio.spark import Column, SparkSession


def current_timestamp_utc(spark: SparkSession) -> Column:
    """Get the current timestamp in UTC"""
    tz_session = spark.conf.get("spark.sql.session.timeZone", "UTC")
    tz = tz_session if tz_session else "UTC"

    return F.to_utc_timestamp(F.current_timestamp(), tz)
