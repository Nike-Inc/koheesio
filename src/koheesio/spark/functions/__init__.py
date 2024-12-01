from pyspark.sql import functions as f

from koheesio.spark import Column, SparkSession


def current_timestamp_utc(spark: SparkSession) -> Column:
    """Get the current timestamp in UTC"""
    tz_session = spark.conf.get("spark.sql.session.timeZone", "UTC")
    tz = tz_session if tz_session else "UTC"

    return f.to_utc_timestamp(f.current_timestamp(), tz)
