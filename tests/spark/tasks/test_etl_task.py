import pytest

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from koheesio.logger import LoggingFactory
from koheesio.spark.delta import DeltaTableStep
from koheesio.spark.etl_task import EtlTask
from koheesio.spark.readers.delta import DeltaTableReader, DeltaTableStreamReader
from koheesio.spark.readers.dummy import DummyReader
from koheesio.spark.transformations.sql_transform import SqlTransform
from koheesio.spark.transformations.transform import Transform
from koheesio.spark.utils import SPARK_MINOR_VERSION
from koheesio.spark.writers.delta import DeltaTableStreamWriter, DeltaTableWriter
from koheesio.spark.writers.dummy import DummyWriter

pytestmark = pytest.mark.spark


def dummy_function(df: DataFrame):
    return df.withColumn("hello", lit("world"))


def dummy_function2(df: DataFrame, name: str):
    return df.withColumn("name", lit(name))


def test_dummy_task(spark):
    my_etl_logger = LoggingFactory.get_logger(name="dummy_etl_task")
    my_etl_logger.setLevel("DEBUG")
    dummy_task = EtlTask(
        source=DummyReader(range=15),
        target=DummyWriter(vertical=False),
        transformations=[Transform(dummy_function), Transform(dummy_function2, name="pari")],
    )
    my_etl_logger.debug("Testing DEBUG log level")
    results = dummy_task.execute().target_df
    actual = results.head().asDict()
    expected = {"hello": "world", "id": 0, "name": "pari"}
    my_etl_logger.info("Testing INFO log level")

    assert actual == expected
    assert results.count() == 15
    assert dummy_task.etl_date is not None


def test_delta_task(spark):
    delta_table = DeltaTableStep(table="delta_table")
    DummyReader(range=5).read().write.format("delta").mode("append").saveAsTable("delta_table")

    delta_task = EtlTask(
        source=DeltaTableReader(table=delta_table),
        target=DeltaTableWriter(table="delta_table_out"),
        transformations=[
            SqlTransform(
                sql="SELECT ${field} FROM ${table_name} WHERE id = 0",
                table_name="delta_table_out",
                params={"field": "id"},
            ),
            Transform(dummy_function2, name="pari"),
        ],
    )
    delta_task.run()

    results = spark.table("delta_table_out").orderBy(col("id")).limit(1)

    actual = results.head().asDict()
    expected = {"id": 0, "name": "pari"}
    assert actual == expected


def test_delta_stream_task(spark, checkpoint_folder):
    from koheesio.spark.utils.connect import is_remote_session

    delta_table = DeltaTableStep(table="delta_stream_table")
    DummyReader(range=5).read().write.format("delta").mode("append").saveAsTable("delta_stream_table")
    writer = DeltaTableStreamWriter(table="delta_stream_table_out", checkpoint_location=checkpoint_folder)
    transformations = [
        SqlTransform(
            sql="SELECT ${field} FROM ${table_name} WHERE id = 0",
            table_name="temp_view",
            field="id",
        ),
        Transform(dummy_function2, name="pari"),
    ]

    delta_task = EtlTask(
        source=DeltaTableStreamReader(table=delta_table),
        target=writer,
        transformations=transformations,
    )

    if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
        with pytest.raises(RuntimeError) as excinfo:
            delta_task.run()

        assert "https://issues.apache.org/jira/browse/SPARK-45957" in str(excinfo.value.args[0])
    else:
        delta_task.run()
        writer.streaming_query.awaitTermination(timeout=20)  # type: ignore

        out_df = spark.table("delta_stream_table_out")
        actual = out_df.head().asDict()
        expected = {"id": 0, "name": "pari"}
        assert actual == expected


def test_transformations_alias(spark: SparkSession) -> None:
    my_etl_logger = LoggingFactory.get_logger(name="transformations_alias")
    my_etl_logger.setLevel("DEBUG")
    alias_task = EtlTask(
        source=DummyReader(range=15),
        target=DummyWriter(vertical=False),
        transforms=[Transform(dummy_function), Transform(dummy_function2, name="mias")],
    )
    my_etl_logger.debug("Testing DEBUG log level")
    results = alias_task.execute().target_df
    actual = results.head().asDict()
    expected = {"hello": "world", "id": 0, "name": "mias"}
    my_etl_logger.info("Testing INFO log level")

    assert actual == expected
    assert results.count() == 15
    assert alias_task.etl_date is not None
