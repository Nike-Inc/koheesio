from typing import List, Optional
import datetime

from delta import DeltaTable
from delta.tables import DeltaMergeBuilder
import pytest

from pydantic import Field

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import Row

from koheesio.spark import DataFrame
from koheesio.spark.delta import DeltaTableStep
from koheesio.spark.functions import current_timestamp_utc
from koheesio.spark.utils import SPARK_MINOR_VERSION
from koheesio.spark.writers.delta.scd import SCD2DeltaTableWriter
from koheesio.spark.writers.delta.utils import SparkConnectDeltaTableException

pytestmark = pytest.mark.spark


def test_scd2_custom_logic(spark):
    from koheesio.spark.utils.connect import is_remote_session

    def _get_result(target_df: DataFrame, expr: str):
        res = (
            target_df.where(expr)
            .select(
                "id",
                "last_updated_at",
                "meta.valid_from_timestamp",
                "meta.valid_to_timestamp",
                "value",
            )
            .orderBy("id", "valid_from_timestamp")
            .collect()[0]
            .asDict()
        )

        return res

    class SpecialSCD2DeltaTableWriter(SCD2DeltaTableWriter):
        orphaned_records_close: bool = Field(default=False, description="Close orphaned records")
        orphaned_records_close_ts: Optional[Column] = Field(
            default=None, description="Close orphaned records timestamp.Default to current timestamp UTC"
        )

        @staticmethod
        def _scd2_effective_time(meta_scd2_effective_time_col: str, **_kwargs) -> Column:
            scd2_effective_time = F.expr(
                f"""CASE 
                        WHEN __meta_scd2_system_merge_action='UC' AND cross.__meta_scd2_rn=1 THEN __meta_scd2_timestamp
                        WHEN __meta_scd2_system_merge_action='I' THEN TIMESTAMP'1970-01-01'
                        ELSE COALESCE({meta_scd2_effective_time_col},__meta_scd2_timestamp)
                    END 
                """
            )

            return scd2_effective_time

        @staticmethod
        def _scd2_end_time(meta_scd2_end_time_col: str, **_kwargs) -> Column:
            scd2_end_time = F.expr(
                f"""CASE 
                    WHEN __meta_scd2_system_merge_action='UC' AND cross.__meta_scd2_rn=2 THEN __meta_scd2_timestamp
                    WHEN __meta_scd2_system_merge_action='I' THEN TIMESTAMP'2999-12-31'
                    ELSE tgt.{meta_scd2_end_time_col} END"""
            )

            return scd2_end_time

        def _prepare_merge_builder(
            self,
            delta_table: DeltaTable,
            dest_alias: str,
            staged: DataFrame,
            merge_key: str,
            columns_to_process: List[str],
            meta_scd2_effective_time_col: str,
            **_kwargs,
        ) -> DeltaMergeBuilder:
            merge_builder = super()._prepare_merge_builder(
                delta_table=delta_table,
                dest_alias=dest_alias,
                staged=staged,
                merge_key=merge_key,
                columns_to_process=columns_to_process,
                meta_scd2_effective_time_col=meta_scd2_effective_time_col,
            )

            if self.orphaned_records_close:
                if self.orphaned_records_close_ts is None:
                    self.orphaned_records_close_ts = current_timestamp_utc(spark=self.spark)

                merge_builder = merge_builder.whenNotMatchedBySourceUpdate(
                    condition=F.col(
                        f"{self.meta_scd2_struct_col_name}.{self.meta_scd2_is_current_col_name}"
                    ).eqNullSafe(F.lit(True)),
                    set={
                        f"{self.meta_scd2_struct_col_name}.{self.meta_scd2_end_time_col_name}": self.orphaned_records_close_ts,
                    },
                )  # type: ignore

            return merge_builder

    target_table = "test_deltatable_writer_scd2"

    spark.sql(
        """CREATE OR REPLACE TABLE test_deltatable_writer_scd2 (
            id int NOT NULL,
            value STRING,
            last_updated_at TIMESTAMP,
            snapshot_at TIMESTAMP,
            meta STRUCT<valid_from_timestamp: TIMESTAMP, valid_to_timestamp: TIMESTAMP, is_current: BOOLEAN>
            )
            USING delta"""
    )

    source_df = spark.createDataFrame(
        [
            {"id": 2, "value": "value-2", "last_updated_at": "2024-02-01"},
            {"id": 3, "value": None, "last_updated_at": "2024-03-01"},
            {"id": 4, "value": "value-4", "last_updated_at": "2024-04-01"},
            {"id": 5, "value": "value-5", "last_updated_at": "2024-05-01"},
        ]
    )

    source_df = source_df.withColumn("last_updated_at", F.expr("to_timestamp(last_updated_at)")).withColumn(
        "snapshot_at", F.expr("TIMESTAMP'2024-12-31'")
    )

    writer = SpecialSCD2DeltaTableWriter(
        table=DeltaTableStep(table=target_table),
        merge_key="id",
        scd2_columns=["value"],
        scd2_timestamp_col=F.col("last_updated_at"),
        meta_scd2_struct_col_name="meta",
        meta_scd2_effective_time_col_name="valid_from_timestamp",
        meta_scd2_end_time_col_name="valid_to_timestamp",
        df=source_df,
    )

    if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
        with pytest.raises(SparkConnectDeltaTableException) as exc_info:
            writer.execute()

        assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
    else:
        writer.execute()

        expected = {
            "id": 4,
            "last_updated_at": datetime.datetime(2024, 4, 1, 0, 0),
            "valid_from_timestamp": datetime.datetime(1970, 1, 1, 0, 0),
            "valid_to_timestamp": datetime.datetime(2999, 12, 31, 0, 0),
            "value": "value-4",
        }

        target_df = spark.read.table(target_table)
        result = _get_result(target_df, "id = 4")

        assert spark.table(target_table).count() == 4
        assert spark.table(target_table).where("meta.valid_to_timestamp = '2999-12-31'").count() == 4
        assert result == expected

    source_df2 = source_df.withColumn(
        "value", F.expr("CASE WHEN id = 2 THEN 'value-2-change' ELSE value END")
    ).withColumn("last_updated_at", F.expr("CASE WHEN id = 2 THEN TIMESTAMP'2024-02-02' ELSE last_updated_at END"))

    writer.df = source_df2

    if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
        with pytest.raises(SparkConnectDeltaTableException) as exc_info:
            writer.execute()

        assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
    else:
        writer.execute()

        expected_insert = {
            "id": 2,
            "last_updated_at": datetime.datetime(2024, 2, 2, 0, 0),
            "valid_from_timestamp": datetime.datetime(2024, 2, 2, 0, 0),
            "valid_to_timestamp": datetime.datetime(2999, 12, 31, 0, 0),
            "value": "value-2-change",
        }

        expected_update = {
            "id": 2,
            "last_updated_at": datetime.datetime(2024, 2, 1, 0, 0),
            "valid_from_timestamp": datetime.datetime(1970, 1, 1, 0, 0),
            "valid_to_timestamp": datetime.datetime(2024, 2, 2, 0, 0),
            "value": "value-2",
        }

        result_insert = _get_result(target_df, "id = 2 and meta.valid_to_timestamp = '2999-12-31'")
        result_update = _get_result(target_df, "id = 2 and meta.valid_from_timestamp = '1970-01-01'")

        assert spark.table(target_table).count() == 5
        assert spark.table(target_table).where("meta.valid_to_timestamp = '2999-12-31'").count() == 4
        assert result_insert == expected_insert
        assert result_update == expected_update

    source_df3 = source_df2.withColumn(
        "value", F.expr("CASE WHEN id = 3 THEN 'value-3-change' ELSE value END")
    ).withColumn("last_updated_at", F.expr("CASE WHEN id = 3 THEN TIMESTAMP'2024-03-02' ELSE last_updated_at END"))

    writer.df = source_df3

    if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
        with pytest.raises(SparkConnectDeltaTableException) as exc_info:
            writer.execute()

        assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
    else:
        writer.execute()

        expected_insert = {
            "id": 3,
            "last_updated_at": datetime.datetime(2024, 3, 2, 0, 0),
            "valid_from_timestamp": datetime.datetime(2024, 3, 2, 0, 0),
            "valid_to_timestamp": datetime.datetime(2999, 12, 31, 0, 0),
            "value": "value-3-change",
        }

        expected_update = {
            "id": 3,
            "last_updated_at": datetime.datetime(2024, 3, 1, 0, 0),
            "valid_from_timestamp": datetime.datetime(1970, 1, 1, 0, 0),
            "valid_to_timestamp": datetime.datetime(2024, 3, 2, 0, 0),
            "value": None,
        }

        result_insert = _get_result(target_df, "id = 3 and meta.valid_to_timestamp = '2999-12-31'")
        result_update = _get_result(target_df, "id = 3 and meta.valid_from_timestamp = '1970-01-01'")

        assert spark.table(target_table).count() == 6
        assert spark.table(target_table).where("meta.valid_to_timestamp = '2999-12-31'").count() == 4
        assert result_insert == expected_insert
        assert result_update == expected_update

    source_df4 = source_df3.where("id != 4")

    writer.df = source_df4
    if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
        with pytest.raises(SparkConnectDeltaTableException) as exc_info:
            writer.execute()

        assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
    else:
        writer.execute()

        assert spark.table(target_table).count() == 6
        assert spark.table(target_table).where("id = 4 and meta.valid_to_timestamp = '2999-12-31'").count() == 1

    writer.orphaned_records_close = True
    if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
        with pytest.raises(SparkConnectDeltaTableException) as exc_info:
            writer.execute()

        assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
    else:
        writer.execute()

        assert spark.table(target_table).count() == 6
        assert spark.table(target_table).where("id = 4 and meta.valid_to_timestamp = '2999-12-31'").count() == 0

    source_df5 = source_df4.where("id != 5")
    writer.orphaned_records_close_ts = F.col("snapshot_at")
    writer.df = source_df5

    if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
        with pytest.raises(SparkConnectDeltaTableException) as exc_info:
            writer.execute()

        assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
    else:
        writer.execute()

        expected = {
            "id": 5,
            "last_updated_at": datetime.datetime(2024, 5, 1, 0, 0),
            "valid_from_timestamp": datetime.datetime(1970, 1, 1, 0, 0),
            "valid_to_timestamp": datetime.datetime(2024, 12, 31, 0, 0),
            "value": "value-5",
        }
        result = _get_result(target_df, "id = 5")

        assert spark.table(target_table).count() == 6
        assert spark.table(target_table).where("id = 5 and meta.valid_to_timestamp = '2999-12-31'").count() == 0
        assert result == expected


def test_scd2_logic(spark):
    from koheesio.spark.utils.connect import is_remote_session

    changes_data = [
        [("key1", "value1", "scd1-value11", "2024-05-01"), ("key2", "value2", "scd1-value21", "2024-04-01")],
        [("key1", "value1_updated", "scd1-value12", "2024-05-02"), ("key3", "value3", "scd1-value31", "2024-05-03")],
        [
            ("key1", "value1_updated_again", "scd1-value13", "2024-05-15"),
            ("key2", "value2_updated", "scd1-value22", "2024-05-14"),
        ],
    ]

    expected_data = [
        [
            Row(
                merge_key="key1",
                value_scd2="value1",
                value_scd1="scd1-value11",
                effective_time=datetime.datetime(2024, 5, 1, 0, 0),
                end_time=None,
                is_current=True,
            ),
            Row(
                merge_key="key2",
                value_scd2="value2",
                value_scd1="scd1-value21",
                effective_time=datetime.datetime(2024, 4, 1, 0, 0),
                end_time=None,
                is_current=True,
            ),
        ],
        [
            Row(
                merge_key="key1",
                value_scd2="value1",
                value_scd1="scd1-value11",
                effective_time=datetime.datetime(2024, 5, 1, 0, 0),
                end_time=datetime.datetime(2024, 5, 2, 0, 0),
                is_current=False,
            ),
            Row(
                merge_key="key1",
                value_scd2="value1_updated",
                value_scd1="scd1-value12",
                effective_time=datetime.datetime(2024, 5, 2, 0, 0),
                end_time=None,
                is_current=True,
            ),
            Row(
                merge_key="key2",
                value_scd2="value2",
                value_scd1="scd1-value21",
                effective_time=datetime.datetime(2024, 4, 1, 0, 0),
                end_time=None,
                is_current=True,
            ),
            Row(
                merge_key="key3",
                value_scd2="value3",
                value_scd1="scd1-value31",
                effective_time=datetime.datetime(2024, 5, 3, 0, 0),
                end_time=None,
                is_current=True,
            ),
        ],
        [
            Row(
                merge_key="key1",
                value_scd2="value1",
                value_scd1="scd1-value11",
                effective_time=datetime.datetime(2024, 5, 1, 0, 0),
                end_time=datetime.datetime(2024, 5, 2, 0, 0),
                is_current=False,
            ),
            Row(
                merge_key="key1",
                value_scd2="value1_updated",
                value_scd1="scd1-value12",
                effective_time=datetime.datetime(2024, 5, 2, 0, 0),
                end_time=datetime.datetime(2024, 5, 15, 0, 0),
                is_current=False,
            ),
            Row(
                merge_key="key1",
                value_scd2="value1_updated_again",
                value_scd1="scd1-value13",
                effective_time=datetime.datetime(2024, 5, 15, 0, 0),
                end_time=None,
                is_current=True,
            ),
            Row(
                merge_key="key2",
                value_scd2="value2",
                value_scd1="scd1-value21",
                effective_time=datetime.datetime(2024, 4, 1, 0, 0),
                end_time=datetime.datetime(2024, 5, 14, 0, 0),
                is_current=False,
            ),
            Row(
                merge_key="key2",
                value_scd2="value2_updated",
                value_scd1="scd1-value22",
                effective_time=datetime.datetime(2024, 5, 14, 0, 0),
                end_time=None,
                is_current=True,
            ),
            Row(
                merge_key="key3",
                value_scd2="value3",
                value_scd1="scd1-value31",
                effective_time=datetime.datetime(2024, 5, 3, 0, 0),
                end_time=None,
                is_current=True,
            ),
        ],
    ]
    spark.sql(
        """CREATE OR REPLACE TABLE scd2_test_data_set (
                merge_key STRING NOT NULL,
                value_scd2 STRING NOT NULL,
                value_scd1 STRING NOT NULL,
                _scd2 STRUCT<effective_time: TIMESTAMP, end_time: TIMESTAMP, is_current: BOOLEAN>
                )
                USING delta"""
    )

    writer = SCD2DeltaTableWriter(
        table=DeltaTableStep(table="scd2_test_data_set"),
        scd2_timestamp_col=F.col("run_date"),
        exclude_columns=["run_date"],
        merge_key="merge_key",
        scd2_columns=["value_scd2"],
        scd1_columns=["value_scd1"],
    )

    # Act & Assert
    for changes, expected in zip(changes_data, expected_data):
        changes_df = spark.createDataFrame(changes, ["merge_key", "value_scd2", "value_scd1", "run_date"])
        changes_df = changes_df.withColumn("run_date", F.to_timestamp("run_date"))
        writer.df = changes_df
        if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
            with pytest.raises(SparkConnectDeltaTableException) as exc_info:
                writer.execute()

            assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
        else:
            writer.execute()
            res = (
                spark.sql("SELECT merge_key,value_scd2, value_scd1, _scd2.* FROM scd2_test_data_set")
                .orderBy("merge_key", "effective_time")
                .collect()
            )

            assert res == expected
