from datetime import datetime
from textwrap import dedent
from unittest import mock

import chispa
from conftest import await_job_completion
import pytest

import pydantic

from koheesio.integrations.snowflake import SnowflakeRunQueryPython
from koheesio.integrations.spark.snowflake import (
    SnowflakeWriter,
    SynchronizeDeltaToSnowflakeTask,
)
from koheesio.spark import DataFrame
from koheesio.spark.delta import DeltaTableStep
from koheesio.spark.readers.delta import DeltaTableReader
from koheesio.spark.writers import BatchOutputMode, StreamingOutputMode
from koheesio.spark.writers.delta import DeltaTableWriter
from koheesio.spark.writers.stream import ForEachBatchStreamWriter

pytestmark = pytest.mark.spark

COMMON_OPTIONS = {
    "target_table": "foo.bar",
    "key_columns": [
        "Country",
    ],
    "url": "url",
    "user": "user",
    "password": "password",
    "database": "db",
    "schema": "schema",
    "role": "role",
    "warehouse": "warehouse",
    "persist_staging": False,
    "checkpoint_location": "some_checkpoint_location",
}


@pytest.fixture(scope="session")
def snowflake_staging_file(tmp_path_factory, random_uuid, logger):
    fldr = tmp_path_factory.mktemp("snowflake_staging.parq" + random_uuid)
    logger.debug(f"Building test checkpoint folder '{fldr}'")
    yield fldr.as_posix()


@pytest.fixture
def foreach_batch_stream_local(checkpoint_folder, snowflake_staging_file):
    def append_to_memory(df: DataFrame, batchId: int):
        df.write.mode("append").parquet(snowflake_staging_file)

    return ForEachBatchStreamWriter(
        output_mode=StreamingOutputMode.APPEND,
        batch_function=append_to_memory,
        checkpoint_location=checkpoint_folder,
    )


class TestSnowflakeSyncTask:
    @mock.patch.object(SynchronizeDeltaToSnowflakeTask, "writer")
    def test_overwrite(self, mock_writer, spark):
        source_table = DeltaTableStep(datbase="klettern", table="test_overwrite")

        df = spark.createDataFrame(
            data=[
                ("Australia", 100, 3000),
                ("USA", 10000, 20000),
                ("UK", 7000, 10000),
            ],
            schema=[
                "Country",
                "NumVaccinated",
                "AvailableDoses",
            ],
        )

        DeltaTableWriter(table=source_table, output_mode=BatchOutputMode.OVERWRITE, df=df).execute()

        task = SynchronizeDeltaToSnowflakeTask(
            streaming=False,
            synchronisation_mode=BatchOutputMode.OVERWRITE,
            **{**COMMON_OPTIONS, "source_table": source_table},
        )

        def mock_drop_table(table):
            pass

        with mock.patch.object(SynchronizeDeltaToSnowflakeTask, "drop_table") as mocked_drop_table:
            mocked_drop_table.return_value = mock_drop_table
            task.execute()
        # Ensure that this call doesn't raise an exception if called on a batch job
        task.writer.await_termination()
        chispa.assert_df_equality(task.output.target_df, df)

    @mock.patch.object(SynchronizeDeltaToSnowflakeTask, "writer")
    def test_overwrite_with_persist(self, mock_writer, spark):
        source_table = DeltaTableStep(datbase="klettern", table="test_overwrite")

        df = spark.createDataFrame(
            data=[
                ("Australia", 100, 3000),
                ("USA", 10000, 20000),
                ("UK", 7000, 10000),
            ],
            schema=[
                "Country",
                "NumVaccinated",
                "AvailableDoses",
            ],
        )

        DeltaTableWriter(table=source_table, output_mode=BatchOutputMode.OVERWRITE, df=df).execute()

        task = SynchronizeDeltaToSnowflakeTask(
            streaming=False,
            synchronisation_mode=BatchOutputMode.OVERWRITE,
            **{**COMMON_OPTIONS, "source_table": source_table, "persist_staging": True},
        )

        def mock_drop_table(table):
            pass

        task.execute()
        chispa.assert_df_equality(task.output.target_df, df)

    def test_merge(self, spark, foreach_batch_stream_local, snowflake_staging_file, mocker):
        # Arrange - Prepare Delta requirements
        mocker.patch("koheesio.integrations.spark.snowflake.SnowflakeRunQueryPython.execute")
        source_table = DeltaTableStep(database="klettern", table="test_merge")
        spark.sql(
            dedent(
                f"""
                CREATE OR REPLACE TABLE {source_table.table_name}
                (Country STRING, NumVaccinated LONG, AvailableDoses LONG)
                USING DELTA
                TBLPROPERTIES ('delta.enableChangeDataFeed' = true);
                """
            )
        )

        # Arrange - Prepare local representation of snowflake
        task = SynchronizeDeltaToSnowflakeTask(
            streaming=True,
            synchronisation_mode=BatchOutputMode.MERGE,
            **{**COMMON_OPTIONS, "source_table": source_table, "account": "sf_account"},
        )

        # Arrange - Add data to previously empty Delta table
        spark.sql(
            dedent(
                f"""
                INSERT INTO {source_table.table_name} VALUES
                ("Australia", 100, 3000),
                ("USA", 10000, 20000),
                ("UK", 7000, 10000);
                """
            )
        )

        # Act - Run code
        # Note: We are using the foreach_batch_stream_local fixture to simulate writing to a live environment
        mocker.patch.object(SynchronizeDeltaToSnowflakeTask, "writer", new=foreach_batch_stream_local)
        task.execute()
        task.writer.await_termination()

        # Assert - Validate result
        df = spark.read.parquet(snowflake_staging_file).select("Country", "NumVaccinated", "AvailableDoses")
        chispa.assert_df_equality(
            df,
            spark.sql(f"SELECT * FROM {source_table.table_name}"),
            ignore_row_order=True,
            ignore_column_order=True,
        )
        assert df.count() == 3

        # Perform update
        spark.sql(f"""INSERT INTO {source_table.table_name} VALUES ("BELGIUM", 10, 100)""")
        spark.sql(f"UPDATE {source_table.table_name} SET NumVaccinated = 20 WHERE Country = 'Belgium'")

        # Run code
        with mock.patch.object(SynchronizeDeltaToSnowflakeTask, "writer", new=foreach_batch_stream_local):
            # Test that this call doesn't raise exception after all queries were completed
            task.writer.await_termination()
            task.execute()
            await_job_completion(spark)

        # Validate result
        df = spark.read.parquet(snowflake_staging_file).select("Country", "NumVaccinated", "AvailableDoses")

        chispa.assert_df_equality(
            df,
            spark.sql(f"SELECT * FROM {source_table.table_name}"),
            ignore_row_order=True,
            ignore_column_order=True,
        )
        assert df.count() == 4

    def test_writer(self, spark):
        source_table = DeltaTableStep(datbase="klettern", table="test_overwrite")
        df = spark.createDataFrame(
            data=[
                ("Australia", 100, 3000),
                ("USA", 10000, 20000),
                ("UK", 7000, 10000),
            ],
            schema=[
                "Country",
                "NumVaccinated",
                "AvailableDoses",
            ],
        )

        DeltaTableWriter(table=source_table, output_mode=BatchOutputMode.OVERWRITE, df=df).execute()

        task = SynchronizeDeltaToSnowflakeTask(
            streaming=False,
            synchronisation_mode=BatchOutputMode.OVERWRITE,
            **{**COMMON_OPTIONS, "source_table": source_table},
        )

        assert task.writer is task.writer

    @pytest.mark.parametrize(
        "output_mode,streaming",
        [(BatchOutputMode.MERGE, True), (BatchOutputMode.APPEND, True), (BatchOutputMode.OVERWRITE, False)],
    )
    def test_schema_tracking_location(self, output_mode, streaming):
        source_table = DeltaTableStep(datbase="klettern", table="test_overwrite")

        task = SynchronizeDeltaToSnowflakeTask(
            streaming=streaming,
            synchronisation_mode=output_mode,
            schema_tracking_location="/schema/tracking/location",
            **{**COMMON_OPTIONS, "source_table": source_table},
        )

        reader = task.reader
        assert reader.schema_tracking_location == "/schema/tracking/location"


class TestMerge:
    def test_non_key_columns(self, spark):
        table = DeltaTableStep(database="klettern", table="sync_test_table")
        spark.sql(
            f"""
        CREATE OR REPLACE TABLE {table.table_name}
        (Country STRING, NumVaccinated INT, AvailableDoses INT)
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = true);
        """
        )

        df = spark.createDataFrame(
            data=[
                (
                    "Australia",
                    100,
                    3000,
                    "insert",
                    2,
                    datetime(2021, 4, 14, 20, 26, 37),
                ),
                (
                    "USA",
                    10000,
                    20000,
                    "update_preimage",
                    3,
                    datetime(2021, 4, 14, 20, 26, 39),
                ),
                (
                    "USA",
                    11000,
                    20000,
                    "update_postimage",
                    3,
                    datetime(2021, 4, 14, 20, 26, 39),
                ),
                ("UK", 7000, 10000, "delete", 4, datetime(2021, 4, 14, 20, 26, 40)),
            ],
            schema=[
                "Country",
                "NumVaccinated",
                "AvailableDoses",
                "_change_type",
                "_commit_version",
                "_commit_timestamp",
            ],
        )
        with mock.patch.object(DeltaTableReader, "read") as mocked_read:
            mocked_read.return_value = df
            task = SynchronizeDeltaToSnowflakeTask(
                streaming=False,
                synchronisation_mode=BatchOutputMode.APPEND,
                **{**COMMON_OPTIONS, "source_table": table},
            )
            assert task.non_key_columns == ["NumVaccinated", "AvailableDoses"]

    def test_changed_table(self, spark, sample_df_with_timestamp):
        # Example CDF dataframe from https://docs.databricks.com/en/_extras/notebooks/source/delta/cdf-demo.html
        df = spark.createDataFrame(
            data=[
                (
                    "Australia",
                    100,
                    3000,
                    "insert",
                    2,
                    datetime(2021, 4, 14, 20, 26, 37),
                ),
                (
                    "USA",
                    10000,
                    20000,
                    "update_preimage",
                    3,
                    datetime(2021, 4, 14, 20, 26, 39),
                ),
                (
                    "USA",
                    11000,
                    20000,
                    "update_postimage",
                    3,
                    datetime(2021, 4, 14, 20, 26, 39),
                ),
                ("UK", 7000, 10000, "delete", 4, datetime(2021, 4, 14, 20, 26, 40)),
            ],
            schema=[
                "Country",
                "NumVaccinated",
                "AvailableDoses",
                "_change_type",
                "_commit_version",
                "_commit_timestamp",
            ],
        )

        expected_staging_df = spark.createDataFrame(
            data=[
                ("Australia", 100, 3000, "insert"),
                ("USA", 11000, 20000, "update_postimage"),
                ("UK", 7000, 10000, "delete"),
            ],
            schema=[
                "Country",
                "NumVaccinated",
                "AvailableDoses",
                "_change_type",
            ],
        )

        result_df = SynchronizeDeltaToSnowflakeTask._compute_latest_changes_per_pk(
            df, ["Country"], ["NumVaccinated", "AvailableDoses"]
        )

        chispa.assert_df_equality(
            result_df,
            expected_staging_df,
            ignore_row_order=True,
            ignore_column_order=True,
        )


class TestValidations:
    options = {**COMMON_OPTIONS}

    @pytest.fixture(autouse=True, scope="class")
    def set_spark(self, spark):
        self.options["source_table"] = DeltaTableStep(table="<foo>")
        yield spark

    @pytest.mark.parametrize(
        "sync_mode,streaming",
        [
            (BatchOutputMode.OVERWRITE, False),
            (BatchOutputMode.MERGE, True),
            (BatchOutputMode.APPEND, False),
            (BatchOutputMode.APPEND, True),
        ],
    )
    def test_snowflake_sync_task_allowed_options(self, sync_mode: BatchOutputMode, streaming: bool):
        task = SynchronizeDeltaToSnowflakeTask(
            streaming=streaming,
            synchronisation_mode=sync_mode,
            **self.options,
        )

        assert task.reader.streaming == streaming

    @pytest.mark.parametrize(
        "sync_mode,streaming",
        [
            (BatchOutputMode.OVERWRITE, True),
            (BatchOutputMode.MERGE, False),
        ],
    )
    def test_snowflake_sync_task_unallowed_options(self, sync_mode: BatchOutputMode, streaming: bool):
        with pytest.raises(pydantic.ValidationError):
            SynchronizeDeltaToSnowflakeTask(
                streaming=streaming,
                synchronisation_mode=sync_mode,
                **COMMON_OPTIONS,
            )

    def test_snowflake_sync_task_merge_keys(self):
        with pytest.raises(pydantic.ValidationError):
            SynchronizeDeltaToSnowflakeTask(
                streaming=True,
                synchronisation_mode=BatchOutputMode.MERGE,
                **{**COMMON_OPTIONS, "key_columns": []},
            )

    @pytest.mark.parametrize(
        "sync_mode, streaming, expected_writer_type",
        [
            (BatchOutputMode.OVERWRITE, False, SnowflakeWriter),
            (BatchOutputMode.MERGE, True, ForEachBatchStreamWriter),
            (BatchOutputMode.APPEND, False, SnowflakeWriter),
            (BatchOutputMode.APPEND, True, ForEachBatchStreamWriter),
        ],
    )
    def test_snowflake_sync_task_allowed_writers(
        self, sync_mode: BatchOutputMode, streaming: bool, expected_writer_type: type
    ):
        # Overload dynamic retrieval of source schema
        with mock.patch.object(
            SynchronizeDeltaToSnowflakeTask,
            "non_key_columns",
            new=["NumVaccinated", "AvailableDoses"],
        ):
            task = SynchronizeDeltaToSnowflakeTask(
                streaming=streaming,
                synchronisation_mode=sync_mode,
                **self.options,
            )
            assert isinstance(task.writer, expected_writer_type)

    def test_merge_cdf_enabled(self, spark):
        table = DeltaTableStep(database="klettern", table="sync_test_table")
        spark.sql(
            dedent(
                f"""
                CREATE OR REPLACE TABLE {table.table_name}
                (Country STRING, NumVaccinated INT, AvailableDoses INT)
                USING DELTA
                TBLPROPERTIES ('delta.enableChangeDataFeed' = false);
                """
            )
        )
        task = SynchronizeDeltaToSnowflakeTask(
            streaming=True,
            synchronisation_mode=BatchOutputMode.MERGE,
            **{**COMMON_OPTIONS, "source_table": table},
        )
        assert task.source_table.is_cdf_active is False

        # Fail if ChangeDataFeed is not enabled
        with pytest.raises(RuntimeError):
            task.execute()


class TestMergeQuery:
    def test_merge_query_no_delete(self):
        query = SynchronizeDeltaToSnowflakeTask._build_sf_merge_query(
            target_table="target_table",
            stage_table="tmp_table",
            pk_columns=["Country"],
            non_pk_columns=["NumVaccinated", "AvailableDoses"],
        )
        expected_query = dedent(
            """
            MERGE INTO target_table target
            USING tmp_table temp ON target.Country = temp.Country
            WHEN MATCHED AND (temp._change_type = 'update_postimage' OR temp._change_type = 'insert')
                THEN UPDATE SET NumVaccinated = temp.NumVaccinated, AvailableDoses = temp.AvailableDoses
            WHEN NOT MATCHED AND temp._change_type != 'delete'
                THEN INSERT (Country, NumVaccinated, AvailableDoses)
                VALUES (temp.Country, temp.NumVaccinated, temp.AvailableDoses)"""
        ).strip()

        assert query == expected_query

    def test_merge_query_with_delete(self):
        query = SynchronizeDeltaToSnowflakeTask._build_sf_merge_query(
            target_table="target_table",
            stage_table="tmp_table",
            pk_columns=["Country"],
            non_pk_columns=["NumVaccinated", "AvailableDoses"],
            enable_deletion=True,
        )
        expected_query = dedent(
            """
            MERGE INTO target_table target
            USING tmp_table temp ON target.Country = temp.Country
            WHEN MATCHED AND (temp._change_type = 'update_postimage' OR temp._change_type = 'insert')
                THEN UPDATE SET NumVaccinated = temp.NumVaccinated, AvailableDoses = temp.AvailableDoses
            WHEN NOT MATCHED AND temp._change_type != 'delete'
                THEN INSERT (Country, NumVaccinated, AvailableDoses)
                VALUES (temp.Country, temp.NumVaccinated, temp.AvailableDoses)
            WHEN MATCHED AND temp._change_type = 'delete' THEN DELETE"""
        ).strip()

        assert query == expected_query

    def test_default_staging_table(self):
        task = SynchronizeDeltaToSnowflakeTask(
            streaming=True,
            synchronisation_mode=BatchOutputMode.MERGE,
            **{
                **COMMON_OPTIONS,
                "source_table": DeltaTableStep(database="klettern", table="sync_test_table"),
            },
        )

        assert task.staging_table == "sync_test_table_stg"

    def test_custom_staging_table(self):
        task = SynchronizeDeltaToSnowflakeTask(
            streaming=True,
            synchronisation_mode=BatchOutputMode.MERGE,
            staging_table_name="staging_table",
            **{
                **COMMON_OPTIONS,
                "source_table": DeltaTableStep(database="klettern", table="sync_test_table"),
            },
        )

        assert task.staging_table == "staging_table"

    def test_invalid_staging_table(self):
        with pytest.raises(ValueError):
            SynchronizeDeltaToSnowflakeTask(
                streaming=True,
                synchronisation_mode=BatchOutputMode.MERGE,
                staging_table_name="import.staging_table",
                **{
                    **COMMON_OPTIONS,
                    "source_table": DeltaTableStep(database="klettern", table="sync_test_table"),
                },
            )
