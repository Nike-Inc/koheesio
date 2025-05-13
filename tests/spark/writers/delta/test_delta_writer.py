import os
from unittest.mock import MagicMock, patch

from conftest import await_job_completion
from delta.tables import DeltaMergeBuilder
import pytest

from pydantic import ValidationError

from pyspark.sql import functions as F

from koheesio.spark import AnalysisException
from koheesio.spark.delta import DeltaTableStep
from koheesio.spark.utils import SPARK_MINOR_VERSION
from koheesio.spark.writers import BatchOutputMode, StreamingOutputMode
from koheesio.spark.writers.delta import DeltaTableStreamWriter, DeltaTableWriter
from koheesio.spark.writers.delta.utils import (
    SparkConnectDeltaTableException,
    log_clauses,
)
from koheesio.spark.writers.stream import Trigger

pytestmark = pytest.mark.spark


def test_delta_table_writer(dummy_df, spark):
    table_name = "test_table"
    writer = DeltaTableWriter(table=table_name, output_mode=BatchOutputMode.APPEND, df=dummy_df)
    writer.execute()
    actual_count = spark.read.table(table_name).count()
    assert actual_count == 1
    writer.execute()
    actual_count = spark.read.table(table_name).count()
    assert actual_count == 2
    writer.output_mode = BatchOutputMode.OVERWRITE
    writer.execute()
    actual_count = spark.read.table(table_name).count()
    assert actual_count == 1
    writer.output_mode = BatchOutputMode.IGNORE
    writer.execute()
    actual_count = spark.read.table(table_name).count()
    assert actual_count == 1


def test_delta_partitioning(spark, sample_df_to_partition):
    table_name = "partition_table"
    DeltaTableWriter(
        table=table_name,
        output_mode=BatchOutputMode.OVERWRITE,
        df=sample_df_to_partition,
        partition_by=["partition"],
    ).execute()
    output_df = spark.read.table(table_name)
    assert output_df.count() == 2


def test_delta_table_merge_all(spark):
    from koheesio.spark.utils.connect import is_remote_session

    table_name = "test_merge_all_table"
    target_df = spark.createDataFrame(
        [
            {"id": 1, "value": "no_merge"},
            {"id": 2, "value": "expected_merge"},
            {"id": 5, "value": "xxxx"},
        ]
    )
    source_df = spark.createDataFrame(
        [
            {"id": 2, "value": "updated_value_long"},
            {"id": 3, "value": None},
            {"id": 4, "value": "new_value"},
            {"id": 5, "value": "x"},
        ]
    )
    expected = {
        1: "no_merge",
        2: "updated_value_long",
        # {"id": 3, "value": None},  No merge as new value is NONE
        4: "new_value",
        # No merge as old value is greater
        5: "xxxx",
    }
    DeltaTableWriter(table=table_name, output_mode=BatchOutputMode.APPEND, df=target_df).execute()
    merge_writer = DeltaTableWriter(
        table=table_name,
        output_mode=BatchOutputMode.MERGEALL,
        output_mode_params={
            "merge_cond": "target.id=source.id",
            "update_cond": F.expr("length(source.value)>length(target.value)"),
            "insert_cond": F.expr("source.value IS NOT NULL"),
        },
        df=source_df,
    )

    if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
        with pytest.raises(SparkConnectDeltaTableException) as exc_info:
            merge_writer.execute()

        assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
    else:
        merge_writer.execute()
        result = {
            list(row.asDict().values())[0]: list(row.asDict().values())[1]
            for row in spark.read.table(table_name).collect()
        }
        assert result == expected


def test_deltatablewriter_with_invalid_conditions(spark, dummy_df):
    from koheesio.spark.utils.connect import is_remote_session
    from koheesio.spark.writers.delta.utils import get_delta_table_for_name

    table_name = "delta_test_table"

    if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
        with pytest.raises(SparkConnectDeltaTableException) as exc_info:
            builder = get_delta_table_for_name(spark_session=spark, table_name=table_name)

        assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
    else:
        with pytest.raises(AnalysisException):
            builder = get_delta_table_for_name(spark_session=spark, table_name=table_name)
            merge_builder = builder.alias("target").merge(
                condition="invalid_condition", source=dummy_df.alias("source")
            )
            writer = DeltaTableWriter(
                table=table_name,
                output_mode=BatchOutputMode.MERGE,
                output_mode_params={"merge_builder": merge_builder},
                df=dummy_df,
            )
            writer.execute()


@patch.dict(
    os.environ,
    {
        "DATABRICKS_RUNTIME_VERSION": "",
    },
)
def test_delta_new_table_merge(spark):
    table_name = "test_merge_new_table"
    source_df = spark.createDataFrame(
        [
            {"id": 1, "value": "new_value"},
            {"id": 2, "value": "new_value"},
            {
                "id": 3,
                "value": None,
            },  # Will be not saved, because source.value IS NOT NULL,
            {"id": 4, "value": "new_value"},
            {"id": 5, "value": "new_value"},
        ]
    )
    expected = {1: "new_value", 2: "new_value", 4: "new_value", 5: "new_value"}
    DeltaTableWriter(
        table=DeltaTableStep(table=table_name, create_if_not_exists=True),
        output_mode=BatchOutputMode.MERGEALL,
        output_mode_params={
            "merge_cond": "target.id=source.id",
            "update_cond": "source.value>target.value",
            "insert_cond": "source.value IS NOT NULL",
        },
        df=source_df,
    ).execute()
    result = {
        list(row.asDict().values())[0]: list(row.asDict().values())[1] for row in spark.read.table(table_name).collect()
    }
    assert result == expected


@pytest.mark.parametrize(
    "class_def,bad_output_mode,streaming",
    [
        (DeltaTableWriter, "bad-value", False),
        (DeltaTableStreamWriter, "overwrite", True),
    ],
)
def test_invalid_output_mode(class_def, bad_output_mode, streaming, checkpoint_folder):
    """Passing an invalid output_mode should raise an AttributeError"""
    options = {
        "table": "table",
        "output_mode": bad_output_mode,
    }
    if streaming:
        options["checkpoint_location"] = checkpoint_folder

    with pytest.raises(ValidationError):
        class_def(**options)


@pytest.mark.parametrize(
    "constructor, output_mode, streaming, checkpoint_folder",
    [
        (DeltaTableWriter, "overwrite", False, "dummy_path"),
        (DeltaTableWriter, "append", False, "dummy_path"),
        (DeltaTableStreamWriter, "complete", True, "dummy_path"),
        (DeltaTableStreamWriter, "append", True, "dummy_path"),
    ],
)
def test_valid_output_mode(constructor, output_mode, streaming, checkpoint_folder):
    options = {
        "table": "table",
        "output_mode": output_mode,
    }
    if streaming:
        options["checkpoint_location"] = checkpoint_folder
    x = constructor(**options)
    assert x is not None


def test_delta_stream_table_writer(streaming_dummy_df, spark, checkpoint_folder):
    table_name = "test_streaming_table"
    delta_writer = DeltaTableStreamWriter(
        table=table_name,
        checkpoint_location=checkpoint_folder,
        output_mode="append",
        trigger=Trigger(processing_time="20 seconds"),
        df=streaming_dummy_df,
    )
    delta_writer.write()
    await_job_completion(spark, timeout=20, query_id=delta_writer.streaming_query.id)
    df = spark.read.table(table_name)

    assert df.count() == 10


@pytest.mark.parametrize(
    "trigger_args,expected_exception",
    [
        ({"processing_time": "20 seconds", "available_now": True}, ValueError),
        (None, RuntimeError),
        ({"processing_time": ""}, ValueError),
        ({"continuous": "   "}, ValueError),
        ({"once": "ONCE"}, ValueError),
    ],
)
def test_delta_stream_table_writer_invalid_triggers(
    trigger_args, expected_exception, streaming_dummy_df, checkpoint_folder
):
    table_name = "test_streaming_table"
    with pytest.raises(expected_exception):
        trigger = Trigger(**trigger_args) if trigger_args else None
        DeltaTableStreamWriter(
            table=table_name,
            checkpoint_location=checkpoint_folder,
            output_mode="append",
            trigger=trigger,
            df=streaming_dummy_df,
        )


@pytest.mark.parametrize(
    "choice, options, expected",
    [
        ("APPEND", {BatchOutputMode}, BatchOutputMode.APPEND),
        ("APPEND", {StreamingOutputMode}, StreamingOutputMode.APPEND),
    ],
)
def test_get_output_mode_with_valid_choice(choice, options, expected):
    output_mode = DeltaTableWriter.get_output_mode(choice, options)
    assert output_mode == expected


@pytest.mark.parametrize(
    "choice, options",
    [
        ("INVALID", {BatchOutputMode}),
        ("INVALID", {StreamingOutputMode}),
    ],
)
def test_get_output_mode_with_invalid_choice(choice, options):
    with pytest.raises(AttributeError):
        DeltaTableWriter.get_output_mode(choice, options)


def test_delta_with_options(spark):
    """
    Checks whether the options are passed to the writer in case of batch APPEND and OVERWRITE modes
    """
    sample_df = spark.createDataFrame([{"id": 1, "value": "test_value"}])

    with patch("koheesio.spark.writers.delta.DeltaTableWriter.writer", new_callable=MagicMock) as mock_writer:
        delta_writer = DeltaTableWriter(
            table="test_table",
            output_mode=BatchOutputMode.APPEND,
            testParam1="testValue1",
            testParam2="testValue2",
            df=sample_df,
        )
        delta_writer.execute()
        mock_writer.options.assert_called_once_with(testParam1="testValue1", testParam2="testValue2")

    with patch("koheesio.spark.writers.delta.DeltaTableWriter.writer", new_callable=MagicMock) as mock_writer:
        delta_writer = DeltaTableWriter(
            table="test_table",
            output_mode=BatchOutputMode.OVERWRITE,
            testParam1="testValue1",
            testParam2="testValue2",
            df=sample_df,
        )
        delta_writer.execute()
        mock_writer.options.assert_called_once_with(testParam1="testValue1", testParam2="testValue2")


def test_merge_from_args(spark, dummy_df):
    from koheesio.spark.utils.connect import is_remote_session

    table_name = "test_table_merge_from_args"
    dummy_df.write.format("delta").saveAsTable(table_name)

    with patch("delta.DeltaTable.merge", new_callable=MagicMock) as mock_merge:
        mock_delta_builder = MagicMock()
        mock_merge.return_value = mock_delta_builder

        # Mock the methods to return self for chaining
        mock_delta_builder.whenMatchedUpdate.return_value = mock_delta_builder
        mock_delta_builder.whenNotMatchedInsert.return_value = mock_delta_builder

        writer = DeltaTableWriter(
            df=dummy_df,
            table=table_name,
            output_mode=BatchOutputMode.MERGE,
            output_mode_params={
                "merge_builder": [
                    {
                        "clause": "whenMatchedUpdate",
                        "set": {"id": "source.id"},
                        "condition": "source.id=target.id",
                    },
                    {
                        "clause": "whenNotMatchedInsert",
                        "values": {"id": "source.id"},
                        "condition": "source.id IS NOT NULL",
                    },
                ],
                "merge_cond": "source.id=target.id",
            },
        )

        if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
            with pytest.raises(SparkConnectDeltaTableException) as exc_info:
                writer._merge_builder_from_args()

            assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
        else:
            writer._merge_builder_from_args()
            mock_delta_builder.whenMatchedUpdate.assert_called_once_with(
                set={"id": "source.id"}, condition="source.id=target.id"
            )
            mock_delta_builder.whenNotMatchedInsert.assert_called_once_with(
                values={"id": "source.id"}, condition="source.id IS NOT NULL"
            )
            assert ["clause" in c for c in writer.params["merge_builder"]] == [True] * len(
                writer.params["merge_builder"]
            )


@pytest.mark.parametrize(
    "output_mode_params",
    [
        {
            "merge_builder": [
                {
                    "clause": "NOT-SUPPORTED-MERGE-CLAUSE",
                    "set": {"id": "source.id"},
                    "condition": "source.id=target.id",
                }
            ],
            "merge_cond": "source.id=target.id",
        },
        {"merge_builder": MagicMock()},
    ],
)
def test_merge_from_args_raise_value_error(spark, output_mode_params):
    with pytest.raises(ValueError):
        DeltaTableWriter(
            table="test_table_merge",
            output_mode=BatchOutputMode.MERGE,
            output_mode_params=output_mode_params,
        )


def test_merge_no_table(spark):
    from koheesio.spark.utils.connect import is_remote_session

    table_name = "test_merge_no_table"
    target_df = spark.createDataFrame(
        [
            {"id": 1, "value": "no_merge"},
            {"id": 2, "value": "expected_merge"},
            {"id": 5, "value": "expected_merge"},
        ]
    )
    source_df = spark.createDataFrame(
        [
            {"id": 2, "value": "longer_values_should_be_merged"},
            {"id": 3, "value": None},
            {"id": 4, "value": "not_matches_should_insert"},
            {"id": 5, "value": "longer_values_should_be_merged"},
        ]
    )
    expected = {
        1: "no_merge",
        2: "longer_values_should_be_merged",
        # 3: None - Should not be inserted, value is None
        4: "not_matches_should_insert",
        5: "longer_values_should_be_merged",
    }

    params = {
        "merge_builder": [
            {
                "clause": "whenNotMatchedInsert",
                "values": {"id": "source.id", "value": "source.value"},
                "condition": "source.value IS NOT NULL",
            },
            {
                "clause": "whenMatchedUpdate",
                "set": {"value": "source.value"},
                "condition": "length(source.value)>length(target.value)",
            },
        ],
        "merge_cond": "source.id=target.id",
    }
    writer1 = DeltaTableWriter(
        df=target_df,
        table=table_name,
        output_mode=BatchOutputMode.MERGE,
        output_mode_params=params,
    )
    writer2 = DeltaTableWriter(
        df=source_df,
        table=table_name,
        output_mode=BatchOutputMode.MERGE,
        output_mode_params=params,
    )
    if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
        writer1.execute()

        with pytest.raises(SparkConnectDeltaTableException) as exc_info:
            writer2.execute()

        assert str(exc_info.value).startswith("`DeltaTable.forName` is not supported due to delta calling _sc")
    else:
        writer1.execute()
        writer2.execute()

        result = {
            list(row.asDict().values())[0]: list(row.asDict().values())[1]
            for row in spark.read.table(table_name).collect()
        }

        assert result == expected


def test_log_clauses(mocker):
    # Mocking JavaObject and its methods
    mock_clauses = mocker.MagicMock()
    mock_clauses.isEmpty.return_value = False
    mock_clauses.last.return_value.nodeName.return_value = "DeltaMergeIntoTest"
    mock_clauses.length.return_value = 1

    mock_clause = mocker.MagicMock()
    mock_clause.clauseType.return_value = "test"
    mock_clause.actions.return_value.toList.return_value.apply.return_value.toString.return_value = "test_column"

    mock_condition = mocker.MagicMock()
    mock_condition.value.return_value.toString.return_value = "source_alias == target_alias"
    mock_condition.toString.return_value = "None"
    mock_clause.condition.return_value = mock_condition

    mock_clauses.apply.return_value = mock_clause

    # Call the function with the mocked JavaObject
    result = log_clauses(mock_clauses, "source_alias", "target_alias")

    # Assert the result
    assert result == "Test will perform action:Test columns (test_column) if `source_alias == target_alias`"


def test_merge_builder_type__list_of_merge_builders(mocker, spark):
    table_name = "test_merge_builder_type"
    df = spark.createDataFrame([{"id": 1, "value": "test"}])
    merge_builder = mocker.MagicMock(spec=list)  # mocks a list of merge builders
    # No ValueError should be raised
    DeltaTableWriter(
        df=df,
        table=table_name,
        output_mode=BatchOutputMode.MERGE,
        output_mode_params={"merge_builder": merge_builder},
    )


def test_merge_builder_type___delta_merge_builder(mocker, spark):
    table_name = "test_merge_builder_type"
    df = spark.createDataFrame([{"id": 1, "value": "test"}])
    merge_builder = mocker.MagicMock(spec=DeltaMergeBuilder)
    # No ValueError should be raised
    DeltaTableWriter(
        df=df,
        table=table_name,
        output_mode=BatchOutputMode.MERGE,
        output_mode_params={"merge_builder": merge_builder},
    )


def test_merge_builder_type__connect_delta_merge_builder(mocker, spark):
    table_name = "test_merge_builder_type"
    df = spark.createDataFrame([{"id": 1, "value": "test"}])
    # Not a delta.tables.DeltaMergeBuilder instance but the name is DeltaMergeBuilder (as in delta.connect.tables.DeltaMergeBuilder)
    merge_builder = mocker.MagicMock()
    merge_builder.__class__.__name__ = "DeltaMergeBuilder"
    DeltaTableWriter(
        df=df,
        table=table_name,
        output_mode=BatchOutputMode.MERGE,
        output_mode_params={"merge_builder": merge_builder},
    )


def test_merge_builder_type__invalid_merge_builder(mocker, spark):
    table_name = "test_merge_builder_type"
    df = spark.createDataFrame([{"id": 1, "value": "test"}])
    merge_builder = mocker.MagicMock(spec=str)  # Not a DeltaMergeBuilder instance nor a list
    with pytest.raises(ValueError):
        DeltaTableWriter(
            df=df,
            table=table_name,
            output_mode=BatchOutputMode.MERGE,
            output_mode_params={"merge_builder": merge_builder},
        )
