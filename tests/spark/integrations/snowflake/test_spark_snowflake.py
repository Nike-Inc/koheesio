# flake8: noqa: F811
import logging
from textwrap import dedent
from unittest import mock
from unittest.mock import Mock

import pytest
from pyspark.sql import types as t

from koheesio.integrations.snowflake.test_utils import mock_query  # noqa: F401
from koheesio.integrations.spark.snowflake import (
    AddColumn,
    CreateOrReplaceTableFromDataFrame,
    DbTableQuery,
    GetTableSchema,
    Query,
    RunQuery,
    SnowflakeReader,
    SnowflakeWriter,
    SyncTableAndDataFrameSchema,
    TableExists,
    TagSnowflakeQuery,
    map_spark_type,
)
from koheesio.spark.writers import BatchOutputMode

pytestmark = pytest.mark.spark

COMMON_OPTIONS = {
    "url": "url",
    "user": "user",
    "password": "password",
    "database": "db",
    "schema": "schema",
    "role": "role",
    "warehouse": "warehouse",
}


def test_snowflake_module_import() -> None:
    """Test that the module exports work correctly"""
    # Importing to verify the modules are exposed correctly
    import koheesio.spark.readers.snowflake  # noqa: F401
    import koheesio.spark.writers.snowflake  # noqa: F401


class TestSnowflakeReader:
    """Test the Snowflake reader class including parameter handling and execution"""
    
    reader_options = {"dbtable": "table", **COMMON_OPTIONS}

    def test_get_options(self) -> None:
        """Test that options are properly handled with correct parameter precedence"""
        sf = SnowflakeReader(**(self.reader_options | {"authenticator": None}))
        o = sf.get_options()
        assert sf.format == "snowflake", "format should be set to snowflake by default"
        assert o["sfUser"] == "user", "sfUser should be set by SnowflakeBaseModel"
        assert o["sfCompress"] == "on", "sfCompress should be set by SnowflakeBaseModel"
        assert "authenticator" not in o, "authenticator should not be passed to the Snowflake reader"
        assert o["dbtable"] == "table", "dbtable parameter should be preserved in options"

    def test_execute(self, dummy_spark: Mock) -> None:  # pylint: disable=unused-argument
        """Test that reader can be executed through parent class interface"""
        k = SnowflakeReader(**self.reader_options).execute()
        assert k.df.count() == 3

    def test_execute_with_dbtable(self, dummy_spark: Mock) -> None:  # pylint: disable=unused-argument
        """Test that SnowflakeReader works correctly when given a dbtable argument.
        
        Related to TestJdbcReader.test_execute_w_dbtable in test_jdbc.py
        """
        reader = SnowflakeReader(**COMMON_OPTIONS, dbtable="my_table")
        result = reader.execute()

        options = reader.get_options()
        assert options["dbtable"] == "my_table", "dbtable should be preserved in options"
        assert reader.dbtable == "my_table", "dbtable should be preserved on the instance"
        assert "query" not in options, "query should not be present in options"
        assert result.df.count() == 3, "DataFrame should be read correctly"


class TestRunQuery:
    def test_deprecation(self) -> None:
        """Test for the deprecation warning"""
        with pytest.warns(
            DeprecationWarning, match="The RunQuery class is deprecated and will be removed in a future release."
        ):
            try:
                _ = RunQuery(
                    **COMMON_OPTIONS,
                    query="<deprecated>",
                )
            except RuntimeError:
                pass  # Ignore any RuntimeError that occur after the warning

    def test_spark_connect(self, spark: Mock) -> None:
        """Test that we get a RuntimeError when using a SparkSession without a JVM"""
        from koheesio.spark.utils.connect import is_remote_session

        if not is_remote_session(spark):
            pytest.skip(reason="Test only runs when we have a remote SparkSession")

        with pytest.raises(RuntimeError):
            _ = RunQuery(
                **COMMON_OPTIONS,
                query="<deprecated>",
            )


class TestQuery:
    options = {"query": "query", **COMMON_OPTIONS}

    def test_execute(self, dummy_spark: Mock) -> None:  # pylint: disable=unused-argument
        """Test query execution with basic options"""
        k = Query(**self.options).execute()
        assert k.df.count() == 3


class TestTableQuery:
    options = {"table": "table", **COMMON_OPTIONS}

    def test_execute(self, dummy_spark: Mock) -> None:  # pylint: disable=unused-argument
        """Test table query execution with basic options"""
        k = DbTableQuery(**self.options).execute()
        assert k.df.count() == 3


class TestCreateOrReplaceTableFromDataFrame:
    options = {"table": "table", "account": "bar", **COMMON_OPTIONS}

    def test_execute(self, dummy_spark: Mock, dummy_df: Mock, mock_query: Mock) -> None:  # pylint: disable=unused-argument
        """Test table creation from DataFrame"""
        k = CreateOrReplaceTableFromDataFrame(**self.options, df=dummy_df).execute()  # type: ignore[func-returns-value]
        assert k.snowflake_schema == "id BIGINT"
        assert k.query == "CREATE OR REPLACE TABLE db.schema.table (id BIGINT)"
        assert len(k.input_schema) > 0
        mock_query.assert_called_with(k.query)


class TestGetTableSchema:
    options = {"table": "table", **COMMON_OPTIONS}

    def test_execute(self, dummy_spark: Mock) -> None:  # pylint: disable=unused-argument
        """Test schema retrieval"""
        k = GetTableSchema(**self.options)
        assert len(k.execute().table_schema.fields) == 2


class TestAddColumn:
    options = {"table": "foo", "column": "bar", "type": t.DateType(), "account": "foo", **COMMON_OPTIONS}

    def test_execute(self, dummy_spark: Mock, mock_query: Mock) -> None:  # pylint: disable=unused-argument
        """Test column addition to table"""
        k = AddColumn(**self.options).execute()  # type: ignore[func-returns-value]
        assert k.query == "ALTER TABLE FOO ADD COLUMN BAR DATE"
        mock_query.assert_called_with(k.query)


class TestSnowflakeWriter:
    def test_execute(self, mock_df: Mock) -> None:
        """Test Snowflake writer execution"""
        k = SnowflakeWriter(
            **COMMON_OPTIONS,
            table="foo",
            df=mock_df,
            mode=BatchOutputMode.OVERWRITE,
        )
        k.execute()

        # check that the format was set to snowflake
        mocked_format: Mock = mock_df.write.format
        assert mocked_format.call_args[0][0] == "snowflake"
        mock_df.write.format.assert_called_with("snowflake")


class TestSyncTableAndDataFrameSchema:
    @mock.patch("koheesio.integrations.spark.snowflake.AddColumn")
    @mock.patch("koheesio.integrations.spark.snowflake.GetTableSchema")
    def test_execute(self, mock_get_table_schema: Mock, mock_add_column: Mock, spark: Mock, caplog: Mock) -> None:
        """Test schema synchronization between Snowflake table and Spark DataFrame"""
        # Arrange
        from pyspark.sql.types import StringType, StructField, StructType

        df = spark.createDataFrame(data=[["val"]], schema=["foo"])
        sf_schema_before = StructType([StructField("bar", StringType(), True)])
        sf_schema_after = StructType([StructField("bar", StringType(), True), StructField("foo", StringType(), True)])

        mock_get_table_schema_instance = mock_get_table_schema()
        mock_get_table_schema_instance.execute.side_effect = [
            mock.Mock(table_schema=sf_schema_before),
            mock.Mock(table_schema=sf_schema_after),
        ]

        logger = logging.getLogger("koheesio")
        logger.setLevel(logging.WARNING)

        # Act and Assert -- dry run
        with caplog.at_level(logging.WARNING):
            k = SyncTableAndDataFrameSchema(
                **COMMON_OPTIONS,
                table="foo",
                df=df,
                dry_run=True,
            ).execute()
            assert "Columns to be added to Snowflake table: {'foo'}" in caplog.text
            assert "Columns to be added to Spark DataFrame: {'bar'}" in caplog.text
            assert k.new_df_schema == StructType()

        # Act and Assert -- execute
        k = SyncTableAndDataFrameSchema(
            **COMMON_OPTIONS,
            table="foo",
            df=df,
        ).execute()
        assert sorted(k.df.columns) == ["bar", "foo"]


@pytest.mark.parametrize(
    "input_value,expected",
    [
        (t.BinaryType(), "VARBINARY"),
        (t.BooleanType(), "BOOLEAN"),
        (t.ByteType(), "BINARY"),
        (t.DateType(), "DATE"),
        (t.TimestampType(), "TIMESTAMP"),
        (t.DoubleType(), "DOUBLE"),
        (t.FloatType(), "FLOAT"),
        (t.IntegerType(), "INT"),
        (t.LongType(), "BIGINT"),
        (t.NullType(), "STRING"),
        (t.ShortType(), "SMALLINT"),
        (t.StringType(), "STRING"),
        (t.NumericType(), "FLOAT"),
        (t.DecimalType(0, 1), "DECIMAL(0,1)"),
        (t.DecimalType(0, 100), "DECIMAL(0,100)"),
        (t.DecimalType(10, 0), "DECIMAL(10,0)"),
        (t.DecimalType(), "DECIMAL(10,0)"),
        (t.MapType(t.IntegerType(), t.StringType()), "VARIANT"),
        (t.ArrayType(t.StringType()), "VARIANT"),
        (t.StructType([t.StructField(name="foo", dataType=t.StringType())]), "VARIANT"),
        (t.DayTimeIntervalType(), "STRING"),
    ],
)
def test_map_spark_type(input_value: t.DataType, expected: str) -> None:
    """Test Spark to Snowflake type mapping"""
    assert map_spark_type(input_value) == expected


class TestTableExists:
    options = dict(
        sfURL="url",
        sfUser="user",
        sfPassword="password",
        sfDatabase="database",
        sfRole="role",
        sfWarehouse="warehouse",
        schema="schema",
        table="table",
    )

    def test_table_exists(self, dummy_spark: Mock) -> None:
        """Test table existence check"""
        # Arrange
        te = TableExists(**self.options)
        expected_query = dedent(
            """
            SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG     = 'DATABASE'
              AND TABLE_SCHEMA      = 'SCHEMA'
              AND TABLE_TYPE        = 'BASE TABLE'
              AND UPPER(TABLE_NAME) = 'TABLE'
            """
        ).strip()

        # Act
        output = te.execute()

        # Assert that the query is as expected and that we got exists as True
        assert dummy_spark.options_dict["query"] == expected_query
        assert output.exists


class TestTagSnowflakeQuery:
    def test_tag_query_no_existing_preactions(self) -> None:
        """Test tagging Snowflake query without existing preactions"""
        expected_preactions = (
            """ALTER SESSION SET QUERY_TAG = '{"pipeline_name": "test-pipeline-1","task_name": "test_task_1"}';"""
        )

        tagged_options = (
            TagSnowflakeQuery(
                task_name="test_task_1",
                pipeline_name="test-pipeline-1",
            )
            .execute()
            .options
        )

        assert len(tagged_options) == 1
        preactions = tagged_options["preactions"].replace("    ", "").replace("\n", "")
        assert preactions == expected_preactions

    def test_tag_query_present_existing_preactions(self) -> None:
        """Test tagging Snowflake query with existing preactions"""
        options = {
            "otherSfOption": "value",
            "preactions": "SET TEST_VAR = 'ABC';",
        }
        query_tag_preaction = (
            """ALTER SESSION SET QUERY_TAG = '{"pipeline_name": "test-pipeline-2","task_name": "test_task_2"}';"""
        )
        expected_preactions = f"SET TEST_VAR = 'ABC';{query_tag_preaction}"

        tagged_options = (
            TagSnowflakeQuery(task_name="test_task_2", pipeline_name="test-pipeline-2", options=options)
            .execute()
            .options
        )

        assert len(tagged_options) == 2
        assert tagged_options["otherSfOption"] == "value"
        preactions = tagged_options["preactions"].replace("    ", "").replace("\n", "")
        assert preactions == expected_preactions
