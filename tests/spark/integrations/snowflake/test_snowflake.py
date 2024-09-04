from textwrap import dedent
from unittest import mock
from unittest.mock import Mock, patch

import pytest

from pyspark.sql import SparkSession
from pyspark.sql import types as t

from koheesio.spark.snowflake import (
    AddColumn,
    CreateOrReplaceTableFromDataFrame,
    DbTableQuery,
    GetTableSchema,
    GrantPrivilegesOnObject,
    GrantPrivilegesOnTable,
    GrantPrivilegesOnView,
    Query,
    RunQuery,
    SnowflakeBaseModel,
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

def test_snowflake_module_import():
    # test that the pass-through imports in the koheesio.spark snowflake modules are working
    from koheesio.spark.writers import snowflake as snowflake_readers
    from koheesio.spark.readers import snowflake as snowflake_writers


class TestSnowflakeReader:
    reader_options = {"dbtable": "table", **COMMON_OPTIONS}

    def test_get_options(self):
        sf = SnowflakeReader(**(self.reader_options | {"authenticator": None}))
        o = sf.get_options()
        assert sf.format == "snowflake"
        assert o["sfUser"] == "user"
        assert o["sfCompress"] == "on"
        assert "authenticator" not in o

    def test_execute(self, dummy_spark):
        """Method should be callable from parent class"""
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            k = SnowflakeReader(**self.reader_options).execute()
            assert k.df.count() == 1


class TestRunQuery:
    query_options = {"query": "query", **COMMON_OPTIONS}

    def test_get_options(self):
        k = RunQuery(**self.query_options)
        o = k.get_options()

        assert o["host"] == o["sfURL"]

    def test_execute(self, dummy_spark):
        pass


class TestQuery:
    query_options = {"query": "query", **COMMON_OPTIONS}

    def test_execute(self, dummy_spark):
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            k = Query(**self.query_options)
            assert k.df.count() == 1


class TestTableQuery:
    options = {"table": "table", **COMMON_OPTIONS}

    def test_execute(self, dummy_spark):
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            k = DbTableQuery(**self.options).execute()
            assert k.df.count() == 1


class TestTableExists:
    table_exists_options = {"table": "table", **COMMON_OPTIONS}

    def test_execute(self, dummy_spark):
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            k = TableExists(**self.table_exists_options).execute()
            assert k.exists is True


class TestCreateOrReplaceTableFromDataFrame:
    options = {"table": "table", **COMMON_OPTIONS}

    def test_execute(self, dummy_spark, dummy_df):
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            k = CreateOrReplaceTableFromDataFrame(**self.options, df=dummy_df).execute()
            assert k.snowflake_schema == "id BIGINT"
            assert k.query == "CREATE OR REPLACE TABLE db.schema.table (id BIGINT)"
            assert len(k.input_schema) > 0


class TestGetTableSchema:
    get_table_schema_options = {"table": "table", **COMMON_OPTIONS}

    def test_execute(self, dummy_spark):
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            k = GetTableSchema(**self.get_table_schema_options)
            assert len(k.execute().table_schema.fields) == 1


class TestAddColumn:
    options = {"table": "foo", "column": "bar", "type": t.DateType(), **COMMON_OPTIONS}

    def test_execute(self, dummy_spark):
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            k = AddColumn(**self.options).execute()
            assert k.query == "ALTER TABLE FOO ADD COLUMN BAR DATE"


def test_grant_privileges_on_object(dummy_spark):
    options = dict(
        **COMMON_OPTIONS, object="foo", type="TABLE", privileges=["DELETE", "SELECT"], roles=["role_1", "role_2"]
    )
    del options["role"]  # role is not required for this step as we are setting "roles"

    kls = GrantPrivilegesOnObject(**options)

    with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
        mock_spark.return_value = dummy_spark
        k = kls.execute()

        assert len(k.query) == 2, "expecting 2 queries (one for each role)"
        assert "DELETE" in k.query[0]
        assert "SELECT" in k.query[0]


def test_grant_privileges_on_table(dummy_spark):
    options = {**COMMON_OPTIONS, **dict(table="foo", privileges=["SELECT"], roles=["role_1"])}
    del options["role"]  # role is not required for this step as we are setting "roles"

    kls = GrantPrivilegesOnTable(
        **options,
    )
    with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
        mock_spark.return_value = dummy_spark

        k = kls.execute()
        assert k.query == [
            "GRANT SELECT ON TABLE DB.SCHEMA.FOO TO ROLE ROLE_1",
        ]


class TestGrantPrivilegesOnView:
    options = {**COMMON_OPTIONS}

    def test_execute(self, dummy_spark):
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            k = GrantPrivilegesOnView(**self.options, view="foo", privileges=["SELECT"], roles=["role_1"]).execute()
            assert k.query == [
                "GRANT SELECT ON VIEW DB.SCHEMA.FOO TO ROLE ROLE_1",
            ]


class TestSnowflakeWriter:
    def test_execute(self, dummy_spark):
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            k = SnowflakeWriter(
                **COMMON_OPTIONS,
                table="foo",
                df=dummy_spark.load(),
                mode=BatchOutputMode.OVERWRITE,
            )
            k.execute()


class TestSyncTableAndDataFrameSchema:
    @mock.patch("koheesio.spark.snowflake.AddColumn")
    @mock.patch("koheesio.spark.snowflake.GetTableSchema")
    def test_execute(self, mock_get_table_schema, mock_add_column, spark, caplog):
        from pyspark.sql.types import StringType, StructField, StructType

        df = spark.createDataFrame(data=[["val"]], schema=["foo"])
        sf_schema_before = StructType([StructField("bar", StringType(), True)])
        sf_schema_after = StructType([StructField("bar", StringType(), True), StructField("foo", StringType(), True)])

        mock_get_table_schema_instance = mock_get_table_schema()
        mock_get_table_schema_instance.execute.side_effect = [
            mock.Mock(table_schema=sf_schema_before),
            mock.Mock(table_schema=sf_schema_after),
        ]

        with caplog.at_level("DEBUG"):
            k = SyncTableAndDataFrameSchema(
                **COMMON_OPTIONS,
                table="foo",
                df=df,
                dry_run=True,
            ).execute()
            print(f"{caplog.text = }")
            assert "Columns to be added to Snowflake table: {'foo'}" in caplog.text
            assert "Columns to be added to Spark DataFrame: {'bar'}" in caplog.text
            assert k.new_df_schema == StructType()

        k = SyncTableAndDataFrameSchema(
            **COMMON_OPTIONS,
            table="foo",
            df=df,
        ).execute()
        assert k.df.columns == ["bar", "foo"]


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
def test_map_spark_type(input_value, expected):
    assert map_spark_type(input_value) == expected


class TestSnowflakeBaseModel:
    def test_get_options(self, dummy_spark):
        k = SnowflakeBaseModel(
            sfURL="url",
            sfUser="user",
            sfPassword="password",
            sfDatabase="database",
            sfRole="role",
            sfWarehouse="warehouse",
            schema="schema",
        )
        options = k.get_options()
        assert options["sfURL"] == "url"
        assert options["sfUser"] == "user"
        assert options["sfDatabase"] == "database"
        assert options["sfRole"] == "role"
        assert options["sfWarehouse"] == "warehouse"
        assert options["sfSchema"] == "schema"


class TestTagSnowflakeQuery:
    def test_tag_query_no_existing_preactions(self):
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

    def test_tag_query_present_existing_preactions(self):
        options = {
            "otherSfOption": "value",
            "preactions": "SET TEST_VAR = 'ABC';",
        }
        query_tag_preaction = (
            """ALTER SESSION SET QUERY_TAG = '{"pipeline_name": "test-pipeline-2","task_name": "test_task_2"}';"""
        )
        expected_preactions = f"SET TEST_VAR = 'ABC';{query_tag_preaction}" ""

        tagged_options = (
            TagSnowflakeQuery(task_name="test_task_2", pipeline_name="test-pipeline-2", options=options)
            .execute()
            .options
        )

        assert len(tagged_options) == 2
        assert tagged_options["otherSfOption"] == "value"
        preactions = tagged_options["preactions"].replace("    ", "").replace("\n", "")
        assert preactions == expected_preactions


def test_table_exists(spark):
    # Create a TableExists instance
    te = TableExists(
        sfURL="url",
        sfUser="user",
        sfPassword="password",
        sfDatabase="database",
        sfRole="role",
        sfWarehouse="warehouse",
        schema="schema",
        table="table",
    )

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

    # Create a Mock object for the Query class
    mock_query = Mock(spec=Query)
    mock_query.read.return_value = spark.range(1)

    # Patch the Query class to return the mock_query when instantiated
    with patch("koheesio.spark.snowflake.Query", return_value=mock_query) as mock_query_class:
        # Execute the SnowflakeBaseModel instance
        te.execute()

        # Assert that the query is as expected
        assert mock_query_class.call_args[1]["query"] == expected_query
