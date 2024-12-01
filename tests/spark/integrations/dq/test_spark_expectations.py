from typing import List, Union

import pytest

from pyspark.sql import SparkSession

from koheesio.utils import get_project_root

PROJECT_ROOT = get_project_root()

pytestmark = pytest.mark.spark


class TestSparkExpectationsTransform:
    """
    Test the transform step in isolation by providing sample df and rules. Will check if rows are dropped and written
    to err_table, and also if meta columns are dropped.

    dq rules for test case:
    - col1 should not be null
    - col2 should not be null
    """

    input_data = [
        ["col1-1", "col2-1", "col3-1"],
        ["col1-2", "col2-2", "col3-2"],
        ["col1-3", None, "col3-3"],
        [None, "col2-4", "col3-4"],
    ]
    input_schema = ["col1", "col2", "col3"]
    expected = [
        {"col1": "col1-1", "col2": "col2-1", "col3": "col3-1"},
        {"col1": "col1-2", "col2": "col2-2", "col3": "col3-2"},
    ]

    @pytest.fixture(scope="session")
    def prepare_tables(self, spark, data_path):
        TestSparkExpectationsTransform.apply_spark_sql(
            spark,
            [
                f"{data_path}/transformations/spark_expectations_resources/test_product_dq_stats.ddl",
                f"{data_path}/transformations/spark_expectations_resources/test_product_rules.ddl",
                f"{data_path}/transformations/spark_expectations_resources/test_product_rules.sql",
            ],
        )

    def test_rows_are_dropped(self, spark: SparkSession, prepare_tables):
        from koheesio.integrations.spark.dq.spark_expectations import (
            SparkExpectationsTransformation,
        )

        input_df = spark.createDataFrame(
            TestSparkExpectationsTransform.input_data,
            schema=TestSparkExpectationsTransform.input_schema,
        )

        se_transform = SparkExpectationsTransformation(
            product_id="test_product",
            product_rules_table="default.test_product_rules",
            dq_stats_table_name="default.test_product_dq_stats",
            target_table_name="default.output_table",
        )

        output_df = se_transform.transform(input_df)

        actual = [k.asDict() for k in output_df.select("col1", "col2", "col3").collect()]
        assert actual == TestSparkExpectationsTransform.expected

        err_table_df = spark.read.table("default.output_table_error")
        assert err_table_df.count() == 2

        spark.sql("drop table default.output_table_error")

    def test_meta_columns_are_not_dropped(self, spark, prepare_tables):
        from koheesio.integrations.spark.dq.spark_expectations import (
            SparkExpectationsTransformation,
        )

        input_df = spark.createDataFrame(
            TestSparkExpectationsTransform.input_data,
            schema=TestSparkExpectationsTransform.input_schema,
        )

        se_transform = SparkExpectationsTransformation(
            product_id="test_product",
            product_rules_table="default.test_product_rules",
            dq_stats_table_name="default.test_product_dq_stats",
            target_table_name="default.output_table",
            drop_meta_column=False,
        )

        output_df = se_transform.transform(input_df)
        output_columns = output_df.columns

        assert "meta_dq_run_id" in output_columns
        # Spark Expectations should add either meta_dq_run_date (<=1.1.0) or meta_dq_run_datetime (>= v1.1.1)
        assert "meta_dq_run_date" in output_columns or "meta_dq_run_datetime" in output_columns

        spark.sql("drop table default.output_table_error")

    def test_meta_columns_are_dropped(self, spark, prepare_tables):
        from koheesio.integrations.spark.dq.spark_expectations import (
            SparkExpectationsTransformation,
        )

        input_df = spark.createDataFrame(
            TestSparkExpectationsTransform.input_data,
            schema=TestSparkExpectationsTransform.input_schema,
        )

        se_transform = SparkExpectationsTransformation(
            product_id="test_product",
            product_rules_table="default.test_product_rules",
            dq_stats_table_name="default.test_product_dq_stats",
            target_table_name="default.output_table",
            drop_meta_column=True,
        )

        output_df = se_transform.transform(input_df)
        output_columns = output_df.columns

        assert "meta_dq_run_id" not in output_columns
        assert "meta_dq_run_datetime" not in output_columns and "meta_dq_run_datetime" not in output_columns

        spark.sql("drop table default.output_table_error")

    @staticmethod
    def apply_spark_sql(spark: SparkSession, source_sql_files: Union[List[str], str]) -> None:
        if isinstance(source_sql_files, str):
            source_sql_files = [source_sql_files]

        for source_sql_file in source_sql_files:
            with open(source_sql_file, "r") as f:
                content = f.read()
                spark.sql(content)

    def test_with_full_se_user_conf(self):
        from koheesio.integrations.spark.dq.spark_expectations import (
            SparkExpectationsTransformation,
        )

        conf = {
            "spark.expectations.notifications.email.enabled": False,
            "spark.expectations.notifications.email.smtp_host": "mailhost.email.com",
            "spark.expectations.notifications.email.smtp_port": 25,
            "spark.expectations.notifications.email.from": "",
            "spark.expectations.notifications.email.to.other.mail.com": "",
            "spark.expectations.notifications.email.subject": "{} - data quality - notifications",
            "spark.expectations.notifications.slack.enabled": True,
            "spark.expectations.notifications.slack.webhook_url": "https://hooks.slack.com/...",
            "spark.expectations.notifications.on_start": True,
            "spark.expectations.notifications.on_completion": True,
            "spark.expectations.notifications.on_fail": True,
            "spark.expectations.notifications.on.error.drop.exceeds.threshold.breach": False,
            "spark.expectations.notifications.error.drop.threshold": 15,
        }
        instance = SparkExpectationsTransformation(
            product_id="test_product",
            product_rules_table="default.test_product_rules",
            dq_stats_table_name="default.test_product_dq_stats",
            target_table_name="default.output_table",
            drop_meta_column=True,
            se_user_conf=conf,
        )
        assert instance.se_user_conf == conf

    def test_overwrite_error_writer(self):
        from koheesio.integrations.spark.dq.spark_expectations import (
            SparkExpectationsTransformation,
        )

        """
        Test that the error_writer can be overwritten using error_writer_mode and error_writer_format
        """
        se_transform = SparkExpectationsTransformation(
            product_id="test_product",
            rules_table="default.test_product_rules",
            statistics_table="default.test_product_dq_stats",
            target_table="default.output_table",
            drop_meta_column=True,
            se_user_conf={},
            mode="append",
            format="delta",
            error_writer_mode="overwrite",
            error_writer_format="parquet",
            error_writing_options={"mergeSchema": "true"},
        )
        error_writer = se_transform._error_table_writer
        stats_writer = se_transform._stats_table_writer
        assert error_writer._mode == "overwrite"
        assert error_writer._format == "parquet"
        assert stats_writer._mode == "append"
        assert stats_writer._format == "delta"
        assert error_writer._options == {"mergeSchema": "true"}

    def test_overwrite_stats_writer(self):
        from koheesio.integrations.spark.dq.spark_expectations import (
            SparkExpectationsTransformation,
        )

        """
        Test that the stats_writer can be overwritten using stats_writer_mode and stats_writer_format and that the
        error_writer_options default = {}.
        """
        se_transform = SparkExpectationsTransformation(
            product_id="test_product",
            rules_table="default.test_product_rules",
            statistics_table="default.test_product_dq_stats",
            target_table="default.output_table",
            drop_meta_column=True,
            se_user_conf={},
            mode="append",
            format="delta",
            stats_writer_mode="overwrite",
            stats_writer_format="parquet",
        )
        error_writer = se_transform._error_table_writer
        stats_writer = se_transform._stats_table_writer
        assert error_writer._mode == "append"
        assert error_writer._format == "delta"
        assert error_writer._options == {}
        assert stats_writer._mode == "overwrite"
        assert stats_writer._format == "parquet"
