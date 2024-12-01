"""
Koheesio step for running data quality rules with Spark Expectations engine.
"""

from typing import Any, Dict, Optional, Union

# noinspection PyUnresolvedReferences,PyPep8Naming
from spark_expectations.config.user_config import Constants as user_config

# noinspection PyUnresolvedReferences
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
)

from pydantic import Field

from koheesio.spark import DataFrame
from koheesio.spark.transformations import Transformation
from koheesio.spark.writers import BatchOutputMode


class SparkExpectationsTransformation(Transformation):
    """
    Run DQ rules for an input dataframe with Spark Expectations engine.

    References
    ----------
    Spark Expectations: https://engineering.nike.com/spark-expectations/1.0.0/
    """

    product_id: str = Field(default=..., description="Spark Expectations product identifier")
    rules_table: str = Field(default=..., alias="product_rules_table", description="DQ rules table")
    statistics_table: str = Field(default=..., alias="dq_stats_table_name", description="DQ stats table")
    target_table: str = Field(
        default=...,
        alias="target_table_name",
        description="The table that will contain good records. Won't write to it, but will write to the err table with "
        "same name plus _err suffix",
    )
    mode: Union[str, BatchOutputMode] = Field(
        default=BatchOutputMode.APPEND,
        alias="dataframe_writer_mode",
        description="The write mode that will be used to write to the err and stats table. Separate output modes can "
        "be specified for each table using the error_writer_mode and stats_writer_mode params",
    )
    format: str = Field(
        default="delta",
        alias="dataframe_writer_format",
        description="The format used to write to the stats and err table. Separate output formats can be specified for "
        "each table using the error_writer_format and stats_writer_format params",
    )
    error_writing_options: Optional[Dict[str, str]] = Field(
        default_factory=dict, alias="error_writing_options", description="Options for writing to the error table"
    )
    error_writer_mode: Optional[Union[str, BatchOutputMode]] = Field(
        default=BatchOutputMode.APPEND,
        alias="dataframe_writer_mode",
        description="The write mode that will be used to write to the err table",
    )
    error_writer_format: Optional[str] = Field(
        default="delta", alias="dataframe_writer_format", description="The format used to write to the err table"
    )
    stats_writer_mode: Optional[Union[str, BatchOutputMode]] = Field(
        default=BatchOutputMode.APPEND,
        alias="stats_writer_mode",
        description="The write mode that will be used to write to the stats table",
    )
    stats_writer_format: Optional[str] = Field(
        default="delta", alias="stats_writer_format", description="The format used to write to the stats table"
    )
    enable_debugger: bool = Field(default=False, alias="debugger", description="...")
    statistics_streaming: Dict[str, Any] = Field(
        default={user_config.se_enable_streaming: False},
        alias="stats_streaming_options",
        description="SE stats streaming options ",
        validate_default=False,
    )
    se_user_conf: Dict[str, Any] = Field(
        default={
            user_config.se_notifications_enable_email: False,
            user_config.se_notifications_enable_slack: False,
        },
        alias="user_conf",
        description="SE user provided confs",
        validate_default=False,
    )
    drop_meta_column: bool = Field(
        default=False,
        alias="drop_meta_columns",
        description="Whether to drop meta columns added by spark expectations on the output df",
    )

    class Output(Transformation.Output):
        """Output of the SparkExpectationsTransformation step."""

        rules_df: DataFrame = Field(default=..., description="Output dataframe")
        se: SparkExpectations = Field(default=..., description="Spark Expectations object")
        error_table_writer: WrappedDataFrameWriter = Field(
            default=..., description="Spark Expectations error table writer"
        )
        stats_table_writer: WrappedDataFrameWriter = Field(
            default=..., description="Spark Expectations stats table writer"
        )

    @property
    def _error_table_writer(self) -> WrappedDataFrameWriter:
        """Wrapper around Spark DataFrameWriter to be used by Spark Expectations"""
        if not (_writer := self.output.get("error_table_writer", None)):
            _mode = self.error_writer_mode or self.mode
            _format = self.error_writer_format or self.format
            _options = self.error_writing_options
            _writer = WrappedDataFrameWriter().mode(_mode).format(_format).options(**_options)
            self.output.error_table_writer = _writer
        return _writer

    @property
    def _stats_table_writer(self) -> WrappedDataFrameWriter:
        """Wrapper around Spark DataFrameWriter to be used by Spark Expectations"""
        if not (_writer := self.output.get("stats_table_writer", None)):
            _mode = self.stats_writer_mode or self.mode
            _format = self.stats_writer_format or self.format
            _writer = WrappedDataFrameWriter().mode(_mode).format(self.stats_writer_format)
            self.output.stats_table_writer = _writer
        return _writer

    @property
    def _se(self) -> SparkExpectations:
        """Spark Expectations object"""
        if not (_se := self.output.get("se", None)):
            _se = SparkExpectations(
                product_id=self.product_id,
                rules_df=self.output.rules_df,
                stats_table=self.statistics_table,
                stats_table_writer=self._stats_table_writer,
                target_and_error_table_writer=self._error_table_writer,
                debugger=self.enable_debugger,
                stats_streaming_options=self.statistics_streaming,
            )
            self.output.se = _se
        return _se

    def execute(self) -> Output:
        """
        Apply data quality rules to a dataframe using the out-of-the-box SE decorator
        """
        # read rules table
        rules_df = self.spark.read.table(self.rules_table).cache()
        self.output.rules_df = rules_df

        @self._se.with_expectations(
            target_table=self.target_table,
            user_conf=self.se_user_conf,
            # Below params are `False` by default, however exposing them here for extra visibility
            # The writes can be handled by downstream Koheesio steps
            write_to_table=False,
            write_to_temp_table=False,
        )
        def inner(df: DataFrame) -> DataFrame:
            """Just a wrapper to be able to use Spark Expectations decorator"""
            return df

        output_df = inner(self.df)

        if self.drop_meta_column:
            output_df = output_df.drop("meta_dq_run_id", "meta_dq_run_datetime")

        self.output.df = output_df
