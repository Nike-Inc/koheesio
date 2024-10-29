"""
This module defines writers to write Slowly Changing Dimension (SCD) Type 2 data to a Delta table.

Slowly Changing Dimension (SCD) is a technique used in data warehousing to handle changes to dimension data over time.
SCD Type 2 is one of the most common types of SCD, where historical changes are tracked by creating new records for each change.

Koheesio is a powerful data processing framework that provides advanced capabilities for working with Delta tables in Apache Spark.
It offers a convenient and efficient way to handle SCD Type 2 operations on Delta tables.

To learn more about Slowly Changing Dimension and SCD Type 2, you can refer to the following resources:
- [Slowly Changing Dimension (SCD) - Wikipedia](https://en.wikipedia.org/wiki/Slowly_changing_dimension)

By using Koheesio, you can benefit from its efficient merge logic, support for SCD Type 2 and SCD Type 1 attributes,
and seamless integration with Delta tables in Spark.

"""

from typing import List, Optional
from logging import Logger

from delta.tables import DeltaMergeBuilder, DeltaTable

from pydantic import InstanceOf

from pyspark.sql import functions as f
from pyspark.sql.types import DateType, TimestampType

from koheesio.models import Field
from koheesio.spark import Column, DataFrame, SparkSession
from koheesio.spark.delta import DeltaTableStep
from koheesio.spark.functions import current_timestamp_utc
from koheesio.spark.writers import Writer
from koheesio.spark.writers.delta.utils import get_delta_table_for_name


class SCD2DeltaTableWriter(Writer):
    """
    A class used to write Slowly Changing Dimension (SCD) Type 2 data to a Delta table.

    Attributes
    ----------
    table : InstanceOf[DeltaTableStep]
        The table to merge to.
    merge_key : str
        The key used for merging data.
    include_columns : List[str]
        Columns to be merged. Will be selected from DataFrame. Default is all columns.
    exclude_columns : List[str]
        Columns to be excluded from DataFrame.
    scd2_columns : List[str]
        List of attributes for SCD2 type (track changes).
    scd2_timestamp_col : Optional[Column]
        Timestamp column for SCD2 type (track changes). Default to current_timestamp.
    scd1_columns : List[str]
        List of attributes for SCD1 type (just update).
    meta_scd2_struct_col_name : str
        SCD2 struct name.
    meta_scd2_effective_time_col_name : str
        Effective col name.
    meta_scd2_is_current_col_name : str
        Current col name.
    meta_scd2_end_time_col_name : str
        End time col name.
    target_auto_generated_columns : List[str]
        Auto generated columns from target Delta table. Will be used to exclude from merge logic.
    """

    table: InstanceOf[DeltaTableStep] = Field(..., description="The table to merge to")
    merge_key: str = Field(..., description="Merge key")
    include_columns: List[str] = Field(
        default_factory=list, description="Columns to be merged. Will be selected from DataFrame.Default is all columns"
    )
    exclude_columns: List[str] = Field(default_factory=list, description="Columns to be excluded from DataFrame")
    scd2_columns: List[str] = Field(
        default_factory=list, description="List of attributes for scd2 type (track changes)"
    )
    scd2_timestamp_col: Column = Field(
        default=None,
        description="Timestamp column for SCD2 type (track changes). Default to current_timestamp",
    )
    scd1_columns: List[str] = Field(default_factory=list, description="List of attributes for scd1 type (just update)")
    meta_scd2_struct_col_name: str = Field(default="_scd2", description="SCD2 struct name")
    meta_scd2_effective_time_col_name: str = Field(default="effective_time", description="Effective col name")
    meta_scd2_is_current_col_name: str = Field(default="is_current", description="Current col name")
    meta_scd2_end_time_col_name: str = Field(default="end_time", description="End time col name")
    target_auto_generated_columns: List[str] = Field(
        default_factory=list,
        description="Auto generated columns from target Delta table. Will be used to exclude from merge logic",
    )

    @staticmethod
    def _prepare_attr_clause(attrs: List[str], src_alias: str, dest_alias: str) -> Optional[str]:
        """
        Prepare an attribute clause for SQL query.

        This method takes a list of attributes and two aliases, and prepares a clause for SQL query.
        The clause checks if the attributes in the source alias and destination alias are not equal.

        Parameters
        ----------
        attrs : List[str]
            List of attributes to be checked.
        src_alias : str
            Alias for the source table in the SQL query.
        dest_alias : str
            Alias for the destination table in the SQL query.

        Returns
        -------
        Optional[str]
            The prepared SQL clause if attributes are provided, None otherwise.

        """
        attr_clause = None

        if attrs:
            attr_clause = list(map(lambda attr: f"NOT ({src_alias}.{attr} <=> {dest_alias}.{attr})", attrs))
            attr_clause = " OR ".join(attr_clause)  # type: ignore

        return attr_clause

    @staticmethod
    def _scd2_timestamp(spark: SparkSession, scd2_timestamp_col: Optional[Column] = None, **_kwargs) -> Column:
        """
        Generate a SCD2 timestamp column.

        This method generates a SCD2 timestamp column. If `scd2_timestamp_col` is provided, it is cast to TimestampType.
        Otherwise, the current UTC timestamp is used.

        Parameters
        ----------
        spark : SparkSession
            The SparkSession to use.
        scd2_timestamp_col : Optional[Column], default is None
            The column to cast to TimestampType. If None, the current UTC timestamp is used.

        Returns
        -------
        Column
            The generated SCD2 timestamp column.

        """
        scd2_timestamp = (
            current_timestamp_utc(spark=spark)
            if scd2_timestamp_col is None
            else scd2_timestamp_col.cast(TimestampType())
        )
        return scd2_timestamp

    @staticmethod
    def _scd2_end_time(meta_scd2_end_time_col: str, **_kwargs: dict) -> Column:
        """
        Generate a SCD2 end time column.

        This method generates a SCD2 end time column based on the provided `meta_scd2_end_time_col`.
        If the `__meta_scd2_system_merge_action` is 'UC' and `__meta_scd2_rn` is 2, the `__meta_scd2_timestamp` is used.
        Otherwise, the value from the target's `meta_scd2_end_time_col` is used.

        Parameters
        ----------
        meta_scd2_end_time_col : str
            The column name in the target table that holds the SCD2 end time.

        Returns
        -------
        Column
            The generated SCD2 end time column.

        """
        scd2_end_time = f.expr(
            "CASE WHEN __meta_scd2_system_merge_action='UC' AND cross.__meta_scd2_rn=2 THEN __meta_scd2_timestamp "
            f"     ELSE tgt.{meta_scd2_end_time_col} END"
        )

        return scd2_end_time

    @staticmethod
    def _scd2_effective_time(meta_scd2_effective_time_col: str, **_kwargs: dict) -> Column:
        """
        Generate a SCD2 effective time column.

        This method generates a SCD2 effective time column based on the provided `meta_scd2_effective_time_col`.
        If the `__meta_scd2_system_merge_action` is 'UC' and `__meta_scd2_rn` is 1, the `__meta_scd2_timestamp` is used.
        Otherwise, the value from the target's `meta_scd2_effective_time_col` is used,
        if it exists, or `__meta_scd2_timestamp` is used.

        Parameters
        ----------
        meta_scd2_effective_time_col : str
            The column name in the target table that holds the SCD2 effective time.

        Returns
        -------
        Column
            The generated SCD2 effective time column.

        """
        scd2_effective_time = f.when(
            f.expr("__meta_scd2_system_merge_action='UC' and cross.__meta_scd2_rn=1"),
            f.col("__meta_scd2_timestamp"),
        ).otherwise(f.coalesce(meta_scd2_effective_time_col, "__meta_scd2_timestamp"))

        return scd2_effective_time

    @staticmethod
    def _scd2_is_current(**_kwargs: dict) -> Column:
        """
        Generate a SCD2 is_current column.

        This method generates a SCD2 is_current column. If the `__meta_scd2_system_merge_action` is 'UC'
        and `__meta_scd2_rn` is 2, the method returns False. Otherwise, it returns True.

        Returns
        -------
        Column
            The generated SCD2 is_current column.

        """
        scd2_is_current = f.expr(
            "CASE WHEN __meta_scd2_system_merge_action='UC' AND cross.__meta_scd2_rn=2 THEN False ELSE True END"
        )

        return scd2_is_current

    def _prepare_staging(
        self,
        df: DataFrame,
        delta_table: DeltaTable,
        merge_action_logic: Column,
        meta_scd2_is_current_col: str,
        columns_to_process: List[str],
        src_alias: str,
        dest_alias: str,
        cross_alias: str,
        **_kwargs: dict,
    ) -> DataFrame:
        """
        Prepare a DataFrame for staging.

        This method prepares a DataFrame for staging by selecting the necessary columns, joining with the delta table,
        adding a merge action column, filtering based on the merge action, and cross joining with a DataFrame of two
        rows.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to prepare for staging.
        delta_table : DeltaTable
            The DeltaTable to join with.
        merge_key : str
            The key to use for the join.
        merge_action_logic : Column
            The logic to use for the merge action.
        meta_scd2_is_current_col : str
            The column that indicates whether a row is current.
        columns_to_process : List[str]
            The columns to select from the DataFrame.
        src_alias : str
            The alias to use for the DataFrame.
        dest_alias : str
            The alias to use for the DeltaTable.
        cross_alias : str
            The alias to use for the cross join.

        Returns
        -------
        DataFrame
            The prepared DataFrame.

        """
        df = (
            df.select(*columns_to_process, "__meta_scd2_timestamp")
            .alias(src_alias)
            .join(
                other=delta_table.toDF()
                .filter(f.col(meta_scd2_is_current_col).eqNullSafe(f.lit(True)))
                .alias(dest_alias),
                on=self.merge_key,
                how="left",
            )
            .withColumn("__meta_scd2_system_merge_action", merge_action_logic)
            .filter("__meta_scd2_system_merge_action IS NOT NULL")
            .crossJoin(other=self.spark.sql("SELECT 1 as __meta_scd2_rn UNION ALL SELECT 2").alias(cross_alias))
            # Filter cross joined data so that we have one row for U
            # and another for I in case of closing SCD2
            # and keep just one for SCD1 or NEW row
            .filter(f.expr("__meta_scd2_system_merge_action='UC' OR cross.__meta_scd2_rn=1"))
        )

        return df

    @staticmethod
    def _preserve_existing_target_values(
        df: DataFrame,
        meta_scd2_struct_col_name: str,
        target_auto_generated_columns: List[str],
        src_alias: str,
        cross_alias: str,
        dest_alias: str,
        logger: Logger,
        **_kwargs: dict,
    ) -> DataFrame:
        """
        Preserve existing target values in the DataFrame.

        This method preserves existing target values in the DataFrame. It skips certain columns and for
        the remaining columns,if the `__meta_scd2_system_merge_action` is 'UC' and `__meta_scd2_rn`
        is 2, it keeps the value from the destination table.
        Otherwise, it keeps the value from the source table.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to process.
        meta_scd2_struct_col_name : str
            The column name of the SCD2 struct.
        target_auto_generated_columns : List[str]
            The list of auto-generated columns in the target.
        src_alias : str
            The alias to use for the source DataFrame.
        cross_alias : str
            The alias to use for the cross join.
        dest_alias : str
            The alias to use for the destination DataFrame.
        logger : Logger
            The logger to use.

        Returns
        -------
        DataFrame
            The processed DataFrame.

        """
        skipped_columns = [
            meta_scd2_struct_col_name,
            "__meta_scd2_system_merge_action",
            "__meta_scd2_timestamp",
            "__meta_scd2_rn",
        ] + target_auto_generated_columns

        for c in set(df.columns):
            if c not in skipped_columns:
                df = (
                    df.withColumn(
                        f"newly_{c}",
                        f.when(
                            f.col("__meta_scd2_system_merge_action").eqNullSafe(f.lit("UC"))
                            & f.col(f"{cross_alias}.__meta_scd2_rn").eqNullSafe(f.lit(2)),
                            f.col(f"{dest_alias}.{c}"),
                        ).otherwise(f.col(f"{src_alias}.{c}")),
                    )
                    .drop(f.col(f"{src_alias}.{c}"))
                    .drop(f.col(f"{dest_alias}.{c}"))
                    .withColumnRenamed(f"newly_{c}", c)
                )

        logger.debug(f"Keep old values for columns:{df.columns}")

        return df

    @staticmethod
    def _add_scd2_columns(
        df: DataFrame,
        meta_scd2_struct_col_name: str,
        meta_scd2_effective_time_col_name: str,
        meta_scd2_end_time_col_name: str,
        meta_scd2_is_current_col_name: str,
        **_kwargs: dict,
    ) -> DataFrame:
        """
        Add SCD2 columns to the DataFrame.

        This method adds SCD2 columns to the DataFrame.
        Parameters
        ----------
        df : DataFrame
            The DataFrame to add the SCD2 columns to.
        meta_scd2_struct_col_name : str
            The name of the struct column to add.
        meta_scd2_effective_time_col_name : str
            The name of the effective time column inside the struct.
        meta_scd2_end_time_col_name : str
            The name of the end time column inside the struct.
        meta_scd2_is_current_col_name : str
            The name of the is_current column inside the struct.

        Returns
        -------
        DataFrame
            The DataFrame with the added SCD2 columns.

        """
        df = df.withColumn(
            meta_scd2_struct_col_name,
            f.struct(
                f.col("__meta_scd2_effective_time").alias(meta_scd2_effective_time_col_name),
                f.col("__meta_scd2_end_time").alias(meta_scd2_end_time_col_name),
                f.col("__meta_scd2_is_current").alias(meta_scd2_is_current_col_name),
            ),
        ).drop(
            "__meta_scd2_end_time",
            "__meta_scd2_is_current",
            "__meta_scd2_effective_time",
            "__meta_scd2_timestamp",
            "__meta_scd2_system_merge_action",
            "__meta_scd2_rn",
        )

        return df

    def _prepare_merge_builder(
        self,
        delta_table: DeltaTable,
        dest_alias: str,
        staged: DataFrame,
        merge_key: str,
        columns_to_process: List[str],
        meta_scd2_effective_time_col: str,
        **_kwargs: dict,
    ) -> DeltaMergeBuilder:
        """
        Prepare a DeltaMergeBuilder for merging data.

        This method prepares a DeltaMergeBuilder for merging data. It sets up the merge conditions and specifies
        the columns to update or insert when a match is found or not found, respectively.

        Parameters
        ----------
        delta_table : DeltaTable
            The DeltaTable to merge with.
        dest_alias : str
            The alias to use for the DeltaTable.
        staged : DataFrame
            The DataFrame to merge.
        merge_key : str
            The key to use for the merge.
        columns_to_process : List[str]
            The columns to update or insert.
        meta_scd2_effective_time_col : str
            The effective time column to use in the merge condition.

        Returns
        -------
        DeltaMergeBuilder
            The prepared DeltaMergeBuilder.

        """
        when_matched_columns = {c: f"src.{c}" for c in columns_to_process + [self.meta_scd2_struct_col_name]}

        merge_builder = (
            delta_table.alias(dest_alias)
            .merge(
                source=staged.alias("src"),
                condition=f"src.{merge_key}={dest_alias}.{merge_key}"
                f" AND src.{meta_scd2_effective_time_col}={dest_alias}.{meta_scd2_effective_time_col}",
            )
            .whenMatchedUpdate(set=when_matched_columns)  # type: ignore
            .whenNotMatchedInsert(values=when_matched_columns)  # type: ignore
        )

        return merge_builder

    def execute(self) -> None:
        """
        Execute the SCD Type 2 operation.

        This method executes the SCD Type 2 operation on the DataFrame.
        It validates the existing Delta table, prepares the merge conditions, stages the data,
        and then performs the merge operation.

        Raises
        ------
        TypeError
            If the scd2_timestamp_col is not of date or timestamp type.
            If the source DataFrame is missing any of the required merge columns.

        """
        self.df: DataFrame
        self.spark: SparkSession
        delta_table = get_delta_table_for_name(spark_session=self.spark, table_name=self.table.table_name)
        src_alias, cross_alias, dest_alias = "src", "cross", "tgt"

        # Prepare required merge columns
        required_merge_columns = [self.merge_key]

        if self.scd2_columns:
            required_merge_columns += self.scd2_columns

        if self.scd1_columns:
            required_merge_columns += self.scd1_columns

        if not all(c in self.df.columns for c in required_merge_columns):
            missing_columns = [c for c in required_merge_columns if c not in self.df.columns]
            raise TypeError(f"The source DataFrame is missing the columns: {missing_columns!r}")

        # Check that required columns are present in the source DataFrame
        if self.scd2_timestamp_col is not None:
            timestamp_col_type = self.df.select(self.scd2_timestamp_col).schema.fields[0].dataType

            if not isinstance(timestamp_col_type, (DateType, TimestampType)):
                raise TypeError(
                    f"The scd2_timestamp_col '{self.scd2_timestamp_col}' must be of date "
                    f"or timestamp type.Current type is {timestamp_col_type}"
                )

        # Prepare columns to process
        include_columns = self.include_columns if self.include_columns else self.df.columns
        exclude_columns = self.exclude_columns
        columns_to_process = [c for c in include_columns if c not in exclude_columns]

        # Constructing column names for SCD2 attributes
        meta_scd2_is_current_col = f"{self.meta_scd2_struct_col_name}.{self.meta_scd2_is_current_col_name}"
        meta_scd2_effective_time_col = f"{self.meta_scd2_struct_col_name}.{self.meta_scd2_effective_time_col_name}"
        meta_scd2_end_time_col = f"{self.meta_scd2_struct_col_name}.{self.meta_scd2_end_time_col_name}"

        # Constructing system merge action logic
        system_merge_action = f"CASE WHEN tgt.{self.merge_key} is NULL THEN 'I' "

        if updates_attrs_scd2 := self._prepare_attr_clause(
            attrs=self.scd2_columns, src_alias=src_alias, dest_alias=dest_alias
        ):
            system_merge_action += f" WHEN {updates_attrs_scd2} THEN 'UC' "

        if updates_attrs_scd1 := self._prepare_attr_clause(
            attrs=self.scd1_columns, src_alias=src_alias, dest_alias=dest_alias
        ):
            system_merge_action += f" WHEN {updates_attrs_scd1} THEN 'U' "

        system_merge_action += " ELSE NULL END"

        # Prepare the staged DataFrame
        staged = (
            self.df.withColumn(
                "__meta_scd2_timestamp",
                self._scd2_timestamp(scd2_timestamp_col=self.scd2_timestamp_col, spark=self.spark),
            )
            .transform(
                func=self._prepare_staging,
                delta_table=delta_table,
                merge_action_logic=f.expr(system_merge_action),
                meta_scd2_is_current_col=meta_scd2_is_current_col,
                columns_to_process=columns_to_process,
                src_alias=src_alias,
                dest_alias=dest_alias,
                cross_alias=cross_alias,
            )
            .transform(
                func=self._preserve_existing_target_values,
                meta_scd2_struct_col_name=self.meta_scd2_struct_col_name,
                target_auto_generated_columns=self.target_auto_generated_columns,
                src_alias=src_alias,
                cross_alias=cross_alias,
                dest_alias=dest_alias,
                logger=self.log,
            )
            .withColumn("__meta_scd2_end_time", self._scd2_end_time(meta_scd2_end_time_col=meta_scd2_end_time_col))
            .withColumn("__meta_scd2_is_current", self._scd2_is_current())
            .withColumn(
                "__meta_scd2_effective_time",
                self._scd2_effective_time(meta_scd2_effective_time_col=meta_scd2_effective_time_col),
            )
            .transform(
                func=self._add_scd2_columns,
                meta_scd2_struct_col_name=self.meta_scd2_struct_col_name,
                meta_scd2_effective_time_col_name=self.meta_scd2_effective_time_col_name,
                meta_scd2_end_time_col_name=self.meta_scd2_end_time_col_name,
                meta_scd2_is_current_col_name=self.meta_scd2_is_current_col_name,
            )
        )

        self._prepare_merge_builder(
            delta_table=delta_table,
            dest_alias=dest_alias,
            staged=staged,
            merge_key=self.merge_key,
            columns_to_process=columns_to_process,
            meta_scd2_effective_time_col=meta_scd2_effective_time_col,
        ).execute()
