"""
This module defines the DeltaTableWriter class, which is used to write both batch and streaming dataframes
to Delta tables.

DeltaTableWriter supports two output modes: `MERGEALL` and `MERGE`.

- The `MERGEALL` mode merges all incoming data with existing data in the table based on certain conditions.
- The `MERGE` mode allows for more custom merging behavior using the DeltaMergeBuilder class from the `delta.tables`
  library.

The `output_mode_params` dictionary is used to specify conditions for merging, updating, and inserting data.
The `target_alias` and `source_alias` keys are used to specify the aliases for the target and source dataframes in the
merge conditions.

Classes
-------
DeltaTableWriter
    A class for writing data to Delta tables.
DeltaTableStreamWriter
    A class for writing streaming data to Delta tables.

Example
-------
```python
DeltaTableWriter(
    table="test_table",
    output_mode=BatchOutputMode.MERGEALL,
    output_mode_params={
        "merge_cond": "target.id=source.id",
        "update_cond": "target.col1_val>=source.col1_val",
        "insert_cond": "source.col_bk IS NOT NULL",
    },
)
```
"""

from typing import Callable, Dict, List, Optional, Set, Type, Union
from functools import partial

from delta.tables import DeltaMergeBuilder
from py4j.protocol import Py4JError

from pyspark.sql import DataFrameWriter

from koheesio.models import ExtraParamsMixin, Field, field_validator
from koheesio.spark.delta import DeltaTableStep
from koheesio.spark.utils import on_databricks
from koheesio.spark.writers import BatchOutputMode, StreamingOutputMode, Writer
from koheesio.spark.writers.delta.utils import get_delta_table_for_name, log_clauses


class DeltaTableWriter(Writer, ExtraParamsMixin):
    """Delta table Writer for both batch and streaming dataframes.

    Example
    -------
    ### Example for `MERGEALL`
    ```python
    DeltaTableWriter(
        table="test_table",
        output_mode=BatchOutputMode.MERGEALL,
        output_mode_params={
            "merge_cond": "target.id=source.id",
            "update_cond": "target.col1_val>=source.col1_val",
            "insert_cond": "source.col_bk IS NOT NULL",
            "target_alias": "target",  # <------ DEFAULT, can be changed by providing custom value
            "source_alias": "source",  # <------ DEFAULT, can be changed by providing custom value
        },
    )
    ```

    ### Example for `MERGE`
    ```python
    DeltaTableWriter(
        table="test_table",
        output_mode=BatchOutputMode.MERGE,
        output_mode_params={
            'merge_builder': (
                DeltaTable
                .forName(sparkSession=spark, tableOrViewName=<target_table_name>)
                .alias(target_alias)
                .merge(source=df, condition=merge_cond)
                .whenMatchedUpdateAll(condition=update_cond)
                .whenNotMatchedInsertAll(condition=insert_cond)
                )
            }
        )
    ```

    ### Example for `MERGE`
    in case the table isn't created yet, first run will execute an APPEND operation
    ```python
    DeltaTableWriter(
        table="test_table",
        output_mode=BatchOutputMode.MERGE,
        output_mode_params={
            "merge_builder": [
                {
                    "clause": "whenMatchedUpdate",
                    "set": {"value": "source.value"},
                    "condition": "<update_condition>",
                },
                {
                    "clause": "whenNotMatchedInsert",
                    "values": {
                        "id": "source.id",
                        "value": "source.value",
                    },
                    "condition": "<insert_condition>",
                },
            ],
            "merge_cond": "<merge_condition>",
        },
    )
    ```

    ### Example for APPEND
    dataframe writer options can be passed as keyword arguments
    ```python
    DeltaTableWriter(
        table="test_table",
        output_mode=BatchOutputMode.APPEND,
        partitionOverwriteMode="dynamic",
        mergeSchema="false",
    )
    ```

    Parameters
    ----------
    table : Union[DeltaTableStep, str]
        The table to write to
    output_mode : Optional[Union[str, BatchOutputMode, steps.writers.StreamingOutputMode]]
        The output mode to use. Default is BatchOutputMode.APPEND. For streaming, use StreamingOutputMode.
    params : Optional[dict]
        Additional parameters to use for specific mode
    """

    table: Union[DeltaTableStep, str] = Field(default=..., description="The table to write to")
    output_mode: Optional[Union[BatchOutputMode, StreamingOutputMode]] = Field(
        default=BatchOutputMode.APPEND,
        alias="outputMode",
        description=f"{BatchOutputMode.__doc__}\n{StreamingOutputMode.__doc__}",
    )
    params: dict = Field(
        default_factory=dict,
        alias="output_mode_params",
        description="Additional parameters to use for specific mode",
    )

    partition_by: Optional[List[str]] = Field(
        default=None, alias="partitionBy", description="The list of fields to partition the Delta table on"
    )
    format: str = "delta"  # The format to use for writing the dataframe to the Delta table

    _merge_builder: Optional[DeltaMergeBuilder] = None

    # noinspection PyProtectedMember
    def __merge(self, merge_builder: Optional[DeltaMergeBuilder] = None) -> Union[DeltaMergeBuilder, DataFrameWriter]:
        """Merge dataframes using DeltaMergeBuilder or DataFrameWriter"""
        builder, writer = None, None

        if self.table.exists:
            merge_builder = self._get_merge_builder(merge_builder)
            from koheesio.spark.utils.connect import is_remote_session

            if on_databricks() and not is_remote_session():
                try:
                    source_alias = merge_builder._jbuilder.getMergePlan().source().alias()
                    target_alias = merge_builder._jbuilder.getMergePlan().target().alias()
                    self.log.debug(
                        f"The following aliases are used during Merge operation: source={source_alias}, target={target_alias}"
                    )
                    patched__log_clauses = partial(log_clauses, source_alias=source_alias, target_alias=target_alias)
                    self.log.debug(
                        patched__log_clauses(clauses=merge_builder._jbuilder.getMergePlan().matchedClauses())
                    )
                    self.log.debug(
                        patched__log_clauses(clauses=merge_builder._jbuilder.getMergePlan().notMatchedClauses())
                    )
                    self.log.debug(
                        patched__log_clauses(clauses=merge_builder._jbuilder.getMergePlan().notMatchedBySourceClauses())
                    )
                except Py4JError:
                    self.log.debug("Merge plan has not been received, skipping logging of merge details")

            builder = merge_builder

        else:
            writer = self.df

            if self.output_mode == BatchOutputMode.MERGEALL.value:
                writer = writer.alias(self.params.get("source_alias", "source"))
                insert_cond = self.params.get("insert_cond")
                filter_cond = insert_cond if insert_cond else "1=1"
                self.log.info(
                    f"Table `{self.table.table_name}` doesn't exist. Using `append` instead of `{self.output_mode}`."
                    f"Data will be filtered using filter from whenNotMatchedInsertAll: `{filter_cond}`"
                )
                writer = writer.filter(filter_cond)
            elif self.output_mode == BatchOutputMode.MERGE.value:
                self.log.info(
                    f"Table `{self.table.table_name}` doesn't exist. Using `append` instead of `{self.output_mode}`."
                    f"Insert filter from DeltaMergeBuilder will NOT be applied."
                    f"As workaround, create Delta table beforehand."
                )
            else:
                raise ValueError(f"Unsupported output_mode `{self.output_mode}` for merge operation.")

            writer = writer.write.format("delta").mode("append")

        return builder or writer

    def __merge_all(self) -> Union[DeltaMergeBuilder, DataFrameWriter]:
        """Merge dataframes using DeltaMergeBuilder or DataFrameWriter"""
        if (merge_cond := self.params.get("merge_cond")) is None:
            raise ValueError(
                "Provide `merge_cond` in DeltaTableWriter(output_mode_params={'merge_cond':'<str or Column>'})"
            )

        update_cond = self.params.get("update_cond", None)
        insert_cond = self.params.get("insert_cond", None)
        target_alias = self.params.get("target_alias", "target")
        source_alias = self.params.get("target_alias", "source")

        if self.table.exists:
            builder = (
                get_delta_table_for_name(spark_session=self.spark, table_name=self.table.table_name)
                .alias(target_alias)
                .merge(source=self.df.alias(source_alias), condition=merge_cond)
                .whenMatchedUpdateAll(condition=update_cond)
                .whenNotMatchedInsertAll(condition=insert_cond)
            )
        else:
            builder = None

        return self.__merge(merge_builder=builder)

    def _get_merge_builder(self, provided_merge_builder: DeltaMergeBuilder = None) -> "DeltaMergeBuilder":
        """Resolves the merge builder. If provided, it will be used, otherwise it will be created from the args"""

        # A merge builder has been already created - case for merge_all
        if provided_merge_builder:
            self._merge_builder = provided_merge_builder
            return provided_merge_builder

        if self._merge_builder:
            return self._merge_builder

        merge_builder = self.params.get("merge_builder", None)

        # Merge builder can be built in two ways: directly from the Delta table (if it already exists), or from the
        # provided args
        if merge_builder:
            if isinstance(merge_builder, DeltaMergeBuilder):
                return merge_builder

            if type(merge_builder).__name__ == "DeltaMergeBuilder":
                # This check is to account for the case when the merge_builder is not a DeltaMergeBuilder instance, but
                # still a compatible object
                return merge_builder  # type: ignore

            if isinstance(merge_builder, list) and "merge_cond" in self.params:  # type: ignore
                return self._merge_builder_from_args()

        raise ValueError(
            "Provide `merge_builder` in "
            "DeltaTableWriter(output_mode_params={'merge_builder': DeltaMergeBuilder()}). "
            "See documentation for options."
        )

    def _merge_builder_from_args(self) -> DeltaMergeBuilder:
        """Creates the DeltaMergeBuilder from the provided configuration"""
        merge_clauses = self.params.get("merge_builder", None)
        merge_cond = self.params.get("merge_cond", None)
        source_alias = self.params.get("source_alias", "source")
        target_alias = self.params.get("target_alias", "target")

        builder = (
            get_delta_table_for_name(spark_session=self.spark, table_name=self.table.table_name)
            .alias(target_alias)
            .merge(self.df.alias(source_alias), merge_cond)
        )

        for merge_clause in merge_clauses:
            _merge_clause = merge_clause.copy()
            clause_type = _merge_clause.pop("clause", None)
            self.log.debug(f"Adding {clause_type} clause to the merge builder")
            method = getattr(builder, clause_type)
            builder = method(**_merge_clause)

        return builder

    @field_validator("output_mode")
    def _validate_output_mode(cls, mode: Union[str, BatchOutputMode, StreamingOutputMode]) -> str:
        """Validate `output_mode` value"""
        if isinstance(mode, str):
            mode = cls.get_output_mode(mode, options={StreamingOutputMode, BatchOutputMode})

        if not isinstance(mode, BatchOutputMode) and not isinstance(mode, StreamingOutputMode):
            raise AttributeError(
                f"""
                Invalid outputMode specified '{mode}'. Allowed values are:
                Batch Mode - {BatchOutputMode.__doc__}
                Streaming Mode - {StreamingOutputMode.__doc__}
                """
            )

        return str(mode.value)

    @field_validator("table")
    def _validate_table(cls, table: Union[DeltaTableStep, str]) -> Union[DeltaTableStep, str]:
        """Validate `table` value"""
        if isinstance(table, str):
            return DeltaTableStep(table=table)
        return table

    @field_validator("params")
    def _validate_params(cls, params: dict) -> dict:
        """Validates params. If an array of merge clauses is provided, they will be validated against the available
        ones in DeltaMergeBuilder"""

        valid_clauses = {m for m in dir(DeltaMergeBuilder) if m.startswith("when")}

        if "merge_builder" in params:
            merge_builder = params["merge_builder"]
            if isinstance(merge_builder, list):
                for merge_conf in merge_builder:
                    clause = merge_conf.get("clause")
                    if clause not in valid_clauses:
                        raise ValueError(f"Invalid merge clause '{clause}' provided")
            elif not (
                isinstance(merge_builder, DeltaMergeBuilder) or type(merge_builder).__name__ == "DeltaMergeBuilder"
            ):
                raise ValueError("merge_builder must be a list of merge clauses or a DeltaMergeBuilder instance")

        return params

    @classmethod
    def get_output_mode(cls, choice: str, options: Set[Type]) -> Union[BatchOutputMode, StreamingOutputMode]:
        """Retrieve an OutputMode by validating `choice` against a set of option OutputModes.

        Currently supported output modes can be found in:

        - BatchOutputMode
        - StreamingOutputMode
        """
        from koheesio.spark.utils.connect import is_remote_session

        if (
            choice.upper() in (BatchOutputMode.MERGEALL, BatchOutputMode.MERGE_ALL, BatchOutputMode.MERGE)
            and is_remote_session()
        ):
            raise RuntimeError(f"Output mode {choice.upper()} is not supported in remote mode")

        for enum_type in options:
            if choice.upper() in [om.value.upper() for om in enum_type]:  # type: ignore
                return getattr(enum_type, choice.upper())
        raise AttributeError(
            f"""
            Invalid outputMode specified '{choice}'. Allowed values are:
            Batch Mode - {BatchOutputMode.__doc__}
            Streaming Mode - {StreamingOutputMode.__doc__}
            """
        )

    def __data_frame_writer(self) -> DataFrameWriter:
        """Specify DataFrameWriter for batch mode"""
        _writer = self.df.write.format(self.format)
        if self.partition_by:
            _writer = _writer.partitionBy(self.partition_by)
        return _writer.mode(self.output_mode)

    @property
    def writer(self) -> Union[DeltaMergeBuilder, DataFrameWriter]:
        """Specify DeltaTableWriter"""
        map_mode_to_writer: Dict[str, Callable] = {
            BatchOutputMode.MERGEALL.value: self.__merge_all,
            BatchOutputMode.MERGE.value: self.__merge,
        }
        return map_mode_to_writer.get(self.output_mode, self.__data_frame_writer)()  # type: ignore

    def execute(self) -> Writer.Output:
        _writer = self.writer

        if self.table.create_if_not_exists and not self.table.exists:
            _writer = _writer.options(**self.table.default_create_properties)
            message = (
                f"Table `{self.table}` doesn't exist. The `create_if_not_exists` flag is set to True. "
                "Therefore the table will be created."
            )
            self.log.info(message)
        if isinstance(_writer, DeltaMergeBuilder) or type(_writer).__name__ == "DeltaMergeBuilder":
            _writer.execute()
        else:
            if options := self.params:
                # should we add options only if mode is not merge?
                _writer = _writer.options(**options)
            _writer.saveAsTable(self.table.table_name)
