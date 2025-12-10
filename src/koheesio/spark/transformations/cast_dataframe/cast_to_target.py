"""This class provides the CastToTarget transformation to cast a DataFrame to match a target schema."""

from typing import Union

from pyspark.sql import functions as sf
from pyspark.sql.types import StructType

from koheesio.models import Field, InstanceOf
from koheesio.spark.readers import Reader
from koheesio.spark.transformations import Transformation
from koheesio.spark.utils.common import DataFrame


class CastToTarget(Transformation):
    """
        This transformation can be used to cast the input DataFrame to match a given target schema. The behavior is the
        following:
        - fields not present in the target schema will be dropped
        - fields missing from the input schema but present in the target one, will be added with null values but proper
          type
        - in general, this transformation will always cast a given input field to the type specified in the target

        The target schema can be provided as one of Reader, DataFrame or StructType.


    Example
    -------
    ```python
    from koheesio.spark.readers.delta import DeltaTableReader

    input_df = spark.createDataFrame(
        [
            ("val1", 123, 45.67, "extra1"),
        ],
        "col1:string, col2:int, col3:float, col6:string",
    )
    delta_reader = DeltaTableReader(
        database="some_db", table="some_table"
    )

    transformation = CastToTarget(target=delta_reader, df=input_df)
    ```
    """

    target: Union[InstanceOf[Reader], DataFrame, StructType] = Field(
        ...,
        description="The target schema that the input dataframe should be casted to, provided as one of Reader, "
        "Dataframe or StructType.",
    )

    def execute(self) -> "CastToTarget.Output":
        # Get the target columns
        target_schema = self._get_schema()
        self.log.info(f"Casting to target schema: {target_schema}")

        input_cols = self.df.columns

        # Filter the input dataframe to only include columns present in the target table
        columns_to_select = list(
            (
                sf.col(column_name).cast(column_type)
                if column_name in input_cols
                else sf.lit(None).alias(column_name).cast(column_type)
            )
            for (column_name, column_type) in target_schema
        )

        self.output.df = self.df.select(*columns_to_select)

    def _get_schema(self) -> StructType:
        target = self.target

        if isinstance(self.target, Reader):
            target = self.target.read().schema

        if isinstance(self.target, DataFrame):
            target = self.target.schema

        schema = [(field.name, field.dataType) for field in target]
        return schema
