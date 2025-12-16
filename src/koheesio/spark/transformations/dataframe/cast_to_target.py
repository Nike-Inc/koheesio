"""This class provides the CastToTarget transformation to cast a DataFrame to match a target schema."""

from typing import Union

from pyspark.sql.types import StructType

from koheesio.models import Field, InstanceOf
from koheesio.spark.readers import Reader
from koheesio.spark.transformations.dataframe.schema import SchemaTransformation
from koheesio.spark.utils.common import DataFrame


class CastToTarget(SchemaTransformation):
    """Cast a DataFrame to match a given target schema.

    This transformation can be used to cast the input DataFrame to match a given
    target schema. The behavior is the following:

    - Fields not present in the target schema will be dropped
    - Fields missing from the input schema but present in the target will be added
      with null values but proper type
    - Fields are cast to the type specified in the target schema
    - Nested structures (StructTypes, ArrayTypes) are handled recursively

    The target schema can be provided as one of Reader, DataFrame or StructType.

    Parameters
    ----------
    target : Union[Reader, DataFrame, StructType]
        The target schema that the input DataFrame should be cast to.

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
    output_df = transformation.transform()
    # output_df now matches the target table's schema
    ```
    """

    target: Union[InstanceOf[Reader], DataFrame, StructType] = Field(
        ...,
        description="The target schema that the input dataframe should be casted to, provided as one of Reader, "
        "DataFrame or StructType.",
    )

    def execute(self) -> "CastToTarget.Output":
        """Execute the transformation to cast the DataFrame to the target schema."""
        target_schema = self._get_schema()
        self.log.debug(f"Casting to target schema: {target_schema}")

        # Build select expressions using the base class utility
        expressions = self.build_select_expressions(target_schema=target_schema, source_columns=self.df.columns)
        self.output.df = self.df.select(*expressions)

    def _get_schema(self) -> list:
        """Extract the schema from the target as a list of (name, dataType) tuples.

        Returns
        -------
        list
            List of tuples containing (column_name, column_type) for each field.
        """
        target = self.target

        if isinstance(self.target, Reader):
            target = self.target.read().schema

        if isinstance(self.target, DataFrame):
            target = self.target.schema

        return [(field.name, field.dataType) for field in target]
