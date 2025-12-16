"""Transformation for renaming DataFrame columns using a custom function."""

from typing import Callable, Optional

from pydantic import Field, PrivateAttr

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from koheesio.spark.transformations.dataframe.schema import SchemaTransformation
from koheesio.spark.utils.string import AnyToSnakeConverter


class RenameColumns(SchemaTransformation):
    """Rename DataFrame column names using a custom renaming function.

    This transformation renames all column names (including nested fields in
    structs and arrays) using the provided renaming function. By default, it
    converts any naming convention to snake_case (camelCase, PascalCase,
    Ada_Case, kebab-case, CONSTANT_CASE, etc.) using AnyToSnakeConverter.

    This class inherits from SchemaTransformation which provides utilities for
    recursively processing schemas with nested structures.

    Parameters
    ----------
    rename_func : Optional[Callable[[str], str]]
        Custom function to rename columns. Defaults to AnyToSnakeConverter().convert
        if not provided.

    Example
    -------
    ```python
    from koheesio.spark.transformations.renames import RenameColumns

    # Using default snake_case conversion
    transform = RenameColumns()
    output_df = transform.transform(input_df)

    # Using custom rename function
    transform = RenameColumns(rename_func=str.upper)
    output_df = transform.transform(input_df)
    ```
    """

    _converter: AnyToSnakeConverter = PrivateAttr(default_factory=AnyToSnakeConverter)

    rename_func: Optional[Callable[[str], str]] = Field(
        default=None,
        description="Function to rename columns. Defaults to AnyToSnakeConverter().convert if not provided.",
    )

    def _get_rename_func(self) -> Callable[[str], str]:
        """Get the renaming function to use.

        Returns
        -------
        Callable[[str], str]
            The renaming function.
        """
        return self.rename_func or self._converter.convert

    def rename_schema(self, schema) -> "StructType":
        """Rename the fields of a given schema using the renaming function.

        This method uses the base class's process_schema utility to recursively
        process the schema, handling nested StructTypes and ArrayTypes.

        Parameters
        ----------
        schema : StructType
            The schema whose fields need to be renamed.

        Returns
        -------
        StructType
            A new schema with renamed fields.
        """
        return self.process_schema(
            schema=schema,
            name_func=self._get_rename_func(),
        )

    def execute(self) -> None:
        """Execute the column renaming transformation.

        This method:
        1. Renames the schema using the renaming function (handles nested structures)
        2. Applies the new schema to the DataFrame by casting and aliasing columns
        3. Sets the output DataFrame
        """
        self.df: DataFrame
        new_schema = self.rename_schema(self.df.schema)
        renaming_func = self._get_rename_func()

        # Apply the new schema by casting each column to the new type
        self.output.df = self.df.select(
            [F.col(c).cast(new_schema[renaming_func(c)].dataType).alias(renaming_func(c)) for c in self.df.columns]
        )
