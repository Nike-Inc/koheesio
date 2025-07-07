from __future__ import annotations

from typing import Callable

from pydantic import Field

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructField, StructType

from koheesio.spark import DataFrame
from koheesio.spark.transformations import ColumnsTransformation


class RenameColumns(ColumnsTransformation):
    rename_func:Callable[[str], str] = Field(..., description="Function to rename columns")

    def rename_schema(self, schema: StructType):
        """Renames the fields of a given schema using a specified renaming function.
        Args:
            schema: The schema whose fields need to be renamed.
        Returns:
            StructType: A new schema with renamed fields.
        Notes:
            - If the renaming function is not provided, it defaults to `RenameColumns.camel_to_snake`.
            - The function handles nested StructTypes and ArrayTypes containing StructTypes.

        Steps:
            1. Initialize an empty list to hold the new fields.
            2. Determine the renaming function to use, defaulting to `RenameColumns.camel_to_snake` if not provided.
            3. Iterate over each field in the provided schema.
            4. Rename the field using the renaming function.
            5. Check if the field's data type is a StructType:
                - Recursively rename the schema of the nested StructType.
            6. Check if the field's data type is an ArrayType containing a StructType:
                - Recursively rename the schema of the nested StructType within the ArrayType.
            7. For other data types, simply rename the field.
            8. Append the newly created field to the list of new fields.
            9. Return a new StructType constructed from the list of new fields.
        """
        new_fields = []
        _columns= list(self.get_columns())
        
        if self.rename_func is None:
            raise ValueError("rename_func must be provided")

        for field in schema.fields:
            if field.name in _columns:
                new_name = self.rename_func(field.name)

                if isinstance(field.dataType, StructType):
                    new_field = StructField(new_name, self.rename_schema(field.dataType), field.nullable)
                elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                    new_field = StructField(
                        new_name, ArrayType(self.rename_schema(field.dataType.elementType)), field.nullable
                    )
                else:
                    new_field = StructField(new_name, field.dataType, field.nullable)
                new_fields.append(new_field)
            else:
                # If the field is not in the columns to be renamed, keep it as is
                new_fields.append(field)

        return StructType(new_fields)

    def execute(self):
        self.df: DataFrame
        new_schema = self.rename_schema(self.df.schema) # pylint: disable=E1102
        _columns= list(self.get_columns())
        _not_renamed= set(self.df.columns) - set(_columns)
        renamed_select=[ F.col(c).cast(new_schema[self.rename_func(c)].dataType).alias(self.rename_func(c))  # pylint: disable=E1102 
                        for c in _columns]
        not_renamed_select= [ F.col(c).alias(c) for c in _not_renamed]
    
        # Apply the new schema by casting each column to the new type
        self.output.df = self.df.select(renamed_select+not_renamed_select)