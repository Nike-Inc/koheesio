"""Base class and utilities for schema-transforming DataFrame operations.

This module provides the SchemaTransformation base class which handles
recursive schema traversal and transformation, including nested StructTypes
and ArrayTypes.

Classes
-------
SchemaTransformation
    Base class for transformations that modify DataFrame schemas.
"""

from typing import Callable, List, Optional, Set, Tuple, Union
from abc import ABC

from pyspark.sql import functions as sf
from pyspark.sql.types import ArrayType, DataType, MapType, StructField, StructType

from koheesio.models import Field, ListOfColumns
from koheesio.spark.transformations import Transformation
from koheesio.spark.utils.common import DataFrame


class SchemaTransformation(Transformation, ABC):
    """Base class for transformations that modify DataFrame schemas.

    This class provides utilities for recursively traversing and transforming
    schemas, including nested StructTypes, ArrayTypes, and MapTypes.

    Subclasses must implement the `execute` method and can use the provided
    schema utilities for their transformation logic.

    Parameters
    ----------
    columns : Optional[ListOfColumns]
        The column or list of columns to apply the transformation to.
        If not provided, all columns will be processed.
        Alias: column

    Methods
    -------
    process_schema(schema, name_func, type_func, columns)
        Recursively process a schema, transforming field names and/or types.

    build_select_expressions(target_schema, source_columns, column_mapping)
        Build select expressions to transform DataFrame to target schema.

    Example
    -------
    ```python
    class MySchemaTransform(SchemaTransformation):
        def execute(self):
            # Use process_schema to transform the schema
            new_schema = self.process_schema(
                self.df.schema,
                name_func=lambda name: name.lower(),
            )
            # Apply transformation...


    # Transform all columns
    transform = MySchemaTransform()

    # Transform only specific columns
    transform = MySchemaTransform(columns=["col1", "col2"])
    ```
    """

    columns: ListOfColumns | None = Field(
        default=None,
        alias="column",
        description="The column (or list of columns) to apply the transformation to. "
        "If not provided, all columns will be processed. Alias: column",
    )

    def process_schema(
        self,
        schema: StructType,
        name_func: Optional[Callable[[str], str]] = None,
        type_func: Optional[Callable[[StructField], DataType]] = None,
        columns: Optional[List[str]] = None,
    ) -> StructType:
        """Recursively process a schema, transforming field names and/or types.

        This method handles nested structures:
        - StructTypes: Recursively processes nested struct fields
        - ArrayTypes containing StructTypes: Recursively processes array element schemas
        - MapTypes containing StructTypes: Recursively processes map value schemas

        Parameters
        ----------
        schema : StructType
            The schema to process.
        name_func : Optional[Callable[[str], str]]
            Function to transform field names. If None, names are unchanged.
        type_func : Optional[Callable[[StructField], DataType]]
            Function to transform field types. Receives the full StructField
            and returns the new DataType. If None, types are unchanged.
            Note: For nested structures, this is applied AFTER recursive processing.
        columns : Optional[List[str]]
            List of column names to process at the root level. If None, uses
            self.get_columns() which returns self.columns if set, otherwise all columns.
            This parameter only affects root-level fields; nested fields are always
            processed recursively.

        Returns
        -------
        StructType
            A new schema with transformed field names and/or types.

        Example
        -------
        ```python
        # Rename all fields to snake_case
        new_schema = self.process_schema(
            df.schema,
            name_func=to_snake_case,
        )

        # Rename only specific columns
        new_schema = self.process_schema(
            df.schema,
            name_func=to_snake_case,
            columns=["col1", "col2"],
        )


        # Cast all fields based on custom logic
        def my_type_func(field: StructField) -> DataType:
            if field.name == "id":
                return IntegerType()
            return field.dataType


        new_schema = self.process_schema(
            df.schema,
            type_func=my_type_func,
        )
        ```
        """
        # Determine which columns to process at root level
        # Use passed columns parameter if provided, otherwise use self.columns
        # columns=[] means "process all" (used in recursive calls for nested structures)
        columns_filter = columns if columns is not None else self.columns
        new_fields = []

        for field in schema.fields:
            # If columns filter is set (non-empty), only transform matching fields
            if columns_filter and field.name not in columns_filter:
                # Keep the field unchanged
                new_fields.append(field)
            else:
                new_name = name_func(field.name) if name_func else field.name
                new_type = self._process_field_type(field, name_func, type_func)
                new_fields.append(StructField(new_name, new_type, field.nullable, field.metadata))

        return StructType(new_fields)

    def _process_field_type(
        self,
        field: StructField,
        name_func: Optional[Callable[[str], str]],
        type_func: Optional[Callable[[StructField], DataType]],
    ) -> DataType:
        """Process a field's data type, handling nested structures recursively.

        Parameters
        ----------
        field : StructField
            The field whose type should be processed.
        name_func : Optional[Callable[[str], str]]
            Function to transform field names (passed to nested calls).
        type_func : Optional[Callable[[StructField], DataType]]
            Function to transform field types.

        Returns
        -------
        DataType
            The processed data type.
        """
        data_type = field.dataType

        # Handle nested StructType
        # Pass columns=[] to process all nested fields (filter only applies at root level)
        if isinstance(data_type, StructType):
            return self.process_schema(data_type, name_func, type_func, columns=[])

        # Handle ArrayType containing StructType
        if isinstance(data_type, ArrayType) and isinstance(data_type.elementType, StructType):
            nested_schema = self.process_schema(data_type.elementType, name_func, type_func, columns=[])
            return ArrayType(nested_schema, data_type.containsNull)

        # Handle MapType containing StructType values
        if isinstance(data_type, MapType) and isinstance(data_type.valueType, StructType):
            nested_schema = self.process_schema(data_type.valueType, name_func, type_func, columns=[])
            return MapType(data_type.keyType, nested_schema, data_type.valueContainsNull)

        # For non-nested types, apply type_func if provided
        if type_func:
            return type_func(field)

        return data_type

    def build_select_expressions(
        self,
        target_schema: Union[StructType, List[Tuple[str, DataType]]],
        source_columns: ListOfColumns,
        column_mapping: Optional[Callable[[str], str]] = None,
    ) -> List:
        """Build select expressions to transform a DataFrame to match a target schema.

        This method creates Column expressions that can be used with df.select()
        to transform a DataFrame. It handles:
        - Casting existing columns to target types
        - Adding missing columns as NULL with correct type
        - Column name mapping (for renames)

        Parameters
        ----------
        target_schema : Union[StructType, List[Tuple[str, DataType]]]
            The target schema, either as a StructType or list of (name, type) tuples.
        source_columns : List[str]
            List of column names available in the source DataFrame.
        column_mapping : Optional[Callable[[str], str]]
            Function that maps target column names to source column names.
            If None, assumes same names (identity mapping).

        Returns
        -------
        List
            List of Column expressions for use with df.select().

        Example
        -------
        ```python
        # Build expressions to cast to target schema
        exprs = self.build_select_expressions(
            target_schema=target_df.schema,
            source_columns=self.df.columns,
        )
        self.output.df = self.df.select(*exprs)
        ```
        """
        # Normalize target_schema to list of tuples
        if isinstance(target_schema, StructType):
            schema_tuples = [(f.name, f.dataType) for f in target_schema.fields]
        else:
            schema_tuples = target_schema

        expressions = []
        mapping = column_mapping or (lambda x: x)

        for target_name, target_type in schema_tuples:
            source_name = mapping(target_name)

            if source_name in source_columns:
                expr = sf.col(source_name).cast(target_type).alias(target_name)
            else:
                expr = sf.lit(None).cast(target_type).alias(target_name)

            expressions.append(expr)

        return expressions
