"""Utilities for DataFrame transformations and nested structure handling."""

from typing import Callable

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, DataType, MapType, StructType


def _process_column(
    col_expr: Column,
    data_type: DataType,
    transform_func: Callable[[Column, DataType], Column],
    target_types: set,
) -> Column:
    """Recursively transform a column expression based on its data type.

    This function performs a depth-first traversal of nested structures, checking each
    field's data type and either applying the transformation if the type matches target_types,
    recursively traversing into nested structures (structs, arrays), or returning the column
    unchanged if no transformation is needed.

    Args:
        col_expr: A PySpark Column expression representing the current field
        data_type: The PySpark DataType of the current field
        transform_func: A function that takes (col_expr, data_type) and returns a transformed column expression
        target_types: Set of PySpark data types to target for transformation

    Returns:
        A transformed Column expression
    """
    # Use pattern matching to handle different data types
    # Match-case provides a cleaner way to handle type-specific logic
    match data_type:
        # Handle MapType fields
        # Maps are special because they contain key-value pairs (data, not just schema)
        case MapType():
            # First, check if we need to apply transformation to this map
            transformed = transform_func(col_expr, data_type) if MapType in target_types else col_expr

            # Then, check if map values contain complex types that need traversal
            # This handles cases like Map<String, Map<String, String>> or Map<String, Struct<...>>
            if isinstance(data_type.valueType, (MapType, StructType)):
                # Use F.transform_values to recursively process each map value
                transformed = F.transform_values(
                    transformed, lambda k, v: _process_column(v, data_type.valueType, transform_func, target_types)
                )

            return transformed

        # Handle StructType fields (nested records/objects)
        # Structs contain multiple named fields, so we need to traverse each field
        case StructType():
            # Always traverse struct fields to find nested target types within them
            # This ensures we don't miss maps, arrays, or other target types inside structs
            struct_fields = []
            for field in data_type.fields:
                # Handle field names with special characters (e.g., dots) using getField()
                # getField() properly handles field names that contain dots or other special characters
                # The bracket syntax col_expr[field.name] doesn't support special characters
                field_col = col_expr.getField(field.name)

                # Recursively transform each field in the struct
                transformed_field = _process_column(field_col, field.dataType, transform_func, target_types)

                # Preserve the original field name using alias
                struct_fields.append(transformed_field.alias(field.name))

            # Reconstruct the struct with all transformed fields
            return F.struct(*struct_fields)

        # Handle ArrayType fields (lists/arrays of elements)
        # Arrays contain multiple elements of the same type
        case ArrayType():
            # Check if array elements are complex types that need traversal
            if isinstance(data_type.elementType, (MapType, StructType)):
                # Use F.transform to apply our transformation to each array element
                # The lambda receives each element and recursively processes it
                return F.transform(
                    col_expr, lambda x: _process_column(x, data_type.elementType, transform_func, target_types)
                )

            # If array elements match target type directly (e.g., array of strings)
            # and we're targeting that type, apply the transformation
            elif any(isinstance(data_type.elementType, target_type) for target_type in target_types):
                return transform_func(col_expr, data_type)

            # If array elements don't need transformation, return unchanged
            return col_expr

        # Handle primitive types (String, Integer, Boolean, Timestamp, etc.)
        # These are leaf nodes in the schema tree
        case _:
            # Check if this primitive type matches any of our target types
            should_transform = any(isinstance(data_type, target_type) for target_type in target_types)

            # If this type is in our target types, apply transformation
            if should_transform:
                return transform_func(col_expr, data_type)
            # Otherwise, return the column unchanged (IMPORTANT: not dropped, just unchanged)
            # This ensures all columns/fields are preserved in the output
            return col_expr


def apply_transformation_to_nested_structures(
    df: DataFrame,
    transform_func: Callable[[Column, DataType], Column],
    target_types: set | None = None,
    column_processor: Callable[[Column, DataType], Column] | None = _process_column,
) -> DataFrame:
    """Apply a transformation function to specific data types in nested structures recursively.

    This is a generic utility that traverses nested structures (maps, structs, arrays)
    and applies the provided transformation function to columns of specified types.

    The function performs a depth-first traversal of the DataFrame schema, visiting every
    field at every nesting level. When it encounters a data type that matches the target_types,
    it applies the provided transformation function.

    Args:
        df: The DataFrame to transform.
        transform_func: A function that takes (col_expr, data_type) and returns a transformed column expression.
                       This function will be called for each column matching the target types.
                       The function receives:
                         - col_expr: A PySpark Column expression representing the current field
                         - data_type: The PySpark DataType of the current field
        target_types: Set of PySpark data types to target (e.g., {MapType} for maps only).
                     If None, applies to all complex types (MapType, StructType, ArrayType).
                     Use this to limit transformations to specific types.
        column_processor: Optional custom function to process columns recursively. Defaults to _process_column.
                         This allows you to override the default recursive traversal behavior.
                         The function signature should be:
                         (col_expr, data_type, transform_func, target_types) -> Column

    Returns:
        DataFrame: A new DataFrame with transformations applied to all matching fields.
                  The original DataFrame remains unchanged.

    Notes:
        - All columns are preserved in the output DataFrame:
          * Columns matching target_types: transformed
          * Columns NOT matching target_types: passed through unchanged
        - Handles nested structures at any nesting level
        - Recursively processes structs, arrays, and maps (including nested maps)
        - Can be used for any transformation (key renaming, value transformation, type casting, etc.)
        - StructType, ArrayType, and MapType are always traversed to find nested target types within them
        - For MapType: applies transformation to the map itself, then traverses into map values if they're complex types
        - The transformation preserves column names and nesting structure
        - Data types may change if the transformation function performs type casting

    Example:
        ```python
        # Example 1: Transform all map keys to uppercase
        def uppercase_map_keys(col_expr, data_type):
            if isinstance(data_type, MapType):
                return F.transform_keys(col_expr, lambda k, v: F.upper(k))
            return col_expr


        result_df = apply_transformation_to_nested_structures(
            df, uppercase_map_keys, target_types={MapType}
        )
        ```

        ```python
        # Example 2: Apply transformation to all string columns (including nested ones)
        def trim_strings(col_expr, data_type):
            if isinstance(data_type, StringType):
                return F.trim(col_expr)
            return col_expr


        result_df = apply_transformation_to_nested_structures(
            df, trim_strings, target_types={StringType}
        )
        ```

        ```python
        # Example 3: Custom column processor
        def custom_processor(
            col_expr, data_type, transform_func, target_types
        ):
            # Custom recursive logic here
            return _process_column(
                col_expr, data_type, transform_func, target_types
            )


        result_df = apply_transformation_to_nested_structures(
            df,
            my_transform,
            target_types={MapType},
            column_processor=custom_processor,
        )
        ```
    """
    # Set default target types if not specified
    # Default is to target all complex types (maps, structs, arrays)
    if target_types is None:
        target_types = {MapType, StructType, ArrayType}

    # Apply transformations to all top-level columns in the DataFrame
    # We iterate through each column in the DataFrame's schema and apply our recursive transformation
    transformed_cols = []
    for field in df.schema.fields:
        # Create a column expression for the current field
        # F.col with backticks handles field names with special characters (dots, spaces, etc.)
        col_expr = F.col(f"`{field.name}`")

        # Recursively transform the column and any nested structures within it
        # This will traverse the entire depth of the column's structure
        transformed_col = column_processor(col_expr, field.dataType, transform_func, target_types)

        # Preserve the original column name using alias
        # This ensures the output DataFrame has the same column names as the input
        transformed_cols.append(transformed_col.alias(field.name))

    # Create and return a new DataFrame with all transformed columns
    # The select operation constructs a new DataFrame with the transformed columns
    # The original DataFrame remains unchanged (immutability principle)
    return df.select(*transformed_cols)
