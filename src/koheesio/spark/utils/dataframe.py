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

    Parameters
    ----------
    col_expr : Column
        A PySpark Column expression representing the current field
    data_type : DataType
        The PySpark DataType of the current field
    transform_func : Callable[[Column, DataType], Column]
        A function that takes (col_expr, data_type) and returns a transformed column expression
    target_types : set
        Set of PySpark data types to target for transformation

    Returns
    -------
    Column
        A transformed Column expression
    """
    match data_type:
        case MapType():
            transformed = transform_func(col_expr, data_type) if MapType in target_types else col_expr

            # Recursively process nested map values
            if isinstance(data_type.valueType, (MapType, StructType)):
                transformed = F.transform_values(
                    transformed, lambda k, v: _process_column(v, data_type.valueType, transform_func, target_types)
                )

            return transformed

        case StructType():
            struct_fields = []
            for field in data_type.fields:
                # getField() handles field names with special characters (e.g., dots)
                field_col = col_expr.getField(field.name)
                transformed_field = _process_column(field_col, field.dataType, transform_func, target_types)
                struct_fields.append(transformed_field.alias(field.name))

            return F.struct(*struct_fields)

        case ArrayType():
            if isinstance(data_type.elementType, (MapType, StructType)):
                return F.transform(
                    col_expr, lambda x: _process_column(x, data_type.elementType, transform_func, target_types)
                )
            elif any(isinstance(data_type.elementType, target_type) for target_type in target_types):
                return transform_func(col_expr, data_type)

            return col_expr

        case _:
            # Primitive types - apply transformation if type matches, otherwise pass through unchanged
            if any(isinstance(data_type, target_type) for target_type in target_types):
                return transform_func(col_expr, data_type)
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

    Parameters
    ----------
    df : DataFrame
        The DataFrame to transform.
    transform_func : Callable[[Column, DataType], Column]
        A function that takes (col_expr, data_type) and returns a transformed column expression.
        This function will be called for each column matching the target types.
        The function receives:
          - col_expr: A PySpark Column expression representing the current field
          - data_type: The PySpark DataType of the current field
    target_types : set or None, optional
        Set of PySpark data types to target (e.g., {MapType} for maps only).
        If None, applies to all complex types (MapType, StructType, ArrayType).
        Use this to limit transformations to specific types.
    column_processor : Callable[[Column, DataType], Column] or None, optional
        Optional custom function to process columns recursively. Defaults to _process_column.
        This allows you to override the default recursive traversal behavior.
        The function signature should be:
        (col_expr, data_type, transform_func, target_types) -> Column

    Returns
    -------
    DataFrame
        A new DataFrame with transformations applied to all matching fields.
        The original DataFrame remains unchanged.

    Notes
    -----
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

    Examples
    --------
    Transform all map keys to uppercase:

    >>> def uppercase_map_keys(col_expr, data_type):
    ...     if isinstance(data_type, MapType):
    ...         return F.transform_keys(col_expr, lambda k, v: F.upper(k))
    ...     return col_expr
    >>> result_df = apply_transformation_to_nested_structures(
    ...     df, uppercase_map_keys, target_types={MapType}
    ... )

    Apply transformation to all string columns (including nested ones):

    >>> def trim_strings(col_expr, data_type):
    ...     if isinstance(data_type, StringType):
    ...         return F.trim(col_expr)
    ...     return col_expr
    >>> result_df = apply_transformation_to_nested_structures(
    ...     df, trim_strings, target_types={StringType}
    ... )

    Custom column processor:

    >>> def custom_processor(
    ...     col_expr, data_type, transform_func, target_types
    ... ):
    ...     # Custom recursive logic here
    ...     return _process_column(
    ...         col_expr, data_type, transform_func, target_types
    ...     )
    >>> result_df = apply_transformation_to_nested_structures(
    ...     df,
    ...     my_transform,
    ...     target_types={MapType},
    ...     column_processor=custom_processor,
    ... )
    """
    if target_types is None:
        target_types = {MapType, StructType, ArrayType}

    transformed_cols = []
    for field in df.schema.fields:
        # Backticks handle field names with special characters
        col_expr = F.col(f"`{field.name}`")
        transformed_col = column_processor(col_expr, field.dataType, transform_func, target_types)
        transformed_cols.append(transformed_col.alias(field.name))

    return df.select(*transformed_cols)
