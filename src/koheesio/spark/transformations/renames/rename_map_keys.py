"""Transformation for renaming map keys in DataFrame columns."""

from typing import Callable, Optional

from pydantic import Field, PrivateAttr

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType

from koheesio.spark.transformations import Transformation
from koheesio.spark.utils.dataframe import apply_transformation_to_nested_structures
from koheesio.spark.utils.string import AnyToSnakeConverter


class RenameMapKeys(Transformation):
    """Rename map keys in DataFrame columns using a custom renaming function.

    This transformation renames all map keys (including nested maps within structs
    and arrays) using the provided renaming function. By default, it converts any
    naming convention to snake_case (camelCase, PascalCase, Ada_Case, kebab-case,
    CONSTANT_CASE, etc.).

    The transformation only affects map keys (data), not column names (schema).
    All non-map columns are preserved unchanged.

    Parameters
    ----------
    rename_func : Optional[Callable[[str], str]]
        Custom function to rename map keys.
        Defaults to AnyToSnakeConverter().convert if not provided.

    Example
    -------
    ```python
    # DataFrame with map column containing {"firstName": "John", "lastName": "Doe"}
    renamer = RenameMapKeys()
    renamer.df = input_df
    renamer.execute()

    # Result: map column containing {"first_name": "John", "last_name": "Doe"}
    ```
    """

    _converter: AnyToSnakeConverter = PrivateAttr(default_factory=AnyToSnakeConverter)

    rename_func: Optional[Callable] = Field(
        default=None,
        description="Function to rename map keys. Defaults to AnyToSnakeConverter().convert if not provided.",
    )

    def rename_map_keys(self, df: DataFrame) -> DataFrame:
        """Rename map keys in all map columns recursively using the renaming function.

        Parameters
        ----------
        df : DataFrame
            The DataFrame whose map keys need to be renamed.

        Returns
        -------
        DataFrame
            A new DataFrame with renamed map keys.

        Notes
        -----
        - Handles MapType at any nesting level (in structs, arrays, or nested maps)
        - Uses transform_keys to rename map keys
        - apply_transformation_to_nested_structures handles ALL nested traversal:
          * Maps within structs/arrays
          * Maps within map values (e.g., Map<String, Map<String, String>>)
        - All non-map columns are preserved unchanged
        """
        rename_column_func = self.rename_func or self._converter.convert

        def transform_func(col_expr, _):
            return F.transform_keys(col_expr, lambda k, v: rename_column_func(k))

        return apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

    def execute(self) -> None:
        """Execute the map key renaming transformation.

        This method applies the renaming function to all map keys in the DataFrame,
        preserves all non-map columns unchanged, and sets the output DataFrame.
        """
        self.df: DataFrame
        self.output.df = self.rename_map_keys(self.df)
