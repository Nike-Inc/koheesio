"""Transformation for renaming both DataFrame columns and map keys."""

from __future__ import annotations

from koheesio.spark.transformations.renames.rename_columns import RenameColumns
from koheesio.spark.transformations.renames.rename_map_keys import RenameMapKeys


class RenameColumnsMapKeys(RenameColumns, RenameMapKeys):
    """Rename both DataFrame column names and map keys using a custom renaming function.

    This transformation inherits from both RenameColumns and RenameMapKeys to provide
    a combined transformation that renames:

    1. All column names (including nested fields in structs and arrays) - from RenameColumns
    2. All map keys (including maps in nested structures) - from RenameMapKeys

    By default, it converts any naming convention to snake_case for both columns and
    map keys (camelCase, PascalCase, Ada_Case, kebab-case, CONSTANT_CASE, etc.).
    Both transformations share the same rename_func, ensuring consistent renaming.

    Parameters
    ----------
    rename_func : Optional[Callable[[str], str]]
        Custom function to rename both columns and map keys.
        Defaults to AnyToSnakeConverter().convert if not provided.

    Notes
    -----
    - The rename_func is shared between both parent classes
    - Column renaming happens first, then map key renaming
    - This ensures the renamed columns are used when accessing map data

    Example
    -------
    ```python
    # DataFrame with column "userData" containing map {"firstName": "John", "lastName": "Doe"}
    renamer = RenameColumnsMapKeys()
    renamer.df = input_df
    renamer.execute()

    # Result: column "user_data" containing map {"first_name": "John", "last_name": "Doe"}
    ```
    """

    def execute(self) -> None:
        """Execute both column and map key renaming transformations.

        Renames column names first using RenameColumns, then renames map keys
        using RenameMapKeys. The execution order ensures the correct column names
        are used when renaming map keys.
        """
        # Step 1: Rename columns using RenameColumns functionality
        # We call the parent class execute directly to avoid multiple inheritance issues
        RenameColumns.execute(self)

        # Step 2: Rename map keys in the already-renamed columns
        # Use the output from step 1 as input to step 2
        self.output.df = RenameMapKeys.rename_map_keys(self, self.output.df)
