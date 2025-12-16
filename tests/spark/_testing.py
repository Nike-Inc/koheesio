"""
Testing utilities for PySpark DataFrame and Schema assertions.

This module provides compatibility between PySpark 3.5+ (which has pyspark.testing)
and earlier versions (which require chispa library).

Usage
-----
```python
from spark._testing import (
    assert_df_equality,
    assert_schema_equality,
)

# Use assert_df_equality instead of assertDataFrameEqual
assert_df_equality(actual_df, expected_df)

# Use assert_schema_equality instead of assertSchemaEqual
assert_schema_equality(actual_schema, expected_schema)
```

For PySpark 3.5+, this uses the native `pyspark.testing` module.
For PySpark 3.3 and 3.4, this uses the `chispa` library.
"""

from koheesio.spark.utils.common import SPARK_MINOR_VERSION

__all__ = [
    "assert_df_equality",
    "assert_schema_equality",
]


if SPARK_MINOR_VERSION >= 3.5:
    # PySpark 3.5+ has native testing utilities
    from pyspark.testing import assertDataFrameEqual
    from pyspark.testing.utils import assertSchemaEqual

    # Create aliases that match the naming convention
    assert_df_equality = assertDataFrameEqual
    assert_schema_equality = assertSchemaEqual
else:
    # For PySpark 3.3 and 3.4, use chispa
    from chispa.dataframe_comparer import assert_df_equality as _chispa_assert_df_equality
    from chispa.schema_comparer import assert_schema_equality as _chispa_assert_schema_equality

    def assert_df_equality(actual, expected, **kwargs):
        """Assert that two DataFrames are equal.

        This is a compatibility wrapper that works with both PySpark 3.5+ and earlier versions.

        Parameters
        ----------
        actual : DataFrame
            The actual DataFrame to compare.
        expected : DataFrame
            The expected DataFrame to compare against.
        **kwargs
            Additional keyword arguments passed to the underlying comparison function.
            For PySpark < 3.5 (chispa), common options include:
            - ignore_nullable: bool - Ignore nullability in schema comparison
            - ignore_column_order: bool - Ignore column order
            - ignore_row_order: bool - Ignore row order
        """
        # Map pyspark.testing kwargs to chispa kwargs where possible
        chispa_kwargs = {}

        # Handle checkRowOrder (pyspark.testing) -> ignore_row_order (chispa)
        if "checkRowOrder" in kwargs:
            chispa_kwargs["ignore_row_order"] = not kwargs.pop("checkRowOrder")

        # Handle rtol/atol for approximate comparisons
        if "rtol" in kwargs or "atol" in kwargs:
            # chispa uses different parameter names
            rtol = kwargs.pop("rtol", 0.0)
            atol = kwargs.pop("atol", 0.0)
            if rtol > 0 or atol > 0:
                # chispa doesn't support rtol/atol directly in the same way
                # but we can use allow_nan_equality for some cases
                pass

        # Pass through any remaining kwargs
        chispa_kwargs.update(kwargs)

        _chispa_assert_df_equality(actual, expected, **chispa_kwargs)

    def assert_schema_equality(actual, expected, **kwargs):
        """Assert that two schemas are equal.

        This is a compatibility wrapper that works with both PySpark 3.5+ and earlier versions.

        Parameters
        ----------
        actual : StructType
            The actual schema to compare.
        expected : StructType
            The expected schema to compare against.
        **kwargs
            Additional keyword arguments passed to the underlying comparison function.
        """
        _chispa_assert_schema_equality(actual, expected)


# Also export the original function names for backward compatibility
# This allows existing code to continue using assertDataFrameEqual and assertSchemaEqual
assertDataFrameEqual = assert_df_equality
assertSchemaEqual = assert_schema_equality
