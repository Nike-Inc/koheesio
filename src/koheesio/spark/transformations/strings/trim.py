"""
Trim whitespace from the beginning and/or end of a string.

Classes
-------
- `Trim`
    Trim whitespace from the beginning and/or end of a string.
- `LTrim`
    Trim whitespace from the beginning of a string.
- `RTrim`
    Trim whitespace from the end of a string.

See class docstrings for more information.
"""

from typing import Literal

from pyspark.sql import Column
import pyspark.sql.functions as f

from koheesio.models import Field, ListOfColumns
from koheesio.spark.transformations import ColumnsTransformationWithTarget
from koheesio.spark.utils import SparkDatatype

trim_type = Literal["left", "right", "left-right"]


class Trim(ColumnsTransformationWithTarget):
    """
    Trim whitespace from the beginning and/or end of a string.

    This is a wrapper around PySpark ltrim() and rtrim() functions

    The `direction` parameter can be changed to apply either a left or a right trim. Defaults to left AND right trim.

    Note: If the type of the column is not string, Trim will not be run. A Warning will be thrown indicating this

    Parameters
    ----------
    columns : ListOfColumns
        The column (or list of columns) to trim. Alias: column
        If no columns are provided, all string columns will be trimmed.
    target_column : ListOfColumns, optional
        The column to store the result in. If not provided, the result will be stored in the source column.
        Alias: target_suffix - if multiple columns are given as source, this will be used as a suffix.
    direction : trim_type, optional, default "left-right"
        On which side to remove the spaces. Either "left", "right" or "left-right". Defaults to "left-right"

    Examples
    --------

    __input_df:__
    | column    |
    |-----------|
    | " hello " |

    ### Trim whitespace from the beginning of a string

    ```python
    output_df = Trim(
        column="column", target_column="trimmed_column", direction="left"
    ).transform(input_df)
    ```

    __output_df:__
    | column    | trimmed_column |
    |-----------|----------------|
    | " hello " | "hello "       |


    ### Trim whitespace from both sides of a string

    ```python
    output_df = Trim(
        column="column",
        target_column="trimmed_column",
        direction="left-right",  # default value
    ).transform(input_df)
    ```

    __output_df:__
    | column    | trimmed_column |
    |-----------|----------------|
    | " hello " | "hello"        |

    ### Trim whitespace from the end of a string

    ```python
    output_df = Trim(
        column="column", target_column="trimmed_column", direction="right"
    ).transform(input_df)
    ```

    __output_df:__
    | column    | trimmed_column |
    |-----------|----------------|
    | " hello " | " hello"       |

    """

    class ColumnConfig(ColumnsTransformationWithTarget.ColumnConfig):
        """Limit data types to string only."""

        run_for_all_data_type = [SparkDatatype.STRING]
        limit_data_type = [SparkDatatype.STRING]

    columns: ListOfColumns = Field(
        default="*",
        alias="column",
        description="The column (or list of columns) to trim. Alias: column. If no columns are provided, all string"
        "columns will be trimmed.",
    )
    direction: trim_type = Field(
        default="left-right", description="On which side to remove the spaces. Either 'left', 'right' or 'left-right'"
    )

    def func(self, column: Column) -> Column:
        if self.direction == "left":
            return f.ltrim(column)

        if self.direction == "right":
            return f.rtrim(column)

        # both (left-right)
        return f.rtrim(f.ltrim(column))


class LTrim(Trim):
    """Trim whitespace from the beginning of a string. Alias: LeftTrim"""

    direction: trim_type = "left"


class RTrim(Trim):
    """Trim whitespace from the end of a string. Alias: RightTrim"""

    direction: trim_type = "right"
