"""
Extracts a substring from a string column starting at the given position.
"""

from pyspark.sql import Column
from pyspark.sql.functions import substring, when
from pyspark.sql.types import StringType

from koheesio.models import Field, PositiveInt, field_validator
from koheesio.spark.transformations import ColumnsTransformationWithTarget


class Substring(ColumnsTransformationWithTarget):
    """
    Extracts a substring from a string column starting at the given position.

    This is a wrapper around PySpark substring() function

    Notes
    -----
    - Numeric columns will be cast to string
    - start is 1-indexed, not 0-indexed!

    Parameters
    ----------
    columns : Union[str, List[str]]
        The column (or list of columns) to substring. Alias: column
    target_column : Optional[str], optional, default=None
        The column to store the result in. If not provided, the result will be stored in the source column.
        Alias: target_suffix - if multiple columns are given as source, this will be used as a suffix.
    start : PositiveInt
        Positive int. Defines where to begin the substring from. The first character of the field has index 1!
    length : Optional[int], optional, default=-1
        Optional. If not provided, the substring will go until end of string.

    Example
    -------
    ### Extract a substring from a string column starting at the given position.

    __input_df:__

    | column    |
    |-----------|
    | skyscraper|

    ```python
    output_df = Substring(
        column="column",
        target_column="substring_column",
        start=3,  # 1-indexed! So this will start at the 3rd character
        length=4,
    ).transform(input_df)
    ```

    __output_df:__

    | column    | substring_column |
    |-----------|------------------|
    | skyscraper| yscr             |

    """

    start: PositiveInt = Field(default=..., description="The starting position")
    length: int = Field(
        default=-1,
        description="The target length for the string. use -1 to perform until end",
    )

    @field_validator("length")
    def _valid_length(cls, length_value: int) -> int:
        """Integer.maxint fix for Java.
        Python's sys.maxsize is larger which makes f.substring fail"""
        if length_value == -1:
            return 2147483647
        return length_value

    def func(self, column: Column) -> Column:
        return when(column.isNull(), None).otherwise(substring(column, self.start, self.length)).cast(StringType())
