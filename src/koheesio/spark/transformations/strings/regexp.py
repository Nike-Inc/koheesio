"""
String transformations using regular expressions.

This module contains transformations that use regular expressions to transform strings.

Classes
-------
RegexpExtract
    Extract a specific group matched by a Java regexp from the specified string column.

RegexpReplace
    Searches for the given regexp and replaces all instances with what is in 'replacement'.

"""

from pyspark.sql import Column
from pyspark.sql.functions import regexp_extract, regexp_replace

from koheesio.models import Field
from koheesio.spark.transformations import ColumnsTransformationWithTarget


class RegexpExtract(ColumnsTransformationWithTarget):
    """
    Extract a specific group matched by a Java regexp from the specified string column.
    If the regexp did not match, or the specified group did not match, an empty string is returned.

    A wrapper around the pyspark regexp_extract function

    Parameters
    ----------
    columns : ListOfColumns
        The column (or list of columns) to extract from. Alias: column
    target_column : Optional[str], optional, default=None
        The column to store the result in. If not provided, the result will be stored in the source column.
        Alias: target_suffix - if multiple columns are given as source, this will be used as a suffix.
    regexp : str
        The Java regular expression to extract
    index : Optional[int], optional, default=0
        When there are more groups in the match, you can indicate which one you want.
        0 means the whole match. 1 and above are groups within that match.

    Example
    -------

    ### Extracting the year and week number from a string

    Let's say we have a column containing the year and week in a format like `Y## W#` and we would like to extract the
    week numbers.

    __input_df:__

    | YWK      |
    |----------|
    | 2020 W1  |
    | 2021 WK2 |

    ```python
    output_df = RegexpExtract(
        column="YWK",
        target_column="week_number",
        regexp="Y([0-9]+) ?WK?([0-9]+)",
        index=2,  # remember that this is 1-indexed! So 2 will get the week number in this example.
    ).transform(input_df)
    ```

    __output_df:__

    | YWK     | week_number |
    |---------|-------------|
    | 2020 W1 | 1           |
    | 2021 WK2| 2           |

    ### Using the same example, but extracting the year instead

    If you want to extract the year, you can use index=1.

    ```python
    output_df = RegexpExtract(
        column="YWK",
        target_column="year",
        regexp="Y([0-9]+) ?WK?([0-9]+)",
        index=1,  # remember that this is 1-indexed! So 1 will get the year in this example.
    ).transform(input_df)
    ```

    __output_df:__

    | YWK     | year |
    |---------|------|
    | 2020 W1 | 2020 |
    | 2021 WK2| 2021 |
    """

    regexp: str = Field(default=..., description="The Java regular expression to extract")
    index: int = Field(
        default=0,
        description="When there are more groups in the match, you can indicate which one you want. "
        "0 means the whole match. 1 and above are groups within that match.",
    )

    def func(self, column: Column) -> Column:
        return regexp_extract(column, self.regexp, self.index)


class RegexpReplace(ColumnsTransformationWithTarget):
    """
    Searches for the given regexp and replaces all instances with what is in 'replacement'.

    A wrapper around the pyspark regexp_replace function

    Parameters
    ----------
    columns : ListOfColumns
        The column (or list of columns) to replace in. Alias: column
    target_column : Optional[str], optional, default=None
        The column to store the result in. If not provided, the result will be stored in the source column.
        Alias: target_suffix - if multiple columns are given as source, this will be used as a suffix.
    regexp : str
        The regular expression to replace
    replacement : str
        String to replace matched pattern with.

    Examples
    --------
    __input_df:__
    | content    |
    |------------|
    | hello world|

    Let's say you want to replace 'hello'.

    ```python
    output_df = RegexpReplace(
        column="content",
        target_column="replaced",
        regexp="hello",
        replacement="gutentag",
    ).transform(input_df)
    ```

    __output_df:__
    | content    | replaced      |
    |------------|---------------|
    | hello world| gutentag world|

    """

    regexp: str = Field(default=..., description="The regular expression to replace")
    replacement: str = Field(
        default=...,
        description="String to replace matched pattern with.",
    )

    def func(self, column: Column) -> Column:
        return regexp_replace(column, self.regexp, self.replacement)
