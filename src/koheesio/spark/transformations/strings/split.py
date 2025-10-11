"""
Splits the contents of a column on basis of a split_pattern

Classes
-------
SplitAll
    Splits the contents of a column on basis of a split_pattern.

SplitAtFirstMatch
    Like SplitAll, but only splits the string once. You can specify whether you want the first or second part.
"""

from typing import Optional

from pyspark.sql import Column
from pyspark.sql.functions import coalesce, lit, split

from koheesio.models import Field
from koheesio.spark.transformations import ColumnsTransformationWithTarget


class SplitAll(ColumnsTransformationWithTarget):
    """
    This function splits the contents of a column on basis of a split_pattern.

    It splits at al the locations the pattern is found. The new column will be of ArrayType.

    Wraps the pyspark.sql.functions.split function.

    Parameters
    ----------
    columns : Union[str, List[str]]
        The column (or list of columns) to split. Alias: column
    target_column : Optional[str], optional, default=None
        The column to store the result in. If not provided, the result will be stored in the source column.
        Alias: target_suffix - if multiple columns are given as source, this will be used as a suffix.
    split_pattern : str
        This is the pattern that will be used to split the column contents.

    Example
    -------

    ### Splitting with a space as a pattern:

    __input_df:__

    |            product|amount|country|
    |-------------------|------|-------|
    |Banana lemon orange|  1000|    USA|
    |Carrots Blueberries|  1500|    USA|
    |              Beans|  1600|    USA|

    ```python
    output_df = SplitColumn(
        column="product", target_column="split", split_pattern=" "
    ).transform(input_df)
    ```

    __output_df:__

    |product            |amount|country|split                       |
    |-------------------|------|-------|----------------------------|
    |Banana lemon orange|  1000|    USA|["Banana", "lemon" "orange"]|
    |Carrots Blueberries|  1500|    USA|["Carrots", "Blueberries"]  |
    |              Beans|  1600|    USA|["Beans"]                   |
    """

    split_pattern: str = Field(default=..., description="The pattern to split the column contents.")

    def func(self, column: Column) -> Column:
        return split(column, pattern=self.split_pattern)


class SplitAtFirstMatch(SplitAll):
    """
    Like SplitAll, but only splits the string once. You can specify whether you want the first or second part.

    Note
    ----
    * SplitAtFirstMatch splits the string only once. To specify whether you want the first part or the second you
        can toggle the parameter retrieve_first_part.
    * The new column will be of StringType.
    * If you want to split a column more than once, you should call this function multiple times.

    Parameters
    ----------
    columns : Union[str, List[str]]
        The column (or list of columns) to split. Alias: column
    target_column : Optional[str], optional, default=None
        The column to store the result in. If not provided, the result will be stored in the source column.
        Alias: target_suffix - if multiple columns are given as source, this will be used as a suffix.
    split_pattern : str
        This is the pattern that will be used to split the column contents.
    retrieve_first_part : Optional[bool], optional, default=True
        Takes the first part of the split when true, the second part when False. Other parts are ignored.
        Defaults to True.

    Example
    -------

    ### Splitting with a space as a pattern:

    __input_df:__

    |product            |amount|country|
    |-------------------|------|-------|
    |Banana lemon orange|  1000|    USA|
    |Carrots Blueberries|  1500|    USA|
    |              Beans|  1600|    USA|

    ```python
    output_df = SplitColumn(
        column="product", target_column="split_first", split_pattern="an"
    ).transform(input_df)
    ```

    __output_df:__

    |product            |amount|country|split_first        |
    |-------------------|------|-------|-------------------|
    |Banana lemon orange|  1000|    USA|B                  |
    |Carrots Blueberries|  1500|    USA|Carrots Blueberries|
    |              Beans|  1600|    USA|Be                 |
    """

    retrieve_first_part: Optional[bool] = Field(
        default=True,
        description="Takes the first part of the split when true, the second part when False. Other parts are ignored.",
    )

    def func(self, column: Column) -> Column:
        split_func = split(column, pattern=self.split_pattern)

        # first part
        if self.retrieve_first_part:
            return split_func.getItem(0)

        # or, second part
        return coalesce(split_func.getItem(1), lit(""))
