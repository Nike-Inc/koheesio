"""
Transformation to wrap around the pyspark getItem function
"""

from typing import Union

from pyspark.sql import Column

from koheesio.models import Field
from koheesio.spark.transformations import ColumnsTransformationWithTarget
from koheesio.spark.utils import SparkDatatype


def get_item(column: Column, key: Union[str, int]) -> Column:
    """
    Wrapper around pyspark.sql.functions.getItem

    Parameters
    ----------
    column : Column
        The column to get the item from
    key : Union[str, int]
        The key (or index) to get from the list or map. If the column is a list (ArrayType), this should be an integer.
        If the column is a dict (MapType), this should be a string.

    Returns
    -------
    Column
        The column with the item
    """
    return column.getItem(key)


class GetItem(ColumnsTransformationWithTarget):
    """
    Get item from list or map (dictionary)

    Wrapper around `pyspark.sql.functions.getItem`

    `GetItem` is strict about the data type of the column. If the column is not a list or a map, an error will be
    raised.

    Note
    ----
    Only MapType and ArrayType are supported.

    Parameters
    ----------
    columns : Union[str, List[str]]
        The column (or list of columns) to get the item from. Alias: column
    target_column : Optional[str], optional, default=None
        The column to store the result in. If not provided, the result will be stored in the source column.
        Alias: target_suffix - if multiple columns are given as source, this will be used as a suffix.
    key : Union[int, str]
        The key (or index) to get from the list or map. If the column is a list (ArrayType), this should be an integer.
        If the column is a dict (MapType), this should be a string. Alias: index

    Example
    -------

    ### Example with list (ArrayType)

    By specifying an integer for the parameter "key", getItem knows to get the element at index n of a list
    (index starts at 0).

    __input_df__:

    | id | content   |
    |----|-----------|
    | 1  | [1, 2, 3] |
    | 2  | [4, 5]    |
    | 3  | [6]       |
    | 4  | []        |

    ```python
    output_df = GetItem(
        column="content",
        index=1,  # get the second element of the list
        target_column="item",
    ).transform(input_df)
    ```

    __output_df__:

    | id | content   | item |
    |----|-----------|------|
    | 1  | [1, 2, 3] | 2    |
    | 2  | [4, 5]    | 5    |
    | 3  | [6]       | null |
    | 4  | []        | null |

    ### Example with a dict (MapType)

    __input_df__:

    | id | content          |
    |----|------------------|
    |  1 | {key1 -> value1} |
    |  2 | {key1 -> value2} |
    |  3 | {key2 -> hello}  |
    |  4 | {key2 -> world}  |

    ```python
    output_df = GetItem(
        column= "content",
        key="key2,
        target_column="item",
    ).transform(input_df)
    ```
    As we request the key to be "key2", the first 2 rows will be null, because
    it does not have "key2".

    __output_df__:

    | id | content          | item  |
    |----|------------------|-------|
    | 1  | {key1 -> value1} | null  |
    | 2  | {key1 -> value2} | null  |
    | 3  | {key2 -> hello}  | hello |
    | 4  | {key2 -> world}  | world |
    """

    class ColumnConfig(ColumnsTransformationWithTarget.ColumnConfig):
        """Limit the data types to ArrayType and MapType."""

        run_for_all_data_type = [SparkDatatype.ARRAY, SparkDatatype.MAP]
        limit_data_type = run_for_all_data_type
        data_type_strict_mode = True

    key: Union[int, str] = Field(
        default=...,
        alias="index",
        description="The key (or index) to get from the list or map. If the column is a list (ArrayType), this should "
        "be an integer. If the column is a dict (MapType), this should be a string. Alias: index",
    )

    def func(self, column: Column) -> Column:
        return get_item(column, self.key)
