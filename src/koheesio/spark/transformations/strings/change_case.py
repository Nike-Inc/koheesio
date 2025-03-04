"""
Convert the case of a string column to upper case, lower case, or title case

Classes
-------
`Lower`
    Converts a string column to lower case.
`Upper`
    Converts a string column to upper case.
`TitleCase` or `InitCap`
    Converts a string column to title case, where each word starts with a capital letter.
"""

from pyspark.sql import Column
from pyspark.sql.functions import initcap, lower, upper

from koheesio.spark.transformations import ColumnsTransformationWithTarget
from koheesio.spark.utils import SparkDatatype

__all__ = ["LowerCase", "UpperCase", "TitleCase", "InitCap"]


class LowerCase(ColumnsTransformationWithTarget):
    """
    This function makes the contents of a column lower case.

    Wraps the `pyspark.sql.functions.lower` function.

    Warnings
    --------
    If the type of the column is not string, `LowerCase` will not be run. A Warning will be thrown indicating this.

    Parameters
    ----------
    columns : Union[str, List[str]]
        The name of the column or columns to convert to lower case. Alias: column.
        Lower case will be applied to all columns in the list.
        Column is required to be of string type.
    target_column : str, optional
        The name of the column to store the result in. If None, the result will be stored in the same column as the
        input.

    Example
    -------
    __input_df:__

    |product            |amount|country|
    |-------------------|------|-------|
    |Banana lemon orange|  1000|    USA|
    |Carrots Blueberries|  1500|    USA|
    |              Beans|  1600|    USA|

    ```python
    output_df = LowerCase(
        column="product", target_column="product_lower"
    ).transform(df)
    ```

    __output_df:__

    |product            |amount|country|product_lower      |
    |-------------------|------|-------|-------------------|
    |Banana lemon orange|  1000|    USA|banana lemon orange|
    |Carrots Blueberries|  1500|    USA|carrots blueberries|
    |              Beans|  1600|    USA|              beans|

    In this example, the column `product` is converted to `product_lower` and the contents of this column are converted
    to lower case.
    """

    class ColumnConfig(ColumnsTransformationWithTarget.ColumnConfig):
        """Limit data type to string"""

        run_for_all_data_type = [SparkDatatype.STRING]
        limit_data_type = [SparkDatatype.STRING]

    def func(self, column: Column) -> Column:
        return lower(column)


class UpperCase(LowerCase):
    """
    This function makes the contents of a column upper case.

    Wraps the `pyspark.sql.functions.upper` function.

    Warnings
    --------
    If the type of the column is not string, `UpperCase` will not be run. A Warning will be thrown indicating this.

    Parameters
    ----------
    columns : Union[str, List[str]]
        The name of the column or columns to convert to upper case. Alias: column.
        Upper case will be applied to all columns in the list.
        Column is required to be of string type.
    target_column : str, optional
        The name of the column to store the result in. If None, the result will be stored in the same column as the
        input.

    Examples
    -------
    __input_df:__

    |product            |amount|country|
    |-------------------|------|-------|
    |Banana lemon orange|  1000|    USA|
    |Carrots Blueberries|  1500|    USA|
    |              Beans|  1600|    USA|

    ```python
    output_df = UpperCase(
        column="product", target_column="product_upper"
    ).transform(df)
    ```

    __output_df:__

    |product            |amount|country|product_upper      |
    |-------------------|------|-------|-------------------|
    |Banana lemon orange|  1000|    USA|BANANA LEMON ORANGE|
    |Carrots Blueberries|  1500|    USA|CARROTS BLUEBERRIES|
    |              Beans|  1600|    USA|              BEANS|

    In this example, the column `product` is converted to `product_upper` and the contents of this column are converted
    to upper case.
    """

    def func(self, column: Column) -> Column:
        return upper(column)


class TitleCase(LowerCase):
    """
    This function makes the contents of a column title case.
    This means that every word starts with an upper case.

    Wraps the `pyspark.sql.functions.initcap` function.

    Warnings
    --------
    If the type of the column is not string, TitleCase will not be run. A Warning will be thrown indicating this.

    Parameters
    ----------
    columns : ListOfColumns
        The name of the column or columns to convert to title case. Alias: column.
        Title case will be applied to all columns in the list.
        Column is required to be of string type.
    target_column : str, optional
        The name of the column to store the result in. If None, the result will be stored in the same column as the
        input.

    Example
    -------
    __input_df:__

    |product            |amount|country|
    |-------------------|------|-------|
    |Banana lemon orange|  1000|    USA|
    |Carrots blueberries|  1500|    USA|
    |              Beans|  1600|    USA|

    ```python
    output_df = TitleCase(
        column="product", target_column="product_title"
    ).transform(df)
    ```

    __output_df:__

    |product            |amount|country|product_title      |
    |-------------------|------|-------|-------------------|
    |Banana lemon orange|  1000|    USA|Banana Lemon Orange|
    |Carrots blueberries|  1500|    USA|Carrots Blueberries|
    |              Beans|  1600|    USA|              Beans|

    In this example, the column `product` is converted to `product_title` and the contents of this column are converted
    to title case (each word now starts with an upper case).
    """

    def func(self, column: Column) -> Column:
        return initcap(column)


# Alias for TitleCase
InitCap = TitleCase
