"""
This module defines the DropColumn class, a subclass of ColumnsTransformation.
"""

from koheesio.spark.transformations import ColumnsTransformation


class DropColumn(ColumnsTransformation):
    """Drop one or more columns

    The DropColumn class is used to drop one or more columns from a PySpark DataFrame.
    It wraps the `pyspark.DataFrame.drop` function and can handle either a single string
    or a list of strings as input.

    If a specified column does not exist in the DataFrame, no error or warning is thrown,
    and all existing columns will remain.

    Expected behavior
    -----------------
    - When the `column` does not exist, all columns will remain (no error or warning is thrown)
    - Either a single string, or a list of strings can be specified

    Example
    -------
    __df__:

    |            product|amount|country|
    |-------------------|------|-------|
    |Banana lemon orange|  1000|    USA|
    |Carrots Blueberries|  1500|    USA|
    |              Beans|  1600|    USA|

    ```python
    output_df = DropColumn(column="product").transform(df)
    ```

    __output_df__:

    |amount|country|
    |------|-------|
    |  1000|    USA|
    |  1500|    USA|
    |  1600|    USA|

    In this example, the `product` column is dropped from the DataFrame `df`.
    """

    def execute(self) -> ColumnsTransformation.Output:
        self.log.info(f"{self.columns=}")
        self.output.df = self.df.drop(*self.columns)
