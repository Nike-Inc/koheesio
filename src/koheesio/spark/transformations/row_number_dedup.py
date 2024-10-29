"""
This module contains the RowNumberDedup class, which performs a row_number deduplication operation on a DataFrame.

See the docstring of the RowNumberDedup class for more information.
"""

from __future__ import annotations

from typing import List, Optional, Union

from pyspark.sql import Window, WindowSpec
from pyspark.sql.functions import col, desc, row_number

from koheesio.models import Field, conlist, field_validator
from koheesio.spark import Column
from koheesio.spark.transformations import ColumnsTransformation


class RowNumberDedup(ColumnsTransformation):
    """
    A class used to perform a row_number deduplication operation on a DataFrame.

    This class is a specialized transformation that extends the ColumnsTransformation class. It sorts the DataFrame
    based on the provided sort columns and assigns a row_number to each row. It then filters the DataFrame to keep only
    the top-row_number row for each group of duplicates.
    The row_number of each row can be stored in a specified target column or a default column named
    "meta_row_number_column". The class also provides an option to preserve meta columns
    (like the `row_number` column) in the output DataFrame.

    Attributes
    ----------
    columns : list
        List of columns to apply the transformation to. If a single '*' is passed as a column name or if the columns
        parameter is omitted, the transformation will be applied to all columns of the data types specified in
        `run_for_all_data_type` of the ColumnConfig. (inherited from ColumnsTransformation)
    sort_columns : list
        List of columns that the DataFrame will be sorted by.
    target_column : str, optional
        Column where the row_number of each row will be stored.
    preserve_meta : bool, optional
        Flag that determines whether the meta columns should be kept in the output DataFrame.
    """

    sort_columns: conlist(Union[str, Column], min_length=0) = Field(
        default_factory=list,
        alias="sort_column",
        description="List of orderBy columns. If only one column is passed, it can be passed as a single object.",
    )
    target_column: Optional[Union[str, Column]] = Field(
        default="meta_row_number_column",
        alias="target_suffix",
        description="The column to store the result in. If not provided, the result will be stored in the source"
        "column. Alias: target_suffix - if multiple columns are given as source, "
        "this will be used as a suffix",
    )
    preserve_meta: bool = Field(
        default=False,
        description="If true, meta columns are kept in output dataframe. Defaults to 'False'",
    )

    @field_validator("sort_columns", mode="before")
    def set_sort_columns(cls, columns_value: Union[str, Column, List[Union[str, Column]]]) -> List[Union[str, Column]]:
        """
        Validates and optimizes the sort_columns parameter.

        This method ensures that sort_columns is a list (or single object) of unique strings or Column objects.
        It removes any empty strings or None values from the list and deduplicates the columns.

        Parameters
        ----------
        columns_value : Union[str, Column, List[Union[str, Column]]]
            The value of the sort_columns parameter.

        Returns
        -------
        List[Union[str, Column]]
            The optimized and deduplicated list of sort columns.
        """
        columns = [columns_value] if isinstance(columns_value, (str, Column)) else [*columns_value]

        # Remove empty strings, None, etc.
        columns = [c for c in columns if (isinstance(c, Column) and c is not None) or (isinstance(c, str) and c)]

        dedup_columns = []
        seen = set()

        # Deduplicate the columns while preserving the order
        for column in columns:
            if str(column) not in seen:
                dedup_columns.append(column)
                seen.add(str(column))

        return dedup_columns

    @property
    def window_spec(self) -> WindowSpec:
        """
        Builds a WindowSpec object based on the columns defined in the configuration.

        The WindowSpec object is used to define a window frame over which functions are applied in Spark.
        This method partitions the data by the columns returned by the `get_columns` method and then orders the
        partitions by the columns specified in `sort_columns`.

        Notes
        -----
        The order of the columns in the WindowSpec object is preserved. If a column is passed as a string,
        it is converted to a Column object with DESC ordering.

        Returns
        -------
        WindowSpec
            A WindowSpec object that can be used to define a window frame in Spark.
        """
        # preserve the order of column object if it is passed, otherwise convert to column object
        # with DESC ordering
        order_clause = [desc(c) if isinstance(c, str) else c for c in self.sort_columns]

        return Window.partitionBy([*self.get_columns()]).orderBy(*order_clause)

    def execute(self) -> RowNumberDedup.Output:  # type: ignore
        """
        Performs the row_number deduplication operation on the DataFrame.

        This method sorts the DataFrame based on the provided sort columns, assigns a row_number to each row,
        and then filters the DataFrame to keep only the top-row_number row for each group of duplicates.
        The row_number of each row is stored in the target column. If preserve_meta is False,
        the method also drops the target column from the DataFrame.
        """
        df = self.df
        window_spec = self.window_spec

        # if target_column is a string, convert it to a Column object
        if isinstance((target_column := self.target_column), str):
            target_column = col(target_column)

        # dedup the dataframe based on the window spec
        df = df.withColumn(self.target_column, row_number().over(window_spec)).filter(target_column == 1).select("*")

        if not self.preserve_meta:
            df = df.drop(target_column)

        self.output.df = df
