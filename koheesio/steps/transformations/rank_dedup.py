"""
This module contains the deprecated RankDedup class, which performs a row_number deduplication operation on a DataFrame.

See the docstring of the RowNumberDedup class for more information.
"""

import warnings
from typing import Optional, Union

from pyspark.sql import Column

from koheesio.models import Field
from koheesio.steps.transformations.row_number_dedup import RowNumberDedup

warnings.warn("RankDedup is deprecated. Use RowNumberDedup instead.")


class RankDedup(RowNumberDedup):
    """
    A class used to perform a ranked deduplication operation on a DataFrame.
    Deprecated: Use RowNumberDedup instead.

    Concept
    -------
    This class is a specialized transformation that extends the `ColumnsTransformation` class. It sorts the DataFrame
    based on the provided sort columns and assigns a rank to each row. It then filters the DataFrame to keep only
    the top-ranked row for each group of duplicates. The rank of each row can be stored in a specified target column
    or a default column named "meta_rank_column". The class also provides an option to preserve meta columns
    (like the rank column) in the output DataFrame.

    Attributes
    ----------
    columns : Optional[list | str | Column], optional, default=None
        List of columns to apply the transformation to. If a single '*' is passed as a column name or if the columns
        parameter is omitted, the transformation will be applied to all columns of the data types specified in
        `run_for_all_data_type` of the ColumnConfig. (inherited from ColumnsTransformation)
    sort_columns : list
        List of columns that the DataFrame will be sorted by.
    target_column : str, optional, default=meta_rank_column
        Column where the rank of each row will be stored.
    preserve_meta : bool, optional, default=False
        Flag that determines whether the meta columns should be kept in the output DataFrame.
    """

    target_column: Optional[Union[str, Column]] = Field(
        default="meta_rank_column",
        alias="target_suffix",
        description="The column to store the result in. If not provided, the result will be stored in the source"
        "column. Alias: target_suffix - if multiple columns are given as source, "
        "this will be used as a suffix",
    )
