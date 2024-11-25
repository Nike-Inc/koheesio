import pandas as pd
from typing import Callable, List, Union

def rolling_aggregation(data: pd.DataFrame, column: str, window_size: int, agg_function: Union[str, Callable], group_by: List[str] = None):
    """
    Applies rolling window aggregation on a specified column.
    
    Args:
        data (pd.DataFrame): The dataset to process.
        column (str): The column to apply the rolling aggregation on.
        window_size (int): The size of the rolling window.
        agg_function (str or Callable): Aggregation method ('mean', 'sum', 'min', 'max', or custom function).
        group_by (List[str], optional): Columns to group the data before applying the rolling aggregation.

    Returns:
        pd.DataFrame: A new DataFrame with the rolling aggregation applied.
    """
    if group_by:
        data = data.sort_values(by=group_by + [column])
        result = data.groupby(group_by)[column].rolling(window=window_size).apply(agg_function).reset_index()
        data['rolling_agg'] = result[column]
    else:
        data = data.sort_values(by=column)
        data['rolling_agg'] = data[column].rolling(window=window_size).agg(agg_function)
    return data
