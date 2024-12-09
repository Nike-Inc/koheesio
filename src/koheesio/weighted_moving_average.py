import pandas as pd
import numpy as np
from typing import List, Optional

def weighted_moving_average(
    data: pd.DataFrame,
    column: str,
    window_size: int,
    weights: Optional[List[float]] = None,
    group_by: Optional[List[str]] = None,
):
    """
    Applies Weighted Moving Average (WMA) transformation on a specified column.

    Args:
        data (pd.DataFrame): Input dataset.
        column (str): The column to calculate the WMA on.
        window_size (int): The size of the moving window.
        weights (Optional[List[float]]): List of weights. Defaults to linear weights if not provided.
        group_by (Optional[List[str]]): List of columns to group by.

    Returns:
        pd.DataFrame: Dataset with an additional column for the WMA values.
    """
    if weights is None:
        # Default to linear weights (e.g., for window_size=3 -> [1, 2, 3])
        weights = np.arange(1, window_size + 1)

    # Ensure weights match window size
    if len(weights) != window_size:
        raise ValueError("Length of weights must match the window size.")

    def calculate_wma(series):
        # Ensure there are enough values to compute WMA
        if len(series) < window_size:
            return np.nan
        return np.dot(series[-window_size:], weights) / sum(weights)

    if group_by:
        # Apply grouping
        data = data.sort_values(by=group_by)
        data['WMA'] = (
            data.groupby(group_by)[column]
            .transform(lambda x: x.rolling(window=window_size).apply(calculate_wma, raw=True))
        )
    else:
        # Apply directly to the column
        data = data.sort_values(by=column)
        data['WMA'] = data[column].rolling(window=window_size).apply(calculate_wma, raw=True)

    return data
