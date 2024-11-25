import pandas as pd

def impute_missing_values(data, strategy, columns, fill_value=None):
    """
    Imputes missing values in specified columns using a given strategy.

    Args:
        data (pd.DataFrame): The dataset to modify.
        strategy (str): Imputation strategy ('mean', 'median', 'mode', 'constant').
        columns (list): List of columns to impute.
        fill_value (any): Value to use when strategy is 'constant'.

    Returns:
        pd.DataFrame: DataFrame with imputed values.
    """
    if strategy not in ['mean', 'median', 'mode', 'constant']:
        raise ValueError(f"Invalid strategy '{strategy}'. Supported strategies: 'mean', 'median', 'mode', 'constant'.")

    for column in columns:
        if column not in data.columns:
            raise ValueError(f"Column '{column}' does not exist in the DataFrame.")

        if strategy == 'mean':
            data[column] = data[column].fillna(data[column].mean())
        elif strategy == 'median':
            data[column] = data[column].fillna(data[column].median())
        elif strategy == 'mode':
            data[column] = data[column].fillna(data[column].mode()[0])
        elif strategy == 'constant':
            if fill_value is None:
                raise ValueError("fill_value must be provided when strategy='constant'.")
            data[column] = data[column].fillna(fill_value)

    return data
