# Import necessary libraries
from sklearn.preprocessing import OneHotEncoder
import pandas as pd
from pydantic import BaseModel

# Define the EncodingConfig class
class EncodingConfig(BaseModel):
    drop_first: bool = True  # Whether to drop the first dummy column

# Define the categorical_encoding function
def categorical_encoding(data, columns, config: EncodingConfig):
    """
    Encodes categorical columns using one-hot encoding.

    Args:
        data (pd.DataFrame): Input dataset.
        columns (list): List of categorical columns to encode.
        config (EncodingConfig): Configuration for encoding.

    Returns:
        pd.DataFrame: Dataset with one-hot encoded columns.
    """
    encoder = OneHotEncoder(sparse_output=False, drop='first' if config.drop_first else None)
    encoded_array = encoder.fit_transform(data[columns])
    encoded_df = pd.DataFrame(
        encoded_array,
        columns=encoder.get_feature_names_out(columns),
        index=data.index,
    )
    return pd.concat([data.drop(columns, axis=1), encoded_df], axis=1)
