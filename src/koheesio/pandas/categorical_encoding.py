from sklearn.preprocessing import OneHotEncoder
import pandas as pd
from pydantic import BaseModel
from typing import List, Optional
from koheesio.steps import Step

from typing import List, Dict
import pandas as pd
from pydantic import BaseModel

class PandasCategoricalEncoding(BaseModel):
    """
    Encodes categorical columns using one-hot encoding or ordinal encoding.

    Attributes
    ----------
    columns : List[str]
        List of categorical columns to encode.
    encoding_type : str, optional
        Type of encoding, either "one-hot" or "ordinal" (default is "one-hot").
    drop_first : bool, optional
        Whether to drop the first dummy column for one-hot encoding (default is True).
    ordinal_mapping : dict, optional
        A dictionary mapping categorical values to integers for ordinal encoding.
    """

    columns: List[str]
    encoding_type: str = "one-hot"
    drop_first: bool = True
    ordinal_mapping: Dict[str, Dict] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.encoding_type not in ['one-hot', 'ordinal']:
            raise ValueError(f"Invalid encoding type: {self.encoding_type}. Expected 'one-hot' or 'ordinal'.")

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the categorical encoding transformation on the provided dataset.

        Parameters
        ----------
        data : pd.DataFrame
            The input dataset to encode.

        Returns
        -------
        pd.DataFrame
            The dataset with the specified categorical columns encoded.
        """
        if self.encoding_type == 'one-hot':
            data = pd.get_dummies(data, columns=self.columns, drop_first=self.drop_first)
        elif self.encoding_type == 'ordinal':
            for column in self.columns:
                if column in data.columns and self.ordinal_mapping and column in self.ordinal_mapping:
                    data[column] = data[column].map(self.ordinal_mapping[column]).fillna(-1).astype(int)
        return data
