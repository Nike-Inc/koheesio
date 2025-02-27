"""
Create Spark DataFrame directly from the data stored in a Python variable
"""

from typing import Any, Dict, Optional, Union
from enum import Enum
from functools import partial
from io import StringIO
import json

import pandas as pd

from pyspark.sql.types import StructType

from koheesio.models import ExtraParamsMixin, Field
from koheesio.spark import DataFrame
from koheesio.spark.readers import Reader


class DataFormat(Enum):
    """Data formats supported by the InMemoryDataReader"""

    JSON = "json"
    CSV = "csv"


class InMemoryDataReader(Reader, ExtraParamsMixin):
    """Directly read data from a Python variable and convert it to a Spark DataFrame.

    Read the data, that is stored in one of the supported formats (see `DataFormat`) directly from the variable and
    convert it to the Spark DataFrame. The use cases include converting JSON output of the API into the dataframe;
    reading the CSV data via the API (e.g. Box API).

    The advantage of using this reader is that it allows to read the data directly from the Python variable, without
    the need to store it on the disk. This can be useful when the data is small and does not need to be stored
    permanently.

    Parameters
    ----------
    data : Union[str, list, dict, bytes]
        Source data
    format : DataFormat
        File / data format
    schema_ : Optional[StructType], optional, default=None
        Schema that will be applied during the creation of Spark DataFrame
    params : Optional[Dict[str, Any]], optional, default=dict
        Set of extra parameters that should be passed to the appropriate reader (csv / json). Optionally, the user can
        pass the parameters that are specific to the reader (e.g. `multiLine` for JSON reader) as key-word arguments.
        These will be merged with the `params` parameter.

    Example
    -------
    ```python
    # Read CSV data from a string
    df1 = InMemoryDataReader(format=DataFormat.CSV, data='foo,bar\\nA,1\\nB,2')

    # Read JSON data from a string
    df2 = InMemoryDataReader(format=DataFormat.JSON, data='{"foo": A, "bar": 1}'
    df3 = InMemoryDataReader(format=DataFormat.JSON, data=['{"foo": "A", "bar": 1}', '{"foo": "B", "bar": 2}']
    ```
    """

    data: Union[str, list, dict, bytes] = Field(default=..., description="Source data")
    format: DataFormat = Field(default=..., description="File / data format")
    schema_: Optional[StructType] = Field(
        default=None,
        alias="schema",
        description="[Optional] Schema that will be applied during the creation of Spark DataFrame",
    )

    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="[Optional] Set of extra parameters that should be passed to the appropriate reader (csv / json)",
    )

    def _csv(self) -> DataFrame:
        """Method for reading CSV data"""
        if isinstance(self.data, list):
            csv_data: str = "\n".join(self.data)
        else:
            csv_data: str = self.data  # type: ignore

        if "header" in self.params and self.params["header"] is True:
            self.params["header"] = 0

        pandas_df = pd.read_csv(StringIO(csv_data), **self.params)  # type: ignore
        df = self.spark.createDataFrame(pandas_df, schema=self.schema_)  # type: ignore

        return df

    def _json(self) -> DataFrame:
        """Method for reading JSON data"""
        if isinstance(self.data, str):
            json_data = [json.loads(self.data)]
        elif isinstance(self.data, list):
            if all(isinstance(x, str) for x in self.data):
                json_data = [json.loads(x) for x in self.data]
            else:
                json_data = self.data
        else:
            json_data = [self.data]

        # Use pyspark.pandas to read the JSON data from the string
        # noinspection PyUnboundLocalVariable
        pandas_df = pd.read_json(StringIO(json.dumps(json_data)), **self.params)  # type: ignore

        # Convert pyspark.pandas DataFrame to Spark DataFrame
        df = self.spark.createDataFrame(pandas_df, schema=self.schema_)  # type: ignore

        return df

    def execute(self) -> Reader.Output:
        """
        Execute method appropriate to the specific data format
        """
        if self.data is None:
            raise ValueError("Data is not provided")

        if isinstance(self.data, bytes):
            self.data = self.data.decode("utf-8")

        _func = getattr(InMemoryDataReader, f"_{self.format}")
        _df = partial(_func, self)()
        self.output.df = _df
