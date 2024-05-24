"""
Create Spark DataFrame directly from the data stored in a Python variable
"""

import json
from typing import Any, Dict, Optional, Union
from enum import Enum
from functools import partial

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from koheesio.models import ExtraParamsMixin, Field
from koheesio.steps.readers import Reader


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

    params: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="[Optional] Set of extra parameters that should be passed to the appropriate reader (csv / json)",
    )

    @property
    def _rdd(self) -> RDD:
        """
        Read provided data and transform it into Spark RDD

        Returns
        -------
        RDD
        """
        _data = self.data

        if isinstance(_data, bytes):
            _data = _data.decode("utf-8")

        if isinstance(_data, dict):
            _data = json.dumps(_data)

        # 'list' type already compatible with 'parallelize'
        if not isinstance(_data, list):
            _data = _data.splitlines()

        _rdd = self.spark.sparkContext.parallelize(_data)

        return _rdd

    def _csv(self, rdd: RDD) -> DataFrame:
        """Method for reading CSV data"""
        return self.spark.read.csv(rdd, schema=self.schema_, **self.params)

    def _json(self, rdd: RDD) -> DataFrame:
        """Method for reading JSON data"""
        return self.spark.read.json(rdd, schema=self.schema_, **self.params)

    def execute(self):
        """
        Execute method appropriate to the specific data format
        """
        _func = getattr(InMemoryDataReader, f"_{self.format}")
        _df = partial(_func, self, self._rdd)()
        self.output.df = _df
