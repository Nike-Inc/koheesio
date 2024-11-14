"""
Excel reader for Spark

Note
----
Ensure the 'excel' extra is installed before using this reader.
Default implementation uses openpyxl as the engine for reading Excel files.
Other implementations can be used by passing the correct keyword arguments to the reader.

See Also
--------
- https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html
- koheesio.pandas.readers.excel.ExcelReader
"""

from typing import List, Optional, Union
from pathlib import Path

import pandas as pd

from koheesio.models import ExtraParamsMixin, Field
from koheesio.pandas.readers import Reader


class ExcelReader(Reader, ExtraParamsMixin):
    """Read data from an Excel file

    See Also
    --------
    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html

    Attributes
    ----------
    path : Union[str, Path]
        The path to the Excel file
    sheet_name : str
        The name of the sheet to read
    header : Optional[Union[int, List[int]]]
        Row(s) to use as the column names

    Any other keyword arguments will be passed to pd.read_excel.
    """

    path: Union[str, Path] = Field(description="The path to the Excel file")
    sheet_name: str = Field(default="Sheet1", description="The name of the sheet to read")
    header: Optional[Union[int, List[int]]] = Field(default=0, description="Row(s) to use as the column names")

    def execute(self) -> Reader.Output:
        extra_params = self.params or {}
        extra_params.pop("spark", None)
        self.output.df = pd.read_excel(self.path, sheet_name=self.sheet_name, header=self.header, **extra_params)
