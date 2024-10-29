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

from pyspark.pandas import DataFrame as PandasDataFrame

from koheesio.pandas.readers.excel import ExcelReader as PandasExcelReader
from koheesio.spark.readers import Reader


class ExcelReader(Reader, PandasExcelReader):
    """Read data from an Excel file

    This class is a wrapper around the PandasExcelReader class. It reads an Excel file first using pandas, and then
    converts the pandas DataFrame to a Spark DataFrame.

    Attributes
    ----------
    path: str
        The path to the Excel file
    sheet_name: str
        The name of the sheet to read
    header: int
        The row to use as the column names
    """

    def execute(self) -> Reader.Output:
        pdf: PandasDataFrame = PandasExcelReader.from_step(self).execute().df
        self.output.df = self.spark.createDataFrame(pdf)
