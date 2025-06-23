"""
This module contains classes for writing data to a buffer before writing to the final destination.

The `BufferWriter` class is a base class for writers that write to a buffer first. It provides methods for writing,
reading, and resetting the buffer, as well as checking if the buffer is compressed and compressing the buffer.

The `PandasCsvBufferWriter` class is a subclass of `BufferWriter` that writes a Spark DataFrame to CSV file(s) using
Pandas. It is not meant to be used for writing huge amounts of data, but rather for writing smaller amounts of data
to more arbitrary file systems (e.g., SFTP).

The `PandasJsonBufferWriter` class is a subclass of `BufferWriter` that writes a Spark DataFrame to JSON file(s) using
Pandas. It is not meant to be used for writing huge amounts of data, but rather for writing smaller amounts of data
to more arbitrary file systems (e.g., SFTP).
"""

from __future__ import annotations

from typing import AnyStr, Literal, Optional
from abc import ABC
from functools import partial
import gzip
from os import linesep
from tempfile import SpooledTemporaryFile

from packaging import version

# noinspection PyProtectedMember
from pandas._typing import CompressionOptions as PandasCompressionOptions

from pydantic import InstanceOf

from pyspark import pandas

from koheesio.models import ExtraParamsMixin, Field, constr
from koheesio.spark import DataFrame
from koheesio.spark.writers import Writer


# pylint: disable=E1101
class BufferWriter(Writer, ABC):
    """Base class for writers that write to a buffer first, before writing to the final destination.

    `execute()` method should implement how the incoming DataFrame is written to the buffer object (e.g. BytesIO) in the
    output.

    The default implementation uses a `SpooledTemporaryFile` as the buffer. This is a file-like object that starts off
    stored in memory and automatically rolls over to a temporary file on disk if it exceeds a certain size. A
    `SpooledTemporaryFile` behaves similar to `BytesIO`, but with the added benefit of being able to handle larger
    amounts of data.

    This approach provides a balance between speed and memory usage, allowing for fast in-memory operations for smaller
    amounts of data while still being able to handle larger amounts of data that would not otherwise fit in memory.
    """

    class Output(Writer.Output, ABC):
        """Output class for BufferWriter"""

        buffer: InstanceOf[SpooledTemporaryFile] = Field(
            default_factory=partial(SpooledTemporaryFile, mode="w+b", max_size=0), exclude=True
        )

        def read(self) -> AnyStr:
            """Read the buffer"""
            self.rewind_buffer()
            data = self.buffer.read()
            self.rewind_buffer()
            return data

        def rewind_buffer(self):  # type: ignore
            """Rewind the buffer"""
            self.buffer.seek(0)
            return self

        def reset_buffer(self):  # type: ignore
            """Reset the buffer"""
            self.buffer.truncate(0)
            self.rewind_buffer()
            return self

        def is_compressed(self):  # type: ignore
            """Check if the buffer is compressed."""
            self.rewind_buffer()
            magic_number_present = self.buffer.read(2) == b"\x1f\x8b"
            self.rewind_buffer()
            return magic_number_present

        def compress(self):  # type: ignore
            """Compress the file_buffer in place using GZIP"""
            # check if the buffer is already compressed
            if self.is_compressed():
                self.logger.warn("Buffer is already compressed. Nothing to compress...")
                return self

            # compress the file_buffer
            file_buffer = self.buffer
            compressed = gzip.compress(file_buffer.read())

            # write the compressed content back to the buffer
            self.reset_buffer()
            self.buffer.write(compressed)

            return self  # to allow for chaining

    def write(self, df: DataFrame = None) -> Output:
        """Write the DataFrame to the buffer"""
        self.df = df or self.df
        if not self.df:
            raise RuntimeError("No valid Dataframe was passed")
        self.output.reset_buffer()
        self.execute()
        return self.output


# pylint: disable=C0301
class PandasCsvBufferWriter(BufferWriter, ExtraParamsMixin):
    """
    Write a Spark DataFrame to CSV file(s) using Pandas.

    Takes inspiration from https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html

    See also: https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option

    Note
    ----
    This class is not meant to be used for writing huge amounts of data. It is meant to be used for writing smaller
    amounts of data to more arbitrary file systems (e.g. SFTP).

    Pyspark vs Pandas
    -----------------
    The following table shows the mapping between Pyspark, Pandas, and Koheesio properties. Note that the default values
    are mostly the same as Pyspark's `DataFrameWriter` implementation, with some exceptions (see below).

    This class implements the most commonly used properties. If a property is not explicitly implemented, it can be
    accessed through `params`.

    | PySpark Property | Default PySpark | Pandas Property | Default Pandas | Koheesio Property | Default Koheesio | Notes |
    |------------------|-----------------|-----------------|----------------|-------------------|------------------|-------|
    | maxRecordsPerFile| ...             | chunksize       | None           | max_records_per_file | ...           | Spark property name: spark.sql.files.maxRecordsPerFile |
    | sep              | ,               | sep             | ,              | sep               | ,                |       |
    | lineSep          | `\\n `          | line_terminator | os.linesep     | lineSep (alias=line_terminator) | \\n |      |
    | N/A              | ...             | index           | True           | index             | False            | Determines whether row labels (index) are included in the output |
    | header           | False           | header          | True           | header            | True             |       |
    | quote            | "               | quotechar       | "              | quote (alias=quotechar) | "          |       |
    | quoteAll         | False           | doublequote     | True           | quoteAll (alias=doublequote) | False |       |
    | escape           | `\\`              | escapechar      | None           | escapechar (alias=escape) | \\       |       |
    | escapeQuotes     | True            | N/A             | N/A            | N/A               | ...              | Not available in Pandas |
    | ignoreLeadingWhiteSpace | True     | N/A             | N/A            | N/A               | ...              | Not available in Pandas |
    | ignoreTrailingWhiteSpace | True    | N/A             | N/A            | N/A               | ...              | Not available in Pandas |
    | charToEscapeQuoteEscaping | escape or `\0` | N/A       | N/A            | N/A               | ...              | Not available in Pandas |
    | dateFormat       | `yyyy-MM-dd`      | N/A             | N/A            | N/A               | ...              | Pandas implements Timestamp, not Date |
    | timestampFormat  | `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` | date_format | N/A | timestampFormat (alias=date_format) | yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX] | Follows PySpark defaults |
    | timestampNTZFormat | `yyyy-MM-dd'T'HH:mm:ss[.SSS]` | N/A | N/A          | N/A               | ...              | Pandas implements Timestamp, see above |
    | compression      | None            | compression     | infer          | compression       | None             |       |
    | encoding         | utf-8           | encoding        | utf-8          | N/A               | ...              | Not explicitly implemented |
    | nullValue        | ""              | na_rep          | ""             | N/A               | ""               | Not explicitly implemented |
    | emptyValue       | ""              | na_rep          | ""             | N/A               | ""               | Not explicitly implemented |
    | N/A              | ...             | float_format    | N/A            | N/A               | ...              | Not explicitly implemented |
    | N/A              | ...             | decimal         | N/A            | N/A               | ...              | Not explicitly implemented |
    | N/A              | ...             | index_label     | None           | N/A               | ...              | Not explicitly implemented |
    | N/A              | ...             | columns         | N/A            | N/A               | ...              | Not explicitly implemented |
    | N/A              | ...             | mode            | N/A            | N/A               | ...              | Not explicitly implemented |
    | N/A              | ...             | quoting         | N/A            | N/A               | ...              | Not explicitly implemented |
    | N/A              | ...             | errors          | N/A            | N/A               | ...              | Not explicitly implemented |
    | N/A              | ...             | storage_options | N/A            | N/A               | ...              | Not explicitly implemented |

    differences with Pyspark:
    -------------------------
    - dateFormat -> Pandas implements Timestamp, not just Date. Hence, Koheesio sets the default to the python
        equivalent of PySpark's default.
    - compression -> Spark does not compress by default, hence Koheesio does not compress by default. Compression can
        be provided though.

    Parameters
    -----------
    header : bool, optional, default=True
        Whether to write the names of columns as the first line. In Pandas a list of strings can be given assumed to be
        aliases for the column names - this is not supported in this class. Instead, the column names as used in the
        dataframe are used as the header. Koheesio sets this default to True to match Pandas' default.
    sep : str, optional, default=,
        Field delimiter for the output file. Default is ','.
    quote : str, optional, default="
        String of length 1. Character used to quote fields. In PySpark, this is called 'quote', in Pandas this is
        called 'quotechar'. Default is '"'.
    quoteAll : bool, optional, default=False
        A flag indicating whether all values should always be enclosed in quotes in a field. Koheesio sets the default
        (False) to only escape values containing a quote character - this is Pyspark's default behavior. In Pandas, this
        is called 'doublequote'. Default is False.
    escape : str, optional, default=\\
        String of length 1. Character used to escape sep and quotechar when appropriate. Koheesio sets this default to
        `\\` to match Pyspark's default behavior. In Pandas, this field is called 'escapechar', and defaults to None.
        Default is '\\'.
    timestampFormat : str, optional, default=yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]
        Sets the string that indicates a date format for datetime objects. Koheesio sets this default to a close
        equivalent of Pyspark's default (excluding timezone information). In Pandas, this field is called 'date_format'.
        Note: Pandas does not support Java Timestamps, only Python Timestamps. The Pyspark default is
        `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` which mimics the iso8601 format (`datetime.isoformat()`). Default is
        '%Y-%m-%dT%H:%M:%S.%f'.
    lineSep : str, optional, default=\n
        String of length 1. Defines the character used as line separator that should be used for writing. Default is
        os.linesep.
    compression : Optional[Literal["infer", "gzip", "bz2", "zip", "xz", "zstd", "tar"]], optional, default=None
        A string representing the compression to use for on-the-fly compression of the output data. Note: Pandas sets
        this default to 'infer', Koheesio sets this default to 'None' leaving the data uncompressed by default. Can be
        set to one of 'infer', 'gzip', 'bz2', 'zip', 'xz', 'zstd', or 'tar'. See Pandas documentation for more details.
    """

    # pylint: enable=C0301

    header: bool = Field(
        default=True,
        description="Whether to write the names of columns as the first line. In Pandas a list of strings can be given "
        "assumed to be aliases for the column names - this is not supported in this class. Instead, the column names "
        "as used in the dataframe are used as the header. Koheesio sets this default to True to match Pandas' default.",
    )
    sep: constr(max_length=1) = Field(default=",", description="Field delimiter for the output file")
    quote: constr(max_length=1) = Field(
        default='"',
        description="String of length 1. Character used to quote fields. In PySpark, this is called 'quote', in Pandas "
        "this is called 'quotechar'.",
        alias="quotechar",
    )
    quoteAll: bool = Field(
        default=False,
        description="A flag indicating whether all values should always be enclosed in quotes in a field. Koheesio set "
        "the default (False) to only escape values containing a quote character - this is Pyspark's default behavior. "
        "In Pandas, this is called 'doublequote'.",
        alias="doublequote",
    )
    escape: constr(max_length=1) = Field(
        default="\\",
        description="String of length 1. Character used to escape sep and quotechar when appropriate. Koheesio sets "
        "this default to `\\` to match Pyspark's default behavior. In Pandas, this is called 'escapechar', and "
        "defaults to None.",
        alias="escapechar",
    )
    timestampFormat: str = Field(
        default="%Y-%m-%dT%H:%M:%S.%f",  # closely matches Pyspark (Java) `yyyy-MM-dd'T'HH:mm:ss[.SSS]`
        description="Sets the string that indicates a date format for datetime objects. Koheesio sets this default to "
        "a close equivalent of Pyspark's default (excluding timezone information). In Pandas, this field is called "
        "'date_format'. Note: Pandas does not support Java Timestamps, only Python Timestamps. "
        "The Pyspark default is `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` which mimics the iso8601 format "
        "(`datetime.isoformat()`).",
        alias="date_format",
    )
    lineSep: Optional[constr(max_length=1)] = Field(
        default=linesep,
        description="String of length 1. Defines the character used as line separator that should be used for writing.",
        alias="line_terminator",
    )
    compression: Optional[PandasCompressionOptions] = Field(
        default=None,
        description="A string representing the compression to use for on-the-fly compression of the output data."
        "Note: Pandas sets this default to 'infer', Koheesio sets this default to 'None' leaving the data uncompressed "
        "by default. Can be set to one of 'infer', 'gzip', 'bz2', 'zip', 'xz', 'zstd', or 'tar'. "
        "See Pandas documentation for more details.",
    )
    emptyValue: Optional[str] = Field(
        default="",
        description="The string to use for missing values. Koheesio sets this default to an empty string.",
    )

    nullValue: Optional[str] = Field(
        default="",
        description="The string to use for missing values. Koheesio sets this default to an empty string.",
    )

    # -- Pandas specific properties --
    index: bool = Field(
        default=False,
        description="Toggles whether to write row names (index). Default False in Koheesio - pandas default is True.",
    )

    class Output(BufferWriter.Output):
        """Output class for PandasCsvBufferWriter"""

        pandas_df: Optional[pandas.DataFrame] = Field(None, description="The Pandas DataFrame that was written")

    def get_options(self, options_type: str = "csv") -> dict:
        """Returns the options to pass to Pandas' to_csv() method."""
        try:
            import pandas as _pd

            pandas_version = version.parse(_pd.__version__)
        except ImportError:
            raise ImportError("Pandas is required to use this writer")

        # Use line_separator for pandas 2.0.0 and later
        line_sep_option_naming = "line_separator" if pandas_version >= version.parse("2.0.0") else "line_terminator"

        csv_options = {
            "header": self.header,
            "sep": self.sep,
            "quotechar": self.quote,
            "doublequote": self.quoteAll,
            "escapechar": self.escape,
            "na_rep": self.emptyValue or self.nullValue,
            line_sep_option_naming: self.lineSep,
            "index": self.index,
            "date_format": self.timestampFormat,
            "compression": self.compression,
            **self.params,
        }

        if options_type == "spark":
            csv_options["lineterminator"] = csv_options.pop(line_sep_option_naming)
        elif options_type == "koheesio_pandas_buffer_writer":
            csv_options["line_terminator"] = csv_options.pop(line_sep_option_naming)

        return csv_options

    def execute(self) -> BufferWriter.Output:
        """Write the DataFrame to the buffer using Pandas to_csv() method.
        Compression is handled by pandas to_csv() method.
        """
        # convert the Spark DataFrame to a Pandas DataFrame
        self.output.pandas_df = self.df.toPandas()

        # create csv file in memory
        file_buffer = self.output.buffer
        self.output.pandas_df.to_csv(file_buffer, **self.get_options(options_type="spark"))


# pylint: disable=C0301
class PandasJsonBufferWriter(BufferWriter, ExtraParamsMixin):
    """
    Write a Spark DataFrame to JSON file(s) using Pandas.

    Takes inspiration from https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html

    Note
    ----
    This class is not meant to be used for writing huge amounts of data. It is meant to be used for writing
    smaller amounts of data to more arbitrary file systems (e.g. SFTP).

    Parameters
    -----------
    orient : Literal["split", "records", "index", "columns", "values", "table"]
        Format of the resulting JSON string. Default is 'records'.
    lines : bool
        Format output as one JSON object per line. Only used when orient='records'. Default is True.
        - If true, the output will be formatted as one JSON object per line.
        - If false, the output will be written as a single JSON object.
        Note: this value is only used when orient='records' and will be ignored otherwise.
    date_format : Literal["iso", "epoch"]
        Type of date conversion. Default is 'iso'. See `Date and Timestamp Formats` for a detailed description and
        more information.
    double_precision : int
        Number of decimal places for encoding floating point values. Default is 10.
    force_ascii : bool
        Force encoded string to be ASCII. Default is True.
    compression : Optional[Literal["gzip"]]
        A string representing the compression to use for on-the-fly compression of the output data.
        Koheesio sets this default to 'None' leaving the data uncompressed. Can be set to gzip' optionally.
        Other compression options are currently not supported by Koheesio for JSON output.

    Other Possible Parameters
    -------------------------

    ### Date and Timestamp Formats in JSON
    The `date_format` and `date_unit` parameters in pandas `to_json()` method are used to control the representation of
    dates and timestamps in the resulting JSON.

    - `date_format`: This parameter determines the format of date strings. It accepts two options: 'epoch' and 'iso'.
        - 'epoch': Dates are represented as the number of milliseconds since the epoch (1970-01-01).
        - 'iso': Dates are represented in ISO8601 format, 'YYYY-MM-DDTHH:MM:SS'.
        In Pandas the default value depends on the `orient` parameter. For `orient='table'`, the default is 'iso'. For
        all other `orient` values, the default is 'epoch'.
        However, in Koheesio, the default is set to 'iso' irrespective of the `orient` parameter.

    - `date_unit`: This parameter specifies the time unit for encoding timestamps and datetime objects. It accepts
        four options: 's' for seconds, 'ms' for milliseconds, 'us' for microseconds, and 'ns' for nanoseconds.
        The default is 'ms'. Note that this parameter is ignored when `date_format='iso'`.

    ### Orient Parameter
    The `orient` parameter is used to specify the format of the resulting JSON string. Each option is useful in
    different scenarios depending on whether you need to preserve the index, data types, and/or column names of the
    original DataFrame. The set of possible orients is:

    - 'records': List of dictionaries with each dictionary representing a row of data. This is the default orient in
        Koheesio.
        - Does not preserve the index.
        - If `lines=True` (default), each record is written as a separate line in the output file.
            - Example:
            ```json
            {"column1": value1, "column2": value2}
            {"column1": value3, "column2": value4}
            ```
        - If `lines=False`, all records are written within a single JSON array.
            - Example:
            ```json
            [{"column1": value1, "column2": value2}, {"column1": value3, "column2": value4}]
            ```
    - 'split': Dictionary containing indexes, columns, and data.
        - Preserves data types and indexes of the original DataFrame.
        - Example:
        ```json
        {"index": [index], "columns": [columns], "data": [values]}
        ```
    - 'index': Dictionary with DataFrame indexes as keys and dictionaries with column names and values as values.
        - Preserves the index.
        - Example:
        ```json
        {"index1": {"column1": value1, "column2": value2}}
        ```
    - 'columns': Dictionary with column names as keys and dictionaries with indexes and values as values.
        - Preserves data types and indexes of the original DataFrame.
        - Example:
        ```json
        {"column1": {"index1": value1}, "column2": {"index1": value1}}
        ```
    - 'values': Just the values in the DataFrame.
        - Does not preserve the index or columns.
        - Example:
        ```json
        [[value1, value2], ..., [valueN-1, valueN]]
        ```
    - 'table': Dictionary with 'schema' and 'data' keys. 'schema' has information about data types and index, 'data'
        has the actual data.
        - Preserves data types and indexes of the original DataFrame.
        - Example:
        ```json
        {
          "schema": {
            "fields": [
              {
                "name": "index",
                "type": "dtype"
              }
            ],
            "primaryKey": ["index"]
          },
          "pandas_version": "1.4.0",
          "data": [
            {
              "column1": "value1",
              "column2": "value2"
            }
          ]
        }
        ```

    Note
    ----
    For 'records' orient, set `lines` to True to write each record as a separate line. The pandas output will
    then match the PySpark output (orient='records' and lines=True parameters).

    References
    ----------
    - [Pandas DataFrame to_json documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html)
    - [Pandas IO tools (text, CSV, HDF5, …) documentation](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html)
    """

    # pylint: enable=C0301

    orient: Literal["split", "records", "index", "columns", "values", "table"] = Field(
        default="records",
        description="Format of the resulting JSON string. Default is 'records'.",
    )
    lines: bool = Field(
        default=True,
        description="Format output as one JSON object per line. Only used when orient='records'. Default is True.",
    )
    date_format: Literal["iso", "epoch"] = Field(
        default="iso",
        description="Type of date conversion. Default is 'iso'.",
    )
    double_precision: int = Field(
        default=10,
        description="Number of decimal places for encoding floating point values. Default is 10.",
    )
    force_ascii: bool = Field(
        default=True,
        description="Force encoded string to be ASCII. Default is True.",
    )
    columns: Optional[list[str]] = Field(
        default=None,
        description="The columns to write. If None, all columns will be written.",
    )
    compression: Optional[Literal["gzip"]] = Field(
        default=None,
        description="A string representing the compression to use for on-the-fly compression of the output data."
        "Koheesio sets this default to 'None' leaving the data uncompressed by default. "
        "Can be set to 'gzip' optionally.",
    )

    class Output(BufferWriter.Output):
        """Output class for PandasJsonBufferWriter"""

        pandas_df: Optional[pandas.DataFrame] = Field(None, description="The Pandas DataFrame that was written")

    def get_options(self) -> dict:
        """Returns the options to pass to Pandas' to_json() method."""
        json_options = {
            "orient": self.orient,
            "date_format": self.date_format,
            "double_precision": self.double_precision,
            "force_ascii": self.force_ascii,
            "lines": self.lines,
            **self.params,
        }

        # ignore the 'lines' parameter if orient is not 'records'
        if self.orient != "records":
            del json_options["lines"]

        return json_options

    def execute(self) -> BufferWriter.Output:
        """Write the DataFrame to the buffer using Pandas to_json() method."""
        df = self.df
        if self.columns:
            df = df[self.columns]

        # convert the Spark DataFrame to a Pandas DataFrame
        self.output.pandas_df = df.toPandas()

        # create json file in memory
        file_buffer = self.output.buffer
        self.output.pandas_df.to_json(file_buffer, **self.get_options())

        # compress the buffer if compression is set
        if self.compression:
            self.output.compress()
