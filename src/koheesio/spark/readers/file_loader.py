"""
Generic file Readers for different file formats.

Supported file formats:
- CSV
- Parquet
- Avro
- JSON
- ORC
- Text

Examples:
```python
from koheesio.spark.readers import (
    CsvReader,
    ParquetReader,
    AvroReader,
    JsonReader,
    OrcReader,
)

csv_reader = CsvReader(path="path/to/file.csv", header=True)
parquet_reader = ParquetReader(path="path/to/file.parquet")
avro_reader = AvroReader(path="path/to/file.avro")
json_reader = JsonReader(path="path/to/file.json")
orc_reader = OrcReader(path="path/to/file.orc")
```

For more information about the available options, see Spark's
[official documentation](https://spark.apache.org/docs/latest/sql-data-sources.html).
"""

from typing import Optional, Union
from enum import Enum
from pathlib import Path

from pyspark.sql.types import StructType

from koheesio.models import ExtraParamsMixin, Field, field_validator
from koheesio.spark.readers import Reader


class FileFormat(str, Enum):
    """Supported file formats.

    This enum represents the supported file formats that can be used with the FileLoader class.
    The available file formats are:
    - csv: Comma-separated values format
    - parquet: Apache Parquet format
    - avro: Apache Avro format
    - json: JavaScript Object Notation format
    - orc: Apache ORC format
    - text: Plain text format
    """

    csv = "csv"
    parquet = "parquet"
    avro = "avro"
    json = "json"
    orc = "orc"
    # excel = "excel"  # TODO: Add support for Excel
    # xml = "xml"  # TODO: Add support for XML
    # yaml = "yaml"  # TODO: Add support for YAML
    text = "text"


# pylint: disable=line-too-long
class FileLoader(Reader, ExtraParamsMixin):
    """Generic file reader.

    Available file formats:
    - CSV
    - Parquet
    - Avro
    - JSON
    - ORC
    - Text (default)

    Extra parameters can be passed to the reader using the `extra_params` attribute or as keyword arguments.

    Example:
    ```python
    reader = FileLoader(
        path="path/to/textfile.txt",
        format="text",
        header=True,
        lineSep="\n",
    )
    ```

    For more information about the available options, see Spark's
    [official pyspark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.text.html)
    and [read about text data source](https://spark.apache.org/docs/latest/sql-data-sources-text.html).

    Also see the [data sources generic options](https://spark.apache.org/docs/3.5.0/sql-data-sources-generic-options.html).
    """

    format: FileFormat = Field(default=FileFormat.text, description="File format to read")
    path: Union[Path, str] = Field(default=..., description="Path to the file to read")
    schema_: Optional[Union[StructType, str]] = Field(
        default=None, description="Schema to use when reading the file", validate_default=False, alias="schema"
    )
    streaming: Optional[bool] = Field(default=False, description="Whether to read the files as a Stream or not")

    @field_validator("path")
    def ensure_path_is_str(cls, path: Union[Path, str]) -> Union[Path, str]:
        """Ensure that the path is a string as required by Spark."""
        if isinstance(path, Path):
            return str(path.absolute().as_posix())
        return path

    def execute(self) -> Reader.Output:
        """Reads the file, in batch or as a stream, using the specified format and schema, while applying any extra parameters."""
        reader = self.spark.readStream if self.streaming else self.spark.read
        reader = reader.format(self.format)

        if self.schema_:
            reader.schema(self.schema_)

        if self.extra_params:
            reader = reader.options(**self.extra_params)

        self.output.df = reader.load(self.path)  # type: ignore


class CsvReader(FileLoader):
    """Reads a CSV file.

    This class is a convenience class that sets the `format` field to `FileFormat.csv`.

    Extra parameters can be passed to the reader using the `extra_params` attribute or as keyword arguments.

    Example:
    ```python
    reader = CsvReader(path="path/to/file.csv", header=True)
    ```

    For more information about the available options,
    see the
    [official pyspark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html)
    and [read about CSV data source](https://spark.apache.org/docs/latest/sql-data-sources-csv.html).

    Also see the
    [data sources generic options](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html).
    """

    format: FileFormat = FileFormat.csv


class ParquetReader(FileLoader):
    """Reads a Parquet file.

    This class is a convenience class that sets the `format` field to `FileFormat.parquet`.

    Extra parameters can be passed to the reader using the `extra_params` attribute or as keyword arguments.

    Example:
    ```python
    reader = ParquetReader(path="path/to/file.parquet", mergeSchema=True)
    ```

    For more information about the available options,
    see the
    [official pyspark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.parquet.html)
    and [read about Parquet data source](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html).

    Also see the
    [data sources generic options](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html).
    """

    format: FileFormat = FileFormat.parquet


class AvroReader(FileLoader):
    """Reads an Avro file.

    This class is a convenience class that sets the `format` field to `FileFormat.avro`.

    Extra parameters can be passed to the reader using the `extra_params` attribute or as keyword arguments.

    Example:
    ```python
    reader = AvroReader(path="path/to/file.avro", mergeSchema=True)
    ```

    Make sure to have the `spark-avro` package installed in your environment.

    For more information about the available options,
    see the [official documentation](https://spark.apache.org/docs/latest/sql-data-sources-avro.html#content).
    """

    format: FileFormat = FileFormat.avro


class JsonReader(FileLoader):
    """Reads a JSON file.

    This class is a convenience class that sets the `format` field to `FileFormat.json`.

    Extra parameters can be passed to the reader using the `extra_params` attribute or as keyword arguments.

    Example:
    ```python
    reader = JsonReader(path="path/to/file.json", allowComments=True)
    ```

    For more information about the available options,
    see the
    [official pyspark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.json.html#pyspark.sql.DataFrameReader.json)
    and [read about JSON data source](https://spark.apache.org/docs/latest/sql-data-sources-json.html).

    Also see the
    [data sources generic options](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html).
    """

    format: FileFormat = FileFormat.json


class OrcReader(FileLoader):
    """Reads an ORC file.

    This class is a convenience class that sets the `format` field to `FileFormat.orc`.

    Extra parameters can be passed to the reader using the `extra_params` attribute or as keyword arguments.

    Example:
    ```python
    reader = OrcReader(path="path/to/file.orc", mergeSchema=True)
    ```

    For more information about the available options,
    see the
    [official documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.orc.html)
    and [read about ORC data source](https://spark.apache.org/docs/latest/sql-data-sources-orc.html).

    Also see the
    [data sources generic options](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html).
    """

    format: FileFormat = FileFormat.orc
