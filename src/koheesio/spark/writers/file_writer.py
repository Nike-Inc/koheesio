"""
File writers for different formats:
- CSV
- Parquet
- Avro
- JSON
- ORC
- Text

The FileWriter class is a configurable Writer that allows writing to different file formats providing any option needed.
CsvFileWriter, ParquetFileWriter, AvroFileWriter, JsonFileWriter, OrcFileWriter, and TextFileWriter are convenience
classes that just set the `format` field to the corresponding file format.

"""

from __future__ import annotations

from typing import Union
from enum import Enum
from pathlib import Path

from koheesio.models import ExtraParamsMixin, Field, field_validator
from koheesio.spark.writers import BatchOutputMode, Writer

__all__ = [
    "FileFormat",
    "FileWriter",
    "CsvFileWriter",
    "ParquetFileWriter",
    "AvroFileWriter",
    "JsonFileWriter",
    "OrcFileWriter",
    "TextFileWriter",
]


class FileFormat(str, Enum):
    """Supported file formats for the FileWriter class"""

    csv = "csv"
    parquet = "parquet"
    avro = "avro"
    json = "json"
    orc = "orc"
    text = "text"


class FileWriter(Writer, ExtraParamsMixin):
    """
    A configurable Writer that allows writing to different file formats providing any option needed.

    Extra parameters can be passed to the writer as keyword arguments.

    Examples
    --------
    ```python
    writer = FileWriter(
        df=df,
        path="path/to/file.csv",
        output_mode=BatchOutputMode.APPEND,
        format=FileFormat.parquet,
        compression="snappy",
    )
    ```
    """

    output_mode: BatchOutputMode = Field(default=BatchOutputMode.APPEND, description="The output mode to use")
    format: FileFormat = Field(..., description="The file format to use when writing the data.")
    path: Union[Path, str] = Field(default=..., description="The path to write the file to")

    @field_validator("path")
    def ensure_path_is_str(cls, v: Union[Path, str]) -> str:
        """Ensure that the path is a string as required by Spark."""
        if isinstance(v, Path):
            return str(v.absolute().as_posix())
        return v

    def execute(self) -> FileWriter.Output:
        writer = self.df.write

        if self.extra_params:
            self.log.info(f"Setting extra parameters for the writer: {self.extra_params}")
            writer = writer.options(**self.extra_params)

        writer.save(path=self.path, format=self.format, mode=self.output_mode)  # type: ignore

        self.output.df = self.df


class CsvFileWriter(FileWriter):
    """Writes a DataFrame to a CSV file.

    This class is a convenience class that sets the `format` field to `FileFormat.csv`.

    Extra parameters can be passed to the writer as keyword arguments.

    Examples
    --------
    ```python
    writer = CsvFileWriter(
        df=df,
        path="path/to/file.csv",
        output_mode=BatchOutputMode.APPEND,
        header=True,
    )
    ```
    """

    format: FileFormat = FileFormat.csv


class ParquetFileWriter(FileWriter):
    """Writes a DataFrame to a Parquet file.

    This class is a convenience class that sets the `format` field to `FileFormat.parquet`.

    Extra parameters can be passed to the writer as keyword arguments.

    Examples
    --------
    ```python
    writer = ParquetFileWriter(
        df=df,
        path="path/to/file.parquet",
        output_mode=BatchOutputMode.APPEND,
        compression="snappy",
    )
    ```
    """

    format: FileFormat = FileFormat.parquet


class AvroFileWriter(FileWriter):
    """Writes a DataFrame to an Avro file.

    This class is a convenience class that sets the `format` field to `FileFormat.avro`.

    Extra parameters can be passed to the writer as keyword arguments.

    Examples
    --------
    ```python
    writer = AvroFileWriter(
        df=df,
        path="path/to/file.avro",
        output_mode=BatchOutputMode.APPEND,
    )
    ```
    """

    format: FileFormat = FileFormat.avro


class JsonFileWriter(FileWriter):
    """Writes a DataFrame to a JSON file.

    This class is a convenience class that sets the `format` field to `FileFormat.json`.

    Extra parameters can be passed to the writer as keyword arguments.

    Examples
    --------
    ```python
    writer = JsonFileWriter(
        df=df,
        path="path/to/file.json",
        output_mode=BatchOutputMode.APPEND,
    )
    ```
    """

    format: FileFormat = FileFormat.json


class OrcFileWriter(FileWriter):
    """Writes a DataFrame to an ORC file.

    This class is a convenience class that sets the `format` field to `FileFormat.orc`.

    Extra parameters can be passed to the writer as keyword arguments.

    Examples
    --------
    ```python
    writer = OrcFileWriter(
        df=df, path="path/to/file.orc", output_mode=BatchOutputMode.APPEND
    )
    ```
    """

    format: FileFormat = FileFormat.orc


class TextFileWriter(FileWriter):
    """Writes a DataFrame to a text file.

    This class is a convenience class that sets the `format` field to `FileFormat.text`.

    Extra parameters can be passed to the writer as keyword arguments.

    Examples
    --------
    ```python
    writer = TextFileWriter(
        df=df, path="path/to/file.txt", output_mode=BatchOutputMode.APPEND
    )
    ```
    """

    format: FileFormat = FileFormat.text
