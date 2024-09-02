from pathlib import Path
from unittest.mock import MagicMock

from koheesio.spark import DataFrame, SparkSession
from koheesio.spark.writers import BatchOutputMode
from koheesio.spark.writers.file_writer import FileFormat, FileWriter


def test_path_validator():
    output_path = Path("/test/output_path")
    file_writer = FileWriter(output_mode=BatchOutputMode.APPEND, format=FileFormat.parquet, path=output_path)
    assert isinstance(file_writer.path, str)


def test_execute(dummy_df, mocker):
    path = "expected_path"
    output_mode = BatchOutputMode.APPEND
    options = {"option1": "value1", "option2": "value2"}
    format = FileFormat.parquet
    writer = FileWriter(df=dummy_df, output_mode=output_mode, path=path, format=format, **options)

    mock_df_writer = MagicMock()
    mocker.patch.object(DataFrame, "write", mock_df_writer)
    mock_df_writer.options.return_value = mock_df_writer

    writer.execute()
    mock_df_writer.options.assert_called_with(**options)
    mock_df_writer.save.assert_called_with(path=path, format=format.value, mode=output_mode.value)
