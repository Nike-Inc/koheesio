from pathlib import Path
import os

from koheesio.spark.writers import BatchOutputMode
from koheesio.spark.writers.file_writer import FileFormat, FileWriter


def test_path_validator():
    output_path = Path("/test/output_path")
    file_writer = FileWriter(output_mode=BatchOutputMode.APPEND, format=FileFormat.parquet, path=output_path)
    assert isinstance(file_writer.path, str)


def test_execute(dummy_df, tmp_path, random_uuid):
    path = tmp_path / f"test_{random_uuid}"
    output_mode = BatchOutputMode.APPEND
    options = {"option1": "value1", "option2": "value2"}
    fmt = FileFormat.parquet
    writer = FileWriter(df=dummy_df, output_mode=output_mode, path=path, format=fmt, **options)

    writer.execute()

    # Verify the file is written to the temporary location
    assert os.path.exists(path)
