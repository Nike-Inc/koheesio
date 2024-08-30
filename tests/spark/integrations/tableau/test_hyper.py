from datetime import datetime
from pathlib import PurePath, Path

import pytest

from koheesio.integrations.spark.tableau.hyper import (
    HyperFileListWriter,
    HyperFileReader,
    HyperFileParquetWriter,
    TableName,
    TableDefinition,
    SqlType,
    NOT_NULLABLE,
    NULLABLE,
)

pytestmark = pytest.mark.spark


class TestHyper:
    @pytest.fixture()
    def parquet_file(self, data_path):
        path = f"{data_path}/readers/parquet_file"
        return Path(path).glob("**/*.parquet")

    @pytest.fixture()
    def hyper_file(self, data_path):
        return f"{data_path}/readers/hyper_file/dummy.hyper"

    def test_hyper_file_reader(self, hyper_file):
        df = (
            HyperFileReader(
                path=hyper_file,
            )
            .execute()
            .df
        )

        assert df.count() == 3
        assert df.dtypes == [("string", "string"), ("int", "int"), ("timestamp", "timestamp")]

    def test_hyper_file_list_writer(self, spark):
        hw = HyperFileListWriter(
            name="test",
            table_definition=TableDefinition(
                table_name=TableName("Extract", "Extract"),
                columns=[
                    TableDefinition.Column(name="string", type=SqlType.text(), nullability=NOT_NULLABLE),
                    TableDefinition.Column(name="int", type=SqlType.int(), nullability=NULLABLE),
                    TableDefinition.Column(name="timestamp", type=SqlType.timestamp(), nullability=NULLABLE),
                ],
            ),
            data=[
                ["text_1", 1, datetime(2024, 1, 1, 0, 0, 0, 0)],
                ["text_2", 2, datetime(2024, 1, 2, 0, 0, 0, 0)],
                ["text_3", None, None],
            ],
        ).execute()

        df = (
            HyperFileReader(
                path=PurePath(hw.hyper_path),
            )
            .execute()
            .df
        )

        assert df.count() == 3
        assert df.dtypes == [("string", "string"), ("int", "int"), ("timestamp", "timestamp")]

    def test_hyper_file_parquet_writer(self, data_path, parquet_file):
        hw = HyperFileParquetWriter(
            name="test",
            table_definition=TableDefinition(
                table_name=TableName("Extract", "Extract"),
                columns=[
                    TableDefinition.Column(name="string", type=SqlType.text(), nullability=NOT_NULLABLE),
                    TableDefinition.Column(name="int", type=SqlType.int(), nullability=NULLABLE),
                    TableDefinition.Column(name="timestamp", type=SqlType.timestamp(), nullability=NULLABLE),
                ],
            ),
            files=parquet_file,
        ).execute()

        df = HyperFileReader(path=PurePath(hw.hyper_path)).execute().df

        assert df.count() == 6
        assert df.dtypes == [("string", "string"), ("int", "int"), ("timestamp", "timestamp")]
