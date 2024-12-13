from pathlib import Path

import pytest

from koheesio.spark import DataFrame, SparkSession  # type: ignore
from koheesio.spark.transformations.download_files import DownloadFileFromUrlTransformation  # type: ignore


@pytest.fixture
def input_df(spark: SparkSession) -> DataFrame:
    """A simple DataFrame containing two URLs."""
    return spark.createDataFrame(
        [
            (101, "http://www.textfiles.com/100/adventur.txt"),
            (102, "http://www.textfiles.com/100/arttext.fun"),
        ],
        ["key", "url"],
    )


@pytest.fixture
def download_path(tmp_path: Path) -> Path:
    _path = tmp_path / "downloads"
    _path.mkdir(exist_ok=True)
    return _path


class TestDownloadFileFromUrlTransformation:
    """
    Input DataFrame:

    | key | url                                        |
    |-----|--------------------------------------------|
    | 101 |	http://www.textfiles.com/100/adventur.txt  |
    | 102 |	http://www.textfiles.com/100/arttext.fun   |

    Output DataFrame:

    | key | url                                        | downloaded_file_path  |
    |-----|--------------------------------------------|-----------------------|
    | 101 |	http://www.textfiles.com/100/adventur.txt  | downloads/adventur.txt|
    | 102 |	http://www.textfiles.com/100/arttext.fun   | downloads/arttext.fun |

    """

    def test_downloading_files(self, input_df: DataFrame, download_path: Path) -> None:
        """Test that the files are downloaded and the DataFrame is transformed correctly."""
        # Arrange
        expected_data = [
            "downloads/adventur.txt",
            "downloads/arttext.fun",
        ]

        # Act
        transformed_df = DownloadFileFromUrlTransformation(
            column="url",
            download_path=download_path,
            target_column="downloaded_file_path",
        ).transform(input_df)
        actual_data = sorted(
            [row.asDict()["downloaded_file_path"] for row in transformed_df.select("downloaded_file_path").collect()]
        )

        # Assert

        # Check that adventur.txt and arttext.fun are actually downloaded
        assert (download_path / "adventur.txt").exists()
        assert (download_path / "arttext.fun").exists()

        assert transformed_df.count() == 2
        assert transformed_df.columns == ["key", "url", "downloaded_file_path"]

        # check that the rows of the output DataFrame are as expected
        assert actual_data == expected_data
