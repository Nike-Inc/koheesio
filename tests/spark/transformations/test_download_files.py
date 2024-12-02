import pytest

from koheesio.spark.transformations.download_files import DownloadFileFromUrlTransformation


@pytest.fixture
def input_df(spark):
    """A simple DataFrame containing two URLs."""
    return spark.createDataFrame(
        [
            (101, "http://www.textfiles.com/100/adventur.txt"),
            (102, "http://www.textfiles.com/100/arttext.fun"),
        ],
        ["key", "url"],
    )


@pytest.fixture
def download_path(tmp_path):
    _path = tmp_path / "downloads"
    _path.mkdir(exist_ok=True)
    return _path


class TestDownloadFileFromUrlTransformation:
    """
    | key | url                                        |
    |-----|--------------------------------------------|
    | 101 |	http://www.textfiles.com/100/adventur.txt  |
    | 102 |	http://www.textfiles.com/100/arttext.fun   |
    """

    def test_downloading_files(self, input_df, download_path):
        transformed_df = DownloadFileFromUrlTransformation(
            column="url",
            download_path=download_path,
        ).transform(input_df)

        # Check that adventur.txt and arttext.fun are actually downloaded
        assert (download_path / "adventur.txt").exists()
        assert (download_path / "arttext.fun").exists()

        assert transformed_df.count() == 2
        assert transformed_df.columns == ["key", "url", "downloaded_file_path"]
        assert transformed_df.select("downloaded_file_path").collect() == [
            ("downloaded_files/adventur.txt",),
            ("downloaded_files/arttext.fun",),
        ]
