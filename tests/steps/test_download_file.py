import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from requests_mock.mocker import Mocker

from koheesio.steps.download_file import DownloadFileStep

URL = "http://example.com/testfile.txt"


@pytest.fixture
def large_file_path(data_path):
    return Path(f"{data_path}/transformations/string_data/100000_rows_with_strings.json")


@pytest.fixture
def download_path(tmp_path):
    _path = tmp_path / "downloads"
    _path.mkdir(exist_ok=True)
    return _path


@pytest.fixture
def downloaded_file(download_path):
    _file = download_path / "testfile.txt"
    _file.touch(exist_ok=True)
    return _file


class TestDownloadFileStep:
    def test_large_file(self, download_path, large_file_path):
        """Test that a large file can be downloaded using default settings"""
        with Mocker() as mocker:
            # Arrange
            mocker.get(URL, content=large_file_path.read_bytes())

            # Act
            step = DownloadFileStep(url=URL, download_path=download_path)
            step.execute()

        # Assert
        downloaded_file = download_path / "testfile.txt"
        assert downloaded_file.exists()
        assert downloaded_file.read_bytes() == large_file_path.read_bytes()

    def test_invalid_write_mode(self):
        """Test that an error is raised for an invalid write mode"""
        with pytest.raises(ValueError):
            DownloadFileStep(
                mode="invalid_mode",
                url=URL,
            )

    def test_download_file_step_overwrite_mode(self, download_path, downloaded_file):
        """In OVERWRITE mode, the file should be overwritten if it exists"""
        # Arrange
        downloaded_file.write_bytes(b"foo")

        with Mocker() as mocker:
            mocker.get(URL, content=b"bar")

            # Act
            step = DownloadFileStep(url=URL, download_path=download_path, mode="overwrite")
            step.execute()

        # Assert
        assert downloaded_file.exists()
        assert downloaded_file.read_bytes() == b"bar"

    def test_download_file_step_append_mode(self, download_path, downloaded_file):
        """In APPEND mode, the file should be appended to if it exists"""
        # Arrange
        downloaded_file.write_bytes(b"foo")

        with Mocker() as mocker:
            mocker.get(URL, content=b"bar")

            # Act
            step = DownloadFileStep(url=URL, download_path=download_path, mode="append")
            step.execute()

        # Assert
        assert downloaded_file.exists()
        assert downloaded_file.read_bytes() == b"foobar"

    def test_download_file_step_ignore_mode(self, download_path, downloaded_file, caplog):
        """In IGNORE mode, the class should return without writing anything if the file exists"""
        # Arrange
        downloaded_file.write_bytes(b"foo")

        with caplog.at_level("INFO"), Mocker() as mocker:
            mocker.get(URL, content=b"bar")

            # FIXME: logging is not working in the unit tests

            # Act
            step = DownloadFileStep(url=URL, download_path=download_path, mode="ignore")
            step.log.setLevel("INFO")
            step.execute()
            print(f"2 {caplog.record_tuples = }")

            # Assert
            print(f"5 {caplog.text = }")
            assert "Ignoring testfile.txt based on IGNORE mode." in caplog.text

        print(f"3 {caplog.text = }")
        assert downloaded_file.exists()
        print(f"4 {caplog.text = }")
        assert downloaded_file.read_bytes() == b"foo"

    def test_download_file_step_exclusive_mode(self, download_path, downloaded_file):
        """In EXCLUSIVE mode, an error should be raised if the file exists"""
        # Arrange
        downloaded_file.write_bytes(b"foo")

        with Mocker() as mocker:
            mocker.get(URL, content=b"bar")

            # Act and Assert
            with pytest.raises(FileExistsError):
                step = DownloadFileStep(url=URL, download_path=download_path, mode="exclusive")
                step.execute()

    def test_download_file_step_backup_mode(self, download_path, downloaded_file):
        """In BACKUP mode, the file should be backed up before being overwritten"""
        # Arrange
        downloaded_file.write_bytes(b"foo")

        with Mocker() as mocker:
            mocker.get(URL, content=b"bar")

            # Act
            step = DownloadFileStep(url=URL, download_path=download_path, mode="backup")
            step.execute()

        # Assert
        assert downloaded_file.exists()
        assert downloaded_file.read_bytes() == "bar".encode()
        backup_files = list(download_path.glob("testfile.txt.*.bak"))
        assert len(backup_files) == 1
        assert backup_files[0].read_bytes() == "foo".encode()
