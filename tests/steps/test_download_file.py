import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from requests_mock.mocker import Mocker

from koheesio.steps.download_file import DownloadFileStep

URL = "http://example.com/testfile.txt"


@pytest.fixture
def test_data_path(data_path):
    return Path(f"{data_path}/transformations/string_data/100000_rows_with_strings.json")


@pytest.fixture
def test_data_bytes(test_data_path):
    return test_data_path.read_bytes()


@pytest.fixture
def mock_response(test_data_bytes):
    mock_resp = MagicMock()
    mock_resp.iter_content = MagicMock(return_value=test_data_bytes)
    return mock_resp


def test_download_file_step_large_file(mock_response, tmp_path, test_data_path, test_data_bytes):
    with Mocker() as mocker:
        # Arrange
        with test_data_path.open("rb") as f:
            mocker.get(URL, content=f.read())
        download_path = tmp_path / "downloads"
        download_path.mkdir(exist_ok=True)

        # Act
        step = DownloadFileStep(url=URL, download_path=download_path)
        step.execute()

        # Assert
        downloaded_file = download_path / "testfile.txt"
        assert downloaded_file.exists()
        assert downloaded_file.read_bytes() == test_data_bytes


def test_invalid_write_mode():
    """Test that an error is raised for an invalid write mode"""
    with pytest.raises(ValueError):
        DownloadFileStep(
            mode="invalid_mode",
            url=URL,
        )


def test_download_file_step_overwrite_mode(mock_response, tmp_path):
    """In OVERWRITE mode, the file should be overwritten if it exists"""
    # Arrange
    download_path = tmp_path / "downloads"
    download_path.mkdir(exist_ok=True)
    downloaded_file = download_path / "testfile.txt"
    downloaded_file.touch(exist_ok=True)
    downloaded_file.write_bytes(b"foo")
    with Mocker() as mocker:
        mocker.get(URL, content=b"bar")

        # Act
        step = DownloadFileStep(url=URL, download_path=download_path, mode="overwrite")
        step.execute()

    # Assert
    assert downloaded_file.exists()
    assert downloaded_file.read_bytes() == b"bar"


def test_download_file_step_append_mode(mock_response, tmp_path):
    """In APPEND mode, the file should be appended to if it exists"""
    # Arrange
    download_path = tmp_path / "downloads"
    download_path.mkdir(exist_ok=True)
    downloaded_file = download_path / "testfile.txt"
    downloaded_file.touch(exist_ok=True)
    downloaded_file.write_bytes(b"foo")
    with Mocker() as mocker:
        mocker.get(URL, content=b"bar")

        # Act
        step = DownloadFileStep(url=URL, download_path=download_path, mode="append")
        step.execute()

    # Assert
    assert downloaded_file.exists()
    assert downloaded_file.read_bytes() == b"foobar"


def test_download_file_step_ignore_mode(mock_response, tmp_path, caplog):
    """In IGNORE mode, the class should return without writing anything if the file exists"""
    # Arrange
    download_path = tmp_path / "downloads"
    download_path.mkdir(exist_ok=True)
    downloaded_file = download_path / "testfile.txt"
    downloaded_file.touch(exist_ok=True)
    downloaded_file.write_bytes(b"foo")

    # FIXME: logging is not working in the unit tests
    with caplog.at_level("INFO"), Mocker() as mocker:
        mocker.get(URL, content=b"bar")

        # Act
        step = DownloadFileStep(url=URL, download_path=download_path, mode="ignore")
        print(f"1 {caplog.text = }")
        step.execute()
        print(f"2 {caplog.text = }")

        # Assert
        print(f"3 {caplog.text = }")
        assert downloaded_file.exists()
        print(f"4 {caplog.text = }")
        assert downloaded_file.read_bytes() == b"foo"
        print(f"5 {caplog.text = }")
        assert "Ignoring testfile.txt based on IGNORE mode." in caplog.text


def test_download_file_step_exclusive_mode(mock_response, tmp_path):
    """In EXCLUSIVE mode, an error should be raised if the file exists"""
    # Arrange
    download_path = tmp_path / "downloads"
    download_path.mkdir(exist_ok=True)
    downloaded_file = download_path / "testfile.txt"
    downloaded_file.touch(exist_ok=True)
    downloaded_file.write_bytes(b"foo")
    with Mocker() as mocker:
        mocker.get(URL, content=b"bar")

        # Act and Assert
        with pytest.raises(FileExistsError):
            step = DownloadFileStep(url=URL, download_path=download_path, mode="exclusive")
            step.execute()


def test_download_file_step_backup_mode(mock_response, tmp_path):
    """In BACKUP mode, the file should be backed up before being overwritten"""
    # Arrange
    download_path = tmp_path / "downloads"
    download_path.mkdir(exist_ok=True)
    downloaded_file = download_path / "testfile.txt"
    downloaded_file.touch(exist_ok=True)
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
