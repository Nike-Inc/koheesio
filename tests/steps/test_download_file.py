from typing import Any
from pathlib import Path

import pytest
from requests_mock.mocker import Mocker

from koheesio import LoggingFactory  # type: ignore
from koheesio.steps.download_file import DownloadFileStep, FileWriteMode  # type: ignore

URL = "http://example.com/testfile.txt"
log = LoggingFactory.get_logger(name="test_download_file", inherit_from_koheesio=True)


@pytest.fixture
def large_file_path(data_path: str) -> Path:
    return Path(f"{data_path}/transformations/string_data/100000_rows_with_strings.json")


@pytest.fixture
def download_path(tmp_path: Path) -> Path:
    _path = tmp_path / "downloads"
    _path.mkdir(exist_ok=True)
    return _path


@pytest.fixture
def downloaded_file(download_path: Path) -> Path:
    _file = download_path / "testfile.txt"
    _file.touch(exist_ok=True)
    return _file


class TestFileWriteMode:
    """Test the FileWriteMode enum"""

    @pytest.mark.parametrize(
        "mode, expected",
        [
            (FileWriteMode.OVERWRITE, "wb"),
            (FileWriteMode.BACKUP, "wb"),
            (FileWriteMode.EXCLUSIVE, "wb"),
            (FileWriteMode.APPEND, "ab"),
        ],
    )
    def test_write_mode(self, mode: FileWriteMode, expected: str) -> None:
        """Test that the write mode is correctly set for each FileWriteMode"""
        assert mode.write_mode == expected

    @pytest.mark.parametrize(
        "mode_str, expected_mode",
        [
            ("overwrite", FileWriteMode.OVERWRITE),
            ("append", FileWriteMode.APPEND),
            ("ignore", FileWriteMode.IGNORE),
            ("exclusive", FileWriteMode.EXCLUSIVE),
            ("backup", FileWriteMode.BACKUP),
        ],
    )
    def test_from_string(self, mode_str: str, expected_mode: FileWriteMode) -> None:
        """Test that the FileWriteMode enum can be created from a string"""
        assert FileWriteMode.from_string(mode_str) == expected_mode


class TestDownloadFileStep:
    """Test the DownloadFileStep class"""

    def test_default_write_mode(self, download_path: Path) -> None:
        """Test that the default write mode is OVERWRITE"""
        step = DownloadFileStep(url=URL, download_path=download_path)
        assert step.mode == FileWriteMode.OVERWRITE

    def test_write_mode_conversion(self, download_path: Path) -> None:
        """Test that the write mode can be set using a string"""
        step = DownloadFileStep(url=URL, mode="append", download_path=download_path)
        assert step.mode == FileWriteMode.APPEND

    def test_large_file(self, download_path: Path, large_file_path: Path) -> None:
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

    def test_invalid_write_mode(self) -> None:
        """Test that an error is raised for an invalid write mode"""
        with pytest.raises(ValueError):
            DownloadFileStep(
                mode="invalid_mode",
                url=URL,
            )

    def test_download_file_step_overwrite_mode(self, download_path: Path, downloaded_file: Any) -> None:
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

    def test_download_file_step_append_mode(self, download_path: Path, downloaded_file: Any) -> None:
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

    def test_download_file_step_ignore_mode(
        self, download_path: Path, downloaded_file: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """In IGNORE mode, the class should return without writing anything if the file exists"""
        # Arrange
        downloaded_file.write_bytes(b"foo")

        with caplog.at_level("INFO"), Mocker() as mocker:
            mocker.get(URL, content=b"bar")

            # Act
            step = DownloadFileStep(url=URL, download_path=download_path, mode="ignore")
            step.log.setLevel("INFO")
            step.execute()

        # Assert
        assert "Ignoring testfile.txt based on IGNORE mode." in caplog.text
        assert downloaded_file.exists()
        assert downloaded_file.read_bytes() == b"foo"

    def test_download_file_step_exclusive_mode(self, download_path: Path, downloaded_file: Any) -> None:
        """In EXCLUSIVE mode, an error should be raised if the file exists"""
        # Arrange
        downloaded_file.write_bytes(b"foo")

        with Mocker() as mocker:
            mocker.get(URL, content=b"bar")

            # Act and Assert
            with pytest.raises(FileExistsError):
                step = DownloadFileStep(url=URL, download_path=download_path, mode="exclusive")
                step.execute()

    def test_download_file_step_backup_mode(self, download_path: Path, downloaded_file: Any) -> None:
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
