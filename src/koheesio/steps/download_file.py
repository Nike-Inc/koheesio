# TODO: add module description

from __future__ import annotations

from typing import Any, Optional, Type
from enum import Enum
from pathlib import Path
import time

from koheesio import StepOutput
from koheesio.models import DirectoryPath, Field, FilePath
from koheesio.steps.http import HttpGetStep


# pylint: disable=E1101
class FileWriteMode(str, Enum):
    """
    The different write modes for the DownloadFileStep.

    ### OVERWRITE:
    * If the file exists, it will be overwritten.
    * If it does not exist, a new file will be created.

    ### APPEND:
    * If the file exists, the new data will be appended to it.
    * If it does not exist, a new file will be created.

    ### IGNORE:
    * If the file exists, the method will return without writing anything.
    * If it does not exist, a new file will be created.

    ### EXCLUSIVE:
    * If the file exists, an error will be raised.
    * If it does not exist, a new file will be created.

    ### BACKUP:
    * If the file exists, a backup will be created and the original file will be overwritten.
    * If it does not exist, a new file will be created.
    """

    OVERWRITE = "overwrite"
    APPEND = "append"
    IGNORE = "ignore"
    EXCLUSIVE = "exclusive"
    BACKUP = "backup"

    @classmethod
    def from_string(cls, mode: str) -> Type[FileWriteMode[Any]]:
        """Return the FileWriteMode for the given string."""
        return cls[mode.upper()]

    @property
    def write_mode(self) -> str:
        """Return the write mode for the given SFTPWriteMode."""
        if self in {FileWriteMode.OVERWRITE, FileWriteMode.BACKUP, FileWriteMode.EXCLUSIVE}:
            # OVERWRITE, BACKUP, and EXCLUSIVE modes set the file to be written from the beginning
            return "wb"
        if self == FileWriteMode.APPEND:
            # APPEND mode sets the file to be written from the end
            return "ab"


class DownloadFileStep(HttpGetStep):
    """
    Downloads a file from the given URL and saves it to the specified download path.

    Examples
    --------
    # TODO: add examples

    Parameters
    ----------
    url : str
        The URL to download the file from.
    download_path : str
        The local directory path where the file will be downloaded.
    chunk_size : int, optional, default=8192
        The size (in bytes) of the chunks to download the file in, must be greater than 16.
    mode : FileWriteMode, optional, default=FileWriteMode.OVERWRITE
        Write mode: overwrite, append, ignore, exclusive, or backup.
        See the docstring of `FileWriteMode` for more details.
    """

    download_path: DirectoryPath = Field(
        ..., description="The local directory path where the file will be downloaded to."
    )
    chunk_size: int = Field(
        8192,
        ge=16,
        description="The size (in bytes) of the chunks to download the file in, must be greater than or equal to 16.",
    )
    mode: FileWriteMode = Field(
        default=FileWriteMode.OVERWRITE,
        description="Write mode: overwrite, append, ignore, exclusive, backup, or update.",
    )

    class Output(StepOutput):
        download_file_path: FilePath = Field(..., description="The full path where the file was downloaded to.")

    def handle_file_write_modes(self, _filepath: Path, _filename: str) -> Optional[str]:
        """Handle different write modes for the file and return the appropriate write mode."""
        mode = FileWriteMode.from_string(self.mode)  # Convert string to FileWriteMode
        write_mode = str(mode.write_mode)  # Determine the write mode

        # FIXME: logging is not working in the unit tests
        # OVERWRITE and APPEND modes will write the file irrespective of whether it exists or not
        if _filepath.exists() and mode not in {FileWriteMode.OVERWRITE, FileWriteMode.APPEND}:
            if mode == FileWriteMode.IGNORE:
                # If the file exists in IGNORE mode, return without writing
                self.log.info(f"File {_filepath} already exists. Ignoring {_filename} based on IGNORE mode.")
                self.output.download_file_path = _filepath
                return None

            elif mode == FileWriteMode.EXCLUSIVE:
                # If the file exists in EXCLUSIVE mode, raise an error
                raise FileExistsError(
                    f"File {_filepath} already exists. Cannot write to {_filename} based on EXCLUSIVE mode."
                )

            elif mode == FileWriteMode.BACKUP:
                # In BACKUP mode, we first create a timestamped backup before overwriting the existing file.
                file_to_be_backed_up = _filepath
                backup_path = _filepath.with_suffix(f"{_filepath.suffix}.{int(time.time())}.bak")
                # create the backup
                self.log.info(f"Creating backup of {_filename} as {backup_path}...")
                file_to_be_backed_up.rename(backup_path)

        return write_mode

    def execute(self) -> Output:
        """
        Executes the file download process, handling different write modes, and saving the file to the specified path.
        """
        _filename = Path(self.url).name
        _filepath = self.download_path / _filename

        # Handle different write modes. If mode comes back as None, return without writing
        if (mode := self.handle_file_write_modes(_filepath, _filename)) is None:
            return self.output

        # Create the download path if it does not exist
        self.output.download_file_path = _filepath
        self.output.download_file_path.touch(exist_ok=True)

        # Download the file content and write the downloaded content to the file
        with self.request() as response, self.output.download_file_path.open(mode=mode) as f:
            for chunk in response.iter_content(chunk_size=self.chunk_size):
                self.log.debug(f"Downloading chunk of size {len(chunk)}")
                self.log.debug(f"Downloaded {f.tell()} bytes")
                self.log.debug(f"Writing to file {self.output.download_file_path}")
                f.write(chunk)
