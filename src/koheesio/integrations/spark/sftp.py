"""
This module contains the SFTPWriter class and the SFTPWriteMode enum.

The SFTPWriter class is used to write data to a file on an SFTP server.
It uses the Paramiko library to establish an SFTP connection and write data to the server.
The data to be written is provided by a BufferWriter, which generates the data in a buffer.
See the docstring of the SFTPWriter class for more details.
Refer to koheesio.spark.writers.buffer for more details on the BufferWriter interface.

The SFTPWriteMode enum defines the different write modes that the SFTPWriter can use.
These modes determine how the SFTPWriter behaves when the file it is trying to write to already exists on the server.
For more details on each mode, see the docstring of the SFTPWriteMode enum.
"""

from typing import Optional, Union
from enum import Enum
import hashlib
from pathlib import Path
import time

from paramiko.sftp_client import SFTPClient
from paramiko.transport import Transport

from pydantic import PrivateAttr

from koheesio.models import (
    Field,
    InstanceOf,
    SecretStr,
    field_validator,
    model_validator,
)
from koheesio.spark.writers import Writer
from koheesio.spark.writers.buffer import (
    BufferWriter,
    PandasCsvBufferWriter,
    PandasJsonBufferWriter,
)

__all__ = ["SFTPWriteMode", "SFTPWriter", "SendCsvToSftp", "SendJsonToSftp"]


# pylint: disable=E1101
class SFTPWriteMode(str, Enum):
    """
    The different write modes for the SFTPWriter.

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
    * If the file exists and the new data is different from the existing data, a backup will be created and the file
        will be overwritten.
    * If it does not exist, a new file will be created.

    ### UPDATE:
    * If the file exists and the new data is different from the existing data, the file will be overwritten.
    * If the file exists and the new data is the same as the existing data, the method will return without writing
        anything.
    * If the file does not exist, a new file will be created.
    """

    OVERWRITE = "overwrite"
    APPEND = "append"
    IGNORE = "ignore"
    EXCLUSIVE = "exclusive"
    BACKUP = "backup"
    UPDATE = "update"

    @classmethod
    def from_string(cls, mode: str) -> "SFTPWriteMode":
        """Return the SFTPWriteMode for the given string."""
        return cls[mode.upper()]

    @property
    def write_mode(self) -> str:
        """Return the write mode for the given SFTPWriteMode."""
        if self in {SFTPWriteMode.OVERWRITE, SFTPWriteMode.BACKUP, SFTPWriteMode.EXCLUSIVE, SFTPWriteMode.UPDATE}:
            return "wb"  # Overwrite, Backup, Exclusive, Update modes set the file to be written from the beginning
        if self == SFTPWriteMode.APPEND:
            return "ab"  # Append mode sets the file to be written from the end


class SFTPWriter(Writer):
    """
    Write a Dataframe to SFTP through a BufferWriter

    Concept
    -------
    * This class uses Paramiko to connect to an SFTP server and write the contents of a buffer to a file on the server.
    * This implementation takes inspiration from https://github.com/springml/spark-sftp

    Parameters
    ----------
    path : Union[str, Path]
        Path to the folder to write to
    file_name : Optional[str], optional, default=None
        Name of the file. If not provided, the file name is expected to be part of the path. Make sure to add the
        desired file extension.
    host : str
        SFTP Host
    port : int
        SFTP Port
    username : SecretStr, optional, default=None
        SFTP Server Username
    password : SecretStr, optional, default=None
        SFTP Server Password
    buffer_writer : BufferWriter
        This is the writer that will generate the body of the file that will be written to the specified
        file through SFTP. Details on how the DataFrame is written to the buffer should be implemented in the
        implementation of the BufferWriter class. Any BufferWriter can be used here, as long as it implements the
        BufferWriter interface.
    mode : SFTPWriteMode, optional, default=SFTPWriteMode.OVERWRITE
        Write mode: overwrite, append, ignore, exclusive, backup, or update. See the docstring of SFTPWriteMode for
        more details.
    """

    path: Union[str, Path] = Field(default=..., description="Path to the folder to write to", alias="prefix")
    file_name: Optional[str] = Field(
        default=None,
        description="Name of the file. If not provided, the file name is expected to be part of the path. Make sure to "
        "add the desired file extension!",
        alias="filename",
    )
    host: str = Field(default=..., description="SFTP Host")
    port: int = Field(default=..., description="SFTP Port")
    username: Optional[SecretStr] = Field(default=None, description="SFTP Server Username")
    password: Optional[SecretStr] = Field(default=None, description="SFTP Server Password")

    buffer_writer: InstanceOf[BufferWriter] = Field(
        default=...,
        description="This is the writer that will generate the body of the file that will be written to the specified "
        "file through SFTP. Details on how the DataFrame is written to the buffer should be implemented in the "
        "implementation of the BufferWriter class. Any BufferWriter can be used here, as long as it implements the "
        "BufferWriter interface.",
    )

    mode: SFTPWriteMode = Field(
        default=SFTPWriteMode.OVERWRITE,
        description="Write mode: overwrite, append, ignore, exclusive, backup, or update." + SFTPWriteMode.__doc__,  # type: ignore
    )

    # private attrs
    _client: Optional[SFTPClient] = PrivateAttr(default=None)
    _transport: Optional[Transport] = PrivateAttr(default=None)

    @model_validator(mode="before")
    def validate_path_and_file_name(cls, data: dict) -> dict:
        """Validate the path, make sure path and file_name are Path objects."""
        path_or_str = data.get("path")

        if isinstance(path_or_str, str):
            # make sure the path is a Path object
            path_or_str = Path(path_or_str)

        if not isinstance(path_or_str, Path):
            raise ValueError(f"Invalid path: {path_or_str}")

        if file_name := data.get("file_name", data.get("filename")):
            path_or_str = path_or_str / file_name
            try:
                del data["filename"]
            except KeyError:
                pass
            data["file_name"] = file_name

        data["path"] = path_or_str
        return data

    @field_validator("host")
    def validate_sftp_host(cls, host: str) -> str:
        """Validate the host"""
        # remove the sftp:// prefix if present
        if host.startswith("sftp://"):
            host = host.replace("sftp://", "")

        # remove the trailing slash if present
        if host.endswith("/"):
            host = host[:-1]

        return host

    @property
    def write_mode(self) -> str:
        """Return the write mode for the given SFTPWriteMode."""
        mode = SFTPWriteMode.from_string(self.mode)  # Convert string to SFTPWriteMode
        return mode.write_mode

    @property
    def transport(self) -> Transport:
        """Return the transport for the SFTP connection. If it doesn't exist, create it.

        If the username and password are provided, use them to connect to the SFTP server.
        """
        if not self._transport:
            self._transport = Transport((self.host, self.port))
            if self.username and self.password:
                self._transport.connect(
                    username=self.username.get_secret_value(), password=self.password.get_secret_value()
                )
            else:
                self._transport.connect()
        return self._transport

    @property
    def client(self) -> SFTPClient:
        """Return the SFTP client. If it doesn't exist, create it."""
        if not self._client:
            try:
                self._client = SFTPClient.from_transport(self.transport)
            except EOFError as e:
                self.log.error(f"Failed to create SFTP client. Transport active: {self.transport.is_active()}")
                raise e
        return self._client

    def _close_client(self) -> None:
        """Close the SFTP client and transport."""
        if self.client:
            self.client.close()
        if self.transport:
            self.transport.close()

    def write_file(self, file_path: str, buffer_output: InstanceOf[BufferWriter.Output]) -> None:
        """
        Using Paramiko, write the data in the buffer to SFTP.
        """
        with self.client.open(file_path, self.write_mode) as file:
            self.log.debug(f"Writing file {file_path} to SFTP...")
            file.write(buffer_output.read())

    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if a file exists on the SFTP server.
        """
        try:
            self.client.stat(file_path)
            return True
        except IOError:
            return False

    def _handle_write_mode(self, file_path: str, buffer_output: InstanceOf[BufferWriter.Output]) -> None:
        """
        Handle different write modes.

        See SFTPWriteMode for more details.
        """
        if not self.check_file_exists(file_path) or self.mode in {SFTPWriteMode.OVERWRITE, SFTPWriteMode.APPEND}:
            # If the file doesn't exist, write the file (irrespective of the mode)
            # Overwrite and Append modes will write the file irrespective of whether it exists or not
            self.write_file(file_path, buffer_output)
            return

        if self.mode == SFTPWriteMode.EXCLUSIVE:
            # If the file exists in EXCLUSIVE mode, raise an error
            raise FileExistsError(f"File {file_path} already exists on the SFTP server.")

        if self.mode == SFTPWriteMode.IGNORE:
            # If the file exists in IGNORE mode, return without writing
            return

        # If the file exists and the mode is UPDATE or BACKUP, check if the new data is different from the existing data
        new_data = buffer_output.read()
        sha256_new_data = hashlib.sha256(new_data).hexdigest()

        with self.client.open(file_path, "rb") as file:
            existing_data = file.read()
            sha256_existing_data = hashlib.sha256(existing_data).hexdigest()

        if sha256_existing_data == sha256_new_data:
            # If the new data is the same as the existing data, return without writing
            return

        # If the new data is different from the existing data
        if self.mode == SFTPWriteMode.BACKUP:
            # If the mode is BACKUP, create a backup
            timestamp = int(time.time())
            backup_file_path = f"{file_path}.{timestamp}.bak"
            self.log.debug(f"Creating backup of {file_path} as {backup_file_path}...")
            self.client.rename(file_path, backup_file_path)

        # Then overwrite the file
        self.write_file(file_path, buffer_output)

    def execute(self) -> Writer.Output:
        buffer_output: InstanceOf[BufferWriter.Output] = self.buffer_writer.write(self.df)

        # write buffer to the SFTP server
        try:
            self._handle_write_mode(self.path.as_posix(), buffer_output)
        finally:
            self._close_client()


class SendCsvToSftp(PandasCsvBufferWriter, SFTPWriter):
    """
    Write a DataFrame to an SFTP server as a CSV file.

    This class uses the PandasCsvBufferWriter to generate the CSV data and the SFTPWriter to write the data to the
    SFTP server.

    Example
    -------
    ```python
    from koheesio.spark.writers import SendCsvToSftp

    writer = SendCsvToSftp(
        # SFTP Parameters
        host="sftp.example.com",
        port=22,
        username="user",
        password="password",
        path="/path/to/folder",
        file_name="file.tsv.gz",
        # CSV Parameters
        header=True,
        sep="\t",
        quote='"',
        timestampFormat="%Y-%m-%d",
        lineSep=os.linesep,
        compression="gzip",
        index=False,
    )

    writer.write(df)
    ```

    In this example, the DataFrame `df` is written to the file `file.csv.gz` in the folder `/path/to/folder` on the
    SFTP server. The file is written as a CSV file with a tab delimiter (TSV), double quotes as the quote character,
    and gzip compression.

    Parameters
    ----------
    path : Union[str, Path]
        Path to the folder to write to.
    file_name : Optional[str]
        Name of the file. If not provided, it's expected to be part of the path.
    host : str
        SFTP Host.
    port : int
        SFTP Port.
    username : SecretStr
        SFTP Server Username.
    password : SecretStr
        SFTP Server Password.
    mode : SFTPWriteMode
        Write mode: overwrite, append, ignore, exclusive, backup, or update.
    header : bool
        Whether to write column names as the first line. Default is True.
    sep : str
        Field delimiter for the output file. Default is ','.
    quote : str
        Character used to quote fields. Default is '"'.
    quoteAll : bool
        Whether all values should be enclosed in quotes. Default is False.
    escape : str
        Character used to escape sep and quote when needed. Default is '\\'.
    timestampFormat : str
        Date format for datetime objects. Default is '%Y-%m-%dT%H:%M:%S.%f'.
    lineSep : str
        Character used as line separator. Default is os.linesep.
    compression : Optional[Literal["infer", "gzip", "bz2", "zip", "xz", "zstd", "tar"]]
        Compression to use for the output data. Default is None.

    See Also
    --------
    For more details on the CSV parameters, refer to the PandasCsvBufferWriter class documentation.
    """

    buffer_writer: Optional[PandasCsvBufferWriter] = Field(default=None, validate_default=False)

    @model_validator(mode="after")
    def set_up_buffer_writer(self) -> "SendCsvToSftp":
        """Set up the buffer writer, passing all CSV related options to it."""
        self.buffer_writer = PandasCsvBufferWriter(**self.get_options(options_type="koheesio_pandas_buffer_writer"))
        return self

    def execute(self) -> SFTPWriter.Output:
        SFTPWriter.execute(self)


class SendJsonToSftp(PandasJsonBufferWriter, SFTPWriter):
    """
    Write a DataFrame to an SFTP server as a JSON file.

    This class uses the PandasJsonBufferWriter to generate the JSON data and the SFTPWriter to write the data to the
    SFTP server.

    Example
    -------
    ```python
    from koheesio.spark.writers import SendJsonToSftp

    writer = SendJsonToSftp(
        # SFTP Parameters (Inherited from SFTPWriter)
        host="sftp.example.com",
        port=22,
        username="user",
        password="password",
        path="/path/to/folder",
        file_name="file.json.gz",
        # JSON Parameters (Inherited from PandasJsonBufferWriter)
        orient="records",
        date_format="iso",
        double_precision=2,
        date_unit="ms",
        lines=False,
        compression="gzip",
        index=False,
    )

    writer.write(df)
    ```

    In this example, the DataFrame `df` is written to the file `file.json.gz` in the folder `/path/to/folder` on the
    SFTP server. The file is written as a JSON file with gzip compression.

    Parameters
    ----------
    path : Union[str, Path]
        Path to the folder on the SFTP server.
    file_name : Optional[str]
        Name of the file, including extension. If not provided, expected to be part of the path.
    host : str
        SFTP Host.
    port : int
        SFTP Port.
    username : SecretStr
        SFTP Server Username.
    password : SecretStr
        SFTP Server Password.
    mode : SFTPWriteMode
        Write mode: overwrite, append, ignore, exclusive, backup, or update.
    orient : Literal["split", "records", "index", "columns", "values", "table"]
        Format of the JSON string. Default is 'records'.
    lines : bool
        If True, output is one JSON object per line. Only used when orient='records'. Default is True.
    date_format : Literal["iso", "epoch"]
        Type of date conversion. Default is 'iso'.
    double_precision : int
        Decimal places for encoding floating point values. Default is 10.
    force_ascii : bool
        If True, encoded string is ASCII. Default is True.
    compression : Optional[Literal["gzip"]]
        Compression to use for output data. Default is None.

    See Also
    --------
    For more details on the JSON parameters, refer to the PandasJsonBufferWriter class documentation.
    """

    buffer_writer: Optional[PandasJsonBufferWriter] = Field(default=None, validate_default=False)

    @model_validator(mode="after")
    def set_up_buffer_writer(self) -> "SendJsonToSftp":
        """Set up the buffer writer, passing all JSON related options to it."""
        self.buffer_writer = PandasJsonBufferWriter(
            **self.get_options(), compression=self.compression, columns=self.columns
        )
        return self

    def execute(self) -> SFTPWriter.Output:
        SFTPWriter.execute(self)
