from typing import Union
from functools import partial

from pyspark.sql.types import Row

from koheesio.models import DirectoryPath, Field
from koheesio.spark import Column
from koheesio.spark.transformations import Transformation
from koheesio.steps.download_file import DownloadFileStep, FileWriteMode


class DownloadFileFromUrlTransformation(Transformation):
    """
    Downloads content from URLs in the specified column and stores the downloaded file paths in a new column.


    Write Modes
    -----------

    `DownloadFileFromUrlTransformation` supports the following write modes:

    === "OVERWRITE"

        ```python
        DownloadFileFromUrlTransformation(
            ...
            mode=FileWriteMode.OVERWRITE,
        )
        ```

        - If the file exists, it will be overwritten.
        - If it does not exist, a new file will be created.

        <small>(this is the default mode)</small>

    === "APPEND"

        ```python
        DownloadFileFromUrlTransformation(
            ...
            mode=FileWriteMode.APPEND,
        )
        ```

        - If the file exists, the new data will be appended to it.
        - If it does not exist, a new file will be created.

        <br>

    === "IGNORE"

        ```python
        DownloadFileFromUrlTransformation(
            ...
            mode=FileWriteMode.IGNORE,
        )
        ```

        - If the file exists, the method will return without writing anything.
        - If it does not exist, a new file will be created.

        <br>

    === "EXCLUSIVE"

        ```python
        DownloadFileFromUrlTransformation(
            ...
            mode=FileWriteMode.EXCLUSIVE,
        )
        ```

        - If the file exists, an error will be raised.
        - If it does not exist, a new file will be created.

        <br>

    === "BACKUP"

        ```python
        DownloadFileFromUrlTransformation(
            ...
            mode=FileWriteMode.BACKUP,
        )
        ```

        - If the file exists, a backup will be created and the original file will be overwritten.
        - If it does not exist, a new file will be created.

        <br>

    Parameters
    ----------
    columns : ListOfColumns
        The column (or list of columns) containing the URLs to download.
    download_path : str
        The local directory path where the file will be downloaded.
    chunk_size : int, optional, default=8192
        The size (in bytes) of the chunks to download the file in, must be greater than 16.
    mode : FileWriteMode, optional, default=FileWriteMode.OVERWRITE
        Write mode: overwrite, append, ignore, exclusive, or backup.
    """

    column: Union[Column, str] = Field(
        default="",
        description="The column that holds the URLs to download.",
    )
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
        description="Write mode: overwrite, append, ignore, exclusive, or backup.",
    )

    @staticmethod
    def _download_file_step(row: Row, download_path: str, chunk_size: int, mode: FileWriteMode) -> str:
        """
        Download the file from the given URL and save it to the specified download path.
        """
        url = row[0]
        step = DownloadFileStep(url=url, download_path=download_path, mode=mode, chunk_size=chunk_size)
        step.execute()
        return step.output.downloaded_file_path

    def execute(self) -> Transformation.Output:
        """
        Download files from URLs in the specified column.
        """

        self.df.select(self.column).foreach(
            partial(
                self._download_file_step,
                download_path=self.download_path,
                chunk_size=self.chunk_size,
                mode=self.mode,
            )
        )


if __name__ == "__main__":
    # ## using koheesio step -- works
    # print("Downloading file using koheesio core step")
    # step = DownloadFileStep(
    #     url="http://www.textfiles.com/100/adventur.txt",
    #     download_path=download_path,
    #     mode=FileWriteMode.BACKUP,
    # )
    # step.execute()
    #
    # ## using requests library
    # print("Downloading file using python")
    # import requests
    #
    # url = "http://www.textfiles.com/100/adventur.txt"
    # response = requests.get(url)
    # (download_path / "adventur.txt").write_bytes(response.content)
    ...
