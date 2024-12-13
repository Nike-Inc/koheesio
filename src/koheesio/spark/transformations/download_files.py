from typing import Union

from pyspark.sql.types import Row

from koheesio.models import DirectoryPath, Field
from koheesio.spark import Column
from koheesio.spark.transformations import ColumnsTransformationWithTarget
from koheesio.spark.utils.common import get_column_name
from koheesio.steps.download_file import DownloadFileStep, FileWriteMode


class DownloadFileFromUrlTransformation(ColumnsTransformationWithTarget):
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
    column : Union[Column, str]
        The column that holds the URLs to download.
    download_path : str
        The local directory path where the file will be downloaded to.
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

    class Output(ColumnsTransformationWithTarget.Output):
        download_file_paths: dict[str, str] = Field(
            default_factory=dict, description="The downloaded file paths per URL."
        )

    def func(self, partition: set[str]) -> None:
        """
        Takes a set of urls and downloads the files from the URLs in the specified column.

        Parameters
        ----------
        partition : set[str]
            A set of URLs to download the files from.
        """
        for url in partition:
            step = DownloadFileStep(
                url=url, download_path=self.download_path, mode=self.mode, chunk_size=self.chunk_size
            )
            step.execute()
            self.output.download_file_paths[url] = step.output.download_file_path.relative_to(
                self.download_path.parent
            ).as_posix()

    def execute(self) -> Output:
        """
        Download files from URLs in the specified column.
        """
        # Collect the URLs from the DataFrame and process them
        source_column_name = get_column_name(self.column)  # type: ignore
        partition = {row.asDict()[source_column_name] for row in self.df.select(self.column).collect()}  # type: ignore
        self.func(partition)

        # Using join, re-add the download_file_paths to the DataFrame in the target column
        url_df = self.spark.createDataFrame(
            data=self.output.download_file_paths.items(), schema=f"{self.column} string, {self.target_column} string"
        )
        self.output.df = (
            self.df.join(
                other=url_df,  # type: ignore
                on=source_column_name,
                how="left",
            ).select(*self.df.columns, self.target_column)  # type: ignore
        )
