from typing import Optional
from functools import partial

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from koheesio.models import DirectoryPath, Field
from koheesio.spark import Column
from koheesio.spark.transformations import ColumnsTransformationWithTarget
from koheesio.steps.download_file import DownloadFileStep, FileWriteMode


def download_file_udf(download_path: str, chunk_size: int, mode: FileWriteMode):
    @udf(returnType=StringType())
    def download_file(url: str) -> str:
        try:
            print(f"{download_path = }, {chunk_size = }, {mode = }, {url = }")
            step = DownloadFileStep(url=url, download_path=download_path, chunk_size=chunk_size, mode=mode)
            step.execute()
            return str(step.output.download_file_path)
        except Exception as e:
            print(f"Error downloading file from URL {url}: {e}")
            return None

    return download_file


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
    columns : ListOfColumns
        The column (or list of columns) containing the URLs to download.
    target_column : Optional[str], optional, default=None
        The name of the column to store the downloaded file paths. If not provided, the result will be stored in the source column.
    download_path : str
        The local directory path where the file will be downloaded.
    chunk_size : int, optional, default=8192
        The size (in bytes) of the chunks to download the file in, must be greater than 16.
    mode : FileWriteMode, optional, default=FileWriteMode.OVERWRITE
        Write mode: overwrite, append, ignore, exclusive, or backup.
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
        description="Write mode: overwrite, append, ignore, exclusive, or backup.",
    )

    def func(self, col: Column) -> Column:
        """
        Applies a UDF to download files from URLs in the specified column and returns the file paths.

        Parameters
        ----------
        col : Column
            The column containing the URLs to download.

        Returns
        -------
        Column
            A new column with the paths of the downloaded files.
        """
        download_file = download_file_udf(self.download_path, self.chunk_size, self.mode)
        return download_file(col)


if __name__ == "__main__":
    """
    
    | key | url                                        |
    |-----|--------------------------------------------|
    | 101 |	http://www.textfiles.com/100/adventur.txt  |
    | 102 |	http://www.textfiles.com/100/arttext.fun   |
    
    """

    from pathlib import Path

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    data = [
        (101, "http://www.textfiles.com/100/adventur.txt"),
        # (102, "http://www.textfiles.com/100/arttext.fun"),
    ]
    df = spark.createDataFrame(data, ["key", "url"])

    download_path = Path("downloaded_files")
    download_path.mkdir(exist_ok=True)

    transformed_df = DownloadFileFromUrlTransformation(
        column="url",
        download_path="downloaded_files",
        target_column="downloaded_filepath",
    ).transform(df)

    transformed_df.show(truncate=False)
