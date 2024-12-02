from pydantic import DirectoryPath

from koheesio.spark.transformations import ColumnsTransformationWithTarget


class DownloadFileFromUrlTransformation(ColumnsTransformationWithTarget):
    """
    Downloads content from URLs in the specified column and stores the downloaded file paths in a new column.

    Parameters
    ----------
    columns : ListOfColumns
        The column (or list of columns) containing the URLs to download.
    target_column : Optional[str], optional, default=None
        The name of the column to store the downloaded file paths. If not provided, the result will be stored in the source column.
    download_path : DirectoryPath
        The local directory path where the files will be downloaded.
    """

    download_path: DirectoryPath

    def func(self, column):
        pass
