from textwrap import dedent

from koheesio.models import FilePath
from koheesio.spark.utils.common import SparkSession


def setup_test_data(spark: SparkSession, delta_file: FilePath, view_name: str = "delta_test_view") -> None:
    """
    Sets up test data for the Spark session.

    Reads a given Delta file, creates a temporary view, and populates a Delta table with the view's data.

    Parameters
    ----------
    spark : SparkSession
        The Spark session to use.
    delta_file : Union[str, Path]
        The path to the Delta file to read.
    view_name : str, optional
        The name of the temporary view to create, by default "delta_test_view"
    """
    delta_file_str = delta_file.absolute().as_posix()
    spark.read.format("delta").load(delta_file_str).limit(10).createOrReplaceTempView("delta_test_view")
    spark.sql(
        dedent(
            """
            CREATE TABLE IF NOT EXISTS delta_test_table
            USING DELTA
            TBLPROPERTIES ("delta.enableChangeDataFeed" = "true")
            AS SELECT v.* FROM {view_name} v
            """
        ),
        view_name=view_name,
    )  # type: ignore