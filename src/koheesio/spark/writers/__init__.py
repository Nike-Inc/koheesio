"""The Writer class is used to write the DataFrame to a target."""

from typing import Optional
from abc import ABC, abstractmethod
from enum import Enum

from koheesio.models import Field
from koheesio.spark import DataFrame, SparkStep


# TODO: Investigate if we can clean various OutputModes into a more streamlined structure
class BatchOutputMode(str, Enum):
    """For Batch:

    - append: Append the contents of the DataFrame to the output table, default option in Koheesio.
    - overwrite: overwrite the existing data.
    - ignore: ignore the operation (i.e. no-op).
    - error or errorifexists: throw an exception at runtime.
    - merge: update matching data in the table and insert rows that do not exist.
    - merge_all: update matching data in the table and insert rows that do not exist.
    """

    # Batch only modes
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR = "error"
    ERRORIFEXISTS = "error"
    MERGE_ALL = "merge_all"
    MERGEALL = "merge_all"
    MERGE = "merge"


class StreamingOutputMode(str, Enum):
    """For Streaming:

    - append: only the new rows in the streaming DataFrame will be written to the sink.
    - complete: all the rows in the streaming DataFrame/Dataset will be written to the sink every time there are some
       updates.
    - update: only the rows that were updated in the streaming DataFrame/Dataset will be written to the sink every time
       there are some updates. If the query doesn't contain aggregations, it will be equivalent to append mode.
    """

    # Streaming only modes
    APPEND = "append"
    COMPLETE = "complete"
    UPDATE = "update"


class Writer(SparkStep, ABC):
    """The Writer class is used to write the DataFrame to a target."""

    df: Optional[DataFrame] = Field(default=None, description="The Spark DataFrame", exclude=True)
    format: str = Field(default="delta", description="The format of the output")

    @property
    def streaming(self) -> bool:
        """Check if the DataFrame is a streaming DataFrame or not."""
        if not self.df:
            raise RuntimeError("No valid Dataframe was passed")

        return self.df.isStreaming

    @abstractmethod
    def execute(self) -> SparkStep.Output:
        """Execute on a Writer should handle writing of the self.df (input) as a minimum"""
        # self.df  # input dataframe
        ...

    def write(self, df: Optional[DataFrame] = None) -> SparkStep.Output:
        """Write the DataFrame to the output using execute() and return the output.

        If no DataFrame is passed, the self.df will be used.
        If no self.df is set, a RuntimeError will be thrown.
        """
        self.df = df or self.df
        if not self.df:
            raise RuntimeError("No valid Dataframe was passed")
        self.execute()
        return self.output
