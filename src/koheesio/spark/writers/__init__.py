"""The Writer class is used to write the DataFrame to a target.

The module provides output mode enums for both batch and streaming writes, organized in a nested
structure for better organization and case-insensitive access.

Examples
--------
Using the nested OutputMode structure (recommended):

```python
from koheesio.spark.writers import OutputMode

# Case-insensitive access:
mode = OutputMode.BATCH.APPEND
mode = OutputMode.batch.APPEND  # Also works
mode = OutputMode.Batch.OVERWRITE  # Also works

stream_mode = OutputMode.STREAMING.COMPLETE
stream_mode = OutputMode.streaming.COMPLETE  # Also works
```

Backward compatibility (deprecated but supported):

```python
from koheesio.spark.writers import (
    BatchOutputMode,
    StreamingOutputMode,
)

mode = BatchOutputMode.APPEND
stream_mode = StreamingOutputMode.COMPLETE
```
"""

from typing import Optional, Union
from abc import ABC, abstractmethod
from enum import Enum

from koheesio.models import Field, nested_enum
from koheesio.spark import DataFrame, SparkStep


@nested_enum
class OutputMode:
    """Container for batch and streaming output modes with case-insensitive access.

    This nested enum structure organizes output modes by type (batch vs streaming) and supports
    case-insensitive attribute access for convenience.

    Usage
    -----
    Access modes using any case variation:
    - `OutputMode.BATCH.APPEND`
    - `OutputMode.batch.APPEND`
    - `OutputMode.Batch.OVERWRITE`
    - `OutputMode.STREAMING.complete`
    - `OutputMode.streaming.COMPLETE`

    Examples
    --------
    Basic usage in a writer:

    ```python
    from koheesio.spark.writers import OutputMode, Writer


    class MyWriter(Writer):
        output_mode: str = Field(
            default=OutputMode.BATCH.APPEND,
            description="Output mode for writing",
        )
    ```

    Using in DeltaTableWriter:

    ```python
    from koheesio.spark.writers import OutputMode
    from koheesio.spark.writers.delta import DeltaTableWriter

    writer = DeltaTableWriter(
        table="my_table",
        output_mode=OutputMode.batch.MERGE,  # Case-insensitive!
        key_columns=["id"],
    )
    ```

    See Also
    --------
    Writer : Base class for all writers
    """

    class BATCH(str, Enum):
        """Output modes for Batch processing.

        Available modes:
        ---------------
        - append: Append the contents of the DataFrame to the output table (default in Koheesio)
        - overwrite: Overwrite the existing data
        - ignore: Ignore the operation (no-op)
        - error / errorifexists: Throw an exception at runtime if data exists
        - merge: Update matching data and insert rows that do not exist (requires key columns)
        - merge_all: Update all matching data and insert rows that do not exist (full table merge)

        Note
        ----
        MERGE and MERGE_ALL modes are specific to certain writers (e.g., DeltaTableWriter) and require
        additional configuration such as key columns for matching records.

        Examples
        --------
        ```python
        from koheesio.spark.writers import OutputMode

        # All case variations work:
        mode = OutputMode.BATCH.APPEND
        mode = OutputMode.batch.APPEND
        mode = OutputMode.Batch.OVERWRITE
        ```
        """

        APPEND = "append"
        OVERWRITE = "overwrite"
        IGNORE = "ignore"
        ERROR = "error"
        ERRORIFEXISTS = "error"  # Alias for ERROR
        MERGE = "merge"
        MERGE_ALL = "merge_all"
        MERGEALL = "merge_all"  # Alias for MERGE_ALL

    class STREAMING(str, Enum):
        """Output modes for Streaming processing.

        Available modes:
        ---------------
        - append: Only new rows in the streaming DataFrame are written to the sink
        - complete: All rows in the streaming DataFrame are written every time there are updates
        - update: Only rows that were updated are written. Equivalent to append mode for non-aggregation queries

        Note
        ----
        The choice of output mode depends on your streaming query:
        - Use APPEND for queries without aggregations or with append-only aggregations
        - Use COMPLETE for aggregation queries where you need the full result
        - Use UPDATE for aggregation queries where you only want changed rows

        Examples
        --------
        ```python
        from koheesio.spark.writers import OutputMode

        # All case variations work:
        mode = OutputMode.STREAMING.APPEND
        mode = OutputMode.streaming.APPEND
        mode = OutputMode.Streaming.COMPLETE
        ```
        """

        APPEND = "append"
        COMPLETE = "complete"
        UPDATE = "update"


# Backward compatibility aliases
# These are maintained for existing code but new code should use OutputMode.BATCH and OutputMode.STREAMING
BatchOutputMode = OutputMode.BATCH
StreamingOutputMode = OutputMode.STREAMING

# Type alias for type hints - can be used with Union[BatchOutputMode, StreamingOutputMode]
OutputModeType = Union[OutputMode.BATCH, OutputMode.STREAMING]


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
