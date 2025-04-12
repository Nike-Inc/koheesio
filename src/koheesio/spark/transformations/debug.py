"""
In this module you will find utility transformations that are useful for debugging and development purposes.
"""

from typing import Optional
from logging import DEBUG

from koheesio.models import Field
from koheesio.spark import SparkStep
from koheesio.spark.transformations import Transformation


class Peek(Transformation):
    """
    Peek the DataFrame: lets you inspect the content and/or the schema of the DataFrame at any given point of a Koheesio
    pipeline.

    You may want to run or disable this step depending on the environment the pipeline is running in. This can be
    accomplished by configuring the `enabled` and `enabled_by_logger` flags. The idea is that you can simply have the
    `enabled` flag decide whether to run the transformation (in this case, `enabled_by_logger` must be set to False),
    or you can run it based on the current log level. To accomplish that, set both `enabled` and `enabled_by_logger`
    to True: the step will be executed only if the log level is set to DEBUG.

    Here's a table that summarizes the scenarios and the expected behavior of the step:

    +----------------+---------------------+---------------------+---------------------+
    | enabled        | enabled_by_logger   | log level           | Is Peek enabled?    |
    +----------------+---------------------+---------------------+---------------------+
    | False          | *                   | *                   | NO                  |
    | True           | False               | *                   | YES                 |
    | True           | True                | DEBUG               | YES                 |
    | True           | True                | INFO or higher      | NO                  |
    +----------------+---------------------+---------------------+---------------------+

    You can simply slide in this transformation in between the transformations you wish to apply in order to peek the
    DataFrame data and schema:

    Example
    -------
    ```python
    from koheesio.spark.etl_task import EtlTask
    from koheesio.spark.readers.dummy import DummyReader
    from koheesio.spark.transformations.debug import Peek
    from koheesio.spark.transformations.transform import Transform
    from koheesio.spark.writers.dummy import DummyWriter

    reader = DummyReader(n=10)
    writer = DummyWriter()
    transformations = [
        Peek(enabled=True, enabled_by_logger=False, print_schema=True),
        Transform(
            lambda df: df.withColumn("id_squared", df["id"] * df["id"])
        ),
        Peek(enabled=True, enabled_by_logger=False, print_schema=True),
    ]
    task = EtlTask(
        source=reader, transformations=transformations, target=writer
    )
    task.execute()
    ```
    """

    enabled: bool = Field(
        ...,
        description="Whether to run this step or not. The resulting behavior may influenced by "
        "the enabled_by_logger paramter and the current log level. See the class "
        "docstring for more details.",
    )

    enabled_by_logger: Optional[bool] = Field(
        default=False,
        description="Enables the step if the log level of the logger is set to DEBUG or lower. The resulting behavior "
        "may influenced by the enabled parameter and the current log level. See the class docstring for "
        "more details.",
    )

    print_schema: Optional[bool] = Field(default=False, description="Enables printing the schema of the DataFrame")
    truncate_output: Optional[bool] = Field(
        default=False, description="Whether to truncate the output when printing the DataFrame"
    )
    vertical: Optional[bool] = Field(default=False, description="Print the DataFrame vertically")
    n: Optional[int] = Field(default=20, description="Number of rows to show")

    def execute(self) -> SparkStep.Output:
        if self.active:
            self._peek()

    def _peek(self):
        self.df = self.df.cache()

        if self.print_schema:
            self.df.printSchema()

        self.df.show(truncate=self.truncate_output, n=self.n, vertical=self.vertical)
        self.output.df = self.df

    @property
    def active(self) -> bool:
        # The is active parameter takes precedence over the logger level
        if not self.enabled:
            return False

        # Is active is True, and enabled by logger is False, we don't care about current log level
        if not self.enabled_by_logger:
            return True

        # If both flags are True, it will be active based on log level
        if self._on_debug():
            return True

        return False

    def _on_debug(self) -> bool:
        """
        Check if the logger level is set to DEBUG or lower.

        Returns
        --------
        bool
            True if the logger level is set to DEBUG or lower, False otherwise.
        """
        return self.log.isEnabledFor(DEBUG)
