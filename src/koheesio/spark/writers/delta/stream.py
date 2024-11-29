"""
This module defines the DeltaTableStreamWriter class, which is used to write streaming dataframes to Delta tables.
"""

from typing import Optional
from email.policy import default

from pydantic import Field

from koheesio.models import BaseModel
from koheesio.spark.writers.delta.batch import DeltaTableWriter
from koheesio.spark.writers.stream import StreamWriter


class DeltaTableStreamWriter(StreamWriter, DeltaTableWriter):
    """Delta table stream writer"""

    class Options(BaseModel):
        """Options for DeltaTableStreamWriter"""

        allow_population_by_field_name: bool = Field(
            default=True, description=" To do convert to Field and pass as .options(**config)"
        )
        maxBytesPerTrigger: Optional[str] = Field(
            default=None, description="How much data to be processed per trigger. The default is 1GB"
        )
        maxFilesPerTrigger: int = Field(
            default == 1000,
            description="The maximum number of new files to be considered in every trigger (default: 1000).",
        )

    def execute(self) -> DeltaTableWriter.Output:
        if self.batch_function:
            self.streaming_query = self.writer.start()
        else:
            self.streaming_query = self.writer.toTable(tableName=self.table.table_name)
