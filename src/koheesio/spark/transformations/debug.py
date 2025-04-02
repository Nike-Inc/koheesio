from typing import Optional

from koheesio.spark import SparkStep
from koheesio.spark.transformations import Transformation

from koheesio.models import Field


class Peek(Transformation):
    """ Peek the DataFrame. Easily enable / disable peeking with the is_active parameter. """
    is_active: bool = Field(..., description="Enables / disables this transformation to easily switch from peeking during a development phase to skipping this step in a production environment")
    print_schema: Optional[bool] = Field(default=False, description="Enables printing the schema of the DataFrame")
    truncate_output: Optional[bool] = Field(default=False, description="Whether to truncate the output when printing the DataFrame")
    vertical: Optional[bool] = Field(default=False, description="Whether to print the DataFrame vertically")
    n: Optional[int] = Field(default=20, description="Number of rows to show")

    def execute(self) -> SparkStep.Output:
        if self.is_active:
            self._peek()
        return self.output

    def _peek(self):
        if self.print_schema:
            self.df.printSchema()

        self.df.show(truncate=self.truncate_output, n=self.n, vertical=self.vertical)
