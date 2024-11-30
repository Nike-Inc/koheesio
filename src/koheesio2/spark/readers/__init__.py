"""
Readers are a type of Step that read data from a source based on the input parameters and stores the result in
self.output.df.

For a comprehensive guide on the usage, examples, and additional features of Reader classes, please refer to the
[reference/spark/steps/readers](../../../reference/spark/readers.md) section of the Koheesio documentation.
"""

from abc import ABC, abstractmethod

from koheesio.models.reader import BaseReader
from koheesio.spark import SparkStep


class Reader(BaseReader, SparkStep, ABC):
    """Base class for all Readers

    A Reader is a Step that reads data from a source based on the input parameters
    and stores the result in self.output.df (DataFrame).

    When implementing a Reader, the execute() method should be implemented.
    The execute() method should read from the source and store the result in self.output.df.

    The Reader class implements a standard read() method that calls the execute() method and returns the result. This
    method can be used to read data from a Reader without having to call the execute() method directly. Read method
    does not need to be implemented in the child class.

    Every Reader has a SparkSession available as self.spark. This is the currently active SparkSession.

    The Reader class also implements a shorthand for accessing the output Dataframe through the df-property. If the
    output.df is None, .execute() will be run first.
    """

    @abstractmethod
    def execute(self) -> SparkStep.Output:
        """Execute on a Reader should handle self.output.df (output) as a minimum
        Read from whichever source -> store result in self.output.df
        """
