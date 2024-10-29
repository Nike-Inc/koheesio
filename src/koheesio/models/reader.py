"""
Module for the BaseReader class
"""

from typing import Optional
from abc import ABC, abstractmethod

from koheesio import Step
from koheesio.spark import DataFrame


class BaseReader(Step, ABC):
    """Base class for all Readers

    A Reader is a Step that reads data from a source based on the input parameters
    and stores the result in self.output.df (DataFrame).

    When implementing a Reader, the execute() method should be implemented.
    The execute() method should read from the source and store the result in self.output.df.

    The Reader class implements a standard read() method that calls the execute() method and returns the result. This
    method can be used to read data from a Reader without having to call the execute() method directly. Read method
    does not need to be implemented in the child class.

    The Reader class also implements a shorthand for accessing the output Dataframe through the df-property. If the
    output.df is None, .execute() will be run first.
    """

    @property
    def df(self) -> Optional[DataFrame]:
        """Shorthand for accessing self.output.df
        If the output.df is None, .execute() will be run first
        """
        if not self.output.df:  # type: ignore[attr-defined]
            self.execute()
        return self.output.df  # type: ignore[attr-defined]

    @abstractmethod
    def execute(self) -> Step.Output:
        """Execute on a Reader should handle self.output.df (output) as a minimum
        Read from whichever source -> store result in self.output.df
        """
        pass

    def read(self) -> DataFrame:
        """Read from a Reader without having to call the execute() method directly"""
        self.execute()
        return self.output.df  # type: ignore[attr-defined]
