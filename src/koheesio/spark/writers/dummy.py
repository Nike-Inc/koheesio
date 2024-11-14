"""Module for the DummyWriter class."""

from typing import Any, Dict, Union

from koheesio.models import Field, PositiveInt, field_validator
from koheesio.spark import DataFrame
from koheesio.spark.utils import show_string
from koheesio.spark.writers import Writer


class DummyWriter(Writer):
    """
    A simple DummyWriter that performs the equivalent of a df.show() on the given DataFrame and returns the first row of
    data as a dict.

    This Writer does not actually write anything to a source/destination, but is useful for debugging or testing
    purposes.

    Parameters
    ----------
    n : PositiveInt, optional, default=20
        Number of rows to show.
    truncate : bool | PositiveInt, optional, default=True
        If set to `True`, truncate strings longer than 20 chars by default.
        If set to a number greater than one, truncates long strings to length `truncate` and align cells right.
    vertical : bool, optional, default=False
        If set to `True`, print output rows vertically (one line per column value).
    """

    n: PositiveInt = Field(
        default=20,
        description="Number of rows to show.",
        gt=0,
    )
    truncate: Union[bool, PositiveInt] = Field(
        default=True,
        description="If set to ``True``, truncate strings longer than 20 chars by default."
        "If set to a number greater than one, truncates long strings to length ``truncate`` and align cells right.",
    )
    vertical: bool = Field(
        default=False,
        description="If set to ``True``, print output rows vertically (one line per column value).",
    )

    @field_validator("truncate")
    def int_truncate(cls, truncate_value: Union[int, bool]) -> int:
        """
        Truncate is either a bool or an int.

        Parameters:
        -----------
        truncate_value : int | bool, optional, default=True
            If int, specifies the maximum length of the string.
            If bool and True, defaults to a maximum length of 20 characters.

        Returns:
        --------
        int
            The maximum length of the string.

        """
        # Same logic as what is inside DataFrame.show()
        if isinstance(truncate_value, bool) and truncate_value is True:
            return 20  # default is 20 chars
        return int(truncate_value)  # otherwise 0, or whatever the user specified

    class Output(Writer.Output):
        """DummyWriter output"""

        head: Dict[str, Any] = Field(default=..., description="The first row of the DataFrame as a dict")
        df_content: str = Field(default=..., description="The content of the DataFrame as a string")

    def execute(self) -> Output:
        """Execute the DummyWriter"""
        # logs the equivalent of doing df.show()
        df_content = show_string(df=self.df, n=self.n, truncate=self.truncate, vertical=self.vertical)
        self.log.info(f"content of df that was passed to DummyWriter:\n{df_content}")

        self.output.head = self.df.head().asDict()
        self.output.df_content = df_content
