"""
A simple DummyReader that returns a DataFrame with an id-column of the given range
"""

from koheesio.models import Field
from koheesio.spark.readers import Reader


class DummyReader(Reader):
    """A simple DummyReader that returns a DataFrame with an id-column of the given range

    Can be used in place of any Reader without having to read from a real source.

    Wraps SparkSession.range(). Output DataFrame will have a single column named "id" of type Long and length of the
    given range.

    Parameters
    ----------
    range : int
        How large to make the Dataframe

    Example
    -------
    ```python
    from koheesio.spark.readers.dummy import DummyReader

    output_df = DummyReader(range=100).read()
    ```

    __output_df__:
    Output DataFrame will have a single column named "id" of type `Long` containing 100 rows (0-99).

    | id  |
    |-----|
    | 0   |
    | 1   |
    | ... |
    | 99  |
    """

    range: int = Field(default=100, description="How large to make the Dataframe")

    def execute(self) -> Reader.Output:
        self.output.df = self.spark.range(self.range)
