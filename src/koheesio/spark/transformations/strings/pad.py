"""
Pad the values of a column with a character up until it reaches a certain length.

Classes
-------
Pad
    Pads the values of `source_column` with the `character` up until it reaches `length` of characters

LPad
    Pad with a character on the left side of the string.

RPad
    Pad with a character on the right side of the string.
"""

from typing import Literal, Optional

from pyspark.sql import Column
from pyspark.sql.functions import lpad, rpad

from koheesio.models import Field, PositiveInt, constr
from koheesio.spark.transformations import ColumnsTransformationWithTarget

pad_directions = Literal["left", "right"]


class Pad(ColumnsTransformationWithTarget):
    """
    Pads the values of `source_column` with the `character` up until it reaches `length` of characters
    The `direction` param can be changed to apply either a left or a right pad. Defaults to left pad.

    Wraps the `lpad` and `rpad` functions from PySpark.

    Parameters
    ----------
    columns : Union[str, List[str]]
        The column (or list of columns) to pad. Alias: column
    target_column : Optional[str], optional, default=None
        The column to store the result in. If not provided, the result will be stored in the source column.
        Alias: target_suffix - if multiple columns are given as source, this will be used as a suffix.
    character : constr(min_length=1)
        The character to use for padding
    length : PositiveInt
        Positive integer to indicate the intended length
    direction : Optional[pad_directions], optional, default=left
        On which side to add the characters. Either "left" or "right". Defaults to "left"

    Example
    -------
    __input_df:__

    | column |
    |--------|
    | hello  |
    | world  |

    ```python
    output_df = Pad(
        column="column",
        target_column="padded_column",
        character="*",
        length=10,
        direction="right",
    ).transform(input_df)
    ```

    __output_df:__

    | column | padded_column |
    |--------|---------------|
    | hello  | hello*****    |
    | world  | world*****    |

    > Note: in the example above, we could have used the RPad class instead of Pad with direction="right" to achieve the
    same result.

    """

    character: constr(min_length=1) = Field(default=..., description="The character to use for padding")
    length: PositiveInt = Field(default=..., description="Positive integer to indicate the intended length")
    direction: Optional[pad_directions] = Field(
        default="left", description='On which side to add the characters . Either "left" or "right". Defaults to "left"'
    )

    def func(self, column: Column) -> Column:
        func = lpad if self.direction == "left" else rpad
        return func(column, self.length, self.character)


LPad = Pad  # Pad's default is already 'left', so LPad is just an alias of Pad


class RPad(Pad):
    """Pad with a character on the right side of the string.

    See Pad class docstring for more information.
    """

    direction: Optional[pad_directions] = "right"
