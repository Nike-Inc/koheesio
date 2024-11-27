"""
String replacements without using regular expressions.
"""

from typing import Optional

from pyspark.sql import Column
from pyspark.sql.functions import lit, when

from koheesio.models import Field, field_validator
from koheesio.spark.transformations import ColumnsTransformationWithTarget


class Replace(ColumnsTransformationWithTarget):
    """
    Replace all instances of a string in a column with another string.

    This transformation uses PySpark when().otherwise() functions.

    Notes
    -----
    - If original_value is not set, the transformation will replace all null values with new_value
    - If original_value is set, the transformation will replace all values matching original_value with new_value
    - Numeric values are supported, but will be cast to string in the process
    - Replace is meant for simple string replacements. If more advanced replacements are needed, use the `RegexpReplace`
        transformation instead.

    Parameters
    ----------
    columns : Union[str, List[str]]
        The column (or list of columns) to replace values in. Alias: column
    target_column : Optional[str], optional, default=None
        The column to store the result in. If not provided, the result will be stored in the source column.
        Alias: target_suffix - if multiple columns are given as source, this will be used as a suffix.
    original_value : Optional[str], optional, default=None
        The original value that needs to be replaced. Alias: from
    new_value : str
        The new value to replace this with. Alias: to

    Examples
    --------
    __input_df:__

    | column |
    |--------|
    | hello  |
    | world  |
    | None   |

    ### Replace all null values with a new value
    ```python
    output_df = Replace(
        column="column",
        target_column="replaced_column",
        original_value=None,  # This is the default value, so it can be omitted
        new_value="programmer",
    ).transform(input_df)
    ```

    __output_df:__

    | column | replaced_column |
    |--------|-----------------|
    | hello  | hello           |
    | world  | world           |
    | None   | programmer      |

    ### Replace all instances of a string in a column with another string
    ```python
    output_df = Replace(
        column="column",
        target_column="replaced_column",
        original_value="world",
        new_value="programmer",
    ).transform(input_df)
    ```

    __output_df:__

    | column | replaced_column |
    |--------|-----------------|
    | hello  | hello           |
    | world  | programmer      |
    | None   | None            |

    """

    original_value: Optional[str] = Field(
        default=None, alias="from", description="The original value that needs to be replaced"
    )
    new_value: str = Field(default=..., alias="to", description="The new value to replace this with")

    @field_validator("original_value", "new_value", mode="before")
    def cast_values_to_str(cls, value: Optional[str]) -> Optional[str]:
        """Cast values to string if they are not None"""
        if value:
            return str(value)

    def func(self, column: Column) -> Column:
        when_statement = (
            when(column.isNull(), lit(self.new_value))
            if not self.original_value
            else when(
                column == self.original_value,
                lit(self.new_value),
            )
        )
        return when_statement.otherwise(column)
