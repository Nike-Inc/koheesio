"""
Class for converting DataFrame column names from camel case to snake case.
"""

import re
from typing import Optional

from koheesio.models import Field, ListOfColumns
from koheesio.spark.transformations import ColumnsTransformation

camel_to_snake_re = re.compile("([a-z0-9])([A-Z])")


def convert_camel_to_snake(name: str):
    """
    Converts a string from camelCase to snake_case.

    Parameters:
    ----------
    name : str
        The string to be converted.

    Returns:
    --------
    str
        The converted string in snake_case.
    """
    return camel_to_snake_re.sub(r"\1_\2", name).lower()


class CamelToSnakeTransformation(ColumnsTransformation):
    """
    Converts column names from camel case to snake cases

    Parameters
    ----------
    columns : Optional[ListOfColumns], optional, default=None
        The column or columns to convert. If no columns are specified, all columns will be converted. A list of columns
        or a single column can be specified.
        For example: `["column1", "column2"]` or `"column1"`

    Example
    -------
    __input_df:__

    | camelCaseColumn    | snake_case_column |
    |--------------------|-------------------|
    | ...                | ...               |

    ```python
    output_df = CamelToSnakeTransformation(column="camelCaseColumn").transform(
        input_df
    )
    ```

    __output_df:__

    | camel_case_column | snake_case_column |
    |-------------------|-------------------|
    | ...               | ...               |

    In this example, the column `camelCaseColumn` is converted to `camel_case_column`.

    > Note: the data in the columns is not changed, only the column names.

    """

    columns: Optional[ListOfColumns] = Field(
        default="",
        alias="column",
        description="The column or columns to convert. If no columns are specified, all columns will be converted. "
        "A list of columns or a single column can be specified. For example: `['column1', 'column2']` or `'column1'` ",
    )

    def execute(self):
        _df = self.df

        # Prepare columns input:
        columns = self.df.columns if self.columns == ["*"] else self.columns

        for column in columns:
            _df = _df.withColumnRenamed(column, convert_camel_to_snake(column))

        self.output.df = _df
