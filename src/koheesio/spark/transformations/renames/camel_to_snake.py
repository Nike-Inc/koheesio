"""
Class for converting DataFrame column names from camel case to snake case.
"""

from koheesio.spark.transformations.renames import RenameColumns
from koheesio.spark.utils.string import AnyToSnakeConverter


class CamelToSnakeTransformation(RenameColumns):
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
    output_df = CamelToSnakeTransformation(
        column="camelCaseColumn"
    ).transform(input_df)
    ```

    __output_df:__

    | camel_case_column | snake_case_column |
    |-------------------|-------------------|
    | ...               | ...               |

    In this example, the column `camelCaseColumn` is converted to `camel_case_column`.

    > Note: the data in the columns is not changed, only the column names.

    """

    rename_func = AnyToSnakeConverter().convert
