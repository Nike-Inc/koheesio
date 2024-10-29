"""Transformation to replace a particular value in a column with another one"""

from typing import Optional, Union

from pyspark.sql.functions import col, lit, when

from koheesio.models import Field
from koheesio.spark import Column
from koheesio.spark.transformations import ColumnsTransformationWithTarget
from koheesio.spark.utils import SparkDatatype


def replace(column: Union[Column, str], to_value: str, from_value: Optional[str] = None) -> Column:
    """Function to replace a particular value in a column with another one"""
    # make sure we have a Column object
    if isinstance(column, str):
        column = col(column)

    if not from_value:
        condition = column.isNull()
    else:
        condition = column == from_value

    return when(condition, lit(to_value)).otherwise(column)


class Replace(ColumnsTransformationWithTarget):
    """
    Replace a particular value in a column with another one

    Can handle empty strings ("") as well as NULL / None values.

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    will raise an error in Spark:

    * binary
    * boolean
    * array<...>
    * map<...,...>

    Supported datatypes:
    --------------------
    Following casts are supported:

    * byte
    * short
    * integer
    * long
    * float
    * double
    * decimal
    * timestamp
    * date
    * string
    * void
        skipped by default

    Any supported none-string datatype will be cast to string before the replacement is done.

    Example
    -------
    __input_df__:

    | id | string |
    |----|--------|
    | 1  | hello  |
    | 2  | world  |
    | 3  |        |

    ```python
    output_df = Replace(
        column="string",
        from_value="hello",
        to_value="programmer",
    ).transform(input_df)
    ```

    __output_df__:

    | id |      string |
    |----|-------------|
    | 1  | programmer  |
    | 2  | world       |
    | 3  |             |

    In this example, the value "hello" in the column "string" is replaced with "programmer".
    """

    class ColumnConfig(ColumnsTransformationWithTarget.ColumnConfig):
        """Column type configurations for the column to be replaced"""

        run_for_all_data_type = [
            SparkDatatype.BYTE,
            SparkDatatype.SHORT,
            SparkDatatype.INTEGER,
            SparkDatatype.LONG,
            SparkDatatype.FLOAT,
            SparkDatatype.DOUBLE,
            SparkDatatype.DECIMAL,
            SparkDatatype.STRING,
            SparkDatatype.TIMESTAMP,
            SparkDatatype.DATE,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.VOID,
        ]

    from_value: Optional[str] = Field(
        default=None,
        alias="from",
        description="The original value that needs to be replaced. If no value is given, all 'null' values will be "
        "replaced with the to_value",
    )
    to_value: str = Field(default=..., alias="to", description="The new value to replace this with")

    def func(self, column: Column) -> Column:
        return replace(column=column, from_value=self.from_value, to_value=self.to_value)
