"""
This module provides a `DateTimeColumn` class that extends the `Column` class from PySpark. It allows for adding or
subtracting an interval value from a datetime column.

This can be used to reflect a change in a given date / time column in a more human-readable way.

Please refer to the Spark SQL documentation for a list of valid interval values:
https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html#interval-literal

Background
----------
The aim is to easily add or subtract an 'interval' value to a datetime column. An interval value is a string that
represents a time interval. For example, '1 day', '1 month', '5 years', '1 minute 30 seconds', '10 milliseconds', etc.
These can be used to reflect a change in a given date / time column in a more human-readable way.

Typically, this can be done using the `date_add()` and `date_sub()` functions in Spark SQL. However, these functions
only support adding or subtracting a single unit of time measured in days. Using an interval gives us much more
flexibility; however, Spark SQL does not provide a function to add or subtract an interval value from a datetime column
through the python API directly, so we have to use the `expr()` function to do this to be able to directly use SQL.

This module provides a `DateTimeColumn` class that extends the `Column` class from PySpark. It allows for adding or
subtracting an interval value from a datetime column using the `+` and `-` operators.

Additionally, this module provides two transformation classes that can be used as a transformation step in a pipeline:

- `DateTimeAddInterval`: adds an interval value to a datetime column
- `DateTimeSubtractInterval`: subtracts an interval value from a datetime column

These classes are subclasses of `ColumnsTransformationWithTarget` and hence can be used to perform transformations on
multiple columns at once.

The above transformations both use the provided `adjust_time()` function to perform the actual transformation.

See also:
---------
Related Koheesio classes:

[ColumnsTransformation]: ../index.md#koheesio.spark.transformations.ColumnsTransformation
[ColumnsTransformationWithTarget]: ../index.md#koheesio.spark.transformations.ColumnsTransformationWithTarget

From the koheesio.spark.transformations module:

* [ColumnsTransformation] : Base class for ColumnsTransformation. Defines column / columns field + recursive logic
* [ColumnsTransformationWithTarget] : Defines target_column / target_suffix field

pyspark.sql.functions:

-   https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html#interval-literal
-   https://spark.apache.org/docs/latest/api/sql/index.html
-   https://spark.apache.org/docs/latest/api/sql/#try_add
-   https://spark.apache.org/docs/latest/api/sql/#try_subtract

Classes
-------

DateTimeColumn
    A datetime column that can be adjusted by adding or subtracting an interval value using the `+` and `-` operators.

DateTimeAddInterval
    A transformation that adds an interval value to a datetime column.
    This class is a subclass of `ColumnsTransformationWithTarget` and hence can be used as a transformation step in a
    pipeline. See `ColumnsTransformationWithTarget` for more information.

DateTimeSubtractInterval
    A transformation that subtracts an interval value from a datetime column.
    This class is a subclass of `ColumnsTransformationWithTarget` and hence can be used as a transformation step in a
    pipeline. See `ColumnsTransformationWithTarget` for more information.

Note
----
the `DateTimeAddInterval` and `DateTimeSubtractInterval` classes are very similar. The only difference is that one
adds an interval value to a datetime column, while the other subtracts an interval value from a datetime column.

Functions
---------
dt_column(column: Union[str, Column]) -> DateTimeColumn
    Converts a column to a `DateTimeColumn`. This function aims to be a drop-in replacement for
    `pyspark.sql.functions.col` that returns a `DateTimeColumn` instead of a `Column`.

adjust_time(column: Column, operation: Operations, interval: str) -> Column
    Adjusts a datetime column by adding or subtracting an interval value.

validate_interval(interval: str)
    Validates a given interval string.

Example
-------
#### Various ways to create and interact with `DateTimeColumn`:
- Create a `DateTimeColumn` from a string: `dt_column("my_column")`
- Create a `DateTimeColumn` from a `Column`: `dt_column(df.my_column)`
- Use the `+` and `-` operators to add or subtract an interval value from a `DateTimeColumn`:
    - `dt_column("my_column") + "1 day"`
    - `dt_column("my_column") - "1 month"`

#### Functional examples using `adjust_time()`:
- Add 1 day to a column: `adjust_time("my_column", operation="add", interval="1 day")`
- Subtract 1 month from a column: `adjust_time("my_column", operation="subtract", interval="1 month")`

#### As a transformation step:
```python
from koheesio.spark.transformations.date_time.interval import (
    DateTimeAddInterval,
)

input_df = spark.createDataFrame(
    [(1, "2022-01-01 00:00:00")], ["id", "my_column"]
)

# add 1 day to my_column and store the result in a new column called 'one_day_later'
output_df = DateTimeAddInterval(
    column="my_column",
    target_column="one_day_later",
    interval="1 day",
).transform(input_df)
```
__output_df__:

| id | my_column           | one_day_later       |
|----|---------------------|---------------------|
| 1  | 2022-01-01 00:00:00 | 2022-01-02 00:00:00 |

`DateTimeSubtractInterval` works in a similar way, but subtracts an interval value from a datetime column.
"""

from __future__ import annotations

from typing import Literal, Union

from pyspark.sql import Column as SparkColumn
from pyspark.sql.functions import col, expr

from koheesio.models import Field, field_validator
from koheesio.spark import Column, ParseException
from koheesio.spark.transformations import ColumnsTransformationWithTarget
from koheesio.spark.utils import check_if_pyspark_connect_module_is_available, get_column_name
from koheesio.spark.utils.connect import is_remote_session

# create a literal constraining the operations to 'add' and 'subtract'
Operations = Literal["add", "subtract"]


class DateTimeColumn(SparkColumn):
    """A datetime column that can be adjusted by adding or subtracting an interval value  using the `+` and `-`
    operators.
    """

    def __add__(self, value: str) -> Column:
        """Add an `interval` value to a date or time column

        A valid value is a string that can be parsed by the `interval` function in Spark SQL.
        See https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html#interval-literal
        """
        return adjust_time(self, operation="add", interval=value)

    def __sub__(self, value: str) -> Column:
        """Subtract an `interval` value to a date or time column

        A valid value is a string that can be parsed by the `interval` function in Spark SQL.
        See https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html#interval-literal
        """
        return adjust_time(self, operation="subtract", interval=value)

    # noinspection PyProtectedMember
    @classmethod
    def from_column(cls, column: Column) -> Union["DateTimeColumn", "DateTimeColumnConnect"]:
        """Create a DateTimeColumn from an existing Column"""
        if isinstance(column, SparkColumn):
            return DateTimeColumn(column._jc)
        return DateTimeColumnConnect(expr=column._expr)


# if spark version is 3.5 or higher, we have to account for the connect mode
if check_if_pyspark_connect_module_is_available():
    from pyspark.sql.connect.column import Column as ConnectColumn

    class DateTimeColumnConnect(ConnectColumn):
        """A datetime column that can be adjusted by adding or subtracting an interval value  using the `+` and `-`
        operators.

        Optimized for Spark Connect mode.
        """

        __add__ = DateTimeColumn.__add__
        __sub__ = DateTimeColumn.__sub__
        from_column = DateTimeColumn.from_column


def validate_interval(interval: str) -> str:
    """Validate an interval string

    Parameters
    ----------
    interval : str
        The interval string to validate

    Raises
    ------
    ValueError
        If the interval string is invalid
    """
    from koheesio.spark.utils.common import get_active_session

    try:
        if is_remote_session():
            get_active_session().sql(f"SELECT interval '{interval}'")  # type: ignore
        else:
            expr(f"interval '{interval}'")
    except ParseException as e:
        raise ValueError(f"Value '{interval}' is not a valid interval.") from e
    return interval


def dt_column(column: Column) -> DateTimeColumn:
    """Convert a column to a DateTimeColumn

    Aims to be a drop-in replacement for `pyspark.sql.functions.col` that returns a DateTimeColumn instead of a Column.

    Example
    --------
    ### create a DateTimeColumn from a string
    ```python
    dt_column("my_column")
    ```

    ### create a DateTimeColumn from a Column
    ```python
    dt_column(df.my_column)
    ```

    Parameters
    ----------
    column : Union[str, Column]
        The column (or name of the column) to convert to a DateTimeColumn
    """
    if isinstance(column, str):
        column = col(column)
    elif type(column) not in ("pyspark.sql.Column", "pyspark.sql.connect.column.Column"):
        raise TypeError(f"Expected column to be of type str or Column, got {type(column)} instead.")
    return DateTimeColumn.from_column(column)


def adjust_time(column: Column, operation: Operations, interval: str) -> Column:
    """
    Adjusts a datetime column by adding or subtracting an interval value.

    This can be used to reflect a change in a given date / time column in a more human-readable way.


    See also
    --------
    Please refer to the Spark SQL documentation for a list of valid interval values:
    https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html#interval-literal

    ### pyspark.sql.functions:

    * https://spark.apache.org/docs/latest/api/sql/index.html#interval
    * https://spark.apache.org/docs/latest/api/sql/#try_add
    * https://spark.apache.org/docs/latest/api/sql/#try_subtract

    Example
    --------
    ### add 1 day to a column
    ```python
    adjust_time("my_column", operation="add", interval="1 day")
    ```

    ### subtract 1 month from a column
    ```python
    adjust_time("my_column", operation="subtract", interval="1 month")
    ```

    ### or, a much more complicated example

    In this example, we add 5 days, 3 hours, 7 minutes, 30 seconds, and 1 millisecond to a column called `my_column`.
    ```python
    adjust_time(
        "my_column",
        operation="add",
        interval="5 days 3 hours 7 minutes 30 seconds 1 millisecond",
    )
    ```

    Parameters
    ----------
    column : Column
        The datetime column to adjust.
    operation : Operations
        The operation to perform. Must be either 'add' or 'subtract'.
    interval : str
        The value to add or subtract. Must be a valid interval string.

    Returns
    -------
    Column
        The adjusted datetime column.
    """

    # check that value is a valid interval
    interval = validate_interval(interval)

    column_name = get_column_name(column)

    # determine the operation to perform
    try:
        operation = {
            "add": "try_add",
            "subtract": "try_subtract",
        }[operation]  # type: ignore
    except KeyError as e:
        raise ValueError(f"Operation '{operation}' is not valid. Must be either 'add' or 'subtract'.") from e

    # perform the operation
    _expression = f"{operation}({column_name}, interval '{interval}')"
    column = expr(_expression)

    return column


class DateTimeAddInterval(ColumnsTransformationWithTarget):
    """
    A transformation that adds or subtracts a specified interval from a datetime column.

    See also:
    ---------
    pyspark.sql.functions:

    -   https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html#interval-literal
    -   https://spark.apache.org/docs/latest/api/sql/index.html#interval

    Parameters
    ----------
    interval : str
        The interval to add to the datetime column.
    operation : Operations, optional, default=add
        The operation to perform. Must be either 'add' or 'subtract'.

    Example
    -------
    ### add 1 day to a column
    ```python
    DateTimeAddInterval(
        column="my_column",
        interval="1 day",
    ).transform(df)
    ```

    ### subtract 1 month from `my_column` and store the result in a new column called `one_month_earlier`
    ```python
    DateTimeSubtractInterval(
        column="my_column",
        target_column="one_month_earlier",
        interval="1 month",
    )
    ```
    """

    interval: str = Field(
        default=...,
        description="The interval to add to the datetime column.",
        examples=["1 day", "5 years", "3 months"],
    )
    operation: Operations = Field(
        default="add", description="The operation to perform. Must be either 'add' or 'subtract'."
    )

    # validators
    validate_interval = field_validator("interval")(validate_interval)

    def func(self, column: Column) -> Column:
        return adjust_time(column, operation=self.operation, interval=self.interval)


class DateTimeSubtractInterval(DateTimeAddInterval):
    """Subtracts a specified interval from a datetime column.

    Works in the same way as `DateTimeAddInterval`, but subtracts the specified interval from the datetime column.
    See `DateTimeAddInterval` for more information.
    """

    operation: Operations = Field(
        default="subtract", description="The operation to perform. Must be either 'add' or 'subtract'."
    )
