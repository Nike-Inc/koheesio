"""Module that holds the transformations that can be used for date and time related operations."""

from typing import Optional, Union

from pytz import all_timezones_set

from pyspark.sql import functions as f
from pyspark.sql.functions import (
    col,
    date_format,
    from_utc_timestamp,
    lit,
    to_timestamp,
    to_utc_timestamp,
    when,
)

from koheesio.models import Field, field_validator, model_validator
from koheesio.spark import Column
from koheesio.spark.transformations import ColumnsTransformationWithTarget


def change_timezone(column: Union[str, Column], source_timezone: str, target_timezone: str) -> Column:
    """Helper function to change from one timezone to another

    wrapper around `pyspark.sql.functions.from_utc_timestamp` and `to_utc_timestamp`

    Parameters
    ----------
    column : Union[str, Column]
        The column to change the timezone of
    source_timezone : str
        The timezone of the source_column value. Timezone fields are validated against the `TZ database name` column in
        this list: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    target_timezone : str
        The target timezone. Timezone fields are validated against the `TZ database name` column in this list:
        https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

    """
    column = col(column) if isinstance(column, str) else column
    return from_utc_timestamp((to_utc_timestamp(column, source_timezone)), target_timezone)


class DateFormat(ColumnsTransformationWithTarget):
    """
    wrapper around pyspark.sql.functions.date_format

    See Also
    --------
    * https://spark.apache.org/docs/3.3.2/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_format.html
    * https://spark.apache.org/docs/3.3.2/sql-ref-datetime-pattern.html

    Concept
    -------
    This Transformation allows to convert a date/timestamp/string to a value of string in the format specified by the
    date format given.

    A pattern could be for instance `dd.MM.yyyy` and could return a string like ‘18.03.1993’.
    All pattern letters of datetime pattern can be used, see:
    https://spark.apache.org/docs/3.3.2/sql-ref-datetime-pattern.html

    How to use
    ----------
    If more than one column is passed, the behavior of the Class changes this way

    - the transformation will be run in a loop against all the given columns
    - the target_column will be used as a suffix. Leaving this blank will result in the original columns being renamed.

    Example
    -------
    ```python
    source_column value: datetime.date(2020, 1, 1)
    target: "yyyyMMdd HH:mm"
    output: "20200101 00:00"
    ```
    """

    format: str = Field(
        ...,
        description="The format for the resulting string. See "
        "https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html",
    )

    def func(self, column: Column) -> Column:
        return date_format(column, self.format)


class ChangeTimeZone(ColumnsTransformationWithTarget):
    """
    Allows for the value of a column to be changed from one timezone to another

    Adding useful metadata
    ----------------------
    When `add_target_timezone` is enabled (default), an additional column is created documenting which timezone a
    field has been converted to. Additionally, the suffix added to this column can be customized (default value is
    `_timezone`).

    Example
    -------

    Input:

    ```python
    target_column = "some_column_name"
    target_timezone = "EST"
    add_target_timezone = True  # default value
    timezone_column_suffix = "_timezone"  # default value
    ```

    Output:
    ```python
    column name  = "some_column_name_timezone"  # notice the suffix
    column value = "EST"
    ```

    <!--- TODO: add ability to change timezone depending on column values (future release) -->
    """

    from_timezone: str = Field(
        default=...,
        alias="source_timezone",
        description="Timezone of the source_column value. Timezone fields are validated against the `TZ database name` "
        "column in this list: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones",
    )
    to_timezone: str = Field(
        default=...,
        alias="target_timezone",
        description="Target timezone. Timezone fields are validated against the `TZ database name` column in this "
        "list: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones",
    )
    add_target_timezone: bool = Field(
        default=True, description="Toggles whether the target timezone is added as a column. True by default."
    )
    target_timezone_column_suffix: Optional[str] = Field(
        default="_timezone",
        alias="suffix",
        description="Allows to customize the suffix that is added to the target_timezone column. "
        "Defaults to '_timezone'. "
        "Note: this will be ignored if 'add_target_timezone' is set to False",
    )

    @model_validator(mode="before")
    def validate_no_duplicate_timezones(cls, values: dict) -> dict:
        """Validate that source and target timezone are not the same"""
        from_timezone_value = values.get("from_timezone")
        to_timezone_value = values.get("o_timezone")

        if from_timezone_value == to_timezone_value:
            raise ValueError("Timezone conversions from and to the same timezones are not valid.")

        return values

    @field_validator("from_timezone", "to_timezone")
    def validate_timezone(cls, timezone_value: str) -> str:
        """Validate that the timezone is a valid timezone."""
        if timezone_value not in all_timezones_set:
            raise ValueError(
                "Not a valid timezone. Refer to the `TZ database name` column here: "
                "https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"
            )
        return timezone_value

    def func(self, column: Column) -> Column:
        return change_timezone(column=column, source_timezone=self.from_timezone, target_timezone=self.to_timezone)

    def execute(self) -> ColumnsTransformationWithTarget.Output:
        df = self.df

        for target_column, column in self.get_columns_with_target():
            func = self.func  # select the applicable function
            df = df.withColumn(
                target_column,
                func(f.col(column)),
            )

            # document which timezone a field has been converted to
            if self.add_target_timezone:
                df = df.withColumn(f"{target_column}{self.target_timezone_column_suffix}", f.lit(self.to_timezone))

        self.output.df = df


class ToTimestamp(ColumnsTransformationWithTarget):
    """
    wrapper around `pyspark.sql.functions.to_timestamp`

    Converts a Column (or set of Columns) into `pyspark.sql.types.TimestampType` using the specified format.
    Specify formats according to `datetime pattern https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html`_.

    Functionally equivalent to col.cast("timestamp").


    See Also
    --------
    Related Koheesio classes:

    [ColumnsTransformation]: ../index.md#koheesio.spark.transformations.ColumnsTransformation
    [ColumnsTransformationWithTarget]: ../index.md#koheesio.spark.transformations.ColumnsTransformationWithTarget
    [pyspark.sql.functions]: https://spark.apache.org/docs/3.5.1/api/python/reference/pyspark.sql/functions.html

    From the `koheesio.spark.transformations` module:

    * [ColumnsTransformation] :
        Base class for ColumnsTransformation. Defines column / columns field + recursive logic
    * [ColumnsTransformationWithTarget] :
        Defines target_column / target_suffix field

    [pyspark.sql.functions]:

    * datetime pattern : https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

    Example
    -------

    ## Basic usage example:

    __input_df:__

    | t                     |
    |-----------------------|
    | "1997-02-28 10:30:00" |

    `t` is a string

    ```python
    tts = ToTimestamp(
        # since the source column is the same as the target in this example, 't' will be overwritten
        column="t",
        target_column="t",
        format="yyyy-MM-dd HH:mm:ss",
    )
    output_df = tts.transform(input_df)
    ```

    __output_df:__

    | t                                      |
    |----------------------------------------|
    | datetime.datetime(1997, 2, 28, 10, 30) |

    Now `t` is a timestamp


    ## Multiple columns at once:

    __input_df:__

    | t1                    | t2                     |
    |-----------------------|------------------------|
    | "1997-02-28 10:30:00" | "2007-03-31 11:40:10"  |

    `t1` and `t2` are strings

    ```python
    tts = ToTimestamp(
        columns=["t1", "t2"],
        # 'target_suffix' is synonymous with 'target_column'
        target_suffix="new",
        format="yyyy-MM-dd HH:mm:ss",
    )
    output_df = tts.transform(input_df).select("t1_new", "t2_new")
    ```

    __output_df:__

    | t1_new                                 | t2_new                                  |
    |----------------------------------------|-----------------------------------------|
    | datetime.datetime(1997, 2, 28, 10, 30) | datetime.datetime(2007, 3, 31, 11, 40)  |

    Now `t1_new` and `t2_new` are both timestamps

    """

    format: str = Field(
        default=...,  # TODO: should we set a logic default here? [future release]
        description="The date format for of the timestamp field. See "
        "https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html",
    )

    def func(self, column: Column) -> Column:
        # convert string to timestamp
        converted_col = to_timestamp(column, self.format)
        return when(column.isNull(), lit(None)).otherwise(converted_col)
