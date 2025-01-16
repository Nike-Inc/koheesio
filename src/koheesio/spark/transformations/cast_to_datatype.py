# noinspection PyUnresolvedReferences
"""
Transformations to cast a column or set of columns to a given datatype.

Each one of these have been vetted to throw warnings when wrong datatypes are passed (to prevent errors in any job or
pipeline).

Furthermore, detailed tests have been added to ensure that types are actually compatible as prescribed.

Concept
-------
* One can use the CastToDataType class directly, or use one of the more specific subclasses.
* Each subclass is compatible with 'run_for_all' - meaning that if no column or columns are specified, it will be run
    for all compatible data types.
* Compatible Data types are specified in the ColumnConfig class of each subclass, and are documented in the docstring
    of each subclass.

See class docstrings for more information

Note
----
Dates, Arrays and Maps are not supported by this module.

- for dates, use the [koheesio.spark.transformations.date_time](date_time/index.md) module
- for arrays, use the [koheesio.spark.transformations.arrays](arrays.md) module

Classes
-------
CastToDatatype
    Cast a column or set of columns to a given datatype
CastToByte
    Cast to Byte (a.k.a. tinyint)
CastToShort
    Cast to Short (a.k.a. smallint)
CastToInteger
    Cast to Integer (a.k.a. int)
CastToLong
    Cast to Long (a.k.a. bigint)
CastToFloat
    Cast to Float (a.k.a. real)
CastToDouble
    Cast to Double
CastToDecimal
    Cast to Decimal (a.k.a. decimal, numeric, dec, BigDecimal)
CastToString
    Cast to String
CastToBinary
    Cast to Binary (a.k.a. byte array)
CastToBoolean
    Cast to Boolean
CastToTimestamp
    Cast to Timestamp

Note
----
The following parameters are common to all classes in this module:

Parameters
----------
columns : ListOfColumns
    Name of the source column(s). Alias: column
target_column : str
    Name of the target column or alias if more than one column is specified. Alias: target_alias
datatype : str or SparkDatatype
    Datatype to cast to. Choose from SparkDatatype Enum (only needs to be specified in CastToDatatype, all other
    classes have a fixed datatype)
"""

from typing import Union

from pyspark.sql import Column

from koheesio.models import Field, conint, field_validator, model_validator
from koheesio.spark.transformations import ColumnsTransformationWithTarget
from koheesio.spark.utils import SparkDatatype


class CastToDatatype(ColumnsTransformationWithTarget):
    """
    Cast a column or set of columns to a given datatype

    Wrapper around pyspark.sql.Column.cast

    Concept
    -------
    This class acts as the base class for all the other CastTo* classes. It is compatible with 'run_for_all' - meaning
    that if no column or columns are specified, it will be run for all compatible data types.

    Example
    -------
    __input_df__:

    | c1 | c2 |
    |----|----|
    | 1  | 2  |
    | 3  | 4  |

    ```python
    output_df = CastToDatatype(
        column="c1",
        datatype="string",
        target_alias="c1",
    ).transform(input_df)
    ```

    __output_df__:

    | c1   | c2 |
    |------|----|
    | "1"  | 2  |
    | "3"  | 4  |

    In the example above, the column `c1` is cast to a string datatype. The column `c2` is not affected.

    Parameters
    ----------
    columns : ListOfColumns
        Name of the source column(s). Alias: column
    datatype : str or SparkDatatype
        Datatype to cast to. Choose from SparkDatatype Enum
    target_column : str
        Name of the target column or alias if more than one column is specified. Alias: target_alias
    """

    datatype: Union[str, SparkDatatype] = Field(default=..., description="Datatype. Choose from SparkDatatype Enum")

    @field_validator("datatype")
    def validate_datatype(cls, datatype_value: Union[str, SparkDatatype]) -> SparkDatatype:  # type: ignore
        """Validate the datatype."""
        # handle string input
        try:
            if isinstance(datatype_value, str):
                datatype_value: SparkDatatype = SparkDatatype.from_string(datatype_value)
                return datatype_value

            # and let SparkDatatype handle the rest
            datatype_value: SparkDatatype = SparkDatatype.from_string(datatype_value.value)

        except AttributeError as e:
            raise AttributeError(f"Invalid datatype: {datatype_value}") from e

        return datatype_value

    def func(self, column: Column) -> Column:
        # This is to let the IDE explicitly know that the datatype is not a string, but a `SparkDatatype` Enum
        datatype: SparkDatatype = self.datatype  # type: ignore
        return column.cast(datatype.spark_type())


class CastToByte(CastToDatatype):
    """
    Cast to Byte (a.k.a. tinyint)

    Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    will raise an error in Spark:

    * binary
    * array<...>
    * map<...,...>

    Supported datatypes:
    --------------------
    Following casts are supported:

    * boolean
    * timestamp
    * decimal
    * double
    * float
    * long
    * integer
    * short

    Spark doesn't error on these, but will cast to null or otherwise give mangled data. Hence, Koheesio will skip the
    transformation for these unless specific columns of these types are given as input:

    * string
        converts to null
    * timestamp
        range of values too small for timestamp to have any meaning
    * date
        converts to null
    * void
        skipped by default
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToByte class."""

        run_for_all_data_type = [
            SparkDatatype.SHORT,
            SparkDatatype.INTEGER,
            SparkDatatype.LONG,
            SparkDatatype.FLOAT,
            SparkDatatype.DOUBLE,
            SparkDatatype.DECIMAL,
            SparkDatatype.BOOLEAN,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.STRING,
            SparkDatatype.TIMESTAMP,
            SparkDatatype.DATE,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.BYTE


class CastToShort(CastToDatatype):
    """
    Cast to Short (a.k.a. smallint)

    Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    will raise an error in Spark:

    * binary
    * array<...>
    * map<...,...>

    Supported datatypes:
    --------------------
    Following casts are supported:

    * byte
    * integer
    * long
    * float
    * double
    * decimal
    * string
    * boolean
    * timestamp
    * date
    * void

    Spark doesn't error on these, but will cast to null or otherwise give mangled data. Hence, Koheesio will skip the
    transformation for these unless specific columns of these types are given as input:

    * string
        converts to null
    * timestamp
        range of values too small for timestamp to have any meaning
    * date
        converts to null
    * void
        skipped by default
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToShort class."""

        run_for_all_data_type = [
            SparkDatatype.BYTE,
            SparkDatatype.INTEGER,
            SparkDatatype.LONG,
            SparkDatatype.FLOAT,
            SparkDatatype.DOUBLE,
            SparkDatatype.DECIMAL,
            SparkDatatype.BOOLEAN,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.STRING,
            SparkDatatype.TIMESTAMP,
            SparkDatatype.DATE,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.SHORT


class CastToInteger(CastToDatatype):
    """
    Cast to Integer (a.k.a. int)

    Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    will raise an error in Spark:

    * binary
    * array<...>
    * map<...,...>

    Supported datatypes:
    --------------------
    Following casts are supported:

    * byte
    * short
    * long
    * float
    * double
    * decimal
    * boolean
    * timestamp

    Spark doesn't error on these, but will cast to null or otherwise give mangled data. Hence, Koheesio will skip the
    transformation for these unless specific columns of these types are given as input:

    * string
        converts to null
    * date
        converts to null
    * void
        skipped by default
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToInteger class."""

        run_for_all_data_type = [
            SparkDatatype.BYTE,
            SparkDatatype.SHORT,
            SparkDatatype.LONG,
            SparkDatatype.FLOAT,
            SparkDatatype.DOUBLE,
            SparkDatatype.DECIMAL,
            SparkDatatype.BOOLEAN,
            SparkDatatype.TIMESTAMP,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.STRING,
            SparkDatatype.DATE,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.INTEGER


class CastToLong(CastToDatatype):
    """
    Cast to Long (a.k.a. bigint)

    Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    will raise an error in Spark:

    * binary
    * array<...>
    * map<...,...>

    Supported datatypes:
    --------------------
    Following casts are supported:

    * byte
    * short
    * long
    * float
    * double
    * decimal
    * boolean
    * timestamp

    Spark doesn't error on these, but will cast to null or otherwise give mangled data. Hence, Koheesio will skip the
    transformation for these unless specific columns of these types are given as input:

    * string
        converts to null
    * date
        converts to null
    * void
        skipped by default
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToLong class."""

        run_for_all_data_type = [
            SparkDatatype.BYTE,
            SparkDatatype.SHORT,
            SparkDatatype.INTEGER,
            SparkDatatype.FLOAT,
            SparkDatatype.DOUBLE,
            SparkDatatype.DECIMAL,
            SparkDatatype.BOOLEAN,
            SparkDatatype.TIMESTAMP,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.STRING,
            SparkDatatype.DATE,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.LONG


class CastToFloat(CastToDatatype):
    """
    Cast to Float (a.k.a. real)

    Represents 4-byte single-precision floating point numbers. The range of numbers is from -3.402823E38 to 3.402823E38.

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    will raise an error in Spark:

    * binary
    * array<...>
    * map<...,...>

    Supported datatypes:
    --------------------
    Following casts are supported:

    * byte
    * short
    * integer
    * long
    * double
    * decimal
    * boolean

    Spark doesn't error on these, but will cast to null or otherwise give mangled data. Hence, Koheesio will skip the
    transformation for these unless specific columns of these types are given as input:

    * timestamp
        precision is lost (use CastToDouble instead)
    * string
        converts to null
    * date
        converts to null
    * void
        skipped by default
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToFloat class."""

        run_for_all_data_type = [
            SparkDatatype.BYTE,
            SparkDatatype.SHORT,
            SparkDatatype.INTEGER,
            SparkDatatype.LONG,
            SparkDatatype.DOUBLE,
            SparkDatatype.DECIMAL,
            SparkDatatype.BOOLEAN,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.STRING,
            SparkDatatype.DATE,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.FLOAT


class CastToDouble(CastToDatatype):
    """
    Cast to Double

    Represents 8-byte double-precision floating point numbers. The range of numbers is from -1.7976931348623157E308 to
    1.7976931348623157E308.

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    will raise an error in Spark:

    * binary
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
    * decimal
    * boolean
    * timestamp

    Spark doesn't error on these, but will cast to null or otherwise give mangled data. Hence, Koheesio will skip the
    transformation for these unless specific columns of these types are given as input:

    * string
        converts to null
    * date
        converts to null
    * void
        skipped by default
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToDouble class."""

        run_for_all_data_type = [
            SparkDatatype.BYTE,
            SparkDatatype.SHORT,
            SparkDatatype.INTEGER,
            SparkDatatype.LONG,
            SparkDatatype.FLOAT,
            SparkDatatype.DECIMAL,
            SparkDatatype.BOOLEAN,
            SparkDatatype.TIMESTAMP,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.STRING,
            SparkDatatype.DATE,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.DOUBLE


class CastToDecimal(CastToDatatype):
    """
    Cast to Decimal (a.k.a. decimal, numeric, dec, BigDecimal)

    Represents arbitrary-precision signed decimal numbers. Backed internally by `java.math.BigDecimal`. A BigDecimal
    consists of an arbitrary precision integer unscaled value and a 32-bit integer scale.

    The DecimalType must have fixed precision (the maximum total number of digits) and scale (the number of digits on
    the right of dot). For example, (5, 2) can support the value from [-999.99 to 999.99].

    The precision can be up to 38, the scale must be less or equal to precision.

    Spark sets the default precision and scale to (10, 0) when creating a DecimalType. However, when inferring schema
    from decimal.Decimal objects, it will be DecimalType(38, 18).

    For compatibility reasons, Koheesio sets the default precision and scale to (38, 18) when creating a DecimalType.

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    will raise an error in Spark:

    * binary
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
    * boolean
    * timestamp
    * date
    * string
    * void
    * decimal
        spark will convert existing decimals to null if the precision and scale doesn't fit the data

    Spark doesn't error on these, but will cast to null or otherwise give mangled data. Hence, Koheesio will skip the
    transformation for these unless specific columns of these types are given as input:

    * string
        converts to null
    * date
        converts to null
    * void
        skipped by default

    Parameters
    ----------
    columns : ListOfColumns, optional, default=*
        Name of the source column(s). Alias: column
    target_column : str
        Name of the target column or alias if more than one column is specified. Alias: target_alias
    precision : conint(gt=0, le=38), optional, default=38
        the maximum (i.e. total) number of digits (default: 38). Must be > 0.
    scale : conint(ge=0, le=18), optional, default=18
        the number of digits on right side of dot. (default: 18). Must be >= 0.
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToDecimal class."""

        run_for_all_data_type = [
            SparkDatatype.BYTE,
            SparkDatatype.SHORT,
            SparkDatatype.INTEGER,
            SparkDatatype.LONG,
            SparkDatatype.FLOAT,
            SparkDatatype.DOUBLE,
            SparkDatatype.DECIMAL,
            SparkDatatype.BOOLEAN,
            SparkDatatype.TIMESTAMP,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.STRING,
            SparkDatatype.DATE,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.DECIMAL
    precision: conint(gt=0, le=38) = Field(
        default=38,
        description="The maximum total number of digits (precision) of the decimal. Must be > 0. Default is 38",
    )
    scale: conint(ge=0, le=18) = Field(
        default=18,
        description="The number of digits to the right of the decimal point (scale). Must be >= 0. Default is 18",
    )

    @model_validator(mode="after")
    def validate_scale_and_precisions(self) -> "CastToDecimal":
        """Validate the precision and scale values."""
        precision_value = self.precision
        scale_value = self.scale

        if scale_value == precision_value:
            self.log.warning("scale and precision are equal, this will result in a null value")
        if scale_value > precision_value:
            raise ValueError("scale must be < precision")

        return self

    def func(self, column: Column) -> Column:
        return column.cast(self.datatype.spark_type(precision=self.precision, scale=self.scale))


class CastToString(CastToDatatype):
    """
    Cast to String

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
    * binary
    * boolean
    * timestamp
    * date
    * array
    * map
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToString class."""

        run_for_all_data_type = [
            SparkDatatype.BYTE,
            SparkDatatype.SHORT,
            SparkDatatype.INTEGER,
            SparkDatatype.LONG,
            SparkDatatype.FLOAT,
            SparkDatatype.DOUBLE,
            SparkDatatype.DECIMAL,
            SparkDatatype.BINARY,
            SparkDatatype.BOOLEAN,
            SparkDatatype.TIMESTAMP,
            SparkDatatype.DATE,
            SparkDatatype.ARRAY,
            SparkDatatype.MAP,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.STRING


class CastToBinary(CastToDatatype):
    """
    Cast to Binary (a.k.a. byte array)

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    will raise an error in Spark:

    * float
    * double
    * decimal
    * boolean
    * timestamp
    * date
    * array<...>
    * map<...,...>

    Supported datatypes:
    --------------------
    Following casts are supported:

    * byte
    * short
    * integer
    * long
    * string

    Koheesio will skip the transformation for these unless specific columns of these types are given as input:

    * void
        skipped by default
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToBinary class."""

        run_for_all_data_type = [
            SparkDatatype.BYTE,
            SparkDatatype.SHORT,
            SparkDatatype.INTEGER,
            SparkDatatype.LONG,
            SparkDatatype.STRING,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.BINARY


class CastToBoolean(CastToDatatype):
    """
    Cast to Boolean

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    * binary
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

    Spark doesn't error on these, but will cast to null or otherwise give mangled data. Hence, Koheesio will skip the
    transformation for these unless specific columns of these types are given as input:

    * string
        converts to null
    * date
        converts to null
    * void
        skipped by default
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToBoolean class."""

        run_for_all_data_type = [
            SparkDatatype.BYTE,
            SparkDatatype.SHORT,
            SparkDatatype.INTEGER,
            SparkDatatype.LONG,
            SparkDatatype.FLOAT,
            SparkDatatype.DOUBLE,
            SparkDatatype.DECIMAL,
            SparkDatatype.TIMESTAMP,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.STRING,
            SparkDatatype.DATE,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.BOOLEAN


class CastToTimestamp(CastToDatatype):
    """
    Cast to Timestamp

    Numeric time stamp is the number of seconds since January 1, 1970 00:00:00.000000000 UTC.
    Not advised to be used on small integers, as the range of values is too small for timestamp to have any meaning.

    For more fine-grained control over the timestamp format, use the `date_time` module. This allows for parsing strings
    to timestamps and vice versa.

    See Also
    --------
    * [koheesio.spark.transformations.date_time](date_time/index.md)
    * https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html#timestamp-pattern

    Unsupported datatypes:
    ----------------------
    Following casts are not supported

    * binary
    * array<...>
    * map<...,...>

    Supported datatypes:
    --------------------
    Following casts are supported:

    * integer
    * long
    * float
    * double
    * decimal
    * date

    Spark doesn't error on these, but will cast to null or otherwise give mangled data. Hence, Koheesio will skip the
    transformation for these unless specific columns of these types are given as input:

    * boolean:
        range of values too small for timestamp to have any meaning
    * byte:
        range of values too small for timestamp to have any meaning
    * string:
        converts to null in most cases, use `date_time` module instead
    * short:
        range of values too small for timestamp to have any meaning
    * void:
        skipped by default
    """

    class ColumnConfig(CastToDatatype.ColumnConfig):
        """Set the data types that are compatible with the CastToTimestamp class."""

        run_for_all_data_type = [
            SparkDatatype.INTEGER,
            SparkDatatype.LONG,
            SparkDatatype.FLOAT,
            SparkDatatype.DOUBLE,
            SparkDatatype.DECIMAL,
            SparkDatatype.DATE,
        ]
        limit_data_type = [
            *run_for_all_data_type,
            SparkDatatype.BOOLEAN,
            SparkDatatype.BYTE,
            SparkDatatype.SHORT,
            SparkDatatype.STRING,
            SparkDatatype.VOID,
        ]

    datatype: Union[str, SparkDatatype] = SparkDatatype.TIMESTAMP
