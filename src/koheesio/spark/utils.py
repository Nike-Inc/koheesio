"""
Spark Utility functions
"""

import importlib
import os
from enum import Enum
from types import ModuleType
from typing import Union

from pyspark import sql
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructType,
    TimestampType,
)
from pyspark.version import __version__ as spark_version

try:
    from pyspark.sql.utils import AnalysisException  # type: ignore
except ImportError:
    from pyspark.errors.exceptions.base import AnalysisException


AnalysisException = AnalysisException


def get_spark_minor_version() -> float:
    """Returns the minor version of the spark instance.

    For example, if the spark version is 3.3.2, this function would return 3.3
    """
    return float(".".join(spark_version.split(".")[:2]))


# shorthand for the get_spark_minor_version function
SPARK_MINOR_VERSION: float = get_spark_minor_version()


def check_if_pyspark_connect_is_supported() -> bool:
    result = False
    module_name: str = "pyspark"
    if SPARK_MINOR_VERSION >= 3.5:
        try:
            importlib.import_module(f"{module_name}.sql.connect")
            result = True
        except ModuleNotFoundError:
            result = False
    return result


__all__ = [
    "SparkDatatype",
    "import_pandas_based_on_pyspark_version",
    "on_databricks",
    "schema_struct_to_schema_str",
    "spark_data_type_is_array",
    "spark_data_type_is_numeric",
    "show_string",
    "get_spark_minor_version",
    "SPARK_MINOR_VERSION",
    "AnalysisException",
]


class SparkDatatype(Enum):
    """
    Allowed spark datatypes

    The following table lists the data types that are supported by Spark SQL.

    | Data type	    | SQL name                  |
    |---------------|---------------------------|
    | ByteType      | BYTE, TINYINT             |
    | ShortType     | SHORT, SMALLINT           |
    | IntegerType   | INT, INTEGER              |
    | LongType      | LONG, BIGINT              |
    | FloatType     | FLOAT, REAL               |
    | DoubleType    | DOUBLE                    |
    | DecimalType   | DECIMAL, DEC, NUMERIC     |
    | StringType    | STRING                    |
    | BinaryType    | BINARY                    |
    | BooleanType	| BOOLEAN                   |
    | TimestampType | TIMESTAMP, TIMESTAMP_LTZ  |
    | DateType      | DATE                      |
    | ArrayType     | ARRAY<element_type>       |
    | MapType       | MAP<key_type, value_type> |
    | NullType      | VOID                      |

    Not supported yet
    ----------------
    * __TimestampNTZType__
        TIMESTAMP_NTZ
    * __YearMonthIntervalType__
        INTERVAL YEAR, INTERVAL YEAR TO MONTH, INTERVAL MONTH
    * __DayTimeIntervalType__
        INTERVAL DAY, INTERVAL DAY TO HOUR, INTERVAL DAY TO MINUTE, INTERVAL DAY TO SECOND, INTERVAL HOUR,
        INTERVAL HOUR TO MINUTE, INTERVAL HOUR TO SECOND, INTERVAL MINUTE, INTERVAL MINUTE TO SECOND, INTERVAL SECOND

    See Also
    --------
    https://spark.apache.org/docs/latest/sql-ref-datatypes.html#supported-data-types
    """

    # byte
    BYTE = "byte"
    TINYINT = "byte"

    # short
    SHORT = "short"
    SMALLINT = "short"

    # integer
    INTEGER = "integer"
    INT = "integer"

    # long
    LONG = "long"
    BIGINT = "long"

    # float
    FLOAT = "float"
    REAL = "float"

    # timestamp
    TIMESTAMP = "timestamp"
    TIMESTAMP_LTZ = "timestamp"

    # decimal
    DECIMAL = "decimal"
    DEC = "decimal"
    NUMERIC = "decimal"

    DATE = "date"
    DOUBLE = "double"
    STRING = "string"
    BINARY = "binary"
    BOOLEAN = "boolean"
    ARRAY = "array"
    MAP = "map"
    VOID = "void"

    @property
    def spark_type(self) -> DataType:
        """Returns the spark type for the given enum value"""
        mapping_dict = {
            "byte": ByteType,
            "short": ShortType,
            "integer": IntegerType,
            "long": LongType,
            "float": FloatType,
            "double": DoubleType,
            "decimal": DecimalType,
            "string": StringType,
            "binary": BinaryType,
            "boolean": BooleanType,
            "timestamp": TimestampType,
            "date": DateType,
            "array": ArrayType,
            "map": MapType,
            "void": NullType,
        }
        return mapping_dict[self.value]

    @classmethod
    def from_string(cls, value: str) -> "SparkDatatype":
        """Allows for getting the right Enum value by simply passing a string value
        This method is not case-sensitive
        """
        return getattr(cls, value.upper())


def on_databricks() -> bool:
    """Retrieve if we're running on databricks or elsewhere"""
    dbr_version = os.getenv("DATABRICKS_RUNTIME_VERSION", None)
    return dbr_version is not None and dbr_version != ""


def spark_data_type_is_array(data_type: DataType) -> bool:
    """Check if the column's dataType is of type ArrayType"""
    return isinstance(data_type, ArrayType)


def spark_data_type_is_numeric(data_type: DataType) -> bool:
    """Check if the column's dataType is of type ArrayType"""
    return isinstance(data_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType))


def schema_struct_to_schema_str(schema: StructType) -> str:
    """Converts a StructType to a schema str"""
    if not schema:
        return ""
    return ",\n".join([f"{field.name} {field.dataType.typeName().upper()}" for field in schema.fields])


def import_pandas_based_on_pyspark_version() -> ModuleType:
    """
    This function checks the installed version of PySpark and then tries to import the appropriate version of pandas.
    If the correct version of pandas is not installed, it raises an ImportError with a message indicating which version
    of pandas should be installed.
    """
    try:
        import pandas as pd

        pyspark_version = get_spark_minor_version()
        pandas_version = pd.__version__

        if (pyspark_version < 3.4 and pandas_version >= "2") or (pyspark_version >= 3.4 and pandas_version < "2"):
            raise ImportError(
                f"For PySpark {pyspark_version}, "
                f"please install Pandas version {'< 2' if pyspark_version < 3.4 else '>= 2'}"
            )

        return pd
    except ImportError as e:
        raise ImportError("Pandas module is not installed.") from e


def show_string(
    df: Union["sql.DataFrame", "sql.connect.dataframe.DataFrame"],  # type: ignore
    n: int = 20,
    truncate: Union[bool, int] = True,
    vertical: bool = False,
) -> str:
    """Returns a string representation of the DataFrame
    The default implementation of DataFrame.show() hardcodes a print statement, which is not always desirable.
    With this function, you can get the string representation of the DataFrame instead, and choose how to display it.

    Example
    -------
    ```python
    print(show_string(df))

    # or use with a logger
    logger.info(show_string(df))
    ```

    Parameters
    ----------
    df : DataFrame
        The DataFrame to display
    n : int, optional
        The number of rows to display, by default 20
    truncate : Union[bool, int], optional
        If set to True, truncate the displayed columns, by default True
    vertical : bool, optional
        If set to True, display the DataFrame vertically, by default False
    """
    if SPARK_MINOR_VERSION < 3.5:
        return df._jdf.showString(n, truncate, vertical)  # type: ignore
    # as per spark 3.5, the _show_string method is now available making calls to _jdf.showString obsolete
    return df._show_string(n, truncate, vertical)
