"""
Spark Utility functions
"""

from typing import Union
from enum import Enum
import importlib
import inspect
import os
from types import ModuleType
import warnings

from pyspark import sql
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
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
    "Column",
    "DataFrame",
    "SparkSession",
    "ParseException",
    "DataType",
    "DataFrameReader",
    "DataStreamReader",
    "DataFrameWriter",
    "DataStreamWriter",
    "StreamingQuery",
    "Window",
    "WindowSpec",
    "get_active_session",
    "check_if_pyspark_connect_module_is_available",
    "check_if_pyspark_connect_is_supported",
    "get_column_name",
]

try:
    from pyspark.errors.exceptions.base import AnalysisException  # type: ignore
except (ImportError, ModuleNotFoundError):
    from pyspark.sql.utils import AnalysisException  # type: ignore


AnalysisException = AnalysisException


def get_spark_minor_version() -> float:
    """Returns the minor version of the spark instance.

    For example, if the spark version is 3.3.2, this function would return 3.3
    """
    return float(".".join(spark_version.split(".")[:2]))


# shorthand for the get_spark_minor_version function
SPARK_MINOR_VERSION: float = get_spark_minor_version()


class PysparkConnectModuleNotAvailableWarning(Warning):
    """Warning to be raised when the pyspark connect module is not available"""

    def __init__(self):
        message = (
            "It looks like the required modules for Spark Connect (e.g. grpcio) are not installed. "
            "If not, you can install them using `pip install pyspark[connect]` or `koheesio[pyspark_connect]`. "
        )
        super().__init__(message)


def check_if_pyspark_connect_module_is_available() -> bool:
    """Check if the pyspark connect module is available

    Returns
    -------
    bool
        True if the pyspark connect module is available, False otherwise.

    Raises
    ------
    ImportError
        If the required modules for Spark Connect are not importable.
    """

    # before pyspark 3.4, connect was not supported
    if SPARK_MINOR_VERSION < 3.4:
        return False

    try:
        importlib.import_module("pyspark.sql.connect")
        # check extras: grpcio package is needed for pyspark[connect] to work
        try:
            importlib.import_module("grpc")
        except (ImportError, ModuleNotFoundError) as e_import:
            raise e_import
        return True
    except (ImportError, ModuleNotFoundError):
        warnings.warn(PysparkConnectModuleNotAvailableWarning())
        return False


def check_if_pyspark_connect_is_supported() -> bool:
    warnings.warn(
        message=(
            "The `check_if_pyspark_connect_is_supported` function has been"
            " replaced by `check_if_pyspark_connect_module_is_available`."
            " Import it instead. Current function will be removed in the future."
        ),
        category=DeprecationWarning,
        stacklevel=2,
    )
    return check_if_pyspark_connect_module_is_available()


if check_if_pyspark_connect_module_is_available():
    """Only import the connect module if the current version of PySpark supports it"""
    from pyspark.errors.exceptions.captured import (
        ParseException as CapturedParseException,
    )
    from pyspark.errors.exceptions.connect import (
        ParseException as ConnectParseException,
    )
    from pyspark.sql.connect.column import Column as ConnectColumn
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.sql.connect.proto.types_pb2 import DataType as ConnectDataType
    from pyspark.sql.connect.readwriter import DataFrameReader, DataFrameWriter
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
    from pyspark.sql.connect.streaming.readwriter import (
        DataStreamReader,
        DataStreamWriter,
    )
    from pyspark.sql.connect.window import Window as ConnectWindow
    from pyspark.sql.connect.window import WindowSpec as ConnectWindowSpec
    from pyspark.sql.streaming.query import StreamingQuery
    from pyspark.sql.types import DataType as SqlDataType
    from pyspark.sql.window import Window, WindowSpec

    Column = Union[sql.Column, ConnectColumn]
    DataFrame = Union[sql.DataFrame, ConnectDataFrame]
    SparkSession = Union[sql.SparkSession, ConnectSparkSession]
    ParseException = (CapturedParseException, ConnectParseException)
    DataType = Union[SqlDataType, ConnectDataType]
    DataFrameReader = Union[sql.readwriter.DataFrameReader, DataFrameReader]  # type: ignore
    DataStreamReader = Union[sql.streaming.readwriter.DataStreamReader, DataStreamReader]  # type: ignore
    DataFrameWriter = Union[sql.readwriter.DataFrameWriter, DataFrameWriter]  # type: ignore
    DataStreamWriter = Union[sql.streaming.readwriter.DataStreamWriter, DataStreamWriter]  # type: ignore
    StreamingQuery = StreamingQuery
    Window = Union[Window, ConnectWindow]
    WindowSpec = Union[WindowSpec, ConnectWindowSpec]
else:
    """Import the regular PySpark modules if the current version of PySpark does not support the connect module"""
    try:
        from pyspark.errors.exceptions.captured import ParseException  # type: ignore
    except (ImportError, ModuleNotFoundError):
        from pyspark.sql.utils import ParseException  # type: ignore

    ParseException = ParseException

    from pyspark.sql.column import Column  # type: ignore
    from pyspark.sql.dataframe import DataFrame  # type: ignore
    from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter  # type: ignore
    from pyspark.sql.session import SparkSession  # type: ignore
    from pyspark.sql.types import DataType  # type: ignore
    from pyspark.sql.window import Window, WindowSpec  # type: ignore

    try:
        from pyspark.sql.streaming.query import StreamingQuery  # type: ignore
        from pyspark.sql.streaming.readwriter import DataStreamReader, DataStreamWriter  # type: ignore
    except (ImportError, ModuleNotFoundError):
        from pyspark.sql.streaming import (  # type: ignore
            DataStreamReader,
            DataStreamWriter,
            StreamingQuery,
        )
    DataFrameReader = DataFrameReader
    DataStreamReader = DataStreamReader
    DataFrameWriter = DataFrameWriter
    DataStreamWriter = DataStreamWriter
    StreamingQuery = StreamingQuery
    Window = Window
    WindowSpec = WindowSpec


def get_active_session() -> SparkSession:  # type: ignore
    """Get the active Spark session"""
    if check_if_pyspark_connect_module_is_available() and SPARK_MINOR_VERSION >= 3.5:
        from pyspark.sql.connect.session import SparkSession as _ConnectSparkSession

        session = _ConnectSparkSession.getActiveSession() or sql.SparkSession.getActiveSession()  # type: ignore

    else:
        session = sql.SparkSession.getActiveSession()

    if not session:
        raise RuntimeError(
            "No active Spark session found. Please create a Spark session before using module connect_utils."
            " Or perform local import of the module."
        )

    return session


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
    def spark_type(self) -> type:
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
    spark = get_active_session()
    dbr_spark_version = spark.conf.get("spark.databricks.clusterUsageTags.effectiveSparkVersion", None)
    return (dbr_version is not None and dbr_version != "") or (dbr_spark_version is not None)


def spark_data_type_is_array(data_type: DataType) -> bool:  # type: ignore
    """Check if the column's dataType is of type ArrayType"""
    return isinstance(data_type, ArrayType)


def spark_data_type_is_numeric(data_type: DataType) -> bool:  # type: ignore
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


def show_string(df: DataFrame, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> str:  # type: ignore
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
        # noinspection PyProtectedMember
        return df._jdf.showString(n, truncate, vertical)  # type: ignore
    # as per spark 3.5, the _show_string method is now available making calls to _jdf.showString obsolete
    # noinspection PyProtectedMember
    return df._show_string(n, truncate, vertical)


def get_column_name(col: Column) -> str:  # type: ignore
    """Get the column name from a Column object

    Normally, the name of a Column object is not directly accessible in the regular pyspark API. This function
    extracts the name of the given column object without needing to provide it in the context of a DataFrame.

    Parameters
    ----------
    col: Column
        The Column object

    Returns
    -------
    str
        The name of the given column
    """
    # we have to distinguish between the Column object from column from local session and remote
    if hasattr(col, "_jc"):
        # In case of a 'regular' Column object, we can directly access the name attribute through the _jc attribute
        # noinspection PyProtectedMember
        name = col._jc.toString()  # type: ignore[operator]
    elif any(cls.__module__ == "pyspark.sql.connect.column" for cls in inspect.getmro(col.__class__)):
        # noinspection PyProtectedMember
        name = col._expr.name()
    else:
        raise ValueError("Column object is not a valid Column object")

    return name
