"""
This module contains sample data for testing purposes.

The following functions return DataFrames with sample data for testing various Spark operations.

Functions
---------
- dummy_df:
    A 1-row DataFrame with one column: id.
- sample_df_to_partition:
    DataFrame with two columns: partition and value.
- sample_df_with_strings:
    DataFrame with two columns: id and string.
- sample_df_with_timestamp:
    DataFrame with two columns: a_date and a_timestamp.
- sample_df_with_string_timestamp:
    DataFrame with two columns: id and a_string_timestamp.
- sample_df_with_all_types:
    DataFrame with all supported Spark datatypes.

All the functions are compatible with pytest fixtures and are commonly used for testing purposes.

Koheesio's extensive test suite used these functions to test various Spark operations.

Example
-------
### Using the `sample_df_to_partition` as a fixture in a test:

```python
from koheesio.spark.utils.testing.fixtures import register_fixtures
from koheesio.spark.utils.testing.sample_data import sample_df_to_partition

register_fixtures(sample_df_to_partition)


def test_sample_df_to_partition(sample_df_to_partition):
    # Test code here
    assert sample_df_to_partition.count() == 2
```

In this example, `register_fixtures` is used to register the `sample_df_to_partition` fixture. The fixture is then used
in the test function to access the DataFrame.

### Using the `sample_df_with_strings` function directly in a test:

```python
from koheesio.spark.utils.testing.sample_data import sample_df_with_strings


def test_sample_df_with_strings(spark):
    df = sample_df_with_strings(spark)
    assert df.count() == 3
```

In this example, the `sample_df_with_strings` function is used directly in the test function to create a DataFrame.
"""

import datetime
from decimal import Decimal

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
    StructField,
    StructType,
    TimestampType,
)

from koheesio.spark import DataFrame, SparkSession


def dummy_df(spark: SparkSession) -> DataFrame:
    """
    A 1-row DataFrame with one column: id.

    ## df:
    | id |
    |----|
    | 1  |

    ## schema:
    - id: long (nullable = false)
    """
    return spark.range(1)


def sample_df_to_partition(spark: SparkSession) -> DataFrame:
    """
    DataFrame with two columns: partition and value.

    ## df:
    | partition | Value |
    |-----------|-------|
    | BE        | 12    |
    | FR        | 20    |

    ## schema:
    - partition: string (nullable = true)
    - value: long (nullable = true)
    """
    data = [["BE", 12], ["FR", 20]]
    schema = ["partition", "value"]
    return spark.createDataFrame(data, schema)


def sample_df_with_strings(spark: SparkSession) -> DataFrame:
    """
    DataFrame with two columns: id and string.
    Includes 3 rows of data, including a null value.

    ## df:
    | id | string |
    |----|--------|
    | 1  | hello  |
    | 2  | world  |
    | 3  |        |

    ## schema:
    - id: bigint (nullable = true)
    - string: string (nullable = true)
    """
    data = [[1, "hello"], [2, "world"], [3, ""]]
    schema = ["id", "string"]
    return spark.createDataFrame(data, schema)


def sample_df_with_timestamp(spark: SparkSession) -> DataFrame:
    """
    DataFrame with two columns: a_date and a_timestamp.

    ## df:
    | id | a_date              | a_timestamp         |
    |----|---------------------|---------------------|
    | 1  | 1970-04-20 12:33:09 | 2000-07-01 01:01:00 |
    | 2  | 1980-05-21 13:34:08 | 2010-08-02 02:02:00 |
    | 3  | 1990-06-22 14:35:07 | 2020-09-03 03:03:00 |

    ## schema:
    - id: bigint (nullable = true)
    - a_date: timestamp (nullable = true)
    - a_timestamp: timestamp (nullable = true)
    """
    data = [
        (1, datetime.datetime(1970, 4, 20, 12, 33, 9), datetime.datetime(2000, 7, 1, 1, 1)),
        (2, datetime.datetime(1980, 5, 21, 13, 34, 8), datetime.datetime(2010, 8, 2, 2, 2)),
        (3, datetime.datetime(1990, 6, 22, 14, 35, 7), datetime.datetime(2020, 9, 3, 3, 3)),
    ]
    schema = ["id", "a_date", "a_timestamp"]
    return spark.createDataFrame(data, schema)


def sample_df_with_string_timestamp(spark: SparkSession) -> DataFrame:
    """
    DataFrame with two columns: id and a_string_timestamp.
    The timestamps in this data are stored as strings.

    ## df:
    | id | a_string_timestamp |
    |----|--------------------|
    | 1  | 202301010420       |
    | 2  | 202302020314       |

    ## schema:
    - id: bigint (nullable = true)
    - a_string_timestamp: string (nullable = true)
    """
    data = [(1, "202301010420"), (2, "202302020314")]
    schema = ["id", "a_string_timestamp"]
    return spark.createDataFrame(data, schema)


def sample_df_with_all_types(spark: SparkSession) -> DataFrame:
    """Create a DataFrame with all supported Spark datatypes

    This DataFrame has 1 row and 14 columns, each column containing a single value of the given datatype.

    ## df:
    | BYTE | SHORT | INTEGER | LONG | FLOAT | DOUBLE | DECIMAL | STRING | BINARY | BOOLEAN | TIMESTAMP           | DATE       | ARRAY | MAP        | VOID |
    |------|-------|---------|------|-------|--------|---------|--------|--------|---------|---------------------|------------|-------|------------|------|
    | 1    | 1     | 1       | 1    | 1.0   | 1.0    | 1.0     | a      | a      | true    | 2023-01-01T00:01:01 | 2023-01-01 | ["a"] | {"a": "b"} | null |

    ## schema:
    - BYTE: byte (nullable = true)
    - SHORT: short (nullable = true)
    - INTEGER: integer (nullable = true)
    - LONG: long (nullable = true)
    - FLOAT: float (nullable = true)
    - DOUBLE: double (nullable = true)
    - DECIMAL: decimal(10,0) (nullable = true)
    - STRING: string (nullable = true)
    - BINARY: binary (nullable = true)
    - BOOLEAN: boolean (nullable = true)
    - TIMESTAMP: timestamp (nullable = true)
    - DATE: date (nullable = true)
    - ARRAY: array (nullable = true)
    - MAP: map (nullable = true)
    - VOID: void (nullable = true)

    """
    data = dict(
        BYTE=(1, "byte", ByteType()),
        SHORT=(1, "short", ShortType()),
        INTEGER=(1, "integer", IntegerType()),
        LONG=(1, "long", LongType()),
        FLOAT=(1.0, "float", FloatType()),
        DOUBLE=(1.0, "double", DoubleType()),
        DECIMAL=(Decimal(1.0), "decimal", DecimalType()),
        STRING=("a", "string", StringType()),
        BINARY=(b"a", "binary", BinaryType()),
        BOOLEAN=(True, "boolean", BooleanType()),
        # '2023-01-01T00:01:01'
        TIMESTAMP=(datetime.datetime.utcfromtimestamp(1672531261), "timestamp", TimestampType()),
        DATE=(datetime.date(2023, 1, 1), "date", DateType()),
        ARRAY=(["a"], "array", ArrayType(StringType())),
        MAP=({"a": "b"}, "map", MapType(StringType(), StringType())),
        VOID=(None, "void", NullType()),
    )
    return spark.createDataFrame(
        data=[[v[0] for v in data.values()]],
        schema=StructType([StructField(name=v[1], dataType=v[2]) for v in data.values()]),
    )
