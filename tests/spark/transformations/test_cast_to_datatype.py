"""
Test the CastToDatatype cleansing function
"""

import datetime
from decimal import Decimal

import pytest

from pydantic import ValidationError

from pyspark.sql import functions as f

from koheesio.logger import LoggingFactory
from koheesio.spark import DataFrame
from koheesio.spark.transformations.cast_to_datatype import (
    CastToBinary,
    CastToBoolean,
    CastToByte,
    CastToDatatype,
    CastToDecimal,
    CastToDouble,
    CastToFloat,
    CastToInteger,
    CastToLong,
    CastToShort,
    CastToString,
    CastToTimestamp,
)
from koheesio.spark.utils import SparkDatatype, show_string

pytestmark = pytest.mark.spark


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # description: Test CastToDatatype - byte to int
            # input values
            dict(column="byte", datatype="integer", target_column="integer"),
            # expected output
            1,
        ),
        (
            # description: Test CastToDatatype - short to int
            # input values
            dict(column="short", datatype="integer", target_column="integer"),
            # expected output
            1,
        ),
        (
            # description: Test CastToDatatype - integer to string
            # input values
            dict(column="integer", datatype="string", target_column="string"),
            # expected output
            "1",
        ),
        (
            # description: Test CastToDatatype - long to string
            # input values
            dict(column="long", datatype="string", target_column="string"),
            # expected output
            "1",
        ),
        (
            # description: Test CastToDatatype - float to string
            # input values
            dict(column="float", datatype="string", target_column="string"),
            # expected output
            "1.0",
        ),
        (
            # description: Test CastToDatatype - double to string
            # input values
            dict(column="double", datatype="string", target_column="string"),
            # expected output
            "1.0",
        ),
        (
            # description: Test CastToDatatype - decimal to string
            # input values
            dict(column="decimal", datatype="integer", target_column="integer"),
            # expected output
            1,
        ),
        (
            # description: Test CastToDatatype - boolean to string
            # input values
            dict(column="boolean", datatype="string", target_column="string"),
            # expected output
            "true",
        ),
        (
            # description: Test CastToDatatype - date to string
            # input values
            dict(column="date", datatype="string", target_column="string"),
            # expected output
            "2023-01-01",
        ),
        (
            # description: Test CastToDatatype - timestamp to string
            # input values
            dict(column="timestamp", datatype="string", target_column="string"),
            # expected output
            "2023-01-01 00:01:01",
        ),
        (
            # description: Test CastToDatatype - string to boolean
            # input values
            dict(column="string", datatype="boolean", target_column="boolean"),
            # expected output
            None,
        ),
        (
            # description: Test CastToDatatype - void to string
            # input values
            dict(column="void", datatype="string", target_column="string"),
            # expected output
            None,
        ),
        (
            # description: Test CastToDatatype - array to string
            # input values
            dict(column="array", datatype="string", target_column="string"),
            # expected output
            "[a]",
        ),
        (
            # description: Test CastToDatatype - map to string
            # input values
            dict(column="map", datatype="string", target_column="string"),
            # expected output
            "{a -> b}",
        ),
        (
            # description: Test CastToDatatype - binary to string
            # input values
            dict(column="binary", datatype="string", target_column="string"),
            # expected output
            "a",
        ),
        (
            # description: Test CastToDatatype - boolean to integer
            # input values
            dict(column="boolean", datatype="integer", target_column="integer"),
            # expected output
            1,
        ),
        (
            # description: Test CastToDatatype - using SparkDatatype enum
            # input values
            dict(column="boolean", datatype=SparkDatatype.INTEGER, target_column="integer"),
            # expected output
            1,
        ),
    ],
)
def test_happy_flow(input_values, expected, df_with_all_types: DataFrame):
    log = LoggingFactory.get_logger(name="test_cast_to_datatype")

    cast_to_datatype = CastToDatatype(**input_values)
    output_df = cast_to_datatype.transform(df_with_all_types)
    col = cast_to_datatype.columns[0]
    target_column = cast_to_datatype.target_column

    # log equivalent of doing df.show()
    log.info(f"show output_df: \n{show_string(output_df.select(col, target_column), 20, 20, False)}")

    actual = [row[target_column] for row in output_df.select(target_column).collect()][0]
    assert actual == expected


def test_wrong_datatype():
    """
    Should only accept atomic types as defined in pyspark.sql.types
    """
    with pytest.raises(AttributeError):
        CastToDatatype(column="c1", datatype="sting", target_column="c1")
        CastToDatatype(column="c1", datatype=123, target_column="c1")


@pytest.mark.parametrize(
    "klass,expected",
    [
        (
            CastToByte,
            {
                "short_target": 1,
                "integer_target": 1,
                "long_target": 1,
                "float_target": 1,
                "double_target": 1,
                "decimal_target": 1,
                # 'string_target': None,
                "boolean_target": 1,
                # 'timestamp_target': 61,
                # 'date_target': None,
                # 'void_target': None,
            },
        ),
        (
            CastToShort,
            {
                "byte_target": 1,
                "integer_target": 1,
                "long_target": 1,
                "float_target": 1,
                "double_target": 1,
                "decimal_target": 1,
                # 'string_target': None,
                "boolean_target": 1,
                # 'timestamp_target': -12995,
                # 'date_target': None,
                # 'void_target': None,
            },
        ),
        (
            CastToInteger,
            {
                "byte_target": 1,
                "short_target": 1,
                "long_target": 1,
                "float_target": 1,
                "double_target": 1,
                "decimal_target": 1,
                # 'string_target': None,
                "boolean_target": 1,
                "timestamp_target": 1672531261,  # '2023-01-01T00:01:01'
                # 'date_target': None,
                # 'void_target': None,
            },
        ),
        (
            CastToLong,
            {
                "byte_target": 1,
                "short_target": 1,
                "integer_target": 1,
                "float_target": 1,
                "double_target": 1,
                "decimal_target": 1,
                # 'string_target': None,
                "boolean_target": 1,
                "timestamp_target": 1672531261,  # '2023-01-01T00:01:01'
                # 'date_target': None,
                # 'void_target': None,
            },
        ),
        (
            CastToFloat,
            {
                "byte_target": 1.0,
                "short_target": 1.0,
                "integer_target": 1.0,
                "long_target": 1.0,
                "double_target": 1.0,
                "decimal_target": 1.0,
                # 'string_target': None,
                "boolean_target": 1.0,
                # 'timestamp_target': 1.6725312E9,  # ~ '2023-01-01T00:01:01' - loss of detail with a regular Float
                # 'date_target': None,
                # 'void_target': None,
            },
        ),
        (
            CastToDouble,
            {
                "byte_target": 1.0,
                "short_target": 1.0,
                "integer_target": 1.0,
                "long_target": 1.0,
                "float_target": 1.0,
                "decimal_target": 1.0,
                # 'string_target': None,
                "boolean_target": 1.0,
                "timestamp_target": 1672531261.0,  # '2022-12-31T23:01:01'
                # 'date_target': None,
                # 'void_target': None,
            },
        ),
        (
            CastToDecimal,
            {
                "byte_target": Decimal("1.000000000000000000"),
                "short_target": Decimal("1.000000000000000000"),
                "integer_target": Decimal("1.000000000000000000"),
                "long_target": Decimal("1.000000000000000000"),
                "float_target": Decimal("1.000000000000000000"),
                "double_target": Decimal("1.000000000000000000"),
                "decimal_target": Decimal("1.000000000000000000"),
                # 'string_target': None,
                "boolean_target": Decimal("1.000000000000000000"),
                "timestamp_target": Decimal("1672531261.000000000000000000"),
                # 'date_target': None,
                # 'void_target': None,
            },
        ),
        (
            CastToString,
            {
                "byte_target": "1",
                "short_target": "1",
                "integer_target": "1",
                "long_target": "1",
                "float_target": "1.0",
                "double_target": "1.0",
                "decimal_target": "1",
                "binary_target": "a",
                "boolean_target": "true",
                "timestamp_target": "2023-01-01 00:01:01",
                "date_target": "2023-01-01",
                "array_target": "[a]",
                "map_target": "{a -> b}",
                # 'void_target': None,
            },
        ),
        (
            CastToBinary,
            {
                "byte_target": bytearray(b"\x01"),
                "short_target": bytearray(b"\x00\x01"),
                "integer_target": bytearray(b"\x00\x00\x00\x01"),
                "long_target": bytearray(b"\x00\x00\x00\x00\x00\x00\x00\x01"),
                # 'float_target': AnalysisException,
                # 'double_target': AnalysisException,
                # 'decimal_target': AnalysisException,
                "string_target": bytearray(b"a"),
                # 'boolean_target': AnalysisException,
                # 'timestamp_target': AnalysisException,
                # 'date_target': AnalysisException,
                # 'array_target': AnalysisException,
                # 'map_target': AnalysisException,
                # 'void_target': None,
            },
        ),
        (
            CastToBoolean,
            {
                "byte_target": True,
                "short_target": True,
                "integer_target": True,
                "long_target": True,
                "float_target": True,
                "double_target": True,
                "decimal_target": True,
                # 'string_target': None,
                # 'binary_target': AnalysisException,
                "timestamp_target": True,
                # 'date_target': None,
                # 'array_target': AnalysisException,
                # 'map_target': AnalysisException,
                # 'void_target': None,
            },
        ),
        (
            CastToTimestamp,
            {
                # 0 = '1970-01-01T00:00:01'
                # 'byte_target': datetime.datetime.utcfromtimestamp(1),
                # 'short_target': datetime.datetime.utcfromtimestamp(1),
                "integer_target": datetime.datetime.utcfromtimestamp(1),
                "long_target": datetime.datetime.utcfromtimestamp(1),
                "float_target": datetime.datetime.utcfromtimestamp(1),
                "double_target": datetime.datetime.utcfromtimestamp(1),
                "decimal_target": datetime.datetime.utcfromtimestamp(1),
                # 'string_target': None,
                # 'binary_target': AnalysisException,
                # 'boolean_target': datetime.datetime.utcfromtimestamp(1),
                "date_target": datetime.datetime(2023, 1, 1, 0, 0),
                # 'array_target': AnalysisException,
                # 'map_target': AnalysisException,
                # 'void_target': None,
            },
        ),
    ],
)
def test_cast_to_specific_type(klass, expected, df_with_all_types):
    log = LoggingFactory.get_logger(name="test_cast_to_specific_type")

    klz = klass(df=df_with_all_types, target_suffix="target")
    target_columns = [tc for tc, _ in klz.get_columns_with_target()]
    output = klz.execute()
    output_df = output.df.select(target_columns)
    actual = output_df.head().asDict()

    # log equivalent of doing df.show()
    log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")

    assert target_columns == list(expected.keys())
    assert actual == expected


@pytest.mark.parametrize(
    "precision,scale,alternative_value,expected",
    [
        # with a precision of 1, the scale can only be 0; 1 will result in a null value
        (1, 1, Decimal("1"), [{"c1": None, "c2": None}]),
        (1, 0, Decimal("42"), [{"c1": Decimal("1"), "c2": None}]),
        # with a precision of 10, the scale can be 0 - 10
        (10, 1, Decimal("123456789.0123456789"), [{"c1": Decimal("1.0"), "c2": Decimal("123456789.0")}]),
        (10, 9, Decimal("123456789.0123456789"), [{"c1": Decimal("1.0"), "c2": None}]),
        (10, 10, Decimal("1"), [{"c1": None, "c2": None}]),
        # Koheesio defaults to a precision of 38 and a scale of 18 - c2 is of max precision and scale in this case
        (
            38,
            18,
            Decimal("12345678901234567890.012345678901234567"),
            [{"c1": Decimal("1.000000000000000000"), "c2": Decimal("12345678901234567890.012345678901234567")}],
        ),
        # Unhappy flows
        # Values that are too big or too small for precision and scale will result in an error getting raised
        (-1, 0, ..., ValidationError),
        (39, 0, ..., ValidationError),
        (1, -1, ..., ValidationError),
    ],
)
def test_decimal_precision_and_scale(precision, scale, alternative_value, expected, df_with_all_types):
    log = LoggingFactory.get_logger(name="test_decimal_precision_and_scale")

    if alternative_value == ...:
        with pytest.raises(expected):
            CastToDecimal(columns=["decimal"], scale=scale, precision=precision)
        return

    input_df = (
        df_with_all_types.select("decimal")
        .withColumn("c1", f.col("decimal"))
        .withColumn("c2", f.lit(alternative_value))
        .select("c1", "c2")
    )

    # log equivalent of doing df.show()
    log.info(f"show output_df: \n{show_string(input_df, 20, 20, False)}")

    output_df = CastToDecimal(columns=["c1", "c2"], scale=scale, precision=precision).transform(input_df)
    log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")

    actual = [row.asDict() for row in output_df.collect()]
    assert actual == expected


# TODO: nested functionality does not exists yet
#
#     def test_transform_nested_df(self):
#         """
#         Test with nested dataframes
#         """
#         output_df = (
#             CastToDatatype(
#                 df=self.sample_nested_df,
#                 params={
#                     "source_column": "payload.col2.col_int",
#                     "datatype": "string",
#                 },
#                 columns_to_flatten=["payload{}.col2{}"],
#                 target_alias="casted_int_column",
#             )
#             .execute()
#             .df
#         )
#
#         self.assertEqual(output_df.select("casted_int_column").dtypes[0][1], "string")
