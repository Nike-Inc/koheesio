import math

import pytest

from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from koheesio.spark.transformations.arrays import (
    ArrayDistinct,
    ArrayMax,
    ArrayMean,
    ArrayMedian,
    ArrayMin,
    ArrayRemove,
    ArrayReverse,
    ArraySortAsc,
    ArraySortDesc,
    ArraySum,
    Explode,
    ExplodeDistinct,
)

pytestmark = pytest.mark.spark

input_data = [
    (1, [1, 2, 2, 3, 3, 3], ["a", "b", "b", "c", "c", " ", None], [1.0, 2.0, 2.0, 3.0, 3.0, 3.0, float("nan")]),
    (2, [4, 4, 5, 5, 6, 6, None], ["d", "e", "e", "f", "f", "f"], [4.0, 5.0, 5.0, 6.0, 6.0, 6.0]),
    (3, [7, 7, 8, 8, 9, 9], ["g", "h", "h", "i", "i", "i"], [7.0, 8.0, 8.0, 9.0, 9.0, 9.0]),
    (4, [], [], []),
    (5, None, None, None),
    (6, [-1, -3, -5, 0, +1], [], []),
]
input_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("array_int", ArrayType(IntegerType()), True),
        StructField("array_str", ArrayType(StringType()), True),
        StructField("array_float", ArrayType(FloatType()), True),
    ]
)


@pytest.mark.parametrize(
    "kls,column,params,expected_data",
    [
        (
            # Explode - test with strings and preserve_nulls False
            Explode,
            "array_str",
            {"preserve_nulls": False},
            ["a", "b", "b", "c", "c", " ", None, "d", "e", "e", "f", "f", "f", "g", "h", "h", "i", "i", "i"],
        ),
        (
            # Explode - test with strings and preserve_nulls True
            Explode,
            "array_str",
            {"preserve_nulls": True},
            [
                "a",
                "b",
                "b",
                "c",
                "c",
                " ",
                None,
                "d",
                "e",
                "e",
                "f",
                "f",
                "f",
                "g",
                "h",
                "h",
                "i",
                "i",
                "i",
                None,
                None,
                None,
            ],
        ),
        (
            # Explode - test with numbers
            Explode,
            "array_int",
            None,  # no params
            [1, 2, 2, 3, 3, 3, 4, 4, 5, 5, 6, 6, None, 7, 7, 8, 8, 9, 9, None, None, -1, -3, -5, 0, 1],
        ),
        (
            # Explode - test with numbers and preserve_nulls False
            Explode,
            "array_int",
            {"preserve_nulls": False},
            [1, 2, 2, 3, 3, 3, 4, 4, 5, 5, 6, 6, None, 7, 7, 8, 8, 9, 9, -1, -3, -5, 0, 1],
        ),
        (
            # ArrayDistinct - test with strings
            ArrayDistinct,
            "array_str",
            None,  # no params
            [["a", "b", "c"], ["d", "e", "f"], ["g", "h", "i"], [], None, []],
        ),
        (
            # ArrayDistinct - test with numbers [int]
            ArrayDistinct,
            "array_int",
            None,  # no params
            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [], None, [-1, -3, -5, 1]],
        ),
        (
            # ArrayDistinct - test with numbers [float]
            ArrayDistinct,
            "array_float",
            None,  # no params
            [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0], [], None, []],
        ),
        (
            # ExplodeDistinct - test with strings
            ExplodeDistinct,
            "array_str",
            None,  # no params
            ["a", "b", "c", "d", "e", "f", "g", "h", "i", None, None, None],
        ),
        (
            # ArrayReverse - test with strings
            ArrayReverse,
            "array_str",
            None,  # no params
            [
                [None, " ", "c", "c", "b", "b", "a"],
                ["f", "f", "f", "e", "e", "d"],
                ["i", "i", "i", "h", "h", "g"],
                [],
                None,
                [],
            ],
        ),
        (
            # ArraySortAsc - test with strings
            ArraySortAsc,
            "array_str",
            None,  # no params
            [
                [" ", "a", "b", "b", "c", "c", None],
                ["d", "e", "e", "f", "f", "f"],
                ["g", "h", "h", "i", "i", "i"],
                [],
                None,
                [],
            ],
        ),
        (
            # ArraySortDesc - test with strings
            ArraySortDesc,
            "array_str",
            None,  # no params
            [
                [None, "c", "c", "b", "b", "a", " "],
                ["f", "f", "f", "e", "e", "d"],
                ["i", "i", "i", "h", "h", "g"],
                [],
                None,
                [],
            ],
        ),
        (
            # ArrayRemove - test with strings
            ArrayRemove,
            "array_str",
            {"value": "b", "make_distinct": False},
            [["a", "c", "c", " "], ["d", "e", "e", "f", "f", "f"], ["g", "h", "h", "i", "i", "i"], [], None, []],
        ),
        (
            # ArrayRemove - test with multiple values
            ArrayRemove,
            "array_str",
            {"value": ["b", "f", "i"], "make_distinct": True},
            [["a", "c", " "], ["d", "e"], ["g", "h"], [], None, []],
        ),
        (
            ArrayRemove,
            "array_float",
            {"value": 2.0, "make_distinct": False},
            [
                [1.0, 3.0, 3.0, 3.0],
                [4.0, 5.0, 5.0, 6.0, 6.0, 6.0],
                [7.0, 8.0, 8.0, 9.0, 9.0, 9.0],
                [],
                None,
                [],
            ],
        ),
        (
            ArrayRemove,
            "array_float",
            {"value": 2.0, "make_distinct": False, "keep_nan": True},
            [
                [1.0, 3.0, 3.0, 3.0, float("nan")],
                [4.0, 5.0, 5.0, 6.0, 6.0, 6.0],
                [7.0, 8.0, 8.0, 9.0, 9.0, 9.0],
                [],
                None,
                [],
            ],
        ),
        (
            ArrayRemove,
            "array_int",
            {"value": 6, "make_distinct": False},
            [[1, 2, 2, 3, 3, 3], [4, 4, 5, 5], [7, 7, 8, 8, 9, 9], [], None, [-1, -3, -5, 0, +1]],
        ),
        (
            ArrayRemove,
            "array_int",
            {"value": 6, "make_distinct": False, "keep_null": True},
            [[1, 2, 2, 3, 3, 3], [4, 4, 5, 5, None], [7, 7, 8, 8, 9, 9], [], None, [-1, -3, -5, 0, +1]],
        ),
        (
            # ArrayMin - test with numbers [int]
            ArrayMin,
            "array_int",
            None,  # no params
            [1, 4, 7, None, None, -5],
        ),
        (
            # ArrayMin - test with numbers [float]
            ArrayMin,
            "array_float",
            None,  # no params
            [1.0, 4.0, 7.0, None, None, None],
        ),
        (
            # ArrayMin - test with strings
            ArrayMin,
            "array_str",
            None,  # no params
            [" ", "d", "g", None, None, None],
        ),
        (
            # ArrayMax - test with numbers [int]
            ArrayMax,
            "array_int",
            None,  # no params
            [3, 6, 9, None, None, 1],
        ),
        (
            # ArrayMax - test with numbers [float]
            ArrayMax,
            "array_float",
            None,  # no params
            [3.0, 6.0, 9.0, None, None, None],
        ),
        (
            # ArrayMax - test with numbers [float]
            ArrayMax,
            "array_float",
            {"keep_nan": True},
            [float("nan"), 6.0, 9.0, None, None, None],
        ),
        (
            # ArrayMax - test with strings
            ArrayMax,
            "array_str",
            None,  # no params
            ["c", "f", "i", None, None, None],
        ),
        (
            # ArraySum - test with numbers [int]
            ArraySum,
            "array_int",
            None,  # no params
            [14, 30, 48, 0, None, -8],
        ),
        (
            # ArraySum - test with numbers [float]
            ArraySum,
            "array_float",
            None,  # no params
            [14.0, 32.0, 50.0, 0.0, None, 0.0],
        ),
        (
            # ArrayMean - test with numbers [int]
            ArrayMean,
            "array_int",
            None,  # no params
            [2.3333333333333335, 5.0, 8.0, 0.0, None, -1.6],
        ),
        (
            # ArrayMean - test with numbers [int]
            ArrayMean,
            "array_int",
            {"keep_null": True},
            [2.3333333333333335, None, 8.0, 0.0, None, -1.6],
        ),
        (
            # ArrayMean - test with numbers [float]
            ArrayMean,
            "array_float",
            None,  # no params
            [2.3333333333333335, 5.333333333333333, 8.333333333333334, 0.0, None, 0.0],
        ),
        (
            # ArrayMean - test with numbers [float]
            ArrayMean,
            "array_float",
            {"keep_nan": True},
            [float("nan"), 5.333333333333333, 8.333333333333334, 0.0, None, 0.0],
        ),
        (
            # ArrayMedian - test with numbers [int]
            ArrayMedian,
            "array_int",
            None,  # no params
            [2.5, 5.0, 8.0, 0.0, None, -1.0],
        ),
        (
            # ArrayMedian - test with numbers [float]
            ArrayMedian,
            "array_float",
            None,  # no params
            [2.5, 5.5, 8.5, 0.0, None, 0.0],
        ),
    ],
)
def test_array(kls, column, expected_data, params, spark):
    # set up test data
    test_data = spark.createDataFrame(
        data=input_data,
        schema=input_schema,
    )

    params = params or {}

    # noinspection PyCallingNonCallable
    df = kls(df=test_data, column=column, **params).transform()
    actual_data = [row.asDict()[column] for row in df.select(column).collect()]

    def check_result(_actual_data: list, _expected_data: list):
        _data = _expected_data or _actual_data

        if _data:
            for i in range(len(_data)):
                if isinstance(_actual_data[i], float):
                    if math.isnan(_expected_data[i]):
                        assert math.isnan(_actual_data[i])
                    else:
                        assert _actual_data[i] == _expected_data[i]
                else:
                    assert _actual_data[i] == _expected_data[i]
        else:
            assert _actual_data == _expected_data

    if isinstance(expected_data[0], list):
        for actual, expected in zip(actual_data, expected_data):
            check_result(actual, expected)
    else:
        check_result(actual_data, expected_data)


@pytest.mark.parametrize(
    "column,params,expected_type",
    [
        ("array_int", None, "int"),
        ("array_int", {"distinct": True}, "int"),
        ("array_float", None, "float"),
        ("array_float", {"distinct": True}, "float"),
    ],
)
def test_explode_type_inherited(spark, column, params, expected_type):
    """Type of the target column should be inherited by the array type"""
    test_data = spark.createDataFrame(
        data=input_data,
        schema=input_schema,
    )

    params = params or {}

    result = Explode(df=test_data, column=column, target_column="exploded", **params).transform()
    result.show()
    result_type = result.select("exploded").dtypes[0][1]

    assert result_type == expected_type
