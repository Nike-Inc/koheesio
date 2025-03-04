import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.replace import Replace
from koheesio.spark.utils import show_string

pytestmark = pytest.mark.spark


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # case 0 : no target_column -> should replace the original column
            dict(column="input_column", from_value="hello", to_value="programmer"),
            # expected output
            [
                dict(id=1, input_column="programmer"),
                dict(id=2, input_column="world"),
                dict(id=3, input_column=None),
            ],
        ),
        (
            # case 1 : with target_column -> should add a new column
            dict(column="input_column", from_value="hello", to_value="programmer", target_column="output"),
            # expected output
            [
                dict(id=1, input_column="hello", output="programmer"),
                dict(id=2, input_column="world", output="world"),
                dict(id=3, input_column=None, output=None),
            ],
        ),
        (
            # case 2 : using aliases for input
            {"column": "input_column", "from": "hello", "to": "programmer", "target_column": "output"},
            # expected output
            [
                dict(id=1, input_column="hello", output="programmer"),
                dict(id=2, input_column="world", output="world"),
                dict(id=3, input_column=None, output=None),
            ],
        ),
        (
            # case 3 : no from_value should replace the Null / None
            dict(column="input_column", to_value="NotNone", target_column="output"),
            # expected output
            [
                dict(id=1, input_column="hello", output="hello"),
                dict(id=2, input_column="world", output="world"),
                dict(id=3, input_column=None, output="NotNone"),
            ],
        ),
        (
            # case 4 : run against non-string column
            dict(column="id", from_value="1", to_value="Numeric replace"),
            # expected output
            [
                dict(id="Numeric replace", input_column="hello"),
                dict(id="2", input_column="world"),
                dict(id="3", input_column=None),
            ],
        ),
    ],
)
def test_base(input_values, expected, spark):
    input_df = spark.createDataFrame([[1, "hello"], [2, "world"], [3, None]], ["id", "input_column"])
    df = Replace(**input_values).transform(input_df)

    actual = [k.asDict() for k in df.collect()]
    assert actual == expected


@pytest.mark.parametrize(
    "input_values",
    [
        dict(column="byte", from_value="1"),
        dict(column="short", from_value="1"),
        dict(column="integer", from_value="1"),
        dict(column="long", from_value="1"),
        dict(column="float", from_value="1.0"),
        dict(column="double", from_value="1.0"),
        dict(column="decimal", from_value="1.0"),
        dict(column="binary", from_value=b"a", happy_flow=False),
        dict(column="boolean", from_value="1", happy_flow=False),
        dict(column="timestamp", from_value="2023-01-01 00:01:01"),
        dict(column="date", from_value="2023-01-01"),
        dict(column="array", from_value="1", happy_flow=False),
        dict(column="map", from_value="1", happy_flow=False),
        dict(column="void", from_value="1", happy_flow=False),
        dict(column="string", from_value="a"),
    ],
)
def test_all_data_types(input_values, df_with_all_types):
    log = LoggingFactory.get_logger("test_all_data_types")
    happy_flow = input_values.pop("happy_flow", True)
    column = input_values["column"]

    if happy_flow:
        input_values["to_value"] = input_values.get("to_value", "happy")
        expected = input_values["to_value"]
        df = Replace(**input_values).transform(df_with_all_types)
        log.info(f"show df: \n{show_string(df, 20, 20, False)}")
        actual = df.head().asDict()[column]
        assert actual == expected
    else:
        input_values["to_value"] = "unhappy"
        expected = df_with_all_types.head().asDict()[column]  # stay the same
        df = Replace(**input_values).transform(df_with_all_types)
        log.info(f"show df: \n{show_string(df, 20, 20, False)}")
        actual = df.head().asDict()[column]
        assert actual == expected
