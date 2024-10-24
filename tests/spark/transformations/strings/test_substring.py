"""
Test the Substring columns transformation
"""

import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.strings.substring import Substring
from koheesio.spark.utils import show_string

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name=__file__, inherit_from_koheesio=True)

data_with_strings = [[1, 2, "hello"], [3, 4, "world"], [3, 4, None]]
schema_with_strings = ["c1", "c2", "c3"]


@pytest.mark.parametrize(
    "input_values,data,schema,expected",
    [
        (
            # case 1 : column specified -> trim a specific column
            dict(column="c3", start=1, length=3, target_column="result"),
            # input data and schema
            data_with_strings,
            schema_with_strings,
            # expected output
            ["hel", "wor", None],
        ),
        (
            # case 2 : column specified -> trim multiple columns (no strings)
            # all numeric columns will be converted to strings, c3 is ignored
            dict(columns=["c1", "c2"], start=1, length=1),
            # input data and schema
            data_with_strings,
            schema_with_strings,
            # expected output
            [
                {"c1": "1", "c2": "2", "c3": "hello"},
                {"c1": "3", "c2": "4", "c3": "world"},
                {"c1": "3", "c2": "4", "c3": None},
            ],
        ),
        (
            # case 3 : example from class docstring
            dict(column="c1", start=3, length=4, target_column="result"),
            # input data and schema
            [["skyscraper"]],
            ["c1"],
            # expected output
            ["yscr"],
        ),
        (
            # case 4 : no length specified
            dict(column="c3", start=3, target_column="result"),
            # input data and schema
            data_with_strings,
            schema_with_strings,
            # expected output
            ["llo", "rld", None],
        ),
    ],
)
def test_substring(input_values, data, schema, expected, spark):
    input_df = spark.createDataFrame(data, schema)

    substring = Substring(**input_values)
    output_df = substring.transform(input_df)

    # log equivalent of doing df.show()
    log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")

    if target_column := substring.target_column:
        actual = [row.asDict()[target_column] for row in output_df.collect()]
    else:
        actual = [row.asDict() for row in output_df.collect()]

    assert actual == expected
