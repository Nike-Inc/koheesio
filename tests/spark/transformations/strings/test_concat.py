import datetime

import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.strings.concat import Concat
from koheesio.spark.utils import show_string

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name=__file__, inherit_from_koheesio=True)


string_data = [[1, "text1", "text2", "text3"], [2, "text4", "text5", "text6"]]
string_schema = ["id", "_1", "_2", "_3"]


@pytest.mark.parametrize(
    "input_values,input_data,input_schema,expected",
    [
        (
            # description: Test Concat without Spacer
            # input values
            dict(
                columns=["_1", "_2", "_3"],
                target_column="result",
            ),
            # input data and schema
            string_data,
            string_schema,
            # expected data
            ["text1text2text3", "text4text5text6"],
        ),
        (
            # description: Test Concat with Spacer
            # input values
            dict(
                columns=["_1", "_2", "_3"],
                spacer="|",
                target_column="result",
            ),
            # input data and schema
            string_data,
            string_schema,
            # expected data
            ["text1|text2|text3", "text4|text5|text6"],
        ),
        (
            # description: Test Concat with integer type
            # input values
            dict(
                columns=["_1", "id"],
                target_column="result",
            ),
            # input data and schema
            string_data,
            string_schema,
            # expected data
            ["text11", "text42"],
        ),
        (
            # description: Test that Concat retains column order
            # input values
            dict(
                columns=["_3", "_1"],
                target_column="result",
            ),
            # input data and schema
            string_data,
            string_schema,
            # expected data
            ["text3text1", "text6text4"],
        ),
        (
            # description: Test Concat with a float type
            # input values
            dict(
                columns=["float_col", "_1"],
                target_column="result",
            ),
            # input data and schema
            [[1.0, "text1"], [2.0, "text2"]],
            ["float_col", "_1"],
            # expected data
            ["1.0text1", "2.0text2"],
        ),
        (
            # description: Test Concat with a null field
            # input values
            dict(
                columns=["none_col", "_1"],
                target_column="result",
            ),
            # input data and schema
            [["text1", "text2"], [None, "text2"]],
            ["none_col", "_1"],
            # expected data
            ["text1text2", None],
        ),
        (
            # description: Test Concat with a datetime field
            # input values
            dict(
                columns=["datetime_col", "_1"],
                target_column="result",
            ),
            # input data and schema
            [[datetime.datetime(1997, 2, 28), "text"]],
            ["datetime_col", "_1"],
            # expected data
            ["1997-02-28 00:00:00text"],
        ),
        (
            # description: Test Concat with a array field, including a null value
            # input values
            dict(
                columns=["array_col_1", "array_col_2"],
                target_column="result",
            ),
            # input data and schema
            [[["text1", "text2"], ["text3", None]]],
            ["array_col_1", "array_col_2"],
            # expected data
            [["text1", "text2", "text3", None]],
        ),
        (
            # description: Test Concat with a array field and spacer
            # input values
            dict(columns=["array_col_1", "array_col_2"], target_column="result", spacer="--"),
            # input data and schema
            [[["text1", "text2"], ["text3", "text4"]]],
            ["array_col_1", "array_col_2"],
            # expected data
            ["text1--text2--text3--text4"],
        ),
        (
            # description: Test Concat with a array field, with null value and spacer
            # This is the same as the example in the class docstring
            # input values
            dict(columns=["array_col_1", "array_col_2"], target_column="result", spacer="--"),
            # input data and schema
            [[["text1", "text2"], ["text3", None]]],
            ["array_col_1", "array_col_2"],
            # expected data
            ["text1--text2--text3"],
        ),
        (
            # description: Test Concat with a numeric field
            # input values
            dict(
                columns=["numeric_col", "_1"],
                target_column="result",
            ),
            # input data and schema
            [[1, "text1"], [2, "text2"]],
            ["numeric_col", "_1"],
            # expected data
            ["1text1", "2text2"],
        ),
    ],
)
def test_happy_flow(input_values, input_data, input_schema, expected, spark):
    input_df = spark.createDataFrame(input_data, input_schema)

    concat = Concat(**input_values)
    target_column = concat.target_column
    output_df = concat.transform(input_df)

    # log equivalent of doing df.show()
    log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")

    actual = [row[target_column] for row in output_df.select(target_column).collect()]
    assert actual == expected


# TODO: not supported atm
#
#     def test_concatenate_nested_base_1(self):
#         df = (
#             Concatenate(
#                 df=self.sample_nested_df,
#                 params={"source_columns": ["payload.col2.col3", "payload.col2.col4"]},
#                 target_alias="payload.col2.col5",
#                 columns_to_flatten=["payload{}.col2{}.col3", "payload{}.col2{}.col4"],
#             )
#             .execute()
#             .df
#         )
#         result = df.rdd.map(lambda r: r.payload).collect()
#         self.assertEqual(result[0]["col2"]["col5"], "value1value2")
