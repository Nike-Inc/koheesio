"""
Test the Replace columns transformation
"""

import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.strings.replace import Replace
from koheesio.spark.utils import show_string

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name=__file__, inherit_from_koheesio=True)

data_with_strings = [[1, 2, "hello"], [3, 4, "world"], [3, 4, None]]
schema_with_strings = ["c1", "c2", "c3"]


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # description: Test Replace
            # input values
            dict(column="c2", original_value=2, new_value=100),
            # expected data
            ["100", "4", "4"],
        ),
        (
            # description: Test Replace - testing with None value
            # input values
            dict(column="c3", new_value="NA"),
            # expected data
            ["hello", "world", "NA"],
        ),
        (
            # description: Test Replace - string column value
            # input values
            dict(column="c3", original_value="world", new_value="programmer"),
            # expected data
            ["hello", "programmer", None],
        ),
    ],
)
def test_happy_flow(input_values, expected, spark):
    input_df = spark.createDataFrame(data_with_strings, schema_with_strings)

    replace = Replace(**input_values)
    target_column = replace.columns[0]
    output_df = replace.transform(input_df)

    # log equivalent of doing df.show()
    log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")

    actual = [row.asDict()[target_column] for row in output_df.collect()]
    assert actual == expected


# TODO: not supported yet
#
#     def test_nested(self):
#         df = self.spark.createDataFrame([{"a": {"b": "hello"}}, {"a": {"b": "world"}}], self.schema)
#         df = (
#             Replace(
#                 df=df,
#                 params={
#                     "source_column": "a.b",
#                     "original_value": "world",
#                     "new_value": "programmer",
#                 },
#                 target_alias="a.c",
#                 columns_to_flatten=["a{}.b"],
#             )
#             .execute()
#             .df
#         )
#         result = df.rdd.map(lambda r: r.a.c).collect()
#         self.assertEqual(sorted(result), ["hello", "programmer"])
#
#     def test_nested_1(self):
#         df = self.spark.createDataFrame([{"a": {"b": "hello"}}, {"a": {"b": "world"}}], self.schema)
#         df = (
#             Replace(
#                 df=df,
#                 params={
#                     "source_column": "a.b",
#                     "original_value": "world",
#                     "new_value": "programmer",
#                 },
#                 target_alias="a.c",
#                 columns_to_flatten=["a{}.b"],
#             )
#             .execute()
#             .df
#         )
#         result = df.rdd.map(lambda r: r.a.c).collect()
#         self.assertEqual(sorted(result), ["hello", "programmer"])
#
