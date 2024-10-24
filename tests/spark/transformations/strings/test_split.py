"""
Test the Split transformations
"""

import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.strings.split import SplitAll, SplitAtFirstMatch
from koheesio.spark.utils import show_string

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name=__file__, inherit_from_koheesio=True)

string_data = [
    ["Banana lemon orange", 1000, "USA"],
    ["Carrots Blueberries", 1500, "USA"],
    ["Beans", 1600, "USA"],
]
empty_data = [
    ["   ", 1000, "USA"],
    ["", 1500, "USA"],
]
columns = ["product", "amount", "country"]
default_params = dict(
    split_pattern="\\s",
    column="product",
    target_column="split",
)


@pytest.mark.parametrize(
    "input_values,data,schema,expected",
    [
        (
            # description: Test SplitAll
            # input values
            default_params,
            # input data and schema
            string_data,
            columns,
            # expected data
            [
                ["Banana", "lemon", "orange"],
                ["Carrots", "Blueberries"],
                ["Beans"],
            ],
        ),
        (
            # description: Test SplitAll - Empty string split
            # input values
            default_params,
            # input data and schema
            empty_data,
            columns,
            # expected data
            # 3 spaces in the df_empty means 3 patterns to split. Resulting in 4 elements.
            [
                ["", "", "", ""],
                [""],
            ],
        ),
        (
            # description: Test SplitAll - Transform multi character string split
            # input values
            {**default_params, "split_pattern": "an"},
            # input data and schema
            string_data,
            columns,
            # expected data
            [
                ["B", "", "a lemon or", "ge"],
                ["Carrots Blueberries"],
                ["Be", "s"],
            ],
        ),
    ],
)
def test_split_all(input_values, data, schema, expected, spark):
    input_df = spark.createDataFrame(data, schema)

    split_all = SplitAll(**input_values)
    filter_column = split_all.target_column
    output_df = split_all.transform(df=input_df)

    # log equivalent of doing df.show()
    log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")

    actual = [row.asDict()[filter_column] for row in output_df.collect()]
    assert actual == expected


# TODO: not supported yet
#
#     def test_transform_nested_df(self):
#         output_df = (
#             SplitColumn(
#                 df=self.sample_nested_df,
#                 params={
#                     "split_pattern": "l",
#                     "source_column": "payload.col2.col3",
#                 },
#                 target_alias="split",
#                 columns_to_flatten=["payload{}.col2{}"],
#             )
#             .execute()
#             .df
#         )
#
#         output_list_split = output_df.select("split").rdd.map(lambda r: r.split).collect()
#
#         self.assertIn(["va", "ue1"], output_list_split)


@pytest.mark.parametrize(
    "input_values,data,schema,expected",
    [
        (
            # description: Test SplitAtFirstMatch - Check the flow when retrieve_first_part is set to true
            # input values
            {**default_params, "retrieve_first_part": True},
            # input data and schema
            string_data,
            columns,
            # expected data
            ["Banana", "Carrots", "Beans"],
        ),
        (
            # description: Test SplitAtFirstMatch - Check the flow when retrieve_first_part is set to false
            # input values
            {**default_params, "retrieve_first_part": False},
            # input data and schema
            string_data,
            columns,
            # expected data
            ["lemon", "Blueberries", ""],
        ),
        (
            # description: Test SplitAtFirstMatch - Check empty column
            # input values
            default_params,
            # input data and schema
            empty_data,
            columns,
            # expected data
            ["", ""],
        ),
        (
            # description: Test SplitAtFirstMatch - Check multi character split
            # This is the same as what is mentioned in the docstring for the class
            # input values
            {**default_params, "split_pattern": "an"},
            # input data and schema
            string_data,
            columns,
            # expected data
            ["B", "Carrots Blueberries", "Be"],
        ),
    ],
)
def test_split_at_first_match(input_values, data, schema, expected, spark):
    input_df = spark.createDataFrame(data, schema)

    split_at_first_match = SplitAtFirstMatch(**input_values)
    filter_column = split_at_first_match.target_column
    output_df = split_at_first_match.transform(df=input_df)

    # log equivalent of doing df.show()
    log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")

    actual = [row.asDict()[filter_column] for row in output_df.collect()]
    assert actual == expected


# TODO: not supported yet
#
#     def test_transform_nested_df_single_character_split(self):
#         """
#         Check execute method for a nested dataframe
#         """
#         output_df = (
#             SplitColumnAtFirstMatch(
#                 df=self.sample_nested_df,
#                 params={
#                     "split_pattern": "l",
#                     "source_column": "payload.col2.col3",
#                 },
#                 target_alias="payload.col2.col3_split",
#                 columns_to_flatten=["payload{}.col2{}"],
#             )
#             .execute()
#             .df
#         )
#
#         json_output = output_df.toJSON().map(lambda j: json.loads(j)).collect()
#
#         actual_col3_split1 = json_output[0].get("payload").get("col2").get("col3_split")
#
#         self.assertEqual(actual_col3_split1, "va")
#
#         output_df = (
#             SplitColumnAtFirstMatch(
#                 df=self.sample_nested_df,
#                 params={
#                     "split_pattern": "l",
#                     "source_column": "payload.col2.col3",
#                     "retrieve_first_part": False,
#                 },
#                 target_alias="payload.col2.col3_split",
#                 columns_to_flatten=["payload{}.col2{}"],
#             )
#             .execute()
#             .df
#         )
#         json_output = output_df.toJSON().map(lambda j: json.loads(j)).collect()
#
#         actual_col3_split2 = json_output[0].get("payload").get("col2").get("col3_split")
#
#         self.assertEqual(actual_col3_split2, "ue1")
