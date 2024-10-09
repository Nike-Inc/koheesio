"""
Test the RegexpExtract and RegexpReplace columns transformations
"""

import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.strings.regexp import RegexpExtract, RegexpReplace
from koheesio.spark.utils import show_string

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name=__file__, inherit_from_koheesio=True)

data_year_wk = [["2020 W1"], ["2021 WK2"], ["2022WK3"]]
schema_year_wk = ["year_week"]
regexp_year_wk = "([0-9]{4}) ?WK?([0-9]+)"

data_with_strings = [[1, 2, "hello"], [3, 4, "world"], [3, 4, None]]
schema_with_strings = ["c1", "c2", "c3"]
regexp_with_strings = "([a-zA-Z]+)([0-9]+)"


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # description: Test RegexpExtract - index 0: should be the whole match
            # input values
            dict(column=["year_week"], target_column="result", regexp=regexp_year_wk),
            # expected data
            [
                dict(year_week="2020 W1", result="2020 W1"),
                dict(year_week="2021 WK2", result="2021 WK2"),
                dict(year_week="2022WK3", result="2022WK3"),
            ],
        ),
        (
            # description: Test RegexpExtract - index 1: should be the year in this example
            # input values
            dict(column=["year_week"], target_column="year", regexp=regexp_year_wk, index=1),
            # expected data
            [
                dict(year_week="2020 W1", year="2020"),
                dict(year_week="2021 WK2", year="2021"),
                dict(year_week="2022WK3", year="2022"),
            ],
        ),
        (
            # description: Test RegexpExtract - index 2: should be the week in this example
            # input values
            dict(column=["year_week"], target_column="week", regexp=regexp_year_wk, index=2),
            # expected data
            [
                dict(year_week="2020 W1", week="1"),
                dict(year_week="2021 WK2", week="2"),
                dict(year_week="2022WK3", week="3"),
            ],
        ),
    ],
)
def test_regexp_extract(input_values, expected, spark):
    input_df = spark.createDataFrame(data_year_wk, schema_year_wk)

    output_df = RegexpExtract(**input_values).transform(input_df)

    # log equivalent of doing df.show()
    log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")
    actual = [row.asDict() for row in output_df.collect()]
    assert actual == expected


# TODO: not supported yet
#
#     def test_transform_nested_df(self) -> None:
#         """
#         Test with a nested dataframe
#         """
#         output_df = (
#             RegexpExtract(
#                 df=self.sample_nested_df,
#                 params={
#                     "source_column": "payload.col2.col3",
#                     "reg_exp": "([a-zA-Z]+)([0-9]+)",
#                     "index": 1,
#                 },
#                 target_alias="extracted",
#                 columns_to_flatten=["payload{}.col2{}"],
#             )
#             .execute()
#             .df
#         )
#
#         output_list = [row.extracted for row in output_df.collect()]
#         expected_output = ["value"]
#         self.assertEqual(output_list.sort(), expected_output.sort())


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # description: Test RegexpReplace - super simple regexp
            # input values
            dict(column=["c3"], target_column="replaced", regexp="l", replacement="-L-"),
            # expected data
            ["he-L--L-o", "wor-L-d", None],
        ),
        (
            # description: Test RegexpExtract - a bit more complex regexp
            # input values
            dict(column=["c3"], target_column="replaced", regexp="[ho]", replacement="*"),
            # expected data
            ["*ell*", "w*rld", None],
        ),
    ],
)
def test_regexp_replace(input_values, expected, spark):
    input_df = spark.createDataFrame(data_with_strings, schema_with_strings)

    regexp_replace = RegexpReplace(**input_values)
    target_column = regexp_replace.target_column
    output_df = regexp_replace.transform(input_df)

    # log equivalent of doing df.show()
    log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")
    actual = [row.asDict()[target_column] for row in output_df.collect()]
    assert actual == expected


# TODO: not supported yet
#
#     def test_transform_nested_df(self) -> None:
#         """
#         Test with a nested dataframe
#         Replace a number with #NUMBER#
#         """
#         output_df = (
#             RegexpReplace(
#                 df=self.sample_nested_df,
#                 params={
#                     "source_column": "payload.col2.col3",
#                     "reg_exp": "[0-9]+",
#                     "replacement": "#NUMBER#",
#                 },
#                 target_alias="replaced",
#                 columns_to_flatten=["payload{}.col2{}"],
#             )
#             .execute()
#             .df
#         )
#
#         output_list = [row.replaced for row in output_df.collect()]
#         expected_output = ["value#NUMBER#"]
#         self.assertEqual(
#             output_list,
#             expected_output,
#         )
