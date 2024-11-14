"""
Test the UpperCase columns transformation
"""

import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.strings.change_case import (
    InitCap,
    LowerCase,
    TitleCase,
    UpperCase,
)
from koheesio.spark.utils import show_string

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name=__file__, inherit_from_koheesio=True)
data = [
    ["Banana lemon orange", 1000, "USA"],
    ["Carrots blueberries", 1500, None],
    ["Beans", 1600, "USA"],
]
columns = ["product", "amount", "country"]


@pytest.mark.parametrize(
    "input_values,input_data,input_schema,expected",
    [
        (
            # description: Test a normal call to LowerCase, UpperCase, TitleCase, InitCap
            # input values
            dict(
                column="product",
                target_column="result",
            ),
            # input data and schema
            data,
            columns,
            # expected data
            dict(
                LowerCase=["banana lemon orange", "carrots blueberries", "beans"],
                UpperCase=["BANANA LEMON ORANGE", "CARROTS BLUEBERRIES", "BEANS"],
                TitleCase=["Banana Lemon Orange", "Carrots Blueberries", "Beans"],
                InitCap=["Banana Lemon Orange", "Carrots Blueberries", "Beans"],
            ),
        ),
        (
            # description: Test a call with null values to LowerCase, UpperCase, TitleCase, InitCap
            # input values
            dict(
                columns=["country"],
                target_column="result",
            ),
            # input data and schema
            data,
            columns,
            # expected data
            dict(
                LowerCase=["usa", None, "usa"],
                UpperCase=["USA", None, "USA"],
                TitleCase=["Usa", None, "Usa"],
                InitCap=["Usa", None, "Usa"],
            ),
        ),
    ],
)
def test_happy_flow(input_values, input_data, input_schema, expected, spark):
    input_df = spark.createDataFrame(input_data, input_schema)

    klz = LowerCase, UpperCase, TitleCase, InitCap

    for kls in klz:
        log.info(f"Testing {kls.__name__}")
        change_case = kls(**input_values)
        output_df = change_case.transform(input_df)
        target_column = change_case.target_column

        # log equivalent of doing df.show()
        log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")

        actual = [row[target_column] for row in output_df.select(target_column).collect()]
        assert actual == expected[kls.__name__]


# TODO: not supported yet
#
#     def test_transform_nested_df(self):  # LowerCase
#         """
#         Test a nested dataframe call
#         """
#         output_df = (
#             LowerCase(
#                 df=self.sample_nested_df,
#                 params={"source_column": "payload.col2.col3"},
#                 target_alias="case_converted",
#                 columns_to_flatten=["payload{}.col2{}"],
#             )
#             .execute()
#             .df
#         )
#
#         output_list = output_df.select("case_converted").rdd.map(lambda r: r[0]).collect()
#
#         self.assertEqual(output_list, ["value1"])


# TODO: not supported yet
#
#     def test_transform_nested_df(self):  # UpperCase
#         """
#         Test a nested dataframe call
#         """
#         output_df = (
#             UpperCase(
#                 df=self.sample_nested_df,
#                 params={"source_column": "payload.col2.col3"},
#                 target_alias="case_converted",
#                 columns_to_flatten=["payload{}.col2{}"],
#             )
#             .execute()
#             .df
#         )
#
#         output_list = output_df.select("case_converted").rdd.map(lambda r: r[0]).collect()
#
#         self.assertEqual(output_list, ["VALUE1"])


#   TODO: not supported yet
#
#     def test_transform_nested_df(self):  # TitleCase
#         """
#         Test a nested dataframe call
#         """
#         output_df = (
#             TitleCase(
#                 df=self.sample_nested_df,
#                 params={"source_column": "payload.col2.col3"},
#                 target_alias="case_converted",
#                 columns_to_flatten=["payload{}.col2{}"],
#             )
#             .execute()
#             .df
#         )
#
#         output_list = output_df.select("case_converted").rdd.map(lambda r: r[0]).collect()
#
#         self.assertEqual(output_list, ["Value1"])
