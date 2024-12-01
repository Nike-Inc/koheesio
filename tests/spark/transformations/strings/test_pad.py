"""
Test the Pad, LPad, RPad, PadBoth columns transformations
"""

import pytest

from koheesio.logger import LoggingFactory
from koheesio.models import ValidationError
from koheesio.spark.transformations.strings.pad import LPad, Pad, RPad
from koheesio.spark.utils import show_string

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name=__file__, inherit_from_koheesio=True)

data = [[1, 2, "hello"], [3, 4, "world"], [3, 4, None]]
schema = ["id", "id2", "c1"]
standard_input_values = dict(
    columns=["c1"],
    target_column="result",
    character="*",
    length=10,
)


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # description: Test Pad
            # input values
            standard_input_values,
            # expected data
            dict(
                Pad=["*****hello", "*****world", None],
                LPad=["*****hello", "*****world", None],
                RPad=["hello*****", "world*****", None],
            ),
        ),
        (
            # description: Test Pad - Less than original length
            # input values
            {**standard_input_values, "length": 3},
            # expected data
            dict(
                Pad=["hel", "wor", None],
                LPad=["hel", "wor", None],
                RPad=["hel", "wor", None],
            ),
        ),
        (
            # description: Test Pad - Less than original length
            # input values
            {**standard_input_values, "character": "*-"},
            # expected data
            dict(
                Pad=["*-*-*hello", "*-*-*world", None],
                LPad=["*-*-*hello", "*-*-*world", None],
                RPad=["hello*-*-*", "world*-*-*", None],
            ),
        ),
    ],
)
def test_happy_flow(input_values, expected, spark):
    input_df = spark.createDataFrame(data, schema)

    klz = Pad, LPad, RPad

    for kls in klz:
        log.info(f"Testing for {kls.__name__}")
        trim = kls(**input_values)
        output_df = trim.transform(input_df)
        target_column = trim.target_column

        # log equivalent of doing df.show()
        log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")

        actual = [row[target_column] for row in output_df.select(target_column).collect()]
        assert actual == expected[kls.__name__]


def test_left_without_char():
    log.info("Testing unhappy flow for LPad")
    with pytest.raises(ValidationError) as e:
        _ = LPad(**{**standard_input_values, "character": ""})

    exception = e.value
    log.info(f"{exception = }")
    assert "at least 1 character" in str(exception)


def test_left_negative_length():
    log.info("Testing unhappy flow for LPad")
    with pytest.raises(ValidationError) as e:
        _ = LPad(**{**standard_input_values, "length": -5})

    exception = e.value
    log.info(f"{exception = }")
    assert "greater than 0" in str(exception)


# TODO: not supported yet
#
#     def test_get_result_nested(self):
#         df = (
#             Pad(
#                 df=self.sample_nested_df,
#                 params={
#                     "source_column": "payload.col2.array_col.item",
#                     "length": 10,
#                     "character": "0",
#                     "direction": "left",
#                 },
#                 target_alias="result_column",
#                 columns_to_flatten=["payload{}.col2{}.array_col[].item"],
#             )
#             .execute()
#             .df
#         )
#         df.printSchema()
#         self.assertEqual(df.collect()[0].result_column, "00000000v1")
#         self.assertEqual(df.collect()[1].result_column, "00000000v2")
