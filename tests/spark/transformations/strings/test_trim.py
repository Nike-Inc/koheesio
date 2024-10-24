"""
Test the Trim columns transformation
"""

import pytest

from pydantic import ValidationError

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.strings.trim import LTrim, RTrim, Trim
from koheesio.spark.utils import show_string

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name=__file__, inherit_from_koheesio=True)


@pytest.mark.parametrize(
    "input_values,input_data,input_schema,expected",
    [
        (
            # description: Test Trim
            # input values
            dict(
                columns=["_1"],
                target_column="result",
            ),
            # input data
            [[" ltext"], ["rtext "], [" lrtext "]],
            # input schema
            ["_1"],
            # expected data
            dict(
                Trim=["ltext", "rtext", "lrtext"],
                LTrim=["ltext", "rtext ", "lrtext "],
                RTrim=[" ltext", "rtext", " lrtext"],
            ),
        )
    ],
)
def test_happy_flow(input_values, input_data, input_schema, expected, spark):
    input_df = spark.createDataFrame(input_data, input_schema)

    klz = Trim, LTrim, RTrim

    for kls in klz:
        log.info(f"testing for {kls.__name__}")
        trim = kls(**input_values)
        output_df = trim.transform(input_df)
        target_column = trim.target_column

        # log equivalent of doing df.show()
        log.info(f"show output_df: \n{show_string(output_df, 20, 20, False)}")
        actual = [row[target_column] for row in output_df.select(target_column).collect()]
        assert actual == expected[kls.__name__]


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # case 1 : no column specified -> trim all string columns
            dict(),
            # expected output
            [
                dict(id=1, input_column="foo", other_column="buz"),
                dict(id=2, input_column="bar", other_column=None),
            ],
        ),
        (
            # case 2 : string and integer column specified -> only one string column affected
            dict(column=["input_column", "id"]),
            # expected output
            [
                dict(id=1, input_column="foo", other_column="  buz  "),
                dict(id=2, input_column="bar", other_column=None),
            ],
        ),
    ],
)
def test_base(input_values, expected, spark):
    input_df = spark.createDataFrame(
        [[1, "foo  ", "  buz  "], [2, "  bar  ", None]], ["id", "input_column", "other_column"]
    )

    df = Trim(**input_values).transform(input_df)

    actual = [k.asDict() for k in df.collect()]
    assert actual == expected


def test_invalid_direction():
    with pytest.raises(ValidationError):
        Trim(direction="invalid")


# TODO: not supported yet
#
#     def test_trim_nested(self):
#         df = self.spark.createDataFrame([{"a": {"b": "    hello world   "}}], self.schema)
#         df = (
#             Trim(
#                 df=df,
#                 params={"source_column": "a.b"},
#                 target_alias="a.c",
#                 columns_to_flatten=["a{}.b"],
#             )
#             .execute()
#             .df
#         )
#         result = df.rdd.map(lambda r: r.a).collect()
#         self.assertEqual(result[0]["c"], "hello world")
#
#     def test_field_does_not_exist(self):
#         with self.assertRaises(AnalysisException) as e:
#             Trim(
#                 df=self.sample_df,
#                 params={"source_column": "some_column"},
#                 target_alias="a",
#             ).execute()
#
#         self.assertIn(
#             "cannot resolve 'some_column' given input columns:",
#             e.exception.desc.replace("'`", "'").replace("`'", "'"),
#         )
