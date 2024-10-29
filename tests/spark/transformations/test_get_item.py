import pytest

from koheesio.spark.transformations.get_item import GetItem

pytestmark = pytest.mark.spark


@pytest.mark.parametrize(
    "input_values,input_data,input_schema,expected",
    [
        (
            # Test GetItem with List
            # description: Test transform method when an integer/ordinal is passed
            #     Deliberately picked a number that is bigger than some of the lengths.
            #     Should return null and not fail.
            # input values
            dict(
                column="content",
                key=1,
                target_column="item",
            ),
            # input data
            [[1, [1, 2, 3]], [2, [4, 5]], [3, [6]], [4, []]],
            # input schema
            ["id", "content"],
            # expected data
            [2, 5, None, None],
        ),
        (
            # Test GetItem with Dict
            # description: Test transform method when a key/string is passed
            #     2 rows have key1 and 2 rows have keys as keys. Meaning
            #     that choosing one of the 2 will result the other 2 being null.
            # input values
            dict(
                column="content",
                key="key2",
                target_column="item",
            ),
            # input data
            [
                [1, {"key1": "value1"}],
                [2, {"key1": "value2"}],
                [3, {"key2": "hello"}],
                [4, {"key2": "world"}],
            ],
            # input schema
            ["id", "content"],
            # expected data
            [None, None, "hello", "world"],
        ),
    ],
)
def test_transform_get_item(input_values, input_data, input_schema, expected, spark):
    input_df = spark.createDataFrame(data=input_data, schema=input_schema)
    gi = GetItem(**input_values)
    output_df = gi.transform(input_df)
    target_column = gi.target_column
    actual = [
        row.asDict()[target_column] for row in output_df.orderBy(input_schema[0]).select(gi.target_column).collect()
    ]
    assert actual == expected


# def test_transform_nested_df(self):
#     """
#     TODO: implement... this functionality is not supported yet
#     Check whether a nested dataframe can be accessed.
#     """
#     output_df = (
#         GetItem(
#             df=self.sample_nested_df,
#             params={
#                 "source_column": "payload.col2.array_col",
#                 "key_or_ordinal": 0,
#             },
#             target_alias="item",
#             columns_to_flatten=["payload{}.col2{}.array_col{}"],
#         )
#         .execute()
#         .df
#     )
#
#         output_list = output_df.select("item").rdd.map(lambda r: r[0]).collect()
#
#         self.assertEqual([Row(item="v1")], output_list)
