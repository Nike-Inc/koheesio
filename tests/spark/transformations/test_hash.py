from textwrap import dedent

import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.hash import Sha2Hash

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name="test_hash")


@pytest.mark.parametrize(
    "input_values,input_data,expected",
    [
        pytest.param(
            # input values,
            dict(target_column="hash", column="id"),
            None,
            # expected data
            [
                dict(id=1, string="hello", hash="6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b"),
                dict(id=2, string="world", hash="d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35"),
                dict(id=3, string="", hash="4e07408562bedb8b60ce05c1decfe3ad16b72230967de01f640b7e4729b49fce"),
            ],
            id="base test",
        ),
        pytest.param(
            # input values,
            dict(target_column="hash", columns=["id", "string"]),
            None,
            # expected data
            [
                dict(id=1, string="hello", hash="79057cc0d6ced65f8dafc2cc0d7f93d628c3605f97e800416a344802dcf5407d"),
                dict(id=2, string="world", hash="939ac689cbc0affd630000575a8edb83583130ca570ab6000124c25f985603cf"),
                dict(id=3, string="", hash="626938c3fd46f4155cc77ec411626ef8188af25ad6829f2ad4c068ebc5d92fbf"),
            ],
            id="multiple column input",
        ),
        pytest.param(
            # input values,
            dict(target_column="hash", column="string"),
            ([["hello"], ["world"], [None]], ["string"]),
            # expected data
            [
                dict(string="hello", hash="2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"),
                dict(string="world", hash="486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7"),
                dict(string=None, hash=None),
            ],
            id="null column input",
        ),
        pytest.param(
            # input values,
            dict(target_column="hash", columns=["id", "string"]),
            ([[1, "hello"], [2, "world"], [3, None]], ["id", "string"]),
            # expected data
            [
                dict(id=1, string="hello", hash="79057cc0d6ced65f8dafc2cc0d7f93d628c3605f97e800416a344802dcf5407d"),
                dict(id=2, string="world", hash="939ac689cbc0affd630000575a8edb83583130ca570ab6000124c25f985603cf"),
                dict(id=3, string=None, hash="4e07408562bedb8b60ce05c1decfe3ad16b72230967de01f640b7e4729b49fce"),
            ],
            id="null column input along with other columns",
        ),
    ],
)
def test_happy_flow(input_values, input_data, expected, sample_df_with_strings, spark):
    print(input_values)
    sha2_hash = Sha2Hash(**input_values)

    if input_data:
        input_df = spark.createDataFrame(*input_data)
    else:
        input_df = sample_df_with_strings

    df = sha2_hash.transform(input_df)
    actual = [k.asDict() for k in df.collect()]
    assert actual == expected
    log.info("HashUUID5 unit test completed")


def test_with_same_data_as_from_spark_docs(spark):
    input_df = spark.createDataFrame([["Alice"], ["Bob"]], ["name"])
    expected = [
        dict(name="Alice", sha2="3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043"),
        dict(name="Bob", sha2="cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961"),
    ]
    df = Sha2Hash(column="name", target_column="sha2", num_bits=256).transform(input_df)
    actual = [k.asDict() for k in df.collect()]
    assert actual == expected


@pytest.mark.parametrize(
    "input_values,error",
    [
        pytest.param(
            # input values,
            dict(target_column="hash", columns="column_does_not_exist"),
            # expected error
            ValueError,
            id="non-existent column",
        ),
    ],
)
def test_unhappy_flow(input_values, error, sample_df_with_strings):
    with pytest.raises(error):
        Sha2Hash(**input_values).transform(sample_df_with_strings)
