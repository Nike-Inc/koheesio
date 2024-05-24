import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.uuid5 import HashUUID5

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name="test_uuid5")


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # description: base test
            # input values,
            dict(target_column="uuid5_hash", source_columns=["id", "string"]),
            # expected data
            [
                dict(id=1, string="hello", uuid5_hash="66bee5a3-651f-57fd-b070-b873c39ffb56"),
                dict(id=2, string="world", uuid5_hash="4a90c3a5-7173-5764-8905-94455c661e6c"),
                dict(id=3, string="", uuid5_hash="3ee49156-19d1-5335-b31f-8e11e9a17ed1"),
            ],
        ),
        (
            # description: test with custom namespace
            # input values,
            dict(target_column="uuid5_hash", source_columns=["id", "string"], namespace="unittest"),
            # expected data
            [
                dict(id=1, string="hello", uuid5_hash="fd12783f-ac26-5624-bbdd-dc0ae289d099"),
                dict(id=2, string="world", uuid5_hash="0e6862a7-6327-5277-9666-9adbb5dbd918"),
                dict(id=3, string="", uuid5_hash="741e42f0-baf6-547e-9f6d-cbf42dfb41e2"),
            ],
        ),
        (
            # description: test with extra string and namespace
            # input values,
            dict(target_column="id", source_columns=["id", "string"], namespace="unittest", extra_string="llama_drama"),
            # expected data
            [
                dict(id="f3e99bbd-85ae-5dc3-bf6e-cd0022a0ebe6", string="hello"),
                dict(id="b48e880f-c289-5c94-b51f-b9d21f9616c0", string="world"),
                dict(id="2193a99d-222e-5a0c-a7d6-48fbe78d2708", string=""),
            ],
        ),
        (
            # description: test with custom delimiter
            # input values,
            dict(target_column="string", source_columns=["id", "string"], delimiter=","),
            # expected data
            [
                dict(id=1, string="f0e5b058-78e0-5e33-b8a6-e434a0e9b8b0"),
                dict(id=2, string="5ad87a13-799c-5699-870e-10ce1196cc1b"),
                dict(id=3, string="81d87123-40fe-5612-b2c3-1c2e294ee636"),
            ],
        ),
    ],
)
def test_hash_uuid5(input_values, expected, sample_df_with_strings):
    df = HashUUID5(**input_values).transform(sample_df_with_strings)
    actual = [k.asDict() for k in df.collect()]
    assert actual == expected
    log.info("HashUUID5 unit test completed")
