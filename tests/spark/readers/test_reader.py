import pytest

from koheesio.steps.readers.dummy import DummyReader

pytestmark = pytest.mark.spark


def test_reader(spark):
    test_reader = DummyReader(range=1)
    actual = test_reader.read().head().asDict()
    expected = {"id": 0}
    assert actual == expected


def test_execute(spark):
    test_reader = DummyReader(range=1)
    actual = test_reader.execute().df.head().asDict()
    expected = {"id": 0}
    assert actual == expected
