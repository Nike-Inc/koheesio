import pytest

from koheesio.spark.writers.dummy import DummyWriter

pytestmark = pytest.mark.spark

expected = {"id": 0}


def test_write(dummy_df):
    test_writer = DummyWriter()
    actual = test_writer.write(dummy_df)
    assert actual.get("head") == expected


def test_write_unhappy():
    """Calling .write() when no DataFrame was passed or set should result in a RuntimeError"""
    test_writer = DummyWriter()
    with pytest.raises(RuntimeError):
        test_writer.write()


def test_execute(dummy_df):
    test_writer = DummyWriter(df=dummy_df)
    actual = test_writer.execute()
    assert actual.get("head") == expected
