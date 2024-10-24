import pytest

from koheesio.spark import DataFrame
from koheesio.spark.readers.metastore import MetastoreReader

pytestmark = pytest.mark.spark


def test_metastore_reader(spark):
    df = MetastoreReader(table="klettern.delta_test_table").read()
    actual = df.head().asDict()
    expected = {"id": 0}

    assert isinstance(df, DataFrame)
    assert actual == expected
