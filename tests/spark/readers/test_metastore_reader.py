from pyspark.sql.dataframe import DataFrame

from koheesio.steps.readers.metastore import MetastoreReader


def test_metastore_reader(spark):
    df = MetastoreReader(table="klettern.delta_test_table").read()
    actual = df.head().asDict()
    expected = {"id": 0}

    assert isinstance(df, DataFrame)
    assert actual == expected
