import pytest

from pyspark.sql import DataFrame

from koheesio.models import ValidationError
from koheesio.spark.transformations.repartition import Repartition

pytestmark = pytest.mark.spark


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # case 1 : both column and num_partitions
            dict(column="country", num_partitions=2),
            # expected num_partitions
            2,
        ),
        (
            # case 2 : just column and no num_partitions
            dict(column="country"),
            # expected num_partitions
            1,
        ),
        (
            # case 3 : just num_partitions
            dict(num_partitions=3),
            # expected num_partitions
            3,
        ),
        (
            # case 4 : way too many num_partitions
            dict(num_partitions=5000),
            # expected num_partitions
            5000,
        ),
    ],
)
def test_repartition(input_values, expected, spark):
    input_df = spark.createDataFrame(
        [
            ["Banana", 1000, "USA"],
            ["Carrots", 1500, "USA"],
            ["Beans", 1600, "USA"],
            ["Orange", 1234, "USA"],
            ["Orange", 5555, "USA"],
            ["Banana", 400, "China"],
            ["Carrots", 1200, "China"],
            ["Beans", 1500, "China"],
            ["Orange", 4000, "China"],
            ["Banana", 2000, "Canada"],
            ["Carrots", 2000, "Canada"],
            ["Beans", 2000, "Mexico"],
        ],
        schema="product string, amount int, country string",
    )
    df = Repartition(**input_values).transform(input_df)
    if isinstance(input_df, DataFrame):
        assert df.rdd.getNumPartitions() == expected


def test_repartition_should_raise_error():
    with pytest.raises(ValidationError):
        Repartition()  # no input
