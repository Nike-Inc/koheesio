import pytest

from pyspark.sql import functions as F

from koheesio.spark.readers.dummy import DummyReader
from koheesio.spark.transformations.camel_to_snake import CamelToSnakeTransformation

pytestmark = pytest.mark.spark


class TestCamelToSnakeTransformation:
    @pytest.fixture
    def input_df(self):
        df = (
            DummyReader(range=1)
            .read()
            .withColumn("camelCaseColumn", F.col("id"))
            .withColumn("snake_case_column", F.col("id"))
        )
        yield df

    def test_transformation(self, input_df):
        transformation = CamelToSnakeTransformation()

        df = transformation.transform(input_df)
        actual = df.head().asDict()

        expected = {"id": 0, "camel_case_column": 0, "snake_case_column": 0}
        assert actual == expected

    def test_transform_on_specific_column_only(self, input_df):
        input_df = input_df.withColumn("anotherCamelCaseColumn", F.col("id"))
        transformation = CamelToSnakeTransformation(column="anotherCamelCaseColumn")

        df = transformation.transform(input_df)
        actual = df.head().asDict()

        expected = {"id": 0, "camelCaseColumn": 0, "snake_case_column": 0, "another_camel_case_column": 0}
        assert actual == expected
