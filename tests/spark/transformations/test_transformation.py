import pytest

from pyspark.sql import Column
from pyspark.sql import functions as f

from koheesio.spark.transformations import (
    ColumnsTransformation,
    ColumnsTransformationWithTarget,
)
from koheesio.spark.transformations.dummy import DummyTransformation
from koheesio.spark.utils import SparkDatatype

pytestmark = pytest.mark.spark


def test_transform(dummy_df):
    tf = DummyTransformation()
    df = tf.transform(dummy_df)
    actual = df.head().asDict()
    expected = {"id": 0, "hello": "world"}
    assert actual == expected
    assert df == tf.output.df


def test_transform_unhappy():
    """Calling transform without having set or passed a DataFrame should raise a RuntimeError"""
    tf = DummyTransformation()
    with pytest.raises(RuntimeError):
        tf.transform()


def test_execute(dummy_df):
    tf = DummyTransformation(df=dummy_df)
    output = tf.execute()
    df = output.df
    actual = df.head().asDict()
    expected = {"id": 0, "hello": "world"}
    assert actual == expected
    assert df == tf.output.df


class TestColumnsTransformation:
    data = [["a", 1], ["b", 2]]
    columns = ["str_column", "long_column"]

    @pytest.fixture(scope="class")
    def input_df(self, spark):
        df = spark.createDataFrame(self.data, self.columns)
        yield df

    class AddOne(ColumnsTransformation):
        def execute(self):
            for column in self.get_columns():
                self.output.df = self.df.withColumn(column, f.col(column) + 1)

    @pytest.fixture(scope="class")
    def add_one(self, input_df):
        yield self.AddOne(columns=self.columns, df=input_df)

    def test_columns_transformation(self, add_one):
        expected = {"str_column": "a", "long_column": 2}
        output_df = add_one.execute().df
        actual = output_df.head().asDict()
        assert actual == expected

    def test_column_type_of_col(self, add_one):
        assert add_one.column_type_of_col(f.col("str_column")) == "string"
        assert add_one.column_type_of_col(f.col("long_column")) == "long"

    class AddOneWithTarget(ColumnsTransformationWithTarget):
        _limit_data_type = "long"

        def func(self, column: Column):
            return column + 1

    @pytest.fixture()
    def add_one_with_target(self, input_df):
        yield self.AddOneWithTarget(columns=["long_column"], df=input_df, target_column="target_column")

    def test_columns_transformation_with_target(self, add_one_with_target):
        expected = {"long_column": 1, "str_column": "a", "target_column": 2}
        output_df = add_one_with_target.execute().df
        actual = output_df.head().asDict()
        assert actual == expected


class TestColumnsTransformationDataTypeLimitations:
    data = [["a", 1], ["b", 2]]
    columns = ["str_column", "long_column"]

    @pytest.fixture(scope="class")
    def str_long_df(self, spark):
        df = spark.createDataFrame(self.data, self.columns)
        yield df

    class AddTwoStrict(ColumnsTransformationWithTarget):
        class ColumnConfig:
            limit_data_type = [SparkDatatype.LONG]
            data_type_strict_mode = True
            run_for_all_data_type = [SparkDatatype.LONG]

        def func(self, column: Column):
            return column + 2

    def test_strict_config_with_columns_happy(self, str_long_df):
        # explicitly pass a value for columns
        add_two = self.AddTwoStrict(
            columns=["long_column"],
            df=str_long_df,
            target_column="target_column",
        )

        # we should get None on run_for_all_is_set since we have specified columns
        assert add_two.run_for_all_is_set is True
        assert add_two.ColumnConfig.run_for_all_data_type == [SparkDatatype.LONG]

        assert add_two.limit_data_type_is_set is True
        assert add_two.ColumnConfig.limit_data_type == [SparkDatatype.LONG]

        assert add_two.data_type_strict_mode_is_set is True
        assert add_two.ColumnConfig.data_type_strict_mode is True

        columns = [col for col in add_two.get_columns()]
        assert columns == ["long_column"]

    def test_strict_config_with_columns_unhappy(self, str_long_df):
        """Passing a column with a 'wrong' datatype should raise a ValueError when strict mode is enabled"""
        # explicitly pass a value for columns
        add_two = self.AddTwoStrict(
            columns=["str_column"],
            df=str_long_df,
            target_column="target_column",
        )

        assert add_two.data_type_strict_mode_is_set is True
        assert add_two.ColumnConfig.data_type_strict_mode is True

        assert add_two.limit_data_type_is_set is True
        assert add_two.ColumnConfig.limit_data_type == [SparkDatatype.LONG]

        with pytest.raises(ValueError):
            _ = [col for col in add_two.get_columns()]

    def test_strict_mode(self, str_long_df):
        add_two = self.AddTwoStrict(
            columns=["str_column"],
            df=str_long_df,
        )
        with pytest.raises(ValueError):
            add_two.execute()

    class AddTwoLongOnly(AddTwoStrict):
        class ColumnConfig:
            limit_data_type = [None]
            data_type_strict_mode = False
            run_for_all_data_type = [SparkDatatype.LONG]

    def test_run_all_without_columns_happy(self, str_long_df):
        add_two = self.AddTwoLongOnly(
            df=str_long_df,
            target_suffix="_add_two",
        )

        assert add_two.data_type_strict_mode_is_set is False
        assert add_two.ColumnConfig.data_type_strict_mode is False

        assert add_two.limit_data_type_is_set is False
        assert add_two.ColumnConfig.limit_data_type == [None]

        assert add_two.run_for_all_is_set is True
        assert add_two.ColumnConfig.run_for_all_data_type == [SparkDatatype.LONG]

        columns = [col for col in add_two.get_columns()]
        assert columns == ["long_column"]

    class MultipleDataTypes(ColumnsTransformationWithTarget):
        class ColumnConfig:
            limit_data_type = [SparkDatatype.LONG, SparkDatatype.STRING]
            data_type_strict_mode = True
            run_for_all_data_type = [SparkDatatype.LONG, SparkDatatype.STRING]

        def func(self, column: Column):
            return column

    def test_multiple_datatypes_happy(self, str_long_df):
        multiple_data_types = self.MultipleDataTypes(
            df=str_long_df,
        )

        assert multiple_data_types.data_type_strict_mode_is_set is True
        assert multiple_data_types.ColumnConfig.data_type_strict_mode is True

        assert multiple_data_types.limit_data_type_is_set is True
        assert multiple_data_types.ColumnConfig.limit_data_type == [SparkDatatype.LONG, SparkDatatype.STRING]

        assert multiple_data_types.run_for_all_is_set is True
        assert multiple_data_types.ColumnConfig.run_for_all_data_type == [SparkDatatype.LONG, SparkDatatype.STRING]

        actual = [col for col in multiple_data_types.get_columns()]
        expected = ["str_column", "long_column"]
        assert sorted(actual) == sorted(expected)

    def test_column_does_not_exist(self, str_long_df):
        class TestTransformation(ColumnsTransformation):
            def execute(self):
                pass

        tf = TestTransformation(columns=["non_existent_column"], df=str_long_df)

        with pytest.raises(ValueError, match="Column 'non_existent_column' does not exist in the DataFrame schema"):
            tf.column_type_of_col("non_existent_column")
