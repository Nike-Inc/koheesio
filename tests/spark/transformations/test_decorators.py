"""
Tests for decorator-based transformations.

This module tests the @transformation, @column_transformation, and @multi_column_transformation
decorators, including type validation, equivalence with .from_func(), and all features.

Note: The decorators transform functions into Transformation/ColumnsTransformation classes dynamically.
IDEs may show type errors for decorated function calls, but this is expected behavior - the decorators
return classes, not functions. At runtime, the decorated names are classes that accept parameters
via __init__ (not the original function signature).
"""

from functools import reduce

from pyspark.sql import functions as f

from koheesio.spark import Column, DataFrame
from koheesio.spark.transformations import (
    ColumnsTransformation,
    Transformation,
    column_transformation,
    multi_column_transformation,
    transformation,
)
from koheesio.spark.utils import SparkDatatype


class TestTransformationDecorator:
    """Test @transformation decorator"""

    def test_with_parameters(self, spark):
        """Test transformation with parameters"""

        @transformation
        def add_constant(df: DataFrame, value: int) -> DataFrame:
            return df.withColumn("result", f.lit(value))

        df = spark.range(3)

        # Should work with proper parameter
        output_df = add_constant(value=100).transform(df)

        assert "result" in output_df.columns
        assert output_df.select("result").first()[0] == 100

    def test_with_default_parameters(self, spark):
        """Test decorator with default parameters"""

        @transformation(value=100)
        def add_constant(df: DataFrame, value: int) -> DataFrame:
            return df.withColumn("result", f.lit(value))

        df = spark.range(3)

        # Use default value
        output_df = add_constant().transform(df)
        assert output_df.select("result").first()[0] == 100

        # Override default
        output_df = add_constant(value=200).transform(df)
        assert output_df.select("result").first()[0] == 200

    def test_parameterized_transformation(self, spark):
        """Test transformation with multiple parameters"""

        @transformation
        def filter_and_select(df: DataFrame, column: str, threshold: int, select_cols: list) -> DataFrame:
            return df.filter(f.col(column) > threshold).select(*select_cols)

        df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
        output_df = filter_and_select(column="id", threshold=1, select_cols=["value"]).transform(df)

        assert output_df.columns == ["value"]
        assert output_df.count() == 2


class TestColumnTransformationDecorator:
    """Test @column_transformation decorator"""

    def test_simple_column_transformation(self, spark):
        """Test basic column transformation"""

        @column_transformation
        def to_upper(col: Column) -> Column:
            return f.upper(col)

        df = spark.createDataFrame([("hello",), ("world",)], ["name"])
        output_df = to_upper(columns=["name"]).transform(df)

        assert output_df.select("name").collect()[0][0] == "HELLO"
        assert output_df.select("name").collect()[1][0] == "WORLD"

    def test_with_columnconfig(self, spark):
        """Test column transformation with ColumnConfig"""

        @column_transformation(
            run_for_all_data_type=[SparkDatatype.STRING],
            limit_data_type=[SparkDatatype.STRING],
        )
        def trim_strings(col: Column) -> Column:
            return f.trim(col)

        df = spark.createDataFrame([(" hello ", 123, " world ")], ["name", "value", "city"])
        output_df = trim_strings().transform(df)

        # Should apply to all string columns
        assert output_df.select("name").collect()[0][0] == "hello"
        assert output_df.select("city").collect()[0][0] == "world"
        # Should not affect non-string column (because of limit_data_type)
        assert output_df.select("value").collect()[0][0] == 123

    def test_paired_parameters(self, spark):
        """Test paired parameters"""

        @column_transformation
        def add_tax(amount: Column, rate: float) -> Column:
            return amount * (1 + rate)

        df = spark.createDataFrame([(100.0, 200.0)], ["food", "non_food"])

        # Valid: list of floats
        output_df = add_tax(columns=["food", "non_food"], rate=[0.08, 0.13]).transform(df)
        assert abs(output_df.select("food").collect()[0][0] - 108.0) < 0.01
        assert abs(output_df.select("non_food").collect()[0][0] - 226.0) < 0.01

    def test_broadcast_single_value(self, spark):
        """Test broadcasting single value to all columns"""

        @column_transformation
        def multiply(col: Column, factor: int) -> Column:
            return col * factor

        df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        output_df = multiply(columns=["a", "b", "c"], factor=10).transform(df)

        assert output_df.select("a").collect()[0][0] == 10
        assert output_df.select("b").collect()[0][0] == 20
        assert output_df.select("c").collect()[0][0] == 30

    def test_explicit_for_each_true(self, spark):
        """Test explicit for_each=True"""

        @column_transformation(for_each=True)
        def double(col: Column) -> Column:
            return col * 2

        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        output_df = double(columns=["a", "b"]).transform(df)

        assert output_df.select("a").collect()[0][0] == 2
        assert output_df.select("b").collect()[0][0] == 4

    def test_explicit_for_each_false(self, spark):
        """Test explicit for_each=False (multi-column mode)"""

        @column_transformation(for_each=False)
        def sum_two(a: Column, b: Column) -> Column:
            return a + b

        df = spark.createDataFrame([(1, 2)], ["col_a", "col_b"])
        output_df = sum_two(columns=["col_a", "col_b"], target_column="sum").transform(df)

        assert output_df.select("sum").collect()[0][0] == 3

    def test_with_parameters(self, spark):
        """Test column transformation with parameters"""

        @column_transformation
        def add_value(col: Column, value: int) -> Column:
            return col + value

        df = spark.createDataFrame([(1,)], ["num"])

        # Should work with proper parameter
        output_df = add_value(columns=["num"], value=5).transform(df)
        assert output_df.select("num").collect()[0][0] == 6


class TestMultiColumnTransformationDecorator:
    """Test @multi_column_transformation decorator"""

    def test_fixed_arity_aggregation(self, spark):
        """Test multi-column aggregation with fixed arity"""

        @multi_column_transformation
        def sum_two(a: Column, b: Column) -> Column:
            return a + b

        df = spark.createDataFrame([(1, 2)], ["col_a", "col_b"])
        output_df = sum_two(columns=["col_a", "col_b"], target_column="sum").transform(df)

        assert output_df.select("sum").collect()[0][0] == 3

    def test_variadic_aggregation(self, spark):
        """Test variadic aggregation"""

        @multi_column_transformation
        def sum_all(*cols: Column) -> Column:
            return reduce(lambda a, b: a + b, cols)

        df = spark.createDataFrame([(1, 2, 3, 4)], ["a", "b", "c", "d"])
        output_df = sum_all(columns=["a", "b", "c", "d"], target_column="sum").transform(df)

        assert output_df.select("sum").collect()[0][0] == 10

    def test_with_parameters(self, spark):
        """Test multi-column with parameters"""

        @multi_column_transformation
        def weighted_sum(a: Column, b: Column, weights: list) -> Column:
            return a * weights[0] + b * weights[1]

        df = spark.createDataFrame([(10, 20)], ["x", "y"])
        output_df = weighted_sum(columns=["x", "y"], weights=[0.3, 0.7], target_column="result").transform(df)

        assert abs(output_df.select("result").collect()[0][0] - 17.0) < 0.01

    def test_concat_columns(self, spark):
        """Test concatenating columns"""

        @multi_column_transformation
        def concat_names(first: Column, last: Column) -> Column:
            return f.concat(first, f.lit(" "), last)

        df = spark.createDataFrame([("John", "Doe")], ["first_name", "last_name"])
        output_df = concat_names(columns=["first_name", "last_name"], target_column="full_name").transform(df)

        assert output_df.select("full_name").collect()[0][0] == "John Doe"


class TestDecoratorEquivalence:
    """Test that decorators are equivalent to .from_func()"""

    def test_transformation_equivalence(self, spark):
        """Test @transformation is equivalent to Transformation.from_func()"""

        # Decorator version
        @transformation
        def add_col_decorator(df: DataFrame, value: int) -> DataFrame:
            return df.withColumn("result", f.lit(value))

        # from_func version
        def add_col_func(df: DataFrame, value: int) -> DataFrame:
            return df.withColumn("result", f.lit(value))

        AddColFunc = Transformation.from_func(add_col_func)

        df = spark.range(3)

        # Both should produce same result
        output_decorator = add_col_decorator(value=42).transform(df)
        output_func = AddColFunc(value=42).transform(df)

        assert output_decorator.collect() == output_func.collect()

    def test_column_transformation_equivalence(self, spark):
        """Test @column_transformation is equivalent to ColumnsTransformation.from_func()"""

        # Decorator version
        @column_transformation
        def to_upper_decorator(col: Column) -> Column:
            return f.upper(col)

        # from_func version
        def to_upper_func(col: Column) -> Column:
            return f.upper(col)

        ToUpperFunc = ColumnsTransformation.from_func(to_upper_func)

        df = spark.createDataFrame([("hello",)], ["name"])

        # Both should produce same result
        output_decorator = to_upper_decorator(columns=["name"]).transform(df)
        output_func = ToUpperFunc(columns=["name"]).transform(df)

        assert output_decorator.collect() == output_func.collect()

    def test_multi_column_transformation_equivalence(self, spark):
        """Test @multi_column_transformation is equivalent to from_multi_column_func()"""

        # Decorator version
        @multi_column_transformation
        def sum_decorator(a: Column, b: Column) -> Column:
            return a + b

        # from_multi_column_func version
        def sum_func(a: Column, b: Column) -> Column:
            return a + b

        SumFunc = ColumnsTransformation.from_multi_column_func(sum_func)

        df = spark.createDataFrame([(1, 2)], ["a", "b"])

        # Both should produce same result
        output_decorator = sum_decorator(columns=["a", "b"], target_column="sum").transform(df)
        output_func = SumFunc(columns=["a", "b"], target_column="sum").transform(df)

        assert output_decorator.collect() == output_func.collect()


class TestDecoratorEdgeCases:
    """Test edge cases and special scenarios"""

    def test_lambda_with_decorator(self, spark):
        """Test that lambdas work with decorators"""

        @column_transformation
        def transform(col: Column) -> Column:
            return f.lower(col)

        # Also test inline lambda
        ToUpper = column_transformation(lambda col: f.upper(col))

        df = spark.createDataFrame([("Hello",)], ["name"])

        output_lower = transform(columns=["name"]).transform(df)
        assert output_lower.select("name").collect()[0][0] == "hello"

        output_upper = ToUpper(columns=["name"]).transform(df)
        assert output_upper.select("name").collect()[0][0] == "HELLO"

    def test_decorator_with_partial(self, spark):
        """Test using .partial() with decorated transformations"""

        @column_transformation
        def add_value(col: Column, value: int) -> Column:
            return col + value

        # Create specialized version
        AddTen = add_value.partial(value=10)

        df = spark.createDataFrame([(1,), (2,)], ["num"])
        output_df = AddTen(columns=["num"]).transform(df)

        assert output_df.select("num").collect()[0][0] == 11
        assert output_df.select("num").collect()[1][0] == 12

    def test_decorator_with_columnconfig_and_limit(self, spark):
        """Test ColumnConfig with limit_data_type"""

        @column_transformation(
            run_for_all_data_type=[SparkDatatype.STRING],
            limit_data_type=[SparkDatatype.STRING],
            data_type_strict_mode=True,
        )
        def process_strings(col: Column) -> Column:
            return f.upper(col)

        df = spark.createDataFrame([("hello", 123)], ["name", "value"])

        # Should only apply to string columns (due to limit_data_type)
        output_df = process_strings().transform(df)
        assert output_df.select("name").collect()[0][0] == "HELLO"
        # Integer column should be unchanged
        assert output_df.select("value").collect()[0][0] == 123

    def test_decorator_without_parentheses(self, spark):
        """Test decorator without parentheses (no arguments)"""

        @transformation
        def simple_filter(df: DataFrame) -> DataFrame:
            return df.filter(f.col("id") > 0)

        df = spark.range(-1, 3)  # Creates rows with id: -1, 0, 1, 2
        output_df = simple_filter().transform(df)

        # Filter > 0 gives: 1, 2 = 2 rows
        assert output_df.count() == 2

    def test_decorator_with_parentheses_no_args(self, spark):
        """Test decorator with empty parentheses"""

        @transformation()
        def simple_filter(df: DataFrame) -> DataFrame:
            return df.filter(f.col("id") > 0)

        df = spark.range(-1, 3)  # Creates rows with id: -1, 0, 1, 2
        output_df = simple_filter().transform(df)

        # Filter > 0 gives: 1, 2 = 2 rows
        assert output_df.count() == 2
