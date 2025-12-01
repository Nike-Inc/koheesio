import pytest

from pyspark.sql import Column
from pyspark.sql import functions as f

from koheesio.spark.transformations import (
    ColumnsTransformation,
    ColumnsTransformationWithTarget,
    Transformation,
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
    tf.execute()
    df = tf.output.df
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
        add_one.execute()
        output_df = add_one.output.df
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
        yield self.AddOneWithTarget(
            columns=["long_column"], df=input_df, target_column="target_column"
        )

    def test_columns_transformation_with_target(self, add_one_with_target):
        expected = {"long_column": 1, "str_column": "a", "target_column": 2}
        add_one_with_target.execute()
        output_df = add_one_with_target.output.df
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
        assert multiple_data_types.ColumnConfig.limit_data_type == [
            SparkDatatype.LONG,
            SparkDatatype.STRING,
        ]

        assert multiple_data_types.run_for_all_is_set is True
        assert multiple_data_types.ColumnConfig.run_for_all_data_type == [
            SparkDatatype.LONG,
            SparkDatatype.STRING,
        ]

        actual = [col for col in multiple_data_types.get_columns()]
        expected = ["str_column", "long_column"]
        assert sorted(actual) == sorted(expected)

    def test_column_does_not_exist(self, str_long_df):
        class TestTransformation(ColumnsTransformation):
            def execute(self):
                pass

        tf = TestTransformation(columns=["non_existent_column"], df=str_long_df)

        with pytest.raises(
            ValueError,
            match="Column 'non_existent_column' does not exist in the DataFrame schema",
        ):
            tf.column_type_of_col("non_existent_column")


class TestFunctionBasedTransformations:
    """Test function-based transformations using Transformation.from_func()"""

    def test_simple_dataframe_transformation(self, spark):
        """Test creating a simple DataFrame transformation from a function"""

        # Define a function
        def lowercase_columns(df):
            return df.select([f.col(c).alias(c.lower()) for c in df.columns])

        # Create transformation
        LowercaseColumns = Transformation.from_func(lowercase_columns)

        # Verify it's a proper class
        assert issubclass(LowercaseColumns, Transformation)
        assert LowercaseColumns.__name__ == "lowercase_columns"

        # Test it works
        input_df = spark.createDataFrame([("John", 25)], ["NAME", "AGE"])
        output_df = LowercaseColumns().transform(input_df)

        assert output_df.columns == ["name", "age"]
        assert output_df.count() == 1

    def test_parameterized_dataframe_transformation(self, spark):
        """Test creating a parameterized DataFrame transformation"""

        def add_audit_columns(df, user: str, system: str = "koheesio"):
            return df.withColumn("created_by", f.lit(user)).withColumn(
                "system", f.lit(system)
            )

        AddAudit = Transformation.from_func(add_audit_columns)

        input_df = spark.createDataFrame([("John", 25)], ["name", "age"])
        output_df = AddAudit(user="test_user", system="test_system").transform(input_df)

        assert "created_by" in output_df.columns
        assert "system" in output_df.columns
        assert output_df.select("created_by").first()[0] == "test_user"
        assert output_df.select("system").first()[0] == "test_system"

    def test_transformation_partial(self, spark):
        """Test using .partial() to create specialized transformations"""

        def add_audit_columns(df, user: str, system: str = "koheesio"):
            return df.withColumn("created_by", f.lit(user)).withColumn(
                "system", f.lit(system)
            )

        AddAudit = Transformation.from_func(add_audit_columns)
        AddMyAudit = AddAudit.partial(user="my_pipeline", system="production")

        input_df = spark.createDataFrame([("John", 25)], ["name", "age"])
        output_df = AddMyAudit().transform(input_df)

        assert output_df.select("created_by").first()[0] == "my_pipeline"
        assert output_df.select("system").first()[0] == "production"

    def test_lambda_transformation_naming(self):
        """Test that lambda functions get a generic name"""
        LambdaTransform = Transformation.from_func(lambda df: df)
        assert LambdaTransform.__name__ == "FunctionBasedTransformation"

    def test_lambda_transformation_custom_name(self):
        """Test that lambda transformations respect a custom instance name"""

        LambdaTransform = Transformation.from_func(lambda df: df)
        instance = LambdaTransform(name="custom_lambda")

        # Class name stays generic
        assert LambdaTransform.__name__ == "FunctionBasedTransformation"
        # Instance name reflects the explicit override
        assert instance.name == "custom_lambda"

    def test_named_function_transformation_naming(self):
        """Test that named functions preserve their class and default instance name"""

        def my_custom_transform(df):
            return df

        NamedTransform = Transformation.from_func(my_custom_transform)
        # Class name should reflect the original function name
        assert NamedTransform.__name__ == "my_custom_transform"

        # Default instance name should also be derived from the class name
        default_instance = NamedTransform()
        assert default_instance.name == "my_custom_transform"

    def test_named_function_transformation_custom_name(self):
        """Test that passing a custom name parameter overrides the default instance name"""

        def my_custom_transform(df):
            return df

        NamedTransform = Transformation.from_func(my_custom_transform)

        # Default instance name is derived from the class name
        default_instance = NamedTransform()
        assert default_instance.name == "my_custom_transform"

        # Instance-level name should reflect the explicitly provided value
        custom_instance = NamedTransform(name="custom_name")
        assert custom_instance.name == "custom_name"


class TestFunctionBasedColumnsTransformations:
    """Test function-based column transformations using ColumnsTransformation.from_func()"""

    @pytest.fixture(scope="class")
    def string_df(self, spark):
        return spark.createDataFrame(
            [("JOHN", "ENGINEER"), ("JANE", "MANAGER")], ["name", "title"]
        )

    def test_simple_column_transformation(self, string_df):
        """Test creating a simple column transformation"""
        LowerCase = ColumnsTransformation.from_func(
            lambda col: f.lower(col),
            run_for_all_data_type=[SparkDatatype.STRING],
            limit_data_type=[SparkDatatype.STRING],
        )

        assert issubclass(LowerCase, ColumnsTransformation)

        # Apply to specific column
        output_df = LowerCase(columns=["name"]).transform(string_df)
        assert output_df.select("name").first()[0] == "john"
        assert output_df.select("title").first()[0] == "ENGINEER"  # unchanged

    def test_column_transformation_with_target(self, string_df):
        """Test column transformation with target_column"""
        LowerCase = ColumnsTransformation.from_func(
            lambda col: f.lower(col),
            run_for_all_data_type=[SparkDatatype.STRING],
            limit_data_type=[SparkDatatype.STRING],
        )

        output_df = LowerCase(columns=["name"], target_column="name_lower").transform(
            string_df
        )

        assert "name_lower" in output_df.columns
        assert output_df.select("name").first()[0] == "JOHN"  # original unchanged
        assert output_df.select("name_lower").first()[0] == "john"

    def test_column_transformation_run_for_all(self, string_df):
        """Test automatic column selection with run_for_all_data_type"""
        LowerCase = ColumnsTransformation.from_func(
            lambda col: f.lower(col),
            run_for_all_data_type=[SparkDatatype.STRING],
            limit_data_type=[SparkDatatype.STRING],
        )

        # Don't specify columns - should apply to all STRING columns
        output_df = LowerCase().transform(string_df)

        assert output_df.select("name").first()[0] == "john"
        assert output_df.select("title").first()[0] == "engineer"

    def test_parameterized_column_transformation(self, string_df):
        """Test parameterized column transformation"""

        def add_prefix(col, prefix: str):
            return f.concat(f.lit(prefix), col)

        AddPrefix = ColumnsTransformation.from_func(add_prefix)

        output_df = AddPrefix(columns=["name"], prefix="Mr. ").transform(string_df)
        assert output_df.select("name").first()[0] == "Mr. JOHN"

    def test_column_transformation_partial(self, string_df):
        """Test using .partial() with column transformations"""

        def add_prefix(col, prefix: str):
            return f.concat(f.lit(prefix), col)

        AddPrefix = ColumnsTransformation.from_func(add_prefix)
        AddMrPrefix = AddPrefix.partial(prefix="Mr. ")

        output_df = AddMrPrefix(columns=["name"]).transform(string_df)
        assert output_df.select("name").first()[0] == "Mr. JOHN"

    def test_factory_pattern(self, string_df):
        """Test factory pattern for creating related transformations"""

        def string_transform(func):
            return ColumnsTransformation.from_func(
                func,
                run_for_all_data_type=[SparkDatatype.STRING],
                limit_data_type=[SparkDatatype.STRING],
            )

        LowerCase = string_transform(lambda col: f.lower(col))
        UpperCase = string_transform(lambda col: f.upper(col))
        TitleCase = string_transform(lambda col: f.initcap(col))

        # Test they're independent
        assert LowerCase is not UpperCase
        assert LowerCase is not TitleCase

        # Test they work
        lower_df = LowerCase(columns=["name"]).transform(string_df)
        assert lower_df.select("name").first()[0] == "john"

        upper_df = UpperCase(columns=["name"]).transform(string_df)
        assert upper_df.select("name").first()[0] == "JOHN"

    def test_column_config_fields_set_correctly(self, string_df):
        """Test that ColumnConfig parameters are set correctly on generated classes"""
        LowerCase = ColumnsTransformation.from_func(
            lambda col: f.lower(col),
            run_for_all_data_type=[SparkDatatype.STRING],
            limit_data_type=[SparkDatatype.STRING, SparkDatatype.INTEGER],
            data_type_strict_mode=True,
        )

        # Check field defaults
        assert LowerCase.model_fields["run_for_all_data_type"].default == [
            SparkDatatype.STRING
        ]
        assert LowerCase.model_fields["limit_data_type"].default == [
            SparkDatatype.STRING,
            SparkDatatype.INTEGER,
        ]
        assert LowerCase.model_fields["data_type_strict_mode"].default is True

    def test_column_config_independence(self):
        """Test that different generated classes have independent ColumnConfig"""
        LowerCase = ColumnsTransformation.from_func(
            lambda col: f.lower(col), run_for_all_data_type=[SparkDatatype.STRING]
        )

        UpperCase = ColumnsTransformation.from_func(
            lambda col: f.upper(col), run_for_all_data_type=[SparkDatatype.INTEGER]
        )

        # Verify independence
        assert LowerCase.model_fields["run_for_all_data_type"].default == [
            SparkDatatype.STRING
        ]
        assert UpperCase.model_fields["run_for_all_data_type"].default == [
            SparkDatatype.INTEGER
        ]

    def test_multiple_columns_with_target_suffix(self, string_df):
        """Test that target_column becomes a suffix when multiple columns are given"""
        LowerCase = ColumnsTransformation.from_func(
            lambda col: f.lower(col), run_for_all_data_type=[SparkDatatype.STRING]
        )

        output_df = LowerCase(
            columns=["name", "title"], target_column="lower"
        ).transform(string_df)

        # Target column should be used as suffix
        assert "name_lower" in output_df.columns
        assert "title_lower" in output_df.columns
        assert output_df.select("name_lower").first()[0] == "john"
        assert output_df.select("title_lower").first()[0] == "engineer"

    def test_backward_compatibility_with_subclasses(self, string_df):
        """Test that existing subclass-based transformations still work (backward compatibility)"""

        class LowerCaseOldStyle(ColumnsTransformationWithTarget):
            class ColumnConfig:
                run_for_all_data_type = [SparkDatatype.STRING]
                limit_data_type = [SparkDatatype.STRING]

            def func(self, column: Column):
                return f.lower(column)

        # Old style should still work
        output_df = LowerCaseOldStyle(columns=["name"]).transform(string_df)
        assert output_df.select("name").first()[0] == "john"

    def test_multi_column_aggregation(self, spark):
        """Test multi-column aggregation pattern"""

        def sum_columns(col1: Column, col2: Column, col3: Column) -> Column:
            return col1 + col2 + col3

        SumThree = ColumnsTransformation.from_func(sum_columns)

        df = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
        output_df = SumThree(columns=["a", "b", "c"], target_column="sum").transform(df)

        assert "sum" in output_df.columns
        assert output_df.select("sum").collect()[0][0] == 6
        assert output_df.select("sum").collect()[1][0] == 15

    def test_multi_column_concat(self, spark):
        """Test multi-column concatenation"""

        def concat_names(first: Column, last: Column) -> Column:
            return f.concat(first, f.lit(" "), last)

        ConcatNames = ColumnsTransformation.from_func(concat_names)

        df = spark.createDataFrame(
            [("John", "Doe"), ("Jane", "Smith")], ["first_name", "last_name"]
        )
        output_df = ConcatNames(
            columns=["first_name", "last_name"], target_column="full_name"
        ).transform(df)

        assert "full_name" in output_df.columns
        assert output_df.select("full_name").collect()[0][0] == "John Doe"
        assert output_df.select("full_name").collect()[1][0] == "Jane Smith"

    def test_variadic_args(self, spark):
        """Test variadic *args pattern"""
        from functools import reduce

        def sum_all(*cols: Column) -> Column:
            return reduce(lambda a, b: a + b, cols)

        SumAll = ColumnsTransformation.from_func(sum_all)

        # Test with 3 columns
        df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        output_df = SumAll(columns=["a", "b", "c"], target_column="sum").transform(df)
        assert output_df.select("sum").collect()[0][0] == 6

        # Test with 4 columns
        df = spark.createDataFrame([(1, 2, 3, 4)], ["a", "b", "c", "d"])
        output_df = SumAll(columns=["a", "b", "c", "d"], target_column="sum").transform(
            df
        )
        assert output_df.select("sum").collect()[0][0] == 10

    def test_paired_parameters_exact_match(self, spark):
        """Test paired parameters with exact column count match"""

        def add_tax(amount: Column, rate: float = 0.08) -> Column:
            return amount * (1 + rate)

        AddTax = ColumnsTransformation.from_func(add_tax)

        df = spark.createDataFrame([(100.0, 200.0)], ["food_price", "non_food_price"])
        output_df = AddTax(
            columns=["food_price", "non_food_price"], rate=[0.08, 0.13]
        ).transform(df)

        # food_price: 100 * 1.08 = 108.0
        # non_food_price: 200 * 1.13 = 226.0
        assert output_df.select("food_price").collect()[0][0] == pytest.approx(108.0)
        assert output_df.select("non_food_price").collect()[0][0] == pytest.approx(
            226.0
        )

    def test_paired_parameters_length_mismatch(self, spark):
        """Test that mismatched list length raises error (strict=True)"""

        def add_tax(amount: Column, rate: float = 0.08) -> Column:
            return amount * (1 + rate)

        AddTax = ColumnsTransformation.from_func(add_tax)

        df = spark.createDataFrame([(100.0, 200.0, 300.0)], ["a", "b", "c"])

        with pytest.raises(ValueError, match="has 2 values but 3 columns provided"):
            AddTax(columns=["a", "b", "c"], rate=[0.08, 0.13]).transform(df)

    def test_paired_parameters_single_value_broadcast(self, spark):
        """Test that single values are broadcast to all columns"""

        def add_tax(amount: Column, rate: float = 0.08) -> Column:
            return amount * (1 + rate)

        AddTax = ColumnsTransformation.from_func(add_tax)

        df = spark.createDataFrame([(100.0, 200.0)], ["a", "b"])
        output_df = AddTax(columns=["a", "b"], rate=0.10).transform(df)

        # Both should get 10% tax
        assert output_df.select("a").collect()[0][0] == pytest.approx(110.0)
        assert output_df.select("b").collect()[0][0] == pytest.approx(220.0)

    def test_paired_multiple_parameters(self, spark):
        """Test multiple paired parameters"""

        def scale_offset(col: Column, scale: float, offset: float) -> Column:
            return col * scale + offset

        ScaleOffset = ColumnsTransformation.from_func(scale_offset)

        df = spark.createDataFrame([(10.0, 20.0)], ["a", "b"])
        output_df = ScaleOffset(
            columns=["a", "b"], scale=[2.0, 3.0], offset=[5.0, 10.0]
        ).transform(df)

        # a: 10 * 2.0 + 5.0 = 25.0
        # b: 20 * 3.0 + 10.0 = 70.0
        assert output_df.select("a").collect()[0][0] == 25.0
        assert output_df.select("b").collect()[0][0] == 70.0

    def test_columns_transformation_with_target_still_works(self, spark):
        """Test that ColumnsTransformationWithTarget still works (backward compatibility)"""
        # Note: Deprecated in docstring only, no runtime warning to avoid spamming users
        # with warnings from Koheesio's own internal classes

        class MyTransform(ColumnsTransformationWithTarget):
            def func(self, col):
                return f.upper(col)

        df = spark.createDataFrame([("hello",)], ["text"])
        output_df = MyTransform(columns=["text"]).transform(df)
        assert output_df.select("text").collect()[0][0] == "HELLO"

    def test_nested_columnconfig_backward_compatibility(self, spark):
        """Test that nested ColumnConfig still works (backward compatibility)"""

        class OldStyle(ColumnsTransformation):
            class ColumnConfig:
                run_for_all_data_type = [SparkDatatype.STRING]
                limit_data_type = None
                data_type_strict_mode = False

            def execute(self):
                # Simple transformation for testing
                for col in self.get_columns():
                    self.output.df = self.df.withColumn(col, f.upper(f.col(col)))

        df = spark.createDataFrame([("test",)], ["col"])
        transform = OldStyle(df=df)
        transform.execute()
        output_df = transform.output.df

        # Verify it still works
        assert output_df.select("col").collect()[0][0] == "TEST"

    def test_for_each_explicit_true(self, spark):
        """Test explicit for_each=True parameter"""

        def add_one(col: Column) -> Column:
            return col + 1

        AddOne = ColumnsTransformation.from_func(add_one, for_each=True)

        df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        output_df = AddOne(columns=["a", "b"]).transform(df)

        # Should apply to each column separately
        assert output_df.select("a").collect()[0][0] == 2
        assert output_df.select("b").collect()[0][0] == 3

    def test_for_each_explicit_false(self, spark):
        """Test explicit for_each=False parameter"""

        def sum_two(col1: Column, col2: Column) -> Column:
            return col1 + col2

        SumTwo = ColumnsTransformation.from_func(sum_two, for_each=False)

        df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        output_df = SumTwo(columns=["a", "b"], target_column="sum").transform(df)

        # Should pass both columns at once
        assert output_df.select("sum").collect()[0][0] == 3
        assert output_df.select("sum").collect()[1][0] == 7

    def test_from_multi_column_func_method(self, spark):
        """Test from_multi_column_func method"""

        def concat_three(c1: Column, c2: Column, c3: Column) -> Column:
            return f.concat(c1, f.lit("-"), c2, f.lit("-"), c3)

        ConcatThree = ColumnsTransformation.from_multi_column_func(concat_three)

        df = spark.createDataFrame([("a", "b", "c")], ["col1", "col2", "col3"])
        output_df = ConcatThree(
            columns=["col1", "col2", "col3"], target_column="result"
        ).transform(df)

        assert output_df.select("result").collect()[0][0] == "a-b-c"

    def test_from_multi_column_func_default_behavior(self, spark):
        """Test that from_multi_column_func defaults to for_each=False for ambiguous functions"""

        # No type hints - ambiguous
        def mystery_add(a, b):
            return a + b

        # from_func should default to True (column-wise)
        # AddFunc = ColumnsTransformation.from_func(mystery_add)  # Not used
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        # This would fail if it tried to apply column-wise since we need target_column
        # So let's use explicit for_each to test
        AddFunc2 = ColumnsTransformation.from_func(mystery_add, for_each=False)
        output_df = AddFunc2(columns=["a", "b"], target_column="sum").transform(df)
        assert output_df.select("sum").collect()[0][0] == 3

        # from_multi_column_func should default to False (multi-column)
        AddMulti = ColumnsTransformation.from_multi_column_func(mystery_add)
        output_df2 = AddMulti(columns=["a", "b"], target_column="sum").transform(df)
        assert output_df2.select("sum").collect()[0][0] == 3

    def test_auto_detect_single_column_param(self, spark):
        """Test auto-detection with single Column parameter"""

        def double_it(col: Column) -> Column:
            return col * 2

        Double = ColumnsTransformation.from_func(double_it)

        df = spark.createDataFrame([(5, 10)], ["a", "b"])
        output_df = Double(columns=["a", "b"]).transform(df)

        # Should apply to each column
        assert output_df.select("a").collect()[0][0] == 10
        assert output_df.select("b").collect()[0][0] == 20

    def test_auto_detect_multiple_column_params(self, spark):
        """Test auto-detection with multiple Column parameters"""

        def multiply_cols(col1: Column, col2: Column) -> Column:
            return col1 * col2

        Multiply = ColumnsTransformation.from_func(multiply_cols)

        df = spark.createDataFrame([(2, 3), (4, 5)], ["a", "b"])
        output_df = Multiply(columns=["a", "b"], target_column="product").transform(df)

        # Should pass both columns at once
        assert output_df.select("product").collect()[0][0] == 6
        assert output_df.select("product").collect()[1][0] == 20

    def test_lambda_with_for_each_override(self, spark):
        """Test lambda function with explicit for_each override"""

        # Lambda with 2 params - ambiguous without type hints
        SumLambda = ColumnsTransformation.from_func(lambda a, b: a + b, for_each=False)

        df = spark.createDataFrame([(1, 2), (3, 4)], ["x", "y"])
        output_df = SumLambda(columns=["x", "y"], target_column="sum").transform(df)

        assert output_df.select("sum").collect()[0][0] == 3
        assert output_df.select("sum").collect()[1][0] == 7
