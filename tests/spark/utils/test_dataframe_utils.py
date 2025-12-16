"""Tests for DataFrame utilities."""

from pyspark.sql import Column
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType, StructField, StructType

from koheesio.spark.utils.dataframe import apply_transformation_to_nested_structures


class TestApplyTransformationToNestedStructures:
    """Test suite for apply_transformation_to_nested_structures function."""

    def test_transform_simple_map_type(self, spark):
        """Test transforming a simple map column."""
        data = [
            ("value1", {"key1": "val1", "key2": "val2"}),
            ("value2", {"key3": "val3"}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        # Transform map keys to uppercase
        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        assert "KEY1" in result_data[0]["data"]
        assert "KEY2" in result_data[0]["data"]
        assert "KEY3" in result_data[1]["data"]

    def test_transform_nested_map_in_struct(self, spark):
        """Test transforming maps nested inside structs."""
        data = [
            ("value1", {"field1": "test", "map_field": {"key1": "val1"}}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "nested",
                    StructType(
                        [
                            StructField("field1", StringType(), True),
                            StructField("map_field", MapType(StringType(), StringType()), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        assert "KEY1" in result_data[0]["nested"]["map_field"]

    def test_transform_map_in_array(self, spark):
        """Test transforming maps inside arrays."""
        data = [
            ("value1", [{"key1": "val1"}, {"key2": "val2"}]),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("array_of_maps", ArrayType(MapType(StringType(), StringType())), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        assert "KEY1" in result_data[0]["array_of_maps"][0]
        assert "KEY2" in result_data[0]["array_of_maps"][1]

    def test_transform_nested_maps(self, spark):
        """Test transforming nested maps (map values are also maps)."""
        data = [
            ("value1", {"outer1": {"inner1": "val1"}, "outer2": {"inner2": "val2"}}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("nested_maps", MapType(StringType(), MapType(StringType(), StringType())), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        # Both outer and inner keys should be uppercase
        assert "OUTER1" in result_data[0]["nested_maps"]
        assert "OUTER2" in result_data[0]["nested_maps"]
        assert "INNER1" in result_data[0]["nested_maps"]["OUTER1"]
        assert "INNER2" in result_data[0]["nested_maps"]["OUTER2"]

    def test_transform_with_default_target_types(self, spark):
        """Test using default target types (all complex types)."""
        data = [
            ("value1", {"key1": "val1"}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        # Don't specify target_types - should default to all complex types
        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types=None)

        result_data = result_df.collect()
        assert "KEY1" in result_data[0]["data"]

    def test_non_target_columns_preserved(self, spark):
        """Test that non-target columns are preserved unchanged."""
        data = [
            ("value1", 123, ["item1", "item2"], {"key1": "val1"}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("count", IntegerType(), True),
                StructField("items", ArrayType(StringType()), True),
                StructField("data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        # Non-map columns should be preserved
        assert result_data[0]["id"] == "value1"
        assert result_data[0]["count"] == 123
        assert result_data[0]["items"] == ["item1", "item2"]
        # Map should be transformed
        assert "KEY1" in result_data[0]["data"]

    def test_transform_string_columns(self, spark):
        """Test transforming string type columns."""
        data = [
            ("hello", "world"),
            ("foo", "bar"),
        ]
        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", StringType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.upper(col_expr)

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={StringType})

        result_data = result_df.collect()
        assert result_data[0]["col1"] == "HELLO"
        assert result_data[0]["col2"] == "WORLD"
        assert result_data[1]["col1"] == "FOO"
        assert result_data[1]["col2"] == "BAR"

    def test_transform_nested_string_in_struct(self, spark):
        """Test transforming string fields nested in structs."""
        data = [
            ("value1", {"field1": "hello", "field2": 123}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "nested",
                    StructType(
                        [
                            StructField("field1", StringType(), True),
                            StructField("field2", IntegerType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.upper(col_expr)

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={StringType})

        result_data = result_df.collect()
        # String fields should be transformed
        assert result_data[0]["id"] == "VALUE1"
        assert result_data[0]["nested"]["field1"] == "HELLO"
        # Integer field should be preserved
        assert result_data[0]["nested"]["field2"] == 123

    def test_deeply_nested_structure(self, spark):
        """Test transforming deeply nested structures."""
        data = [
            (
                "value1",
                {
                    "level1": {
                        "level2": {
                            "level3": [
                                {
                                    "level4": {"key1": "val1"},
                                }
                            ]
                        }
                    }
                },
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "deep",
                    StructType(
                        [
                            StructField(
                                "level1",
                                StructType(
                                    [
                                        StructField(
                                            "level2",
                                            StructType(
                                                [
                                                    StructField(
                                                        "level3",
                                                        ArrayType(
                                                            StructType(
                                                                [
                                                                    StructField(
                                                                        "level4",
                                                                        MapType(StringType(), StringType()),
                                                                        True,
                                                                    ),
                                                                ]
                                                            )
                                                        ),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        # Navigate to the deeply nested map
        level4_map = result_data[0]["deep"]["level1"]["level2"]["level3"][0]["level4"]
        assert "KEY1" in level4_map

    def test_empty_dataframe(self, spark):
        """Test with empty DataFrame."""
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame([], schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        # Should not raise error
        assert result_df.count() == 0
        assert result_df.schema == df.schema

    def test_null_values(self, spark):
        """Test handling of null values."""
        data = [
            ("value1", None),
            ("value2", {"key1": "val1"}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        # Null should be preserved
        assert result_data[0]["data"] is None
        # Non-null should be transformed
        assert "KEY1" in result_data[1]["data"]

    def test_multiple_target_types(self, spark):
        """Test with multiple target types."""
        data = [
            ("hello", {"key1": "val1"}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            if isinstance(data_type, StringType):
                return F.upper(col_expr)
            elif isinstance(data_type, MapType):
                return F.transform_keys(col_expr, lambda k, v: F.upper(k))
            return col_expr

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={StringType, MapType})

        result_data = result_df.collect()
        # Both string and map should be transformed
        assert result_data[0]["id"] == "HELLO"
        assert "KEY1" in result_data[0]["data"]

    def test_array_of_structs_with_maps(self, spark):
        """Test array of structs containing maps."""
        data = [
            (
                "value1",
                [
                    {"name": "item1", "attributes": {"color": "red", "size": "large"}},
                    {"name": "item2", "attributes": {"color": "blue", "size": "small"}},
                ],
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("attributes", MapType(StringType(), StringType()), True),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        # Check both items in the array
        assert "COLOR" in result_data[0]["items"][0]["attributes"]
        assert "SIZE" in result_data[0]["items"][0]["attributes"]
        assert "COLOR" in result_data[0]["items"][1]["attributes"]

    def test_schema_preserved(self, spark):
        """Test that the schema structure is preserved (only data changes)."""
        data = [
            ("value1", {"key1": "val1"}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        # Schema should be the same
        assert result_df.schema == df.schema
        # Column names should be the same
        assert result_df.columns == df.columns

    def test_column_order_preserved(self, spark):
        """Test that column order is preserved."""
        data = [
            ("value1", 123, "test", {"key1": "val1"}),
        ]
        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", IntegerType(), True),
                StructField("col3", StringType(), True),
                StructField("col4", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        # Column order should be preserved
        assert result_df.columns == ["col1", "col2", "col3", "col4"]

    def test_map_with_struct_values(self, spark):
        """Test maps where values are structs."""
        data = [
            (
                "value1",
                {
                    "key1": {"field1": "val1", "field2": 123},
                    "key2": {"field1": "val2", "field2": 456},
                },
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "data",
                    MapType(
                        StringType(),
                        StructType(
                            [
                                StructField("field1", StringType(), True),
                                StructField("field2", IntegerType(), True),
                            ]
                        ),
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        # Map keys should be uppercase
        assert "KEY1" in result_data[0]["data"]
        assert "KEY2" in result_data[0]["data"]
        # Struct values should be preserved
        assert result_data[0]["data"]["KEY1"]["field1"] == "val1"
        assert result_data[0]["data"]["KEY1"]["field2"] == 123

    def test_immutability(self, spark):
        """Test that original DataFrame is not modified."""
        data = [
            ("value1", {"key1": "val1"}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        # Save original data
        original_data = df.collect()[0]["data"]

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        # Original DataFrame should be unchanged
        original_after = df.collect()[0]["data"]
        assert original_data == original_after
        assert "key1" in original_after  # Original should still have lowercase keys

        # Result should have transformed data
        result_data = result_df.collect()[0]["data"]
        assert "KEY1" in result_data

    def test_struct_fields_with_dots_in_names(self, spark):
        """Test handling of struct fields with dots in their names.

        This tests the edge case where struct field names contain special characters
        like dots, which need to be properly escaped when accessing nested fields.
        """
        # Create a struct with field names containing dots
        data = [
            (
                "value1",
                {"field.with.dots": "hello", "normal_field": "world", "another.dotted": 123},
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "nested",
                    StructType(
                        [
                            StructField("field.with.dots", StringType(), True),
                            StructField("normal_field", StringType(), True),
                            StructField("another.dotted", IntegerType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        # Transform all string fields to uppercase
        def transform_func(col_expr: Column, data_type) -> Column:
            return F.upper(col_expr)

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={StringType})

        result_data = result_df.collect()
        # String fields (including those with dots) should be transformed
        assert result_data[0]["id"] == "VALUE1"
        assert result_data[0]["nested"]["field.with.dots"] == "HELLO"
        assert result_data[0]["nested"]["normal_field"] == "WORLD"
        # Integer field should be preserved
        assert result_data[0]["nested"]["another.dotted"] == 123

    def test_deeply_nested_struct_with_dots_and_maps(self, spark):
        """Test struct fields with dots nested alongside maps.

        This tests a complex scenario with nested structs containing both
        field names with dots and maps that need transformation.
        """
        data = [
            (
                "value1",
                {
                    "level1.field": {
                        "level2.field": "test_string",
                        "map_field": {"key1": "val1", "key2": "val2"},
                    }
                },
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "nested",
                    StructType(
                        [
                            StructField(
                                "level1.field",
                                StructType(
                                    [
                                        StructField("level2.field", StringType(), True),
                                        StructField("map_field", MapType(StringType(), StringType()), True),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        # Transform map keys to uppercase
        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        # Navigate to the nested map through dotted field names
        nested_map = result_data[0]["nested"]["level1.field"]["map_field"]
        assert "KEY1" in nested_map
        assert "KEY2" in nested_map
        # String field should be preserved
        assert result_data[0]["nested"]["level1.field"]["level2.field"] == "test_string"

    def test_struct_fields_with_various_special_characters(self, spark):
        """Test handling of struct fields with various special characters.

        This comprehensive test covers:
        - Spaces in field names
        - Special characters (@, #, $, -, :)
        - Unicode characters (Japanese, Chinese, Korean, emojis, Spanish)
        - Mixed special characters (dots + spaces, multiple symbols)
        - Brackets and parentheses
        - Fields starting with numbers
        """
        data = [
            (
                "value1",
                {
                    # Spaces
                    "field with spaces": "hello",
                    "another field": "world",
                    # Special characters
                    "field@special": "test1",
                    "field#hash": "test2",
                    "field$dollar": "test3",
                    "field-dash": "test4",
                    "field:colon": "test5",
                    # Unicode
                    "field_日本語": "japanese",
                    "field_中文": "chinese",
                    "field_한글": "korean",
                    "field_émojis_🎉": "emoji",
                    "field_Ñoño": "spanish",
                    # Mixed special characters
                    "field.with.multiple.dots": "dots",
                    "field with spaces and.dots": "mixed",
                    "field@#$%&*": "symbols",
                    "123_starts_with_number": "number",
                    # Brackets and parentheses
                    "field[0]": "bracket1",
                    "field(1)": "paren1",
                    "field{2}": "brace1",
                    "field[with](brackets)": "bracket2",
                    # Normal field for comparison
                    "normal_field": 123,
                },
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "nested",
                    StructType(
                        [
                            # Spaces
                            StructField("field with spaces", StringType(), True),
                            StructField("another field", StringType(), True),
                            # Special characters
                            StructField("field@special", StringType(), True),
                            StructField("field#hash", StringType(), True),
                            StructField("field$dollar", StringType(), True),
                            StructField("field-dash", StringType(), True),
                            StructField("field:colon", StringType(), True),
                            # Unicode
                            StructField("field_日本語", StringType(), True),
                            StructField("field_中文", StringType(), True),
                            StructField("field_한글", StringType(), True),
                            StructField("field_émojis_🎉", StringType(), True),
                            StructField("field_Ñoño", StringType(), True),
                            # Mixed special characters
                            StructField("field.with.multiple.dots", StringType(), True),
                            StructField("field with spaces and.dots", StringType(), True),
                            StructField("field@#$%&*", StringType(), True),
                            StructField("123_starts_with_number", StringType(), True),
                            # Brackets and parentheses
                            StructField("field[0]", StringType(), True),
                            StructField("field(1)", StringType(), True),
                            StructField("field{2}", StringType(), True),
                            StructField("field[with](brackets)", StringType(), True),
                            # Normal field
                            StructField("normal_field", IntegerType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.upper(col_expr)

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={StringType})

        result_data = result_df.collect()
        nested = result_data[0]["nested"]

        # Test spaces
        assert result_data[0]["id"] == "VALUE1"
        assert nested["field with spaces"] == "HELLO"
        assert nested["another field"] == "WORLD"

        # Test special characters
        assert nested["field@special"] == "TEST1"
        assert nested["field#hash"] == "TEST2"
        assert nested["field$dollar"] == "TEST3"
        assert nested["field-dash"] == "TEST4"
        assert nested["field:colon"] == "TEST5"

        # Test unicode
        assert nested["field_日本語"] == "JAPANESE"
        assert nested["field_中文"] == "CHINESE"
        assert nested["field_한글"] == "KOREAN"
        assert nested["field_émojis_🎉"] == "EMOJI"
        assert nested["field_Ñoño"] == "SPANISH"

        # Test mixed special characters
        assert nested["field.with.multiple.dots"] == "DOTS"
        assert nested["field with spaces and.dots"] == "MIXED"
        assert nested["field@#$%&*"] == "SYMBOLS"
        assert nested["123_starts_with_number"] == "NUMBER"

        # Test brackets and parentheses
        assert nested["field[0]"] == "BRACKET1"
        assert nested["field(1)"] == "PAREN1"
        assert nested["field{2}"] == "BRACE1"
        assert nested["field[with](brackets)"] == "BRACKET2"

        # Test non-target field preserved
        assert nested["normal_field"] == 123

    def test_array_of_structs_with_special_field_names(self, spark):
        """Test arrays containing structs with special character field names."""
        data = [
            (
                "value1",
                [
                    {"field.with.dots": "item1", "field with spaces": 100},
                    {"field.with.dots": "item2", "field with spaces": 200},
                ],
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("field.with.dots", StringType(), True),
                                StructField("field with spaces", IntegerType(), True),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.upper(col_expr)

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={StringType})

        result_data = result_df.collect()
        assert result_data[0]["items"][0]["field.with.dots"] == "ITEM1"
        assert result_data[0]["items"][0]["field with spaces"] == 100
        assert result_data[0]["items"][1]["field.with.dots"] == "ITEM2"
        assert result_data[0]["items"][1]["field with spaces"] == 200

    def test_map_inside_struct_with_special_field_names(self, spark):
        """Test maps inside structs where struct fields have special characters."""
        data = [
            (
                "value1",
                {
                    "field.with.dots": {"key1": "val1", "key2": "val2"},
                    "field with spaces": {"key3": "val3"},
                },
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "nested",
                    StructType(
                        [
                            StructField("field.with.dots", MapType(StringType(), StringType()), True),
                            StructField("field with spaces", MapType(StringType(), StringType()), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.transform_keys(col_expr, lambda k, v: F.upper(k))

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={MapType})

        result_data = result_df.collect()
        # Check maps in struct fields with special names
        assert "KEY1" in result_data[0]["nested"]["field.with.dots"]
        assert "KEY2" in result_data[0]["nested"]["field.with.dots"]
        assert "KEY3" in result_data[0]["nested"]["field with spaces"]

    def test_deeply_nested_struct_all_special_characters(self, spark):
        """Test deeply nested structs where all levels have special character field names."""
        data = [
            (
                "value1",
                {
                    "level.1": {
                        "level 2": {
                            "level@3": "deep_value",
                        }
                    }
                },
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "root.field",
                    StructType(
                        [
                            StructField(
                                "level.1",
                                StructType(
                                    [
                                        StructField(
                                            "level 2",
                                            StructType(
                                                [
                                                    StructField("level@3", StringType(), True),
                                                ]
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.upper(col_expr)

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={StringType})

        result_data = result_df.collect()
        # Navigate through all the special-named fields
        assert result_data[0]["root.field"]["level.1"]["level 2"]["level@3"] == "DEEP_VALUE"

    def test_struct_fields_with_confusing_bracket_patterns(self, spark):
        """Test field names with empty brackets and patterns that look like nested access.

        This covers:
        - Empty brackets: field[], field(), field{}
        - Just brackets: [], (), {}
        - Patterns that look like nested access: field_array[].something, struct.field[0].value
        These could be confused with actual array/map access but are literal field names.
        """
        data = [
            (
                "value1",
                {
                    # Empty brackets
                    "field[]": "test1",
                    "field()": "test2",
                    "field{}": "test3",
                    "[]": "test4",
                    "()": "test5",
                    "{}": "test6",
                    # Field names that look like nested access patterns
                    "field_array[].something": "test7",
                    "struct.field[0].value": "test8",
                    "map{key}.property": "test9",
                    "nested[].struct.field": "test10",
                    "a.b.c[0](x).y": "test11",
                },
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "nested",
                    StructType(
                        [
                            # Empty brackets
                            StructField("field[]", StringType(), True),
                            StructField("field()", StringType(), True),
                            StructField("field{}", StringType(), True),
                            StructField("[]", StringType(), True),
                            StructField("()", StringType(), True),
                            StructField("{}", StringType(), True),
                            # Nested-access-like patterns
                            StructField("field_array[].something", StringType(), True),
                            StructField("struct.field[0].value", StringType(), True),
                            StructField("map{key}.property", StringType(), True),
                            StructField("nested[].struct.field", StringType(), True),
                            StructField("a.b.c[0](x).y", StringType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.upper(col_expr)

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={StringType})

        result_data = result_df.collect()
        nested = result_data[0]["nested"]

        # Empty brackets
        assert nested["field[]"] == "TEST1"
        assert nested["field()"] == "TEST2"
        assert nested["field{}"] == "TEST3"
        assert nested["[]"] == "TEST4"
        assert nested["()"] == "TEST5"
        assert nested["{}"] == "TEST6"

        # Nested-access-like patterns
        assert nested["field_array[].something"] == "TEST7"
        assert nested["struct.field[0].value"] == "TEST8"
        assert nested["map{key}.property"] == "TEST9"
        assert nested["nested[].struct.field"] == "TEST10"
        assert nested["a.b.c[0](x).y"] == "TEST11"

    def test_top_level_columns_with_special_characters(self, spark):
        """Test that top-level columns with special characters are handled correctly."""
        data = [
            ("value1", "value2", "value3", "value4"),
        ]
        schema = StructType(
            [
                StructField("normal_field", StringType(), True),
                StructField("field.with.dots", StringType(), True),
                StructField("field with spaces", StringType(), True),
                StructField("field[]", StringType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        def transform_func(col_expr: Column, data_type) -> Column:
            return F.upper(col_expr)

        result_df = apply_transformation_to_nested_structures(df, transform_func, target_types={StringType})

        result_data = result_df.collect()
        assert result_data[0]["normal_field"] == "VALUE1"
        assert result_data[0]["field.with.dots"] == "VALUE2"
        assert result_data[0]["field with spaces"] == "VALUE3"
        assert result_data[0]["field[]"] == "VALUE4"
