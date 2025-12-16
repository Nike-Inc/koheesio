"""Tests for RenameColumnsMapKeys combined transformation."""

from pyspark.sql import Column
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType
from pyspark.testing.utils import assertSchemaEqual

from koheesio.spark.transformations.renames import RenameColumnsMapKeys


class TestRenameColumnsMapKeys:
    """Test suite for RenameColumnsMapKeys combined transformation."""

    def test_rename_both_columns_and_map_keys_simple(self, spark):
        """Test renaming both column names and map keys in a simple case."""
        data = [
            ("value1", {"firstName": "John", "lastName": "Doe"}),
            ("value2", {"firstName": "Jane", "lastName": "Smith"}),
        ]
        schema = StructType(
            [
                StructField("camelCaseId", StringType(), True),
                StructField("personData", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        renamer = RenameColumnsMapKeys()
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df

        # Verify column names are renamed
        expected_schema = StructType(
            [
                StructField("camel_case_id", StringType(), True),
                StructField("person_data", MapType(StringType(), StringType()), True),
            ]
        )
        assertSchemaEqual(result_df.schema, expected_schema)

        # Verify map keys are renamed
        result_data = result_df.collect()
        assert result_data[0]["person_data"]["first_name"] == "John"
        assert result_data[0]["person_data"]["last_name"] == "Doe"

    def test_rename_nested_struct_columns_and_map_keys(self, spark):
        """Test renaming columns and map keys in nested struct."""
        data = [
            (
                "value1",
                {
                    "innerField": "test",
                    "mapField": {"firstName": "John", "lastName": "Doe"},
                },
            ),
        ]
        schema = StructType(
            [
                StructField("topLevelId", StringType(), True),
                StructField(
                    "nestedStruct",
                    StructType(
                        [
                            StructField("innerField", StringType(), True),
                            StructField("mapField", MapType(StringType(), StringType()), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        renamer = RenameColumnsMapKeys()
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df

        # Verify nested column names are renamed
        expected_schema = StructType(
            [
                StructField("top_level_id", StringType(), True),
                StructField(
                    "nested_struct",
                    StructType(
                        [
                            StructField("inner_field", StringType(), True),
                            StructField("map_field", MapType(StringType(), StringType()), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        assertSchemaEqual(result_df.schema, expected_schema)

        # Verify map keys are renamed
        result_data = result_df.collect()
        assert result_data[0]["nested_struct"]["map_field"]["first_name"] == "John"

    def test_rename_deeply_nested_maps_in_maps(self, spark):
        """Test renaming nested map keys (map values are also maps)."""
        data = [
            (
                "value1",
                {
                    "userData": {"firstName": "John", "lastName": "Doe"},
                    "addressData": {"streetName": "Main St"},
                },
            ),
        ]
        schema = StructType(
            [
                StructField("topId", StringType(), True),
                StructField(
                    "nestedMaps",
                    MapType(StringType(), MapType(StringType(), StringType())),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        renamer = RenameColumnsMapKeys()
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df

        # Verify column names are renamed
        expected_schema = StructType(
            [
                StructField("top_id", StringType(), True),
                StructField(
                    "nested_maps",
                    MapType(StringType(), MapType(StringType(), StringType())),
                    True,
                ),
            ]
        )
        assertSchemaEqual(result_df.schema, expected_schema)

        # Verify both outer and inner map keys are renamed
        result_data = result_df.collect()
        assert result_data[0]["nested_maps"]["user_data"]["first_name"] == "John"
        assert result_data[0]["nested_maps"]["address_data"]["street_name"] == "Main St"

    def test_complex_nested_structure(self, spark):
        """Test with a complex nested structure combining structs, arrays, and maps."""
        data = [
            (
                "value1",
                {
                    "companyInfo": {
                        "companyName": "Acme Corp",
                        "employees": [
                            {
                                "employeeId": "emp1",
                                "employeeDetails": {"firstName": "John", "lastName": "Doe"},
                            }
                        ],
                    }
                },
            ),
        ]
        schema = StructType(
            [
                StructField("recordId", StringType(), True),
                StructField(
                    "dataContainer",
                    StructType(
                        [
                            StructField(
                                "companyInfo",
                                StructType(
                                    [
                                        StructField("companyName", StringType(), True),
                                        StructField(
                                            "employees",
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField("employeeId", StringType(), True),
                                                        StructField(
                                                            "employeeDetails",
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
        )
        df = spark.createDataFrame(data, schema)

        renamer = RenameColumnsMapKeys()
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df
        result_data = result_df.collect()

        # Verify column names are renamed
        assert "record_id" in result_df.columns
        assert "data_container" in result_df.columns

        # Verify map keys are renamed
        employee_details = result_data[0]["data_container"]["company_info"]["employees"][0]["employee_details"]
        assert employee_details["first_name"] == "John"
        assert employee_details["last_name"] == "Doe"

    def test_custom_rename_function(self, spark):
        """Test using a custom rename function for both columns and keys."""
        data = [
            ("value1", {"firstName": "John", "lastName": "Doe"}),
        ]
        schema = StructType(
            [
                StructField("userId", StringType(), True),
                StructField("userData", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        # Custom function to add prefix - must handle both str (for column names) and Column (for map keys)
        def add_prefix(s):
            if isinstance(s, Column):
                return F.concat(F.lit("prefix_"), s)
            return f"prefix_{s}"

        renamer = RenameColumnsMapKeys(rename_func=add_prefix)
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df

        # Verify column names use custom function
        assert "prefix_userId" in result_df.columns
        assert "prefix_userData" in result_df.columns

        # Verify map keys use custom function
        result_data = result_df.collect()
        assert result_data[0]["prefix_userData"]["prefix_firstName"] == "John"
