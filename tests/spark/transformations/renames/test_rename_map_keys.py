"""Tests for RenameMapKeys transformation."""

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

from koheesio.spark.transformations.renames import RenameMapKeys


class TestRenameMapKeys:
    """Test suite for RenameMapKeys transformation."""

    def test_simple_map_key_rename(self, spark):
        """Test renaming keys in a simple map column."""
        data = [
            ("value1", {"firstName": "John", "lastName": "Doe"}),
            ("value2", {"firstName": "Jane", "lastName": "Smith"}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("person_data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        renamer = RenameMapKeys()
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df
        result_data = result_df.collect()

        # Verify map keys are renamed
        assert result_data[0]["person_data"]["first_name"] == "John"
        assert result_data[0]["person_data"]["last_name"] == "Doe"
        assert result_data[1]["person_data"]["first_name"] == "Jane"

    def test_map_key_rename_with_nested_struct(self, spark):
        """Test renaming map keys when map is nested in a struct."""
        data = [
            (
                "value1",
                {
                    "userData": {"firstName": "John", "lastName": "Doe"},
                    "metaData": {"createdBy": "admin"},
                },
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "nested_info",
                    StructType(
                        [
                            StructField("userData", MapType(StringType(), StringType()), True),
                            StructField("metaData", MapType(StringType(), StringType()), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        renamer = RenameMapKeys()
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df
        result_data = result_df.collect()

        # Verify map keys are renamed in nested struct
        assert result_data[0]["nested_info"]["userData"]["first_name"] == "John"
        assert result_data[0]["nested_info"]["metaData"]["created_by"] == "admin"

    def test_map_key_rename_with_nested_array(self, spark):
        """Test renaming map keys when map is nested in an array."""
        data = [
            (
                "value1",
                [
                    {"firstName": "John", "lastName": "Doe"},
                    {"firstName": "Jane", "lastName": "Smith"},
                ],
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("people_list", ArrayType(MapType(StringType(), StringType())), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        renamer = RenameMapKeys()
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df
        result_data = result_df.collect()

        # Verify map keys are renamed in array
        assert result_data[0]["people_list"][0]["first_name"] == "John"
        assert result_data[0]["people_list"][1]["first_name"] == "Jane"

    def test_nested_map_key_rename(self, spark):
        """Test renaming keys in nested maps (map values are also maps)."""
        data = [
            (
                "value1",
                {
                    "userData": {"firstName": "John", "lastName": "Doe"},
                    "addressData": {"streetName": "Main St", "cityName": "NYC"},
                },
            ),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField(
                    "all_data",
                    MapType(StringType(), MapType(StringType(), StringType())),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)

        renamer = RenameMapKeys()
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df
        result_data = result_df.collect()

        # Verify both outer and inner map keys are renamed
        assert result_data[0]["all_data"]["user_data"]["first_name"] == "John"
        assert result_data[0]["all_data"]["address_data"]["street_name"] == "Main St"

    def test_custom_rename_function(self, spark):
        """Test using a custom rename function."""
        from pyspark.sql import Column

        data = [
            ("value1", {"firstName": "John", "lastName": "Doe"}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("person_data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        # Custom function to convert to uppercase - must handle Column type for PySpark operations
        def to_upper(s):
            if isinstance(s, Column):
                return F.upper(s)
            return s.upper()

        renamer = RenameMapKeys(rename_func=to_upper)
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df
        result_data = result_df.collect()

        # Verify custom function was used
        assert result_data[0]["person_data"]["FIRSTNAME"] == "John"
        assert result_data[0]["person_data"]["LASTNAME"] == "Doe"

    def test_empty_and_null_maps(self, spark):
        """Test handling of empty and null maps."""
        data = [
            ("value1", {}),
            ("value2", None),
            ("value3", {"firstName": "Jane"}),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("person_data", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        renamer = RenameMapKeys()
        renamer.df = df
        renamer.execute()

        result_df = renamer.output.df
        result_data = result_df.collect()

        # Verify empty and null maps are preserved
        assert result_data[0]["person_data"] == {}
        assert result_data[1]["person_data"] is None
        assert result_data[2]["person_data"]["first_name"] == "Jane"
