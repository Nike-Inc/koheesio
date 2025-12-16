"""Tests for string utilities."""

import pytest

from pyspark.sql import Column
import pyspark.sql.functions as F
from pyspark.sql.types import MapType, StringType, StructField, StructType

from koheesio.spark.utils.string import AnyToSnakeConverter


class TestAnyToSnakeConverter:
    """Test suite for AnyToSnakeConverter class."""

    def test_all_naming_conventions(self):
        """Test converting all supported naming conventions to snake_case."""
        converter = AnyToSnakeConverter()

        # camelCase
        assert converter.convert("camelCase") == "camel_case"
        assert converter.convert("firstName") == "first_name"
        assert converter.convert("getHTTPResponseCode") == "get_http_response_code"

        # PascalCase
        assert converter.convert("PascalCase") == "pascal_case"
        assert converter.convert("FirstName") == "first_name"
        assert converter.convert("XMLParser") == "xml_parser"

        # Ada_Case
        assert converter.convert("Ada_Case") == "ada_case"
        assert converter.convert("First_Name") == "first_name"
        assert converter.convert("HTTP_Response_Code") == "http_response_code"

        # CONSTANT_CASE (SCREAMING_SNAKE_CASE)
        assert converter.convert("CONSTANT_CASE") == "constant_case"
        assert converter.convert("FIRST_NAME") == "first_name"
        assert converter.convert("MAX_RETRIES") == "max_retries"

        # camel_Snake_Case and Pascal_Snake_Case
        assert converter.convert("first_Name") == "first_name"
        assert converter.convert("camel_Snake_Case") == "camel_snake_case"
        assert converter.convert("Pascal_Snake_Case") == "pascal_snake_case"

        # flatcase and UPPERCASE
        assert converter.convert("firstname") == "firstname"
        assert converter.convert("FIRSTNAME") == "firstname"

    def test_convert_kebab_case_variants(self):
        """Test converting kebab-case (dash-based) naming conventions to snake_case."""
        converter = AnyToSnakeConverter()

        # kebab-case (dash-case, lisp-case, spinal-case)
        assert converter.convert("kebab-case") == "kebab_case"
        assert converter.convert("first-name") == "first_name"
        assert converter.convert("http-response-code") == "http_response_code"
        assert converter.convert("user-profile-settings") == "user_profile_settings"

        # TRAIN-CASE (COBOL-CASE, SCREAMING-KEBAB-CASE)
        assert converter.convert("TRAIN-CASE") == "train_case"
        assert converter.convert("FIRST-NAME") == "first_name"
        assert converter.convert("HTTP-STATUS-CODE") == "http_status_code"
        assert converter.convert("MAX-RETRY-COUNT") == "max_retry_count"

        # Train-Case (HTTP-Header-Case)
        assert converter.convert("Train-Case") == "train_case"
        assert converter.convert("First-Name") == "first_name"
        assert converter.convert("Content-Type") == "content_type"
        assert converter.convert("X-Request-Id") == "x_request_id"

    def test_convert_mixed_dash_and_underscore(self):
        """Test converting strings that mix dashes and underscores."""
        converter = AnyToSnakeConverter()

        # Mixed dash and underscore
        assert converter.convert("first_name-value") == "first_name_value"
        assert converter.convert("http-status_code") == "http_status_code"
        assert converter.convert("user-profile_settings-v2") == "user_profile_settings_v2"

        # Multiple consecutive dashes should become single underscore
        assert converter.convert("first--name") == "first_name"
        assert converter.convert("value---test") == "value_test"

    def test_edge_cases_and_special_patterns(self):
        """Test edge cases, consecutive uppercase, numbers, and special patterns."""
        converter = AnyToSnakeConverter()

        # Already snake_case, single words, empty
        assert converter.convert("snake_case") == "snake_case"
        assert converter.convert("word") == "word"
        assert converter.convert("Word") == "word"
        assert converter.convert("") == ""

    def test_convert_string_vs_column_dispatch(self, spark):
        """Test that converter dispatches correctly between str and Column types."""
        converter = AnyToSnakeConverter()

        # Test with string - should return string
        result_str = converter.convert("camelCase")
        assert isinstance(result_str, str)
        assert result_str == "camel_case"

        # Test with Column - should return Column
        col_expr = F.lit("camelCase")
        result_col = converter.convert(col_expr)
        assert isinstance(result_col, Column)

        # Verify Column conversion works in DataFrame
        df = spark.range(1).select(result_col.alias("output"))
        assert df.collect()[0]["output"] == "camel_case"

    def test_convert_column_with_dataframe(self, spark):
        """Test converting PySpark Column expressions in actual DataFrame."""
        converter = AnyToSnakeConverter()

        data = [("camelCase",), ("firstName",), ("XMLParser",)]
        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        # Apply converter to Column
        result_df = df.select(converter.convert(F.col("input")).alias("output"))
        result = [row["output"] for row in result_df.collect()]

        assert result[0] == "camel_case"
        assert result[1] == "first_name"
        assert result[2] == "xml_parser"

    def test_convert_in_transform_keys(self, spark):
        """Test using converter with transform_keys for map renaming."""
        converter = AnyToSnakeConverter()

        data = [
            ({"firstName": "John", "lastName": "Doe"},),
            ({"emailAddress": "test@example.com"},),
        ]
        schema = StructType([StructField("data", MapType(StringType(), StringType()), True)])
        df = spark.createDataFrame(data, schema)

        # Use converter in transform_keys
        result_df = df.select(F.transform_keys(F.col("data"), lambda k, v: converter.convert(k)).alias("data"))

        result = result_df.collect()
        assert "first_name" in result[0]["data"]
        assert "last_name" in result[0]["data"]
        assert "email_address" in result[1]["data"]

    def test_convert_with_nulls(self, spark):
        """Test that Column conversion handles null values correctly."""
        converter = AnyToSnakeConverter()

        data = [("camelCase",), (None,), ("firstName",)]
        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result_df = df.select(converter.convert(F.col("input")).alias("output"))
        result = [row["output"] for row in result_df.collect()]

        assert result[0] == "camel_case"
        assert result[1] is None
        assert result[2] == "first_name"

    def test_convert_consecutive_uppercase(self):
        """Test converting strings with consecutive uppercase letters."""
        converter = AnyToSnakeConverter()

        # Consecutive uppercase at start
        assert converter.convert("HTTPResponse") == "http_response"
        assert converter.convert("XMLParser") == "xml_parser"
        assert converter.convert("URLPath") == "url_path"

        # Consecutive uppercase in middle
        assert converter.convert("parseHTTPHeader") == "parse_http_header"
        assert converter.convert("getAPIKey") == "get_api_key"

    def test_convert_with_numbers(self):
        """Test converting strings with numbers."""
        converter = AnyToSnakeConverter()

        assert converter.convert("base64Encode") == "base64_encode"
        assert converter.convert("utf8Decode") == "utf8_decode"
        assert converter.convert("address2") == "address2"
        assert converter.convert("version2API") == "version2_api"

    def test_convert_mixed_formats(self):
        """Test converting strings that mix different naming conventions."""
        converter = AnyToSnakeConverter()

        # camelCase with some uppercase
        assert converter.convert("getHTTPSConnection") == "get_https_connection"

        # PascalCase with numbers
        assert converter.convert("Version2Parser") == "version2_parser"

        # Ada_Case mixed with camelCase
        assert converter.convert("First_nameValue") == "first_name_value"

    def test_convert_flat_case(self):
        """Test converting flat case (all lowercase, no separators)."""
        converter = AnyToSnakeConverter()

        # Flat case should remain unchanged
        assert converter.convert("firstname") == "firstname"
        assert converter.convert("email") == "email"

    def test_convert_leading_trailing_underscores(self):
        """Test handling of leading and trailing underscores."""
        converter = AnyToSnakeConverter()

        assert converter.convert("_privateVar") == "_private_var"
        assert converter.convert("__dunderMethod") == "_dunder_method"
        assert converter.convert("value_") == "value_"

    def test_convert_multiple_consecutive_underscores(self):
        """Test that multiple consecutive underscores are collapsed."""
        converter = AnyToSnakeConverter()

        # The converter should collapse multiple underscores to single underscore
        assert converter.convert("First__Name") == "first_name"
        assert converter.convert("value___test") == "value_test"

    def test_convert_single_letter_words(self):
        """Test converting strings with single letter words."""
        converter = AnyToSnakeConverter()

        assert converter.convert("aValue") == "a_value"
        assert converter.convert("getARecord") == "get_a_record"
        assert converter.convert("xCoordinate") == "x_coordinate"

    def test_convert_unsupported_type_raises_error(self):
        """Test that unsupported types raise NotImplementedError."""
        converter = AnyToSnakeConverter()

        with pytest.raises(NotImplementedError):
            converter.convert(123)

        with pytest.raises(NotImplementedError):
            converter.convert([])
