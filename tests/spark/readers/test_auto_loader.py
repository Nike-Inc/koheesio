from chispa import assert_df_equality
import pytest

from pyspark.sql.types import *

from koheesio.spark.readers.databricks.autoloader import AutoLoader

pytestmark = pytest.mark.spark


@pytest.mark.parametrize(
    "bad_format",
    [
        ("mp3"),
        ("mp4"),
    ],
)
def test_invalid_format(bad_format):
    with pytest.raises(AttributeError):
        AutoLoader(format=bad_format, location="some_path", schema_location="some_schema_location")


def mock_reader(self):
    reader = self.spark.read.format("json")
    if self.schema_ is not None:
        reader = reader.schema(self.schema_)
    return reader.options(**self.options)


def test_read_json_infer_schema(spark, mocker, data_path):
    mocker.patch("koheesio.spark.readers.databricks.autoloader.AutoLoader.reader", mock_reader)

    options = {"multiLine": "true"}

    json_file_path_str = f"{data_path}/readers/json_file/dummy.json"
    auto_loader = AutoLoader(format="json", location=json_file_path_str, schema_location="dummy_value", options=options)

    auto_loader.execute()
    result = auto_loader.output.df

    schema_expected = StructType(
        [
            StructField("string", StringType(), True),
            StructField("int", LongType(), True),
            StructField("array", ArrayType(LongType()), True),
        ]
    )

    data_expected = [
        {"string": "string1", "int": 1, "array": [1, 11, 111]},
        {"string": "string2", "int": 2, "array": [2, 22, 222]},
    ]
    expected_df = spark.createDataFrame(data_expected, schema_expected)
    assert_df_equality(result, expected_df, ignore_column_order=True)


def test_read_json_exact_explicit_schema_struct(spark, mocker, data_path):
    mocker.patch("koheesio.spark.readers.databricks.autoloader.AutoLoader.reader", mock_reader)

    schema = StructType(
        [
            StructField("string", StringType(), True),
            StructField("int", LongType(), True),
            StructField("array", ArrayType(LongType()), True),
        ]
    )
    options = {"multiLine": "true"}
    json_file_path_str = f"{data_path}/readers/json_file/dummy.json"
    auto_loader = AutoLoader(
        format="json", location=json_file_path_str, schema_location="dummy_value", options=options, schema=schema
    )

    auto_loader.execute()
    result = auto_loader.output.df

    data_expected = [
        {"string": "string1", "int": 1, "array": [1, 11, 111]},
        {"string": "string2", "int": 2, "array": [2, 22, 222]},
    ]
    expected_df = spark.createDataFrame(data_expected, schema)
    assert_df_equality(result, expected_df, ignore_column_order=True)


def test_read_json_different_explicit_schema_string(spark, mocker, data_path):
    mocker.patch("koheesio.spark.readers.databricks.autoloader.AutoLoader.reader", mock_reader)

    schema = "string STRING,array ARRAY<BIGINT>"
    options = {"multiLine": "true"}
    json_file_path_str = f"{data_path}/readers/json_file/dummy.json"
    auto_loader = AutoLoader(
        format="json", location=json_file_path_str, schema_location="dummy_value", options=options, schema=schema
    )

    auto_loader.execute()
    result = auto_loader.output.df

    data_expected = [
        {"string": "string1", "array": [1, 11, 111]},
        {"string": "string2", "array": [2, 22, 222]},
    ]
    expected_df = spark.createDataFrame(data_expected, schema)
    assert_df_equality(result, expected_df, ignore_column_order=True)
