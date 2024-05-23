import pytest
from chispa import assert_df_equality

from pyspark.sql.types import *

from koheesio.spark.readers.databricks.autoloader import AutoLoader


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
    return self.spark.read.format("json").options(**self.options)


def test_read_json(spark, mocker, data_path):
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
