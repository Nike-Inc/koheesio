from datetime import datetime, timezone
import gzip
from importlib.util import find_spec

import pytest

from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from koheesio.spark.writers.buffer import PandasCsvBufferWriter, PandasJsonBufferWriter

pytestmark = pytest.mark.spark

# Test data has two columns: email and sha256_email
test_data = [
    (
        "john.doe@email.com",
        "55f537baf75630a85fda6540cdf43859a51ab13ba33dc314600618f89a6ffa0e",
    ),
    (
        "jane.doe@email.com",
        "390852f40addcb9e2868f180b471ef4315a34063f145447d73f51f5e5ad1ef47",
    ),
    (
        "foo@bar.baz",
        "80c66bdd90ae7fd4378cef780422fe428ee7fb526301f7b236113c4ece3be146",
    ),
]
test_schema = ["email", "sha256_email"]


def test_canary():
    """Check that pandas is installed"""
    assert find_spec("pandas") is not None, "pandas is not installed"


@pytest.mark.parametrize(
    "data,schema,params,expected_data",
    [
        (
            test_data,
            test_schema,
            {"header": True},
            "email,sha256_email\n"
            "john.doe@email.com,55f537baf75630a85fda6540cdf43859a51ab13ba33dc314600618f89a6ffa0e\n"
            "jane.doe@email.com,390852f40addcb9e2868f180b471ef4315a34063f145447d73f51f5e5ad1ef47\n"
            "foo@bar.baz,80c66bdd90ae7fd4378cef780422fe428ee7fb526301f7b236113c4ece3be146",
        ),
        (
            # Change the separator to ; and no header
            test_data,
            test_schema,
            {"header": False, "sep": ";"},
            "john.doe@email.com;55f537baf75630a85fda6540cdf43859a51ab13ba33dc314600618f89a6ffa0e\n"
            "jane.doe@email.com;390852f40addcb9e2868f180b471ef4315a34063f145447d73f51f5e5ad1ef47\n"
            "foo@bar.baz;80c66bdd90ae7fd4378cef780422fe428ee7fb526301f7b236113c4ece3be146",
        ),
        (
            # test index, and columns
            test_data,
            test_schema,
            {"header": True, "index": True, "index_label": "i", "columns": ["email"]},
            "i,email\n0,john.doe@email.com\n1,jane.doe@email.com\n2,foo@bar.baz",
        ),
        (
            # test with compression
            test_data,
            test_schema,
            {"header": True, "compression": "gzip"},
            "email,sha256_email\n"
            "john.doe@email.com,55f537baf75630a85fda6540cdf43859a51ab13ba33dc314600618f89a6ffa0e\n"
            "jane.doe@email.com,390852f40addcb9e2868f180b471ef4315a34063f145447d73f51f5e5ad1ef47\n"
            "foo@bar.baz,80c66bdd90ae7fd4378cef780422fe428ee7fb526301f7b236113c4ece3be146",
        ),
        (
            # test with timestamp_format
            [(datetime(1964, 1, 25, tzinfo=timezone.utc),), (datetime(1971, 5, 30, tzinfo=timezone.utc),)],
            StructType([StructField("date", TimestampType())]),
            {"header": False},  # default timestamp_format is "%Y-%m-%dT%H:%M:%S.%f"
            "1964-01-25T00:00:00.000000\n1971-05-30T00:00:00.000000",
        ),
    ],
)
def test_pandas_csv_buffer_writer(data, schema, params, expected_data, spark):
    writer = PandasCsvBufferWriter(**params)

    # Execute the writer
    writer.write(
        df=spark.createDataFrame(
            data,
            schema,
        )
    )
    # Get the CSV string from the buffer
    actual = writer.output.read()

    if params.get("compression"):
        # Check that the compression is correct by checking the first two bytes (magic number)
        assert actual.startswith(b"\x1f\x8b")
        # Decompress the data to check the content
        actual = gzip.decompress(actual)

    # Check that the CSV string matches the expected data
    assert actual.decode("utf-8").strip() == expected_data


@pytest.mark.parametrize(
    "data,schema,params,expected_data",
    [
        (
            # Test with default parameters
            test_data,
            test_schema,
            {},  # should be the same as {"orient": "records", "lines": True},
            '{"email":"john.doe@email.com",'
            '"sha256_email":"55f537baf75630a85fda6540cdf43859a51ab13ba33dc314600618f89a6ffa0e"}\n'
            '{"email":"jane.doe@email.com",'
            '"sha256_email":"390852f40addcb9e2868f180b471ef4315a34063f145447d73f51f5e5ad1ef47"}\n'
            '{"email":"foo@bar.baz",'
            '"sha256_email":"80c66bdd90ae7fd4378cef780422fe428ee7fb526301f7b236113c4ece3be146"}',
        ),
        (
            # Test with orient="split", only one column
            test_data,
            test_schema,
            {"orient": "split", "columns": ["email"]},
            "{"
            '"columns":["email"],'
            '"index":[0,1,2],'
            '"data":[["john.doe@email.com"],["jane.doe@email.com"],["foo@bar.baz"]]'
            "}",
        ),
        (
            # Test with orient="index"
            test_data,
            test_schema,
            {"orient": "index"},
            "{"
            '"0":{'
            '"email":"john.doe@email.com",'
            '"sha256_email":"55f537baf75630a85fda6540cdf43859a51ab13ba33dc314600618f89a6ffa0e"},'
            '"1":{'
            '"email":"jane.doe@email.com",'
            '"sha256_email":"390852f40addcb9e2868f180b471ef4315a34063f145447d73f51f5e5ad1ef47"},'
            '"2":{'
            '"email":"foo@bar.baz",'
            '"sha256_email":"80c66bdd90ae7fd4378cef780422fe428ee7fb526301f7b236113c4ece3be146"}'
            "}",
        ),
        (
            # Test with orient="columns", only one column
            test_data,
            test_schema,
            {"orient": "columns", "columns": ["email"]},
            '{"email":{"0":"john.doe@email.com","1":"jane.doe@email.com","2":"foo@bar.baz"}}',
        ),
        (
            # Test with orient="values"
            test_data,
            test_schema,
            {"orient": "values"},
            "["
            '["john.doe@email.com","55f537baf75630a85fda6540cdf43859a51ab13ba33dc314600618f89a6ffa0e"],'
            '["jane.doe@email.com","390852f40addcb9e2868f180b471ef4315a34063f145447d73f51f5e5ad1ef47"],'
            '["foo@bar.baz","80c66bdd90ae7fd4378cef780422fe428ee7fb526301f7b236113c4ece3be146"]'
            "]",
        ),
        (
            # Test with orient="records", and lines=False, only one column
            test_data,
            test_schema,
            {"orient": "records", "lines": False, "columns": ["email"]},
            '[{"email":"john.doe@email.com"},{"email":"jane.doe@email.com"},{"email":"foo@bar.baz"}]',
        ),
        (
            # Test with orient="table"
            test_data,
            test_schema,
            {"orient": "table"},
            "{"
            '"schema":{'
            '"fields":[{"name":"index","type":"integer"},'
            '{"name":"email","type":"string"},'
            '{"name":"sha256_email","type":"string"}],'
            '"primaryKey":["index"],'
            '"pandas_version":"1.4.0"},'
            '"data":['
            '{"index":0,"email":"john.doe@email.com",'
            '"sha256_email":"55f537baf75630a85fda6540cdf43859a51ab13ba33dc314600618f89a6ffa0e"},'
            '{"index":1,"email":"jane.doe@email.com",'
            '"sha256_email":"390852f40addcb9e2868f180b471ef4315a34063f145447d73f51f5e5ad1ef47"},'
            '{"index":2,"email":"foo@bar.baz",'
            '"sha256_email":"80c66bdd90ae7fd4378cef780422fe428ee7fb526301f7b236113c4ece3be146"}]}',
        ),
        (
            # Test with integer, float, and boolean columns
            [
                {"age": 30, "rating": 4.5, "is_active": True},
                {"age": 25, "rating": 4.7, "is_active": False},
                {"age": 35, "rating": 4.3, "is_active": True},
            ],
            StructType(
                [
                    StructField("age", IntegerType(), True),
                    StructField("rating", FloatType(), True),
                    StructField("is_active", BooleanType(), True),
                ]
            ),
            {"double_precision": 2},
            '{"age":30,"rating":4.5,"is_active":true}\n'
            '{"age":25,"rating":4.7,"is_active":false}\n'
            '{"age":35,"rating":4.3,"is_active":true}',
        ),
    ],
)
def test_pandas_json_buffer_writer(data, schema, params, expected_data, spark):
    writer = PandasJsonBufferWriter(**params)

    # Execute the writer
    writer.write(
        df=spark.createDataFrame(
            data,
            schema,
        )
    )

    # Get the JSON string from the buffer
    json_str = writer.output.read().decode("utf-8")

    # Check that the JSON string matches the expected data
    assert json_str.strip() == expected_data


def test_pandas_json_with_compression(spark):
    writer = PandasJsonBufferWriter(compression="gzip")

    # Execute the writer
    writer.write(
        df=spark.createDataFrame(
            test_data,
            test_schema,
        )
    )

    # Check that the compression is correct by checking the first two bytes (magic number)
    assert writer.output.read().startswith(b"\x1f\x8b")

    # Decompress the data to check the content
    json_str = gzip.decompress(writer.output.read()).decode("utf-8")

    # Check that the JSON string matches the expected data
    assert json_str.strip() == (
        '{"email":"john.doe@email.com",'
        '"sha256_email":"55f537baf75630a85fda6540cdf43859a51ab13ba33dc314600618f89a6ffa0e"}\n'
        '{"email":"jane.doe@email.com",'
        '"sha256_email":"390852f40addcb9e2868f180b471ef4315a34063f145447d73f51f5e5ad1ef47"}\n'
        '{"email":"foo@bar.baz",'
        '"sha256_email":"80c66bdd90ae7fd4378cef780422fe428ee7fb526301f7b236113c4ece3be146"}'
    )
