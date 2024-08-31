import pytest

import pyspark.sql.types as T

from koheesio.spark import AnalysisException
from koheesio.spark.readers.file_loader import (
    AvroReader,
    CsvReader,
    FileFormat,
    FileLoader,
    JsonReader,
    OrcReader,
    ParquetReader,
)

pytestmark = pytest.mark.spark


@pytest.fixture()
def json_file(data_path):
    return f"{data_path}/readers/json_file/dummy_simple.json"


@pytest.fixture()
def csv_comma_file(data_path):
    return f"{data_path}/readers/csv_file/dummy.csv"


@pytest.fixture()
def csv_semicolon_file(data_path):
    return f"{data_path}/readers/csv_file/dummy_semicolon.csv"


@pytest.fixture()
def parquet_file(data_path):
    return f"{data_path}/readers/delta_file"


@pytest.fixture()
def avro_file(data_path):
    return f"{data_path}/readers/avro_file"


@pytest.fixture()
def orc_file(data_path):
    return f"{data_path}/readers/orc_file"


def test_file_loader(csv_comma_file):
    # test schema
    expected_data = [
        {"value": "string,int,float"},
        {"value": "string1,1,1.0"},
        {"value": "string2,2,2.0"},
        {"value": "string3,3,3.0"},
    ]
    schema = "value STRING"
    reader = FileLoader(path=csv_comma_file, header=True, schema=schema, lineSep="\n")
    assert reader.schema_ == schema
    df = reader.read()
    actual_data = [row.asDict() for row in df.collect()]
    print(f"{actual_data = }")
    assert actual_data == expected_data


def test_invalid_file_format(csv_comma_file):
    with pytest.raises(ValueError):
        FileLoader(format="invalid_format", path=csv_comma_file, header=True)


def test_csv_reader(csv_comma_file, csv_semicolon_file):
    expected_data = [
        {"string": "string1", "int": 1, "float": 1.0},
        {"string": "string2", "int": 2, "float": 2.0},
        {"string": "string3", "int": 3, "float": 3.0},
    ]

    schema = "string STRING, int INT, float FLOAT"

    # comma separated file
    reader = CsvReader(path=csv_comma_file, header=True, schema=schema)
    assert reader.path == csv_comma_file
    assert reader.header is True
    assert reader.format == FileFormat.csv.value
    df = reader.read()
    actual_data = [row.asDict() for row in df.collect()]
    assert actual_data == expected_data

    # semicolon separated file
    reader = CsvReader(path=csv_semicolon_file, header=True, sep=";", schema=schema)
    assert reader.path == csv_semicolon_file
    df = reader.read()
    actual_data = [row.asDict() for row in df.collect()]
    assert actual_data == expected_data


def test_json_reader(json_file):
    expected_data = [
        {"string": "string1", "int": 1, "float": 1.0},
        {"string": "string2", "int": 2, "float": 2.0},
        {"string": "string3", "int": 3, "float": 3.0},
    ]

    reader = JsonReader(path=json_file)
    assert reader.path == json_file
    df = reader.read()
    actual_data = [row.asDict() for row in df.collect()]
    assert actual_data == expected_data


def test_json_stream_reader(json_file):
    schema = "string STRING, int INT, float FLOAT"
    reader = JsonReader(path=json_file, schema=schema, streaming=True)
    assert reader.path == json_file
    df = reader.read()
    assert df.isStreaming
    assert df.schema == T._parse_datatype_string(schema)


def test_parquet_reader(parquet_file):
    expected_data = [
        {"id": 0},
        {"id": 1},
        {"id": 2},
    ]

    reader = ParquetReader(path=parquet_file)
    assert reader.path == parquet_file
    df = reader.read()
    actual_data = [row.asDict() for row in df.collect()[0:3]]
    assert actual_data == expected_data


def test_avro_reader(avro_file):
    expected_data = [
        {"string": "string1", "int": 1, "float": 1.0},
        {"string": "string2", "int": 2, "float": 2.0},
        {"string": "string3", "int": 3, "float": 3.0},
    ]

    reader = AvroReader(path=avro_file)

    try:
        assert reader.path == avro_file
        df = reader.read()
        actual_data = [row.asDict() for row in df.collect()]
        assert actual_data == expected_data
    except AnalysisException as e:
        # Avro is not always supported in all environments
        reader.log.error(e)
        assert True


def test_orc_reader(orc_file):
    expected_data = [
        {"string": "string1", "int": 1, "float": 1.0},
        {"string": "string2", "int": 2, "float": 2.0},
        {"string": "string3", "int": 3, "float": 3.0},
    ]

    reader = OrcReader(path=orc_file)
    assert reader.path == orc_file
    df = reader.read()
    actual_data = [row.asDict() for row in df.collect()]
    assert actual_data == expected_data
