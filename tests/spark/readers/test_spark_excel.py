import datetime
from pathlib import Path

from koheesio.spark.readers.excel import ExcelReader


def test_excel_reader(spark, data_path):
    # Define the path to the test Excel file and the expected DataFrame
    test_file = Path(data_path) / "readers" / "excel_file" / "dummy.xlsx"

    # Initialize the ExcelReader with the path to the test file and the correct sheet name
    reader = ExcelReader(path=test_file, sheet_name="sheet_to_select", header=0)

    # Execute the reader
    reader.execute()

    # Define the expected DataFrame
    expected_df = spark.createDataFrame(
        [
            ("foo", "bar", "baz", None, datetime.datetime(2024, 1, 1, 0, 0)),
            ("so long", "and thanks", "for all the fish", 42, datetime.datetime(2024, 1, 1, 0, 0)),
        ],
        ["a", "b", "c", "d", "e"],
    )

    # Assert that the output DataFrame is as expected
    assert sorted(reader.output.df.collect()) == sorted(expected_df.collect())
