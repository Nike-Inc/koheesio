from pathlib import Path

import pandas as pd

from koheesio.pandas.readers.excel import ExcelReader


def test_excel_reader(data_path):
    # Define the path to the test Excel file and the expected DataFrame
    test_file = Path(data_path) / "readers" / "excel_file" / "dummy.xlsx"
    expected_df = pd.DataFrame(
        {
            "a": ["foo", "so long"],
            "b": ["bar", "and thanks"],
            "c": ["baz", "for all the fish"],
            "d": [None, 42],
            "e": pd.to_datetime(["1/1/24", "1/1/24"], format="%m/%d/%y"),
        }
    )

    # Initialize the ExcelReader with the path to the test file and the correct sheet name
    reader = ExcelReader(path=test_file, sheet_name="sheet_to_select", header=0)

    # Execute the reader
    reader.execute()

    # Assert that the output DataFrame is as expected
    pd.testing.assert_frame_equal(reader.output.df, expected_df, check_dtype=False)
