import unittest
import pandas as pd
from src.koheesio.categorical_encoding  import PandasCategoricalEncoding
import unittest
import pandas as pd

class TestPandasCategoricalEncoding(unittest.TestCase):

    """
    Unit tests for the PandasCategoricalEncoding class.

    Methods
    -------
    setUp():
        Sets up the test environment with sample data.
    test_one_hot_encoding():
        Tests the one-hot encoding functionality.
    test_ordinal_encoding():
        Tests the ordinal encoding functionality.
    """

    def setUp(self):
        """
        Sets up the test environment with sample data.

        Creates a pandas DataFrame with categorical columns for testing.
        """
        self.data = pd.DataFrame({
            'color': ['red', 'blue', 'green', 'blue', 'red'],
            'size': ['S', 'M', 'L', 'S', 'XL']
        })

    def test_one_hot_encoding(self):
        """
        Tests one-hot encoding functionality.

        Ensures that the specified categorical columns are correctly 
        encoded into dummy variables, with an option to drop the first 
        dummy column.
        """
        encoding_step = PandasCategoricalEncoding(
            columns=["color"],
            encoding_type="one-hot",
            drop_first=False  # Adjusted to match expected columns
        )
        
        # Apply encoding
        encoded_data = encoding_step.execute(self.data)
        
        # Expected columns after one-hot encoding
        expected_columns = ['color_blue', 'color_green', 'color_red']
        
        # Check if the encoded data contains expected columns
        self.assertTrue(all(col in encoded_data.columns for col in expected_columns))
        print("One-hot encoding passed.")

    def test_ordinal_encoding(self):
        """
        Tests ordinal encoding functionality.

        Ensures that the specified categorical columns are correctly 
        encoded into ordinal values based on a provided mapping.
        """
        
        ordinal_mapping = {
            'size': {'S': 1, 'M': 2, 'L': 3, 'XL': 4}
        }
        
        encoding_step = PandasCategoricalEncoding(
            columns=["size"], 
            encoding_type="ordinal", 
            ordinal_mapping=ordinal_mapping
        )
        
        # Apply encoding
        encoded_data = encoding_step.execute(self.data)
        
        # Check if the "size" column is correctly ordinal encoded
        expected_values = [1, 2, 3, 1, 4]
        self.assertTrue(all(encoded_data['size'] == expected_values))
        print("Ordinal encoding passed.")
