import unittest
import pandas as pd
from src.koheesio.categorical_encoding import categorical_encoding, EncodingConfig

class TestCategoricalEncoding(unittest.TestCase):
    def setUp(self):
        self.data = pd.DataFrame({
            'color': ['red', 'blue', 'green', 'red'],
            'size': ['S', 'M', 'L', 'XL'],
            'price': [10.0, 20.0, 30.0, 40.0]
        })

    def test_one_hot_encoding(self):
        # Configure encoding to drop the first column (avoid dummy variable trap)
        config = EncodingConfig(drop_first=True)
        result = categorical_encoding(self.data, ['color', 'size'], config)
        # Check if the encoded columns are present
        self.assertIn('color_green', result.columns)
        self.assertIn('color_red', result.columns)
        self.assertIn('size_M', result.columns)
        self.assertIn('size_XL', result.columns)
        # Check if the original categorical columns were removed
        self.assertNotIn('color', result.columns)
        self.assertNotIn('size', result.columns)

    def test_no_drop_first_encoding(self):
        # Configure encoding to retain all dummy columns
        config = EncodingConfig(drop_first=False)
        result = categorical_encoding(self.data, ['color', 'size'], config)
        # Check for all encoded columns
        self.assertIn('color_blue', result.columns)
        self.assertIn('color_green', result.columns)
        self.assertIn('color_red', result.columns)
        self.assertIn('size_S', result.columns)
        self.assertIn('size_M', result.columns)
        self.assertIn('size_XL', result.columns)
        # Check if the original categorical columns were removed
        self.assertNotIn('color', result.columns)
        self.assertNotIn('size', result.columns)

if __name__ == '__main__':
    unittest.main()
