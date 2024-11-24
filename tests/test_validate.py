import unittest
import pandas as pd
from src.koheesio.validate import validate_data, ValidationConfig

class TestValidateData(unittest.TestCase):
    def test_validate_data(self):
        # Prepare test data
        data = pd.DataFrame({
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
        })
        config = ValidationConfig(
            required_columns=['name', 'age'],
            dtype={'name': 'object', 'age': 'int64'}
        )
        result = validate_data(data, config)
        self.assertTrue(result)

    def test_missing_columns(self):
        data = pd.DataFrame({'name': ['Alice']})
        config = ValidationConfig(required_columns=['name', 'age'], dtype={})
        with self.assertRaises(ValueError):
            validate_data(data, config)

if __name__ == '__main__':
    unittest.main()
