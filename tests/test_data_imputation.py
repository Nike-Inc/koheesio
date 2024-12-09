import unittest
import pandas as pd
from src.koheesio.data_imputation import impute_missing_values

class TestImputeMissingValues(unittest.TestCase):
    def setUp(self):
        self.data = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie', None],
            'age': [25, None, 30, 35],
            'score': [90, None, None, 80]
        })

    def test_mean_imputation(self):
        result = impute_missing_values(self.data.copy(), strategy="mean", columns=['age', 'score'])
        self.assertAlmostEqual(result['age'].iloc[1], 30.0, places=1)  # Mean of [25, 30, 35]
        self.assertAlmostEqual(result['score'].iloc[1], 85.0, places=1)  # Mean of [90, 80]

    def test_median_imputation(self):
        result = impute_missing_values(self.data.copy(), strategy="median", columns=['age', 'score'])
        self.assertEqual(result['age'].iloc[1], 30)  # Median of [25, 30, 35]
        self.assertEqual(result['score'].iloc[1], 85)  # Median of [90, 80]

    def test_mode_imputation(self):
        result = impute_missing_values(self.data.copy(), strategy="mode", columns=['name'])
        self.assertEqual(result['name'].iloc[3], 'Alice')  # Mode of ['Alice', 'Bob', 'Charlie']

    def test_constant_imputation(self):
        result = impute_missing_values(self.data.copy(), strategy="constant", fill_value=0, columns=['age', 'score'])
        self.assertEqual(result['age'].iloc[1], 0)
        self.assertEqual(result['score'].iloc[1], 0)

    def test_missing_column_error(self):
        with self.assertRaises(ValueError):
            impute_missing_values(self.data.copy(), strategy="constant", columns=['unknown'])

if __name__ == '__main__':
    unittest.main()
