import unittest
import pandas as pd
from src.koheesio.weighted_moving_average import weighted_moving_average

class TestWeightedMovingAverage(unittest.TestCase):
    def setUp(self):
        self.data = pd.DataFrame({
            'store_id': [1, 1, 1, 2, 2, 2],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-01', '2023-01-02', '2023-01-03'],
            'sales': [100, 120, 110, 200, 210, 220]
        })

    def test_default_weights(self):
        result = weighted_moving_average(self.data, column='sales', window_size=3, group_by=['store_id'])
        self.assertAlmostEqual(result['WMA'].iloc[2], 111.67, places=2)  # For store_id=1
        self.assertAlmostEqual(result['WMA'].iloc[5], 213.33, places=2)  # For store_id=2

    def test_custom_weights(self):
        result = weighted_moving_average(self.data, column='sales', window_size=3, weights=[3, 2, 1], group_by=['store_id'])
        self.assertAlmostEqual(result['WMA'].iloc[2], 108.33, places=2)  # For store_id=1
        self.assertAlmostEqual(result['WMA'].iloc[5], 206.67, places=2)  # For store_id=2

    def test_no_grouping(self):
        data = pd.DataFrame({'sales': [10, 20, 30, 40, 50]})
        result = weighted_moving_average(data, column='sales', window_size=3)
        self.assertAlmostEqual(result['WMA'].iloc[2], 23.33, places=2)
        self.assertAlmostEqual(result['WMA'].iloc[4], 43.33, places=2)

if __name__ == '__main__':
    unittest.main()
