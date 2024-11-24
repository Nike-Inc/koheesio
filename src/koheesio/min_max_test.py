import unittest
from min_max import min_max_normalize

class TestMinMaxNormalize(unittest.TestCase):
    def test_standard_case(self):
        data = [10, 20, 30, 40, 50]
        result = min_max_normalize(data, 0, 1)
        expected = [0.0, 0.25, 0.5, 0.75, 1.0]
        self.assertEqual(result, expected)

    def test_identical_values(self):
        data = [10, 10, 10]
        result = min_max_normalize(data, 0, 1)
        expected = [0.0, 0.0, 0.0]  # All values map to new_min
        self.assertEqual(result, expected)

    def test_negative_values(self):
        data = [-50, -25, 0, 25, 50]
        result = min_max_normalize(data, 0, 1)
        expected = [0.0, 0.25, 0.5, 0.75, 1.0]
        self.assertEqual(result, expected)

    def test_empty_data(self):
        with self.assertRaises(ValueError):
            min_max_normalize([], 0, 1)

    def test_negative_target_range(self):
        """Test normalization with a negative target range."""
        data = [10, 20, 30, 40, 50]
        result = min_max_normalize(data, -5, -1)
        expected = [-5.0, -4.0, -3.0, -2.0, -1.0]  # Map to the range [-5, -1]
        self.assertEqual(result, expected)

if __name__ == "__main__":
    unittest.main()
