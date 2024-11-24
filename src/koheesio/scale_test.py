import unittest
from scale import scale_data

class TestScaleData(unittest.TestCase):

    def test_scale_positive_numbers(self):
        """Test scaling a list of positive numbers."""
        data = [1, 2, 3, 4, 5]
        multiplier = 2
        result = scale_data(data, multiplier)
        expected = [2, 4, 6, 8, 10]
        self.assertEqual(result, expected)

    def test_scale_negative_numbers(self):
        """Test scaling a list of negative numbers."""
        data = [-1, -2, -3, -4, -5]
        multiplier = 3
        result = scale_data(data, multiplier)
        expected = [-3, -6, -9, -12, -15]
        self.assertEqual(result, expected)


        """Test scaling a list with both positive and negative numbers."""
        data = [-1, 0, 1]
        multiplier = 5
        result = scale_data(data, multiplier)
        expected = [-5, 0, 5]
        self.assertEqual(result, expected)

    def test_scale_with_zero_multiplier(self):
        """Test scaling numbers with a multiplier of zero."""
        data = [10, -10, 100]
        multiplier = 0
        result = scale_data(data, multiplier)
        expected = [0, 0, 0]
        self.assertEqual(result, expected)

    def test_scale_with_one_multiplier(self):
        """Test scaling numbers with a multiplier of one (should return the same list)."""
        data = [1.5, 2.5, 3.5]
        multiplier = 1
        result = scale_data(data, multiplier)
        expected = [1.5, 2.5, 3.5]
        self.assertEqual(result, expected)

    def test_scale_empty_list(self):
        """Test scaling an empty list."""
        data = []
        multiplier = 2
        result = scale_data(data, multiplier)
        expected = []
        self.assertEqual(result, expected)

  
        """Test scaling a list with very large numbers."""
        data = [1e10, 2e10, 3e10]
        multiplier = 10
        result = scale_data(data, multiplier)
        expected = [1e11, 2e11, 3e11]
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()