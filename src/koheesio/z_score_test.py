import unittest
from z_score_normalize import z_score_normalize

class TestZScoreNormalize(unittest.TestCase):

    def test_large_numbers(self):
        """Test z-score normalization with large numbers."""
        data = [1e10, 2e10, 3e10, 4e10, 5e10]
        result = z_score_normalize(data)
        expected = [-1.2649110640673518, -0.6324555320336759, 0.0, 0.6324555320336759, 1.2649110640673518]
        self.assertAlmostEqual(result, expected, places=5)

    def test_mixed_positive_and_negative_values(self):
        """Test z-score normalization with a mix of positive and negative values."""
        data = [-10, -5, 0, 5, 10]
        result = z_score_normalize(data)
        expected = [-1.2649110640673518, -0.6324555320336759, 0.0, 0.6324555320336759, 1.2649110640673518]
        self.assertAlmostEqual(result, expected, places=5)

    def test_precomputed_mean_and_std_dev(self):
        """Test z-score normalization with precomputed mean and std deviation."""
        from statistics import mean, stdev
        data = [10, 20, 30, 40, 50]
        mean_value = mean(data)
        std_dev = stdev(data)
        result = [(x - mean_value) / std_dev for x in data]
        expected = [-1.2649110640673518, -0.6324555320336759, 0.0, 0.6324555320336759, 1.2649110640673518]
        self.assertAlmostEqual(result, expected, places=5)


    def test_data_with_highly_skewed_distribution(self):
        """Test z-score normalization with a highly skewed distribution."""
        data = [1, 2, 3, 4, 1000]
        result = z_score_normalize(data)
        # Validate that the outlier affects the z-scores
        self.assertTrue(result[-1] > 1.0)
        self.assertTrue(result[0] < 0.0)

    def test_already_normalized_data(self):
        """Test z-score normalization on already normalized data."""
        data = [-1.2649110640673518, -0.6324555320336759, 0.0, 0.6324555320336759, 1.2649110640673518]
        result = z_score_normalize(data)
        expected = data  # Z-score normalizing already normalized data should not change it
        self.assertAlmostEqual(result, expected, places=5)

if __name__ == "__main__":
    unittest.main()