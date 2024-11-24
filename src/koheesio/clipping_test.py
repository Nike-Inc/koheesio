import unittest
from clipping import clip_data, ClipConfig

class TestClipData(unittest.TestCase):
    def test_clip_data(self):
        data = [5, 15, 25]
        config = ClipConfig(min_value=10, max_value=20)
        result = clip_data(data, config)
        expected = [10, 15, 20]
        self.assertEqual(result, expected)

    def test_clip_data_beyond_limits(self):
        data = [-10, 0, 10, 20, 30]
        config = ClipConfig(min_value=0, max_value=20)
        result = clip_data(data, config)
        expected = [0, 0, 10, 20, 20]
        self.assertEqual(result, expected)

    def test_clip_data_all_below_min(self):
        """Test clipping where all values are below the minimum."""
        data = [-10, -5, -1]
        config = ClipConfig(min_value=0, max_value=20)
        result = clip_data(data, config)
        expected = [0, 0, 0]
        self.assertEqual(result, expected)

    def test_clip_data_all_above_max(self):
        """Test clipping where all values are above the maximum."""
        data = [25, 30, 35]
        config = ClipConfig(min_value=10, max_value=20)
        result = clip_data(data, config)
        expected = [20, 20, 20]
        self.assertEqual(result, expected)

    def test_clip_data_empty_list(self):
        """Test clipping with an empty data list."""
        data = []
        config = ClipConfig(min_value=10, max_value=20)
        result = clip_data(data, config)
        self.assertEqual(result, [])

if __name__ == "__main__":
    unittest.main()
