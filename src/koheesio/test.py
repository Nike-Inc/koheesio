import unittest

from normalize import normalize, NormalizeConfig

class TestNormalize(unittest.TestCase):

    def test_normalize_standard(self):
        data = [10, 20, 30, 40, 50]
        config = NormalizeConfig(min_value=0, max_value=1)
        result = normalize(data, config)
        expected = [0.0, 0.25, 0.5, 0.75, 1.0]
        self.assertEqual(result, expected)

    def test_normalize_negative_values(self):
        data = [-50, -25, 0, 25, 50]
        config = NormalizeConfig(min_value=-1, max_value=1)
        result = normalize(data, config)
        expected = [-1.0, -0.5, 0.0, 0.5, 1.0]
        self.assertEqual(result, expected)

    def test_normalize_float_values(self):
        data = [0.1, 0.2, 0.3, 0.4, 0.5]
        config = NormalizeConfig(min_value=0, max_value=1)
        result = normalize(data, config)
        expected = [0.0, 0.25, 0.49999999999999994, 0.7500000000000001, 1.0]
        self.assertEqual(result, expected)

    def test_normalize_inverted_config(self):
        data = [10, 20, 30, 40, 50]
        config = NormalizeConfig(min_value=1, max_value=0)
        result = normalize(data, config)
        expected = [1.0, 0.75, 0.5, 0.25, 0.0]
        self.assertEqual(result, expected)

    def test_normalize_empty_data(self):
        data = []
        config = NormalizeConfig(min_value=0, max_value=1)
        with self.assertRaises(ValueError):
            normalize(data, config)

if __name__ == '__main__':
    unittest.main()