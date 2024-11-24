import unittest
from exponential import exp_transform, ExpTransformConfig

class TestExpTransform(unittest.TestCase):
    def test_exp_transform_base_2(self):
        data = [1, 2, 3]
        config = ExpTransformConfig(base=2)
        result = exp_transform(data, config)
        expected = [2, 4, 8]
        self.assertEqual(result, expected)

    def test_exp_transform_base_e(self):
        data = [1, 2, 3]
        config = ExpTransformConfig(base=2.71828)
        result = exp_transform(data, config)
        self.assertTrue(all(isinstance(x, float) for x in result))

    def test_exp_transform_large_values(self):
        data = [10, 20]
        config = ExpTransformConfig(base=10)
        result = exp_transform(data, config)
        self.assertTrue(all(isinstance(x, float) for x in result))

    def test_exp_transform_with_invalid_base(self):
        data = [1, 2, 3]
        config = ExpTransformConfig(base=-1)
        with self.assertRaises(ValueError):
            exp_transform(data, config)

    def test_exp_transform_empty_data(self):
        data = []
        config = ExpTransformConfig(base=2)
        result = exp_transform(data, config)
        self.assertEqual(result, [])

if __name__ == "__main__":
    unittest.main()
