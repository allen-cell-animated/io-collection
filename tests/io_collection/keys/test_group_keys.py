import unittest

from io_collection.keys.group_keys import group_keys


class TestGroupKeys(unittest.TestCase):
    def test_group_keys_even_parts(self):
        keys = ["A0_B0_C0", "A0_B1_C1", "A0_B1_C2"]

        expected_groups = {
            "A0": ["A0_B0_C0", "A0_B1_C1", "A0_B1_C2"],
            "B0": ["A0_B0_C0"],
            "B1": ["A0_B1_C1", "A0_B1_C2"],
            "C0": ["A0_B0_C0"],
            "C1": ["A0_B1_C1"],
            "C2": ["A0_B1_C2"],
        }

        groups = group_keys(keys)

        self.assertDictEqual(groups, expected_groups)

    def test_group_keys_uneven_parts(self):
        with self.assertRaises(ValueError):
            group_keys(["A0_B0_C0", "A0_B1"])


if __name__ == "__main__":
    unittest.main()
