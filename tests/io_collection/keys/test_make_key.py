import datetime
import random
import sys
import unittest
from unittest import mock

from io_collection.keys.make_key import make_key


class TestMakeKey(unittest.TestCase):
    def test_make_key(self):
        subkeys = ["aaa", "bbb", "ccc.ext"]
        expected_key = "aaa/bbb/ccc.ext"
        key = make_key(*subkeys)
        self.assertEqual(expected_key, key)

    def test_make_key_double_underscore(self):
        subkeys = ["aaa", "bb__b", "ccc.ext"]
        expected_key = "aaa/bb_b/ccc.ext"
        key = make_key(*subkeys)
        self.assertEqual(expected_key, key)

    def test_make_key_underscore_extension(self):
        subkeys = ["aaa", "bbb", "ccc_.ext"]
        expected_key = "aaa/bbb/ccc.ext"
        key = make_key(*subkeys)
        self.assertEqual(expected_key, key)

    @mock.patch.object(sys.modules["io_collection.keys.make_key"], "datetime")
    def test_make_key_with_timestamp(self, datetime_mock):
        year = random.randint(2000, 2050)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        datetime_mock.datetime.now.return_value = datetime.date(year, month, day)

        subkeys = ["aaa", "{{timestamp}}", "ccc.ext"]
        expected_key = f"aaa/{year}-{month:02d}-{day:02d}/ccc.ext"
        key = make_key(*subkeys)
        self.assertEqual(expected_key, key)


if __name__ == "__main__":
    unittest.main()
