import random
import string
import unittest

import boto3
import numpy as np
import pandas as pd
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.load.load_dataframe import load_dataframe


class TestLoadDataFrame(unittest.TestCase):
    def setUp(self):
        self.dataframe = pd.DataFrame(
            {
                "ints": [random.randint(0, 100) for _ in range(100)],
                "floats": [random.random() for _ in range(100)],
                "strings": [random.choice(string.ascii_letters) for _ in range(100)],
            }
        )

    def test_load_dataframe_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            load_dataframe("", "key.ext")

    @patchfs
    def test_load_dataframe_from_fs(self, fs):
        path = "test/path"
        key = "key.csv"
        contents = self.dataframe.to_csv(index=False)

        fs.create_file(f"{path}/{key}", contents=contents)

        dataframe = load_dataframe(path, key)

        self.assertTrue(np.array_equal(self.dataframe["ints"], dataframe["ints"]))
        self.assertTrue(np.allclose(self.dataframe["floats"], dataframe["floats"]))
        self.assertTrue(np.array_equal(self.dataframe["strings"], dataframe["strings"]))

    @mock_aws
    def test_load_dataframe_from_s3(self):
        bucket = "test-bucket"
        key = "key.csv"
        contents = self.dataframe.to_csv(index=False)

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=contents)

        dataframe = load_dataframe(f"s3://{bucket}", key)

        self.assertTrue(np.array_equal(self.dataframe["ints"], dataframe["ints"]))
        self.assertTrue(np.allclose(self.dataframe["floats"], dataframe["floats"]))
        self.assertTrue(np.array_equal(self.dataframe["strings"], dataframe["strings"]))


if __name__ == "__main__":
    unittest.main()
