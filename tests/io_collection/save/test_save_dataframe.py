import io
import random
import string
import unittest

import boto3
import numpy as np
import pandas as pd
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.save.save_dataframe import save_dataframe


class TestSaveDataframe(unittest.TestCase):
    def setUp(self) -> None:
        self.dataframe = pd.DataFrame(
            {
                "ints": [random.randint(0, 100) for _ in range(100)],
                "floats": [random.random() for _ in range(100)],
                "strings": [random.choice(string.ascii_letters) for _ in range(100)],
            }
        )

    def test_save_dataframe_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            save_dataframe("", "key.ext", None)

    @patchfs
    def test_save_dataframe_to_fs(self, fs):
        path = "test/path"
        key = "key.csv"

        save_dataframe(path, key, self.dataframe)

        dataframe = pd.read_csv(f"{path}/{key}")
        self.assertTrue(np.array_equal(self.dataframe["ints"], dataframe["ints"]))
        self.assertTrue(np.allclose(self.dataframe["floats"], dataframe["floats"]))
        self.assertTrue(np.array_equal(self.dataframe["strings"], dataframe["strings"]))

    @mock_aws
    def test_save_dataframe_to_s3(self):
        bucket = "test-bucket"
        key = "key.csv"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        save_dataframe(f"s3://{bucket}", key, self.dataframe)

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        dataframe = pd.read_csv(io.BytesIO(s3_object["Body"].read()))
        self.assertTrue(np.array_equal(self.dataframe["ints"], dataframe["ints"]))
        self.assertTrue(np.allclose(self.dataframe["floats"], dataframe["floats"]))
        self.assertTrue(np.array_equal(self.dataframe["strings"], dataframe["strings"]))
        self.assertEqual("text/csv", s3_object["ContentType"])


if __name__ == "__main__":
    unittest.main()
