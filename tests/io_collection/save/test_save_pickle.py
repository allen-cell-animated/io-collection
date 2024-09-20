import pickle
import random
import string
import unittest
from pathlib import Path

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.save.save_pickle import save_pickle


class TestSavePickle(unittest.TestCase):
    def setUp(self):
        self.object = {
            "ints": [random.randint(0, 100) for _ in range(100)],
            "floats": [random.random() for _ in range(100)],
            "strings": [random.choice(string.ascii_letters) for _ in range(100)],
        }

    def test_save_pickle_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            save_pickle("", "key.ext", "")

    @patchfs
    def test_save_pickle_to_fs(self, fs):  # noqa: ARG002
        path = "test/path"
        key = "key.pkl"

        save_pickle(path, key, self.object)

        with Path(path, key).open("rb") as f:
            self.assertEqual(self.object, pickle.loads(f.read()))

    @mock_aws
    def test_save_pickle_to_s3(self):
        bucket = "test-bucket"
        key = "key.pkl"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        save_pickle(f"s3://{bucket}", key, self.object)

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        self.assertEqual(self.object, pickle.loads(s3_object["Body"].read()))


if __name__ == "__main__":
    unittest.main()
