import pickle
import random
import string
import unittest

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.load.load_pickle import load_pickle


class TestLoadPickle(unittest.TestCase):
    def setUp(self):
        self.object = {
            "ints": [random.randint(0, 100) for _ in range(100)],
            "floats": [random.random() for _ in range(100)],
            "strings": [random.choice(string.ascii_letters) for _ in range(100)],
        }

    def test_load_pickle_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            load_pickle("", "key.ext")

    @patchfs
    def test_load_pickle_from_fs(self, fs):
        path = "test/path"
        key = "key.pkl"
        contents = pickle.dumps(self.object)

        fs.create_file(f"{path}/{key}", contents=contents)

        pickle_object = load_pickle(path, key)

        self.assertEqual(self.object, pickle_object)

    @mock_aws
    def test_load_pickle_from_s3(self):
        bucket = "test-bucket"
        key = "key.pkl"
        contents = pickle.dumps(self.object)

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=contents)

        pickle_object = load_pickle(f"s3://{bucket}", key)

        self.assertEqual(self.object, pickle_object)


if __name__ == "__main__":
    unittest.main()
