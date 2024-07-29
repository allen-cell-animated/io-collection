import json
import random
import string
import unittest

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.load.load_json import load_json


class TestLoadJson(unittest.TestCase):
    def setUp(self) -> None:
        size = 100

        self.json_dict = {
            "ints": [random.randint(0, size) for _ in range(size)],
            "floats": [random.random() for _ in range(size)],
            "strings": [random.choice(string.ascii_letters) for _ in range(size)],
        }

        self.json_list = [
            [random.randint(0, size) for _ in range(size)],
            [random.random() for _ in range(size)],
            [random.choice(string.ascii_letters) for _ in range(size)],
        ]

    def test_load_json_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            load_json("", "key.ext")

    @patchfs
    def test_load_json_from_fs_as_dict(self, fs):
        path = "test/path"
        key = "key.json"
        contents = json.dumps(self.json_dict)

        fs.create_file(f"{path}/{key}", contents=contents)

        jsn = load_json(path, key)

        self.assertCountEqual(self.json_dict["ints"], jsn["ints"])
        self.assertCountEqual(self.json_dict["floats"], jsn["floats"])
        self.assertCountEqual(self.json_dict["strings"], jsn["strings"])

    @patchfs
    def test_load_json_from_fs_as_list(self, fs):
        path = "test/path"
        key = "key.json"
        contents = json.dumps(self.json_list)

        fs.create_file(f"{path}/{key}", contents=contents)

        jsn = load_json(path, key)

        self.assertCountEqual(self.json_list[0], jsn[0])
        self.assertCountEqual(self.json_list[1], jsn[1])
        self.assertCountEqual(self.json_list[2], jsn[2])

    @mock_aws
    def test_load_json_from_s3_as_dict(self):
        bucket = "test-bucket"
        key = "key.json"
        contents = json.dumps(self.json_dict)

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=contents)

        jsn = load_json(f"s3://{bucket}", key)

        self.assertCountEqual(self.json_dict["ints"], jsn["ints"])
        self.assertCountEqual(self.json_dict["floats"], jsn["floats"])
        self.assertCountEqual(self.json_dict["strings"], jsn["strings"])

    @mock_aws
    def test_load_json_from_s3_as_list(self):
        bucket = "test-bucket"
        key = "key.json"
        contents = json.dumps(self.json_list)

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=contents)

        jsn = load_json(f"s3://{bucket}", key)

        self.assertCountEqual(self.json_list[0], jsn[0])
        self.assertCountEqual(self.json_list[1], jsn[1])
        self.assertCountEqual(self.json_list[2], jsn[2])


if __name__ == "__main__":
    unittest.main()
