import unittest

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.save.save_json import save_json


class TestSaveText(unittest.TestCase):
    def setUp(self) -> None:
        self.json_dict = {
            "ints": [5, 10, 15, 20],
            "floats": [0.1, 0.2, 0.3, 0.4, 0.5],
            "strings": ["a", "B", "c"],
        }

        self.json_dict_string = "\n".join(
            [
                "{",
                '  "ints": [5,10,15,20],',
                '  "floats": [',
                "    0.1,",
                "    0.2,",
                "    0.3,",
                "    0.4,",
                "    0.5",
                "  ],",
                '  "strings": ["a","B","c"]',
                "}",
            ]
        )

        self.json_list = [
            [5, 10, 15, 20],
            [0.1, 0.2, 0.3, 0.4, 0.5],
            ["a", "B", "c"],
        ]

        self.json_list_string = "\n".join(
            [
                "[",
                "  [5,10,15,20],",
                "  [",
                "    0.1,",
                "    0.2,",
                "    0.3,",
                "    0.4,",
                "    0.5",
                "  ],",
                '  ["a","B","c"]',
                "]",
            ]
        )

    def test_save_json_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            save_json("", "key.ext", "")

    @patchfs
    def test_save_json_to_fs_from_dict(self, fs):
        path = "test/path"
        key = "key.json"

        save_json(path, key, self.json_dict, levels=4)

        with open(f"{path}/{key}", "r") as f:
            self.assertEqual(self.json_dict_string, f.read())

    @patchfs
    def test_save_json_to_fs_from_list(self, fs):
        path = "test/path"
        key = "key.json"

        save_json(path, key, self.json_list, levels=4)

        with open(f"{path}/{key}", "r") as f:
            self.assertEqual(self.json_list_string, f.read())

    @mock_aws
    def test_save_json_to_s3_from_dict(self):
        bucket = "test-bucket"
        key = "key.json"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        save_json(f"s3://{bucket}", key, self.json_dict, levels=4)

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        self.assertEqual(self.json_dict_string, s3_object["Body"].read().decode("utf-8"))
        self.assertEqual("application/json", s3_object["ContentType"])

    @mock_aws
    def test_save_json_to_s3_from_list(self):
        bucket = "test-bucket"
        key = "key.json"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        save_json(f"s3://{bucket}", key, self.json_list, levels=4)

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        self.assertEqual(self.json_list_string, s3_object["Body"].read().decode("utf-8"))
        self.assertEqual("application/json", s3_object["ContentType"])


if __name__ == "__main__":
    unittest.main()
