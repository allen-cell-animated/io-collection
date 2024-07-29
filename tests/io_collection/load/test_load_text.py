import random
import string
import unittest

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.load.load_text import load_text


class TestLoadText(unittest.TestCase):
    def setUp(self) -> None:
        self.text = "".join(random.choices(string.ascii_lowercase, k=100))

    def test_load_text_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            load_text("", "key.ext")

    @patchfs
    def test_load_text_from_fs(self, fs):
        path = "test/path"
        key = "key.txt"
        contents = self.text

        fs.create_file(f"{path}/{key}", contents=contents)

        text = load_text(path, key)

        self.assertEqual(self.text, text)

    @mock_aws
    def test_load_text_from_s3(self):
        bucket = "test-bucket"
        key = "key.txt"
        contents = self.text

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=contents)

        text = load_text(f"s3://{bucket}", key)

        self.assertEqual(self.text, text)


if __name__ == "__main__":
    unittest.main()
