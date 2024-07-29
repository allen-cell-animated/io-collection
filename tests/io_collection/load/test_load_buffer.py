import random
import sys
import unittest
from unittest import mock

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.load.load_buffer import load_buffer

TEST_CONTENT_LENGTH = 100


class TestLoadBuffer(unittest.TestCase):
    @patchfs
    def test_load_buffer_from_fs(self, fs):
        path = "test/path"
        key = "key.ext"
        contents = b"abc"

        fs.create_file(f"{path}/{key}", contents=contents)

        buffer = load_buffer(path, key)
        self.assertEqual(contents, buffer.getvalue())

    @mock_aws
    def test_load_buffer_from_s3(self):
        bucket = "test-bucket"
        key = "key.ext"
        contents = b"abc"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=contents)

        buffer = load_buffer(f"s3://{bucket}", key)
        self.assertEqual(contents, buffer.getvalue())

    @mock_aws
    @mock.patch.object(
        sys.modules["io_collection.load.load_buffer"], "MAX_CONTENT_LENGTH", TEST_CONTENT_LENGTH
    )
    def test_load_buffer_from_s3_content_length(self):
        bucket = "test-bucket"
        key = "key.ext"
        contents = random.randbytes(TEST_CONTENT_LENGTH * 2)

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=contents)

        buffer = load_buffer(f"s3://{bucket}", key)
        self.assertEqual(contents, buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
