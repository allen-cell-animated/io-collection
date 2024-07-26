import os
import unittest

import boto3
from botocore.errorfactory import ClientError
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.keys.check_key import check_key


class TestCheckKey(unittest.TestCase):
    @mock_aws
    def test_check_key_on_s3_object_does_not_exist(self):
        bucket = "test-bucket"
        key = "key.ext"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        exists = check_key(f"s3://{bucket}", key)
        self.assertFalse(exists)

    @mock_aws
    def test_check_key_on_s3_object_exists(self):
        bucket = "test-bucket"
        key = "key.ext"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key)

        exists = check_key(f"s3://{bucket}", key)
        self.assertTrue(exists)

    @patchfs
    def test_check_key_on_fs_object_does_not_exist(self, fs):
        path = "test/path"
        key = "key.ext"

        exists = check_key(path, key)
        self.assertFalse(exists)

    @patchfs
    def test_check_key_on_fs_object_exists(self, fs):
        path = "test/path"
        key = "key.ext"

        fs.create_file(f"{path}/{key}")

        exists = check_key(path, key)
        self.assertTrue(exists)


if __name__ == "__main__":
    unittest.main()
