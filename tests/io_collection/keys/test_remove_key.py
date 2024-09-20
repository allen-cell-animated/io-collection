import unittest
from pathlib import Path

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.keys.remove_key import remove_key


class TestRemoveKey(unittest.TestCase):
    @patchfs
    def test_remove_key_on_fs_object_does_not_exist(self, fs):  # noqa: ARG002
        path = "test/path"
        key = "key.ext"

        remove_key(path, key)
        self.assertFalse(Path(path, key).exists())

    @patchfs
    def test_remove_key_on_fs_object_exists(self, fs):
        path = "test/path"
        key = "key.ext"

        fs.create_file(f"{path}/{key}")

        remove_key(path, key)
        self.assertFalse(Path(path, key).exists())

    @mock_aws
    def test_copy_key_on_s3_object_does_not_exist(self):
        bucket = "test-bucket"
        key = "key.ext"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        remove_key(f"s3://{bucket}", key)
        self.assertFalse("Contents" in s3_client.list_objects(Bucket=bucket, Prefix=key))

    @mock_aws
    def test_copy_key_on_s3_object_exists(self):
        bucket = "test-bucket"
        key = "key.ext"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key)

        remove_key(f"s3://{bucket}", key)
        self.assertFalse("Contents" in s3_client.list_objects(Bucket=bucket, Prefix=key))


if __name__ == "__main__":
    unittest.main()
