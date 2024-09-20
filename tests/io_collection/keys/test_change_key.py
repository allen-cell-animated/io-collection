import unittest
from pathlib import Path

import boto3
from botocore.errorfactory import ClientError
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.keys.change_key import change_key


class TestChangeKey(unittest.TestCase):
    def setUp(self):
        self.old_key = "old/key.ext"
        self.new_key = "new/key.ext"
        self.contents = b"abc"

    @patchfs
    def test_change_key_on_fs(self, fs):
        path = "test/path"

        fs.create_file(f"{path}/{self.old_key}", contents=self.contents)

        change_key(path, self.old_key, self.new_key)

        # Check that old key no longer exists
        self.assertFalse(Path(path, self.old_key).exists())

        # Check that new key exists
        self.assertTrue(Path(path, self.new_key).exists())

        # Check that new key has correct contents
        contents = fs.get_object(f"{path}/{self.new_key}").byte_contents
        self.assertEqual(self.contents, contents)

    @mock_aws
    def test_change_key_on_s3(self):
        bucket = "test-bucket"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=self.old_key, Body=self.contents)

        change_key(f"s3://{bucket}", self.old_key, self.new_key)

        # Check that old key no longer exists
        with self.assertRaises(ClientError) as _:
            s3_client.get_object(Bucket=bucket, Key=self.old_key)

        # Check that new key exists and has correct contents
        contents = s3_client.get_object(Bucket=bucket, Key=self.new_key)["Body"].read()
        self.assertEqual(self.contents, contents)


if __name__ == "__main__":
    unittest.main()
