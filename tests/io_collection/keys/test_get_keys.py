import unittest

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.keys.get_keys import get_keys


class TestGetKeys(unittest.TestCase):
    def setUp(self) -> None:
        self.old_key = "old/key.ext"
        self.new_key = "new/key.ext"
        self.contents = b"abc"

    @patchfs
    def test_get_keys_on_fs(self, fs):
        num_files = 10
        path = "test/path"
        prefix = "prefix"

        expected_keys = []
        for first in range(num_files):
            for second in range(num_files):
                key = f"{prefix}/{first:02d}/{second:02d}.ext"
                expected_keys.append(key)
                fs.create_file(f"{path}/{key}")

        keys = get_keys(path, prefix)

        self.assertCountEqual(expected_keys, keys)

    @patchfs
    def test_get_keys_on_fs_no_matches(self, fs):
        path = "test/path"
        prefix = "prefix"

        fs.create_dir(path)

        keys = get_keys(path, prefix)

        self.assertCountEqual([], keys)

    @mock_aws
    def test_get_keys_on_s3(self):
        num_files = 32  # num_files^2 > 1000 to check continuation token
        bucket = "test-bucket"
        prefix = "prefix"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        expected_keys = []
        for first in range(num_files):
            for second in range(num_files):
                key = f"{prefix}/{first:02d}/{second:02d}.ext"
                expected_keys.append(key)
                s3_client.put_object(Bucket=bucket, Key=key)

        keys = get_keys(f"s3://{bucket}", prefix)

        self.assertCountEqual(expected_keys, keys)

    @mock_aws
    def test_get_keys_on_s3_no_matches(self):
        bucket = "test-bucket"
        prefix = "prefix"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        expected_keys = []

        keys = get_keys(f"s3://{bucket}", prefix)

        self.assertCountEqual(expected_keys, keys)


if __name__ == "__main__":
    unittest.main()
