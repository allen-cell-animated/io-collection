import unittest
from unittest import mock

import boto3
import quilt3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.quilt.save_quilt_item import save_quilt_item


class TestSaveQuiltItem(unittest.TestCase):
    @patchfs
    def test_save_quilt_package_to_fs(self, fs):
        path = "test/path"
        key = "key.ext"
        item = "folder"
        contents = b"abc"

        package_mock = mock.MagicMock(spec=quilt3.Package)
        package_mock.__getitem__.return_value.get_bytes.return_value = contents

        save_quilt_item(path, key, package_mock, item)

        package_mock.__getitem__.assert_called_with(item)
        with open(f"{path}/{key}", "rb") as f:
            self.assertEqual(contents, f.read())

    @mock_aws
    def test_save_quilt_package_to_s3(self):
        bucket = "test-bucket"
        key = "key.ext"
        item = "folder"
        contents = b"abc"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        package_mock = mock.MagicMock(spec=quilt3.Package)
        package_mock.__getitem__.return_value.get_bytes.return_value = contents

        save_quilt_item(f"s3://{bucket}", key, package_mock, item)

        package_mock.__getitem__.assert_called_with(item)
        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        self.assertEqual(contents, s3_object["Body"].read())


if __name__ == "__main__":
    unittest.main()
