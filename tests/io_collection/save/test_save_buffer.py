import io
import unittest

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.save.save_buffer import save_buffer


class TestSaveBuffer(unittest.TestCase):
    @patchfs
    def test_save_buffer_to_fs(self, fs):
        path = "test/path"
        key = "key.ext"
        contents = "abc"

        save_buffer(path, key, io.BytesIO(contents.encode("utf-8")))

        with open(f"{path}/{key}", "r") as f:
            self.assertEqual(contents, f.read())

    @mock_aws
    def test_save_buffer_to_s3(self):
        bucket = "test-bucket"
        key = "key.ext"
        contents = "abc"
        content_type = "content-type"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        save_buffer(f"s3://{bucket}", key, io.BytesIO(contents.encode("utf-8")), content_type)

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        self.assertEqual(contents, s3_object["Body"].read().decode("utf-8"))
        self.assertEqual(content_type, s3_object["ContentType"])


if __name__ == "__main__":
    unittest.main()
