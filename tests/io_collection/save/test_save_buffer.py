import io
import unittest
from pathlib import Path

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.save.save_buffer import save_buffer


class TestSaveBuffer(unittest.TestCase):
    @patchfs
    def test_save_buffer_to_fs(self, fs):  # noqa: ARG002
        path = "test/path"
        key = "key.ext"
        contents = io.BytesIO(b"abc")

        save_buffer(path, key, contents)

        with Path(path, key).open("rb") as f:
            self.assertEqual(contents.getvalue(), f.read())

    @mock_aws
    def test_save_buffer_to_s3(self):
        bucket = "test-bucket"
        key = "key.ext"
        contents = io.BytesIO(b"abc")
        content_type = "content-type"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        save_buffer(f"s3://{bucket}", key, contents, content_type)

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        self.assertEqual(contents.getvalue(), s3_object["Body"].read())
        self.assertEqual(content_type, s3_object["ContentType"])


if __name__ == "__main__":
    unittest.main()
