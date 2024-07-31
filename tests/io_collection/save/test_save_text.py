import io
import unittest

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.save.save_text import save_text


class TestSaveText(unittest.TestCase):
    @patchfs
    def test_save_text_to_fs(self, fs):
        path = "test/path"
        key = "key.txt"
        contents = "abc"

        save_text(path, key, contents)

        with open(f"{path}/{key}", "r") as f:
            self.assertEqual(contents, f.read())

    @mock_aws
    def test_save_text_to_s3(self):
        bucket = "test-bucket"
        key = "key.txt"
        contents = "abc"
        content_type = "content-type"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        save_text(f"s3://{bucket}", key, contents, content_type)

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        self.assertEqual(contents, s3_object["Body"].read().decode("utf-8"))
        self.assertEqual(content_type, s3_object["ContentType"])


if __name__ == "__main__":
    unittest.main()
