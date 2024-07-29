import io
import random
import tarfile
import unittest

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.load.load_tar import load_tar


class TestLoadTar(unittest.TestCase):
    def setUp(self) -> None:
        self.contents = {
            "a.ext": io.BytesIO(random.randbytes(100)),
            "b.ext": io.BytesIO(random.randbytes(100)),
            "c.ext": io.BytesIO(random.randbytes(100)),
        }

        with io.BytesIO() as buffer:
            with tarfile.open(fileobj=buffer, mode="w:xz") as tar:
                for content_key, content in self.contents.items():
                    info = tarfile.TarInfo(content_key.split("/")[-1])
                    info.size = content.getbuffer().nbytes
                    tar.addfile(info, fileobj=content)

            self.tarfile = buffer.getvalue()

    def test_load_tar_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            load_tar("", "key.ext")

    @patchfs
    def test_load_tar_from_fs(self, fs):
        path = "test/path"
        key = "key.tar.xz"

        fs.create_file(f"{path}/{key}", contents=self.tarfile)

        tar = load_tar(path, key)

        for member in tar.getmembers():
            contents = tar.extractfile(member).read()
            self.assertEqual(self.contents[member.name].getvalue(), contents)

    @mock_aws
    def test_load_tar_from_s3(self):
        bucket = "test-bucket"
        key = "key.tar.xz"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=self.tarfile)

        tar = load_tar(f"s3://{bucket}", key)

        for member in tar.getmembers():
            contents = tar.extractfile(member).read()
            self.assertEqual(self.contents[member.name].getvalue(), contents)


if __name__ == "__main__":
    unittest.main()
