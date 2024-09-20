import io
import random
import tarfile
import unittest

import boto3
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.save.save_tar import save_tar


class TestSaveTar(unittest.TestCase):
    def setUp(self):
        self.contents = {
            "a.ext": io.BytesIO(random.randbytes(100)),
            "b.ext": io.BytesIO(random.randbytes(100)),
            "c.ext": io.BytesIO(random.randbytes(100)),
            "d.ext": io.BytesIO(random.randbytes(100)),
            "e.ext": io.BytesIO(random.randbytes(100)),
            "f.ext": io.BytesIO(random.randbytes(100)),
        }

        self.old_keys = ["a.ext", "b.ext", "c.ext"]
        self.new_keys = ["d.ext", "e.ext", "f.ext"]

        with io.BytesIO() as buffer:
            with tarfile.open(fileobj=buffer, mode="w:xz") as tar:
                for key in self.old_keys:
                    info = tarfile.TarInfo(key.rsplit("/", maxsplit=1)[-1])
                    info.size = self.contents[key].getbuffer().nbytes
                    tar.addfile(info, fileobj=self.contents[key])

            self.tarfile = buffer.getvalue()

    def test_save_tar_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            save_tar("", "key.ext", [])

    @patchfs
    def test_save_tar_to_fs_new_tar(self, fs):
        path = "test/path"
        key = "key.tar.xz"

        for subkey in self.new_keys:
            fs.create_file(f"{path}/{subkey}", contents=self.contents[subkey].getvalue())

        save_tar(path, key, self.new_keys)

        with tarfile.open(f"{path}/{key}", mode="r:xz") as tar:
            for member in tar.getmembers():
                contents = tar.extractfile(member).read()
                self.assertEqual(self.contents[member.name].getvalue(), contents)

    @patchfs
    def test_save_tar_to_fs_existing_tar(self, fs):
        path = "test/path"
        key = "key.tar.xz"

        fs.create_file(f"{path}/{key}", contents=self.tarfile)
        for subkey in self.new_keys:
            fs.create_file(f"{path}/{subkey}", contents=self.contents[subkey].getvalue())

        save_tar(path, key, self.new_keys)

        with tarfile.open(f"{path}/{key}", mode="r:xz") as tar:
            for member in tar.getmembers():
                contents = tar.extractfile(member).read()
                self.assertEqual(self.contents[member.name].getvalue(), contents)

    @mock_aws
    def test_save_tar_to_s3_new_tar(self):
        bucket = "test-bucket"
        key = "key.tar.xz"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        for subkey in self.new_keys:
            s3_client.put_object(Bucket=bucket, Key=subkey, Body=self.contents[subkey].getvalue())

        save_tar(f"s3://{bucket}", key, self.new_keys)

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        with tarfile.open(fileobj=io.BytesIO(s3_object["Body"].read()), mode="r:xz") as tar:
            for member in tar.getmembers():
                contents = tar.extractfile(member).read()
                self.assertEqual(self.contents[member.name].getvalue(), contents)

    @mock_aws
    def test_save_tar_to_s3_existing_tar(self):
        bucket = "test-bucket"
        key = "key.tar.xz"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=self.tarfile)
        for subkey in self.new_keys:
            s3_client.put_object(Bucket=bucket, Key=subkey, Body=self.contents[subkey].getvalue())

        save_tar(f"s3://{bucket}", key, self.new_keys)

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        with tarfile.open(fileobj=io.BytesIO(s3_object["Body"].read()), mode="r:xz") as tar:
            for member in tar.getmembers():
                contents = tar.extractfile(member).read()
                self.assertEqual(self.contents[member.name].getvalue(), contents)


if __name__ == "__main__":
    unittest.main()
