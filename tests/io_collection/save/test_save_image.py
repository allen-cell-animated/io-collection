import io
import pathlib
import random
import sys
import unittest
from pathlib import Path
from unittest import mock

import boto3
import botocore
import numpy as np
from moto import mock_aws
from PIL import Image
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.save.save_image import save_image


class TestSaveImage(unittest.TestCase):
    def setUp(self):
        self.image_ome = np.zeros((1, 2, 3, 4, 5))
        self.image_bytes = self.image_ome.tobytes()
        self.color = tuple(random.choices(range(256), k=3))
        self.image_rgb = np.array(Image.new(mode="RGB", size=(1, 1), color=self.color))

    def test_save_image_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            save_image("", "key.ext", None)

    @patchfs
    @mock.patch.object(sys.modules["io_collection.save.save_image"], "OmeTiffWriter")
    def test_save_image_to_fs_ome_tiff(self, fs, writer_mock):
        path = "test/path"

        parameters = ["key.ome.tiff", "key.ome.tif"]

        for key in parameters:
            with self.subTest(key=key):
                writer_mock.save.side_effect = lambda image, path: fs.create_file(
                    path, contents=image.tobytes()
                )
                save_image(path, key, self.image_ome)

                with Path(path, key).open("rb") as f:
                    self.assertEqual(self.image_bytes, f.read())

    @patchfs
    def test_save_image_to_fs_png(self, fs):  # noqa: ARG002
        path = "test/path"
        key = "key.png"

        save_image(path, key, self.image_rgb)

        image = Image.open(f"{path}/{key}")
        frame_color = tuple(np.array(image.convert("RGB"), dtype=np.uint8)[0, 0, :])
        self.assertEqual(self.color, frame_color)

    @patchfs
    @mock.patch.object(sys.modules["io_collection.save.save_image"], "OmeTiffWriter")
    @mock_aws
    def test_save_image_to_s3_ome_tiff(self, fs, writer_mock):
        bucket = "test-bucket"

        # Set up pyfakefs to work with moto
        for module in [boto3, botocore]:
            module_dir = pathlib.Path(module.__file__).parent
            fs.add_real_directory(module_dir, lazy_read=False)

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        parameters = ["key.ome.tiff", "key.ome.tif"]

        for key in parameters:
            with self.subTest(key=key):
                writer_mock.save.side_effect = lambda image, path: fs.create_file(
                    path, contents=image.tobytes()
                )
                save_image(f"s3://{bucket}", key, self.image_ome)

                s3_object = s3_client.get_object(Bucket=bucket, Key=key)
                self.assertEqual(self.image_bytes, s3_object["Body"].read())

    @mock_aws
    def test_save_image_to_s3_png(self):
        bucket = "test-bucket"
        key = "key.png"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        save_image(f"s3://{bucket}", key, self.image_rgb)

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        image = Image.open(io.BytesIO(s3_object["Body"].read()))
        frame_color = tuple(np.array(image.convert("RGB"), dtype=np.uint8)[0, 0, :])
        self.assertEqual(self.color, frame_color)
        self.assertEqual("image/png", s3_object["ContentType"])


if __name__ == "__main__":
    unittest.main()
