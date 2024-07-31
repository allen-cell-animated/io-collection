import sys
import unittest
from unittest import mock

import bioio_ome_tiff
import bioio_tifffile

from io_collection.load.load_image import load_image


class TestLoadImage(unittest.TestCase):
    def test_load_image_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            load_image("", "key.ext")

    @mock.patch.object(sys.modules["io_collection.load.load_image"], "BioImage")
    def test_load_image_from_fs_tiff(self, bioimage_mock):
        path = "test/path"
        key = "key.tif"
        dim_order = "ABC"

        _ = load_image(path, key, dim_order=dim_order)
        bioimage_mock.assert_called_with(
            f"{path}/{key}", reader=bioio_tifffile.Reader, dim_order=dim_order
        )

    @mock.patch.object(sys.modules["io_collection.load.load_image"], "BioImage")
    def test_load_image_from_fs_ome_tiff(self, bioimage_mock):
        path = "test/path"
        dim_order = "ABC"

        parameters = [
            "key.ome.tif",
            "key.ome.tiff",
        ]

        for key in parameters:
            with self.subTest(key=key):
                _ = load_image(path, key, dim_order=dim_order)
                bioimage_mock.assert_called_with(
                    f"{path}/{key}", reader=bioio_ome_tiff.Reader, dim_order=dim_order
                )

    @mock.patch.object(sys.modules["io_collection.load.load_image"], "BioImage")
    def test_load_image_from_s3_tiff(self, bioimage_mock):
        bucket = "test-bucket"
        key = "key.tif"
        dim_order = "ABC"

        _ = load_image(f"s3://{bucket}", key, dim_order=dim_order)
        bioimage_mock.assert_called_with(
            f"s3://{bucket}/{key}", reader=bioio_tifffile.Reader, dim_order=dim_order
        )

    @mock.patch.object(sys.modules["io_collection.load.load_image"], "BioImage")
    def test_load_image_from_s3_ome_tiff(self, bioimage_mock):
        bucket = "test-bucket"
        dim_order = "ABC"

        parameters = [
            "key.ome.tif",
            "key.ome.tiff",
        ]

        for key in parameters:
            with self.subTest(key=key):
                _ = load_image(f"s3://{bucket}", key, dim_order=dim_order)
                bioimage_mock.assert_called_with(
                    f"s3://{bucket}/{key}", reader=bioio_ome_tiff.Reader, dim_order=dim_order
                )


if __name__ == "__main__":
    unittest.main()
