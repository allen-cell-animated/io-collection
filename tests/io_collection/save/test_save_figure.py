import unittest
from pathlib import Path
from unittest import mock

import boto3
from matplotlib.figure import Figure
from moto import mock_aws
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.save.save_figure import save_figure


class TestSaveFigure(unittest.TestCase):
    def setUp(self):
        self.kwargs = {"a": 1, "b": "2"}
        self.kwargs_repr = repr(self.kwargs)
        self.kwargs_bytes = self.kwargs_repr.encode("utf-8")

    def test_save_figure_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            save_figure("", "key.ext", None)

    @patchfs
    def test_save_figure_to_fs(self, fs):
        path = "test/path"
        key = "key.png"
        figure = mock.Mock(spec=Figure)

        figure.savefig.side_effect = lambda *args, **kwargs: fs.create_file(
            args[0], contents=repr(kwargs)
        )

        save_figure(path, key, figure, **self.kwargs)

        with Path(path, key).open("r") as f:
            self.assertEqual(self.kwargs_repr, f.read())

    @mock_aws
    def test_save_figure_to_s3(self):
        bucket = "test-bucket"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)

        parameters = [
            ("key.png", "image/png"),
            ("key.jpg", "image/jpeg"),
            ("key.jpeg", "image/jpeg"),
            ("key.svg", "image/svg+xml"),
        ]

        for key, content_type in parameters:
            with self.subTest(key=key):
                figure = mock.Mock(spec=Figure)
                figure.savefig.side_effect = lambda *args, **kwargs: args[0].write(
                    repr(kwargs).encode("utf-8")
                )

                save_figure(f"s3://{bucket}", key, figure, **self.kwargs)

                s3_object = s3_client.get_object(Bucket=bucket, Key=key)
                self.assertEqual(self.kwargs_bytes, s3_object["Body"].read())
                self.assertEqual(content_type, s3_object["ContentType"])


if __name__ == "__main__":
    unittest.main()
