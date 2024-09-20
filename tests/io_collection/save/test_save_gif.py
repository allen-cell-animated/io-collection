import io
import random
import unittest

import boto3
import numpy as np
from moto import mock_aws
from PIL import Image, ImageSequence
from pyfakefs.fake_filesystem_unittest import patchfs

from io_collection.save.save_gif import save_gif


class TestSaveGif(unittest.TestCase):
    def setUp(self):
        self.colors = [tuple(random.choices(range(256), k=3)) for _ in range(5)]

        self.frames = {}
        for index, color in enumerate(self.colors):
            self.frames[f"frame_{index:02d}.png"] = Image.new(mode="RGB", size=(1, 1), color=color)

    def test_save_gif_invalid_extension_throws_exception(self):
        with self.assertRaises(ValueError):
            save_gif("", "key.ext", [])

    @patchfs
    def test_save_gif_to_fs(self, fs):
        path = "test/path"
        key = "key.gif"

        for frame_key, content in self.frames.items():
            buffer = io.BytesIO()
            content.save(buffer, format="png")
            fs.create_file(f"{path}/{frame_key}", contents=buffer.getvalue())

        save_gif(path, key, self.frames.keys())

        image = Image.open(f"{path}/{key}")
        for index, frame in enumerate(ImageSequence.Iterator(image)):
            frame_color = tuple(np.array(frame.convert("RGB"), dtype=np.uint8)[0, 0, :])
            self.assertEqual(self.colors[index], frame_color)

    @mock_aws
    def test_save_gif_to_s3(self):
        bucket = "test-bucket"
        key = "key.gif"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket)
        for frame_key, content in self.frames.items():
            buffer = io.BytesIO()
            content.save(buffer, format="png")
            s3_client.put_object(Bucket=bucket, Key=frame_key, Body=buffer.getvalue())

        save_gif(f"s3://{bucket}", key, self.frames.keys())

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        image = Image.open(io.BytesIO(s3_object["Body"].read()))
        for index, frame in enumerate(ImageSequence.Iterator(image)):
            frame_color = tuple(np.array(frame.convert("RGB"), dtype=np.uint8)[0, 0, :])
            self.assertEqual(self.colors[index], frame_color)


if __name__ == "__main__":
    unittest.main()
