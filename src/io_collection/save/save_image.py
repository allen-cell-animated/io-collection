import io
import os
import tempfile

import boto3
import numpy as np
from aicsimageio.writers import OmeTiffWriter
from PIL import Image

from io_collection.save.save_buffer import save_buffer_to_s3


def save_image(location: str, key: str, image: np.ndarray) -> None:
    if location[:5] == "s3://":
        save_image_to_s3(location[5:], key, image)
    else:
        save_image_to_fs(location, key, image)


def save_image_to_fs(path: str, key: str, image: np.ndarray) -> None:
    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)

    if key.endswith(".ome.tiff") or key.endswith(".ome.tif"):
        OmeTiffWriter.save(image, full_path)
    elif key.endswith(".png"):
        Image.fromarray(image).save(full_path)


def save_image_to_s3(bucket: str, key: str, image: np.ndarray) -> None:
    s3_client = boto3.client("s3")

    if key.endswith(".ome.tiff") or key.endswith(".ome.tif"):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = os.path.join(temp_dir, "temp.ome.tiff")
            OmeTiffWriter.save(image, temp_path)

            with open(temp_path, "rb") as fileobj:
                s3_client.upload_fileobj(fileobj, bucket, key)
    elif key.endswith(".png"):
        with io.BytesIO() as buffer:
            Image.fromarray(image).save(buffer, format="png")
            save_buffer_to_s3(bucket, key, buffer, "image/png")
