import io
import os
import tempfile

import boto3
import numpy as np
from bioio.writers import OmeTiffWriter
from PIL import Image

from io_collection.save.save_buffer import _save_buffer_to_s3

EXTENSIONS = (".ome.tif", ".ome.tiff", ".png")


def save_image(location: str, key: str, image: np.ndarray) -> None:
    """
    Save image array to key at specified location.

    Method will save to the S3 bucket if the location begins with the **s3://**
    protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.ome.tiff` or `.png`.
    image
        Image array.
    """

    if not key.endswith(EXTENSIONS):
        raise ValueError(
            f"key [ {key} ] must have [ {' | '.join([ext[1:] for ext in EXTENSIONS])} ] extension"
        )

    if location[:5] == "s3://":
        _save_image_to_s3(location[5:], key, image)
    else:
        _save_image_to_fs(location, key, image)


def _save_image_to_fs(path: str, key: str, image: np.ndarray) -> None:
    """
    Save image array to key on local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key ending in `.ome.tiff` or `.png`.
    image
        Image array.
    """

    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)

    if key.endswith(".ome.tiff") or key.endswith(".ome.tif"):
        OmeTiffWriter.save(image, full_path)
    elif key.endswith(".png"):
        Image.fromarray(image).save(full_path)  # type: ignore


def _save_image_to_s3(bucket: str, key: str, image: np.ndarray) -> None:
    """
    Save image array to key in AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key ending in `.ome.tiff` or `.png`.
    image
        Image array.
    """

    s3_client = boto3.client("s3")

    if key.endswith(".ome.tiff") or key.endswith(".ome.tif"):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = os.path.join(temp_dir, "temp.ome.tiff")
            OmeTiffWriter.save(image, temp_path)

            with open(temp_path, "rb") as fileobj:
                s3_client.upload_fileobj(fileobj, bucket, key)
    elif key.endswith(".png"):
        with io.BytesIO() as buffer:
            Image.fromarray(image).save(buffer, format="png")  # type: ignore
            _save_buffer_to_s3(bucket, key, buffer, "image/png")
