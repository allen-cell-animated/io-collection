import io
from pathlib import Path

from io_collection.save.save_buffer import _save_buffer_to_s3


def save_text(
    location: str, key: str, text: str, content_type: str = "binary/octet-stream"
) -> None:
    """
    Save text to key at specified location.

    Method will save to the S3 bucket if the location begins with the **s3://**
    protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in valid text extension.
    text
        Text to save.
    content_type
        Content type (S3 only).
    """

    if location[:5] == "s3://":
        _save_text_to_s3(location[5:], key, text, content_type)
    else:
        _save_text_to_fs(location, key, text)


def _save_text_to_fs(path: str, key: str, text: str) -> None:
    """
    Save text to key on local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key ending in valid text extension.
    text
        Text to save.
    """

    full_path = Path(path) / key
    full_path.parent.mkdir(parents=True, exist_ok=True)
    with full_path.open("w") as fileobj:
        fileobj.write(text)


def _save_text_to_s3(bucket: str, key: str, text: str, content_type: str) -> None:
    """
    Save text to key in AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key ending in valid text extension.
    text
        Text to save.
    content_type
        Content type (S3 only).
    """

    with io.BytesIO() as buffer:
        buffer.write(text.encode("utf-8"))
        _save_buffer_to_s3(bucket, key, buffer, content_type)
