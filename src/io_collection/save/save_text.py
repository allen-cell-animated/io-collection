import io
import os

from io_collection.save.save_buffer import save_buffer_to_s3


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
        Object key ending in `.txt`.
    text
        Text to save.
    content_type
        Content type (S3 only).
    """

    if not key.endswith(".txt"):
        raise ValueError(f"key [ {key} ] must have [ txt ] extension")

    if location[:5] == "s3://":
        save_text_to_s3(location[5:], key, text, content_type)
    else:
        save_text_to_fs(location, key, text)


def save_text_to_fs(path: str, key: str, text: str) -> None:
    """
    Save text to key on local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key ending in `.csv`.
    text
        Text to save.
    """

    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)
    with open(full_path, "w", encoding="utf-8") as file:
        file.write(text)


def save_text_to_s3(bucket: str, key: str, text: str, content_type: str) -> None:
    """
    Save text to key in AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key ending in `.csv`.
    text
        Text to save.
    content_type
        Content type (S3 only).
    """

    with io.BytesIO() as buffer:
        buffer.write(text.encode("utf-8"))
        save_buffer_to_s3(bucket, key, buffer, content_type)
