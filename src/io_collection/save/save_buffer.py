import io
import os

import boto3


def save_buffer(
    location: str, key: str, buffer: io.BytesIO, content_type: str = "binary/octet-stream"
) -> None:
    """
    Save buffer to key at specified location.

    Method will save to the S3 bucket if the location begins with the
    **s3://** protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key.
    buffer
        Content buffer.
    content_type
        Content type (S3 only).
    """

    if location[:5] == "s3://":
        _save_buffer_to_s3(location[5:], key, buffer, content_type)
    else:
        _save_buffer_to_fs(location, key, buffer)


def _save_buffer_to_fs(path: str, key: str, buffer: io.BytesIO) -> None:
    """
    Save buffer to key on local file system.

    Directories that do not exist will be created.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key.
    buffer
        Content buffer.
    """

    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)
    with open(full_path, "wb") as file:
        file.write(buffer.getvalue())


def _save_buffer_to_s3(bucket: str, key: str, buffer: io.BytesIO, content_type: str) -> None:
    """
    Save buffer to key in AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key.
    buffer
        Content buffer.
    content_type
        Content type.
    """

    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue(), ContentType=content_type)
