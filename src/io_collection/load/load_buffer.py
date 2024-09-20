import io
from pathlib import Path

import boto3

MAX_CONTENT_LENGTH = 2**31 - 1


def load_buffer(location: str, key: str) -> io.BytesIO:
    """
    Load key into in-memory bytes buffer from specified location.

    Method will load from the S3 bucket if the location begins with the
    **s3://** protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key.

    Returns
    -------
    :
        Loaded object buffer.
    """

    if location[:5] == "s3://":
        return _load_buffer_from_s3(location[5:], key)
    return _load_buffer_from_fs(location, key)


def _load_buffer_from_fs(path: str, key: str) -> io.BytesIO:
    """
    Load key into in-memory bytes buffer from local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key.

    Returns
    -------
    :
        Loaded object buffer.
    """

    full_path = Path(path) / key
    with full_path.open("rb") as fileobj:
        return io.BytesIO(fileobj.read())


def _load_buffer_from_s3(bucket: str, key: str) -> io.BytesIO:
    """
    Load key into in-memory bytes buffer from AWS S3 bucket.

    Objects larger than `MAX_CONTENT_LENGTH` are loaded in chunks.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key.

    Returns
    -------
    :
        Loaded object buffer.
    """

    s3_client = boto3.client("s3")
    obj = s3_client.get_object(Bucket=bucket, Key=key)

    # Check if body needs to be loaded in chunks.
    content_length = obj["ContentLength"]

    if content_length > MAX_CONTENT_LENGTH:
        body = bytearray()
        for chunk in obj["Body"].iter_chunks(chunk_size=MAX_CONTENT_LENGTH):
            body += chunk
        return io.BytesIO(body)

    return io.BytesIO(obj["Body"].read())
