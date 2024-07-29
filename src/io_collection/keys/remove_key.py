import os

import boto3


def remove_key(location: str, key: str) -> None:
    """
    Removes object key at specified location.

    Method will remove from the S3 bucket if the location begins with the
    **s3://** protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key.
    """

    if location[:5] == "s3://":
        remove_key_on_s3(location[5:], key)
    else:
        remove_key_on_fs(location, key)


def remove_key_on_fs(path: str, key: str) -> None:
    """
    Remove object key from local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key.
    """

    full_path = os.path.join(path, key)

    if os.path.isfile(full_path):
        os.remove(full_path)


def remove_key_on_s3(bucket: str, key: str) -> None:
    """
    Remove object key in AWS S3 bucket.

    Parameters
    ----------
    path
        AWS S3 bucket name.
    key
        Object key.
    """

    s3_client = boto3.client("s3")
    s3_client.delete_object(Bucket=bucket, Key=key)
