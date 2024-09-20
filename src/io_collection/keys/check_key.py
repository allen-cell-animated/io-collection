from pathlib import Path

import boto3


def check_key(location: str, key: str) -> bool:
    """
    Check if object key exists at specified location.

    Method will check in the S3 bucket if the location begins with the **s3://**
    protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key.

    Returns
    -------
    :
        True if the object exists in the location, False otherwise.
    """

    if location[:5] == "s3://":
        return _check_key_on_s3(location[5:], key)
    return _check_key_on_fs(location, key)


def _check_key_on_fs(path: str, key: str) -> bool:
    """
    Check if object key exists on local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key.

    Returns
    -------
    bool
        True if the object exists on the local file system, False otherwise.
    """

    full_path = Path(path) / key
    return full_path.is_file()


def _check_key_on_s3(bucket: str, key: str) -> bool:
    """
    Check if object key exists in AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key.

    Returns
    -------
    bool
        True if the object exists in the AWS bucket, False otherwise.
    """

    s3_client = boto3.client("s3")
    result = s3_client.list_objects(Bucket=bucket, Prefix=key)
    return "Contents" in result
