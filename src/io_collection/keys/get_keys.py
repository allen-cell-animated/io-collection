import os
from glob import glob

import boto3


def get_keys(location: str, prefix: str) -> list[str]:
    """
    Get list of objects at specified location with given prefix.

    Method will check in the S3 bucket if the location begins with the **s3://**
    protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    prefix
        Object key prefix.

    Returns
    -------
    :
        List of all object keys at location.
    """

    if location[:5] == "s3://":
        return get_keys_from_s3(location[5:], prefix)
    return get_keys_from_fs(location, prefix)


def get_keys_from_fs(path: str, prefix: str) -> list[str]:
    """
    Get list of objects on local file system with given prefix.

    Parameters
    ----------
    location
        Local object path.
    prefix
        Object key prefix.

    Returns
    -------
    :
        List of all object keys on the local file system.
    """

    glob_pattern = f"{path}/{prefix}/**/*".replace("//", "/")
    all_files = glob(glob_pattern, recursive=True)
    regular_files = [file for file in all_files if not os.path.isdir(file)]
    keys = [file.replace(path, "").strip("/") for file in regular_files]
    return keys


def get_keys_from_s3(bucket: str, prefix: str) -> list[str]:
    """
    Get list of objects in AWS S3 bucket with given prefix.

    Parameters
    ----------
    location
        AWS S3 bucket name.
    prefix
        Object key prefix.

    Returns
    -------
    :
        List of all object keys in the AWS bucket.
    """

    s3_client = boto3.client("s3")

    bucket = bucket.replace("s3://", "")
    prefix = f"{prefix}/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    get_response = True
    keys: list[str] = []

    while get_response:
        if "Contents" not in response:
            break

        content_keys = [content["Key"] for content in response["Contents"]]
        keys = keys + content_keys

        if response["IsTruncated"]:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=response["NextContinuationToken"],
            )
        else:
            get_response = False

    return keys
