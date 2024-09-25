from pathlib import Path

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
        return _get_keys_on_s3(location[5:], prefix)
    return _get_keys_on_fs(location, prefix)


def _get_keys_on_fs(path: str, prefix: str) -> list[str]:
    """
    Get list of objects on local file system with given prefix.

    Parameters
    ----------
    path
        Local object path.
    prefix
        Object key prefix.

    Returns
    -------
    :
        List of all object keys on the local file system.
    """

    glob_pattern = f"{prefix}/**/*".replace("//", "/")
    all_files = Path(path).rglob(glob_pattern)
    regular_files = [str(file) for file in all_files if not file.is_dir()]
    return [file.replace(path, "").strip("/") for file in regular_files]


def _get_keys_on_s3(bucket: str, prefix: str) -> list[str]:
    """
    Get list of objects in AWS S3 bucket with given prefix.

    Parameters
    ----------
    bucket
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
