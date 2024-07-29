import json
import os
from typing import Union

from io_collection.load.load_buffer import load_buffer_from_s3


def load_json(location: str, key: str) -> Union[list, dict]:
    """
    Load key as dict or list from specified location.

    Method will load from the S3 bucket if the location begins with the
    **s3://** protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.json`.

    Returns
    -------
    :
        Loaded json.
    """

    if not key.endswith(".json"):
        raise ValueError(f"key [ {key} ] must have [ json ] extension")

    if location[:5] == "s3://":
        return load_json_from_s3(location[5:], key)
    return load_json_from_fs(location, key)


def load_json_from_fs(path: str, key: str) -> Union[list, dict]:
    """
    Load key as dict or list from local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key ending in `.json`.

    Returns
    -------
    :
        Loaded json.
    """

    full_path = os.path.join(path, key)
    return json.loads(open(full_path, "r", encoding="utf-8").read())


def load_json_from_s3(bucket: str, key: str) -> Union[list, dict]:
    """
    Load key as dict or list from AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key ending in `.json`.

    Returns
    -------
    :
        Loaded json.
    """

    buffer = load_buffer_from_s3(bucket, key)
    return json.loads(buffer.getvalue().decode("utf-8"))
