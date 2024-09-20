from __future__ import annotations

import json
from pathlib import Path

from io_collection.load.load_buffer import _load_buffer_from_s3


def load_json(location: str, key: str) -> list | dict:
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
        message = f"key [ {key} ] must have [ json ] extension"
        raise ValueError(message)

    if location[:5] == "s3://":
        return _load_json_from_s3(location[5:], key)
    return _load_json_from_fs(location, key)


def _load_json_from_fs(path: str, key: str) -> list | dict:
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

    full_path = Path(path) / key
    with full_path.open("r") as fileobj:
        return json.loads(fileobj.read())


def _load_json_from_s3(bucket: str, key: str) -> list | dict:
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

    buffer = _load_buffer_from_s3(bucket, key)
    return json.loads(buffer.getvalue().decode("utf-8"))
