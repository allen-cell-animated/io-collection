from __future__ import annotations

from pathlib import Path

import pandas as pd

from io_collection.load.load_buffer import _load_buffer_from_s3


def load_dataframe(
    location: str, key: str, **kwargs: int | str | list | dict | bool
) -> pd.DataFrame:
    """
    Load key as dataframe from specified location.

    Method will load from the S3 bucket if the location begins with the
    **s3://** protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.csv`.
    **kwargs
        Additional parameters for loading dataframe. The keyword arguments are
        passed to `pandas.read_csv`.

    Returns
    -------
    :
        Loaded dataframe.
    """

    if not key.endswith(".csv"):
        message = f"key [ {key} ] must have [ csv ] extension"
        raise ValueError(message)

    if location[:5] == "s3://":
        return _load_dataframe_from_s3(location[5:], key, **kwargs)
    return _load_dataframe_from_fs(location, key, **kwargs)


def _load_dataframe_from_fs(
    path: str, key: str, **kwargs: int | str | list | dict | bool
) -> pd.DataFrame:
    """
    Load key as dataframe from local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key ending in `.csv`.
    **kwargs
        Additional parameters for loading dataframe. The keyword arguments are
        passed to `pandas.read_csv`.

    Returns
    -------
    :
        Loaded dataframe.
    """

    full_path = Path(path) / key
    return pd.read_csv(full_path, **kwargs)


def _load_dataframe_from_s3(
    bucket: str, key: str, **kwargs: int | str | list | dict | bool
) -> pd.DataFrame:
    """
    Load key as dataframe from AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key ending in `.csv`.
    **kwargs
        Additional parameters for loading dataframe. The keyword arguments are
        passed to `pandas.read_csv`.

    Returns
    -------
    :
        Loaded dataframe.
    """

    buffer = _load_buffer_from_s3(bucket, key)
    return pd.read_csv(buffer, **kwargs)
