import os
from typing import Any

import pandas as pd

from io_collection.load.load_buffer import load_buffer_from_s3


def load_dataframe(location: str, key: str, **kwargs: Any) -> pd.DataFrame:
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
        raise ValueError(f"key [ {key} ] must have [ csv ] extension")

    if location[:5] == "s3://":
        return load_dataframe_from_s3(location[5:], key, **kwargs)
    return load_dataframe_from_fs(location, key, **kwargs)


def load_dataframe_from_fs(path: str, key: str, **kwargs: Any) -> pd.DataFrame:
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

    full_path = os.path.join(path, key)
    return pd.read_csv(full_path, **kwargs)


def load_dataframe_from_s3(bucket: str, key: str, **kwargs: Any) -> pd.DataFrame:
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

    buffer = load_buffer_from_s3(bucket, key)
    return pd.read_csv(buffer, **kwargs)
