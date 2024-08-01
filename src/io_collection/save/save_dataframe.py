import io
import os
from typing import Any

import pandas as pd

from io_collection.save.save_buffer import save_buffer_to_s3


def save_dataframe(location: str, key: str, dataframe: pd.DataFrame, **kwargs: Any) -> None:
    """
    Save dataframe to key at specified location.

    Method will save to the S3 bucket if the location begins with the **s3://**
    protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.csv`.
    dataframe
        Dataframe to save.
    **kwargs
        Additional parameters for saving dataframe. The keyword arguments are
        passed to `pandas.to_csv`.
    """

    if not key.endswith(".csv"):
        raise ValueError(f"key [ {key} ] must have [ csv ] extension")

    if location[:5] == "s3://":
        save_dataframe_to_s3(location[5:], key, dataframe, **kwargs)
    else:
        save_dataframe_to_fs(location, key, dataframe, **kwargs)


def save_dataframe_to_fs(path: str, key: str, dataframe: pd.DataFrame, **kwargs: Any) -> None:
    """
    Save dataframe to key on local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key ending in `.csv`.
    dataframe
        Dataframe to save.
    **kwargs
        Additional parameters for saving dataframe. The keyword arguments are
        passed to `pandas.to_csv`.
    """

    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)
    dataframe.to_csv(full_path, **kwargs)


def save_dataframe_to_s3(bucket: str, key: str, dataframe: pd.DataFrame, **kwargs: Any) -> None:
    """
    Save dataframe to key in AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key ending in `.csv`.
    dataframe
        Dataframe to save.
    **kwargs
        Additional parameters for saving dataframe. The keyword arguments are
        passed to `pandas.to_csv`.
    """

    with io.BytesIO() as buffer:
        dataframe.to_csv(buffer, **kwargs)
        save_buffer_to_s3(bucket, key, buffer, "text/csv")
