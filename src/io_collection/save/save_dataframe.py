from __future__ import annotations

import io
from pathlib import Path
from typing import TYPE_CHECKING

from io_collection.save.save_buffer import _save_buffer_to_s3

if TYPE_CHECKING:
    import pandas as pd


def save_dataframe(
    location: str, key: str, dataframe: pd.DataFrame, **kwargs: int | str | list | dict | bool
) -> None:
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
        message = f"key [ {key} ] must have [ csv ] extension"
        raise ValueError(message)

    if location[:5] == "s3://":
        _save_dataframe_to_s3(location[5:], key, dataframe, **kwargs)
    else:
        _save_dataframe_to_fs(location, key, dataframe, **kwargs)


def _save_dataframe_to_fs(
    path: str, key: str, dataframe: pd.DataFrame, **kwargs: int | str | list | dict | bool
) -> None:
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

    full_path = Path(path) / key
    full_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(full_path, **kwargs)


def _save_dataframe_to_s3(
    bucket: str, key: str, dataframe: pd.DataFrame, **kwargs: int | str | list | dict | bool
) -> None:
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
        _save_buffer_to_s3(bucket, key, buffer, "text/csv")
