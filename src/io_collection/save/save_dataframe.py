import io
import os
from typing import Any

from prefect import task
import pandas as pd

from io_collection.save.save_buffer import save_buffer_to_s3


@task
def save_dataframe(location: str, key: str, dataframe: pd.DataFrame, **kwargs: Any) -> None:
    if location[:5] == "s3://":
        save_dataframe_to_s3(location[5:], key, dataframe, **kwargs)
    else:
        save_dataframe_to_fs(location, key, dataframe, **kwargs)


def save_dataframe_to_fs(path: str, key: str, dataframe: pd.DataFrame, **kwargs: Any) -> None:
    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)
    dataframe.to_csv(full_path, **kwargs)


def save_dataframe_to_s3(bucket: str, key: str, dataframe: pd.DataFrame, **kwargs: Any) -> None:
    """
    Saves dataframe to bucket with given key.
    """
    with io.BytesIO() as buffer:
        dataframe.to_csv(buffer, **kwargs)
        save_buffer_to_s3(bucket, key, buffer)
