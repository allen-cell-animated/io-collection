import io
import os

from prefect import task
import pandas as pd

from io_collection.save.task_save_buffer import save_buffer_to_s3


@task
def save_dataframe(location: str, key: str, dataframe: pd.DataFrame, index: bool = True) -> None:
    if location[:5] == "s3://":
        save_dataframe_to_s3(location[5:], key, dataframe, index)
    else:
        save_dataframe_to_fs(location, key, dataframe, index)


def save_dataframe_to_fs(path: str, key: str, dataframe: pd.DataFrame, index: bool = True) -> None:
    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)
    dataframe.to_csv(full_path, index=index)


def save_dataframe_to_s3(
    bucket: str, key: str, dataframe: pd.DataFrame, index: bool = True
) -> None:
    """
    Saves dataframe to bucket with given key.
    """
    with io.BytesIO() as buffer:
        dataframe.to_csv(buffer, index=index)
        save_buffer_to_s3(bucket, key, buffer)
