import os

import pandas as pd
from prefect import task

from io_collection.load.task_load_buffer import load_buffer_from_s3


@task
def load_dataframe(location: str, key: str) -> pd.DataFrame:
    if location[:5] == "s3://":
        return load_dataframe_from_s3(location[5:], key)
    else:
        return load_dataframe_from_fs(location, key)


def load_dataframe_from_fs(path: str, key: str) -> pd.DataFrame:
    full_path = os.path.join(path, key)
    return pd.read_csv(full_path)


def load_dataframe_from_s3(bucket: str, key: str) -> pd.DataFrame:
    buffer = load_buffer_from_s3(bucket, key)
    return pd.read_csv(buffer)
