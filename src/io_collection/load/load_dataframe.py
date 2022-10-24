import os
from typing import Any

import pandas as pd
from prefect import task

from io_collection.load.load_buffer import load_buffer_from_s3


@task
def load_dataframe(location: str, key: str, **kwargs: Any) -> pd.DataFrame:
    if location[:5] == "s3://":
        return load_dataframe_from_s3(location[5:], key, **kwargs)
    else:
        return load_dataframe_from_fs(location, key, **kwargs)


def load_dataframe_from_fs(path: str, key: str, **kwargs: Any) -> pd.DataFrame:
    full_path = os.path.join(path, key)
    return pd.read_csv(full_path, **kwargs)


def load_dataframe_from_s3(bucket: str, key: str, **kwargs: Any) -> pd.DataFrame:
    buffer = load_buffer_from_s3(bucket, key)
    return pd.read_csv(buffer, **kwargs)
