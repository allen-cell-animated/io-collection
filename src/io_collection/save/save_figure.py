import io
import os
from typing import Any

from prefect import task
import matplotlib.figure as mpl

from io_collection.save.save_buffer import save_buffer_to_s3


@task
def save_figure(location: str, key: str, figure: mpl.Figure, **kwargs: Any) -> None:
    if location[:5] == "s3://":
        save_figure_to_s3(location[5:], key, figure, **kwargs)
    else:
        save_figure_to_fs(location, key, figure, **kwargs)


def save_figure_to_fs(path: str, key: str, figure: mpl.Figure, **kwargs: Any) -> None:
    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)
    figure.savefig(full_path, **kwargs)


def save_figure_to_s3(bucket: str, key: str, figure: mpl.Figure, **kwargs: Any) -> None:
    """
    Saves figure to bucket with given key.
    """
    with io.BytesIO() as buffer:
        figure.savefig(buffer, **kwargs)
        save_buffer_to_s3(bucket, key, buffer)
