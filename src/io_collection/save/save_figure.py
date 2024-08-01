import io
import os
from typing import Any

import matplotlib.figure as mpl

from io_collection.save.save_buffer import save_buffer_to_s3

EXTENSIONS = (".png", ".jpeg", ".jpg", ".svg")

CONTENT_TYPES = {
    "png": "image/png",
    "jpeg": "image/jpeg",
    "jpg": "image/jpeg",
    "svg": "image/svg+xml",
}


def save_figure(location: str, key: str, figure: mpl.Figure, **kwargs: Any) -> None:
    """
    Save matplotlib figure to key at specified location.

    Method will save to the S3 bucket if the location begins with the **s3://**
    protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.png`, `.jpg`, or `.svg`.
    figure
        Figure instance to save.
    **kwargs
        Additional parameters for saving figure. The keyword arguments are
        passed to `matplotlib.pyplot.savefig`.
    """

    if not key.endswith(EXTENSIONS):
        raise ValueError(
            f"key [ {key} ] must have [ {' | '.join([ext[1:] for ext in EXTENSIONS])} ] extension"
        )

    if location[:5] == "s3://":
        save_figure_to_s3(location[5:], key, figure, **kwargs)
    else:
        save_figure_to_fs(location, key, figure, **kwargs)


def save_figure_to_fs(path: str, key: str, figure: mpl.Figure, **kwargs: Any) -> None:
    """
    Save matplotlib figure to key on local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key ending in `.png`, `.jpg`, or `.svg`.
    figure
        Figure instance to save.
    **kwargs
        Additional parameters for saving figure. The keyword arguments are
        passed to `matplotlib.pyplot.savefig`.
    """

    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)
    figure.savefig(full_path, **kwargs)


def save_figure_to_s3(bucket: str, key: str, figure: mpl.Figure, **kwargs: Any) -> None:
    """
    Save matplotlib figure to key in AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key ending in `.png`, `.jpg`, or `.svg`.
    figure
        Figure instance to save.
    **kwargs
        Additional parameters for saving figure. The keyword arguments are
        passed to `matplotlib.pyplot.savefig`.
    """

    with io.BytesIO() as buffer:
        figure.savefig(buffer, **kwargs)
        content_type = CONTENT_TYPES[key.split(".")[-1]]
        save_buffer_to_s3(bucket, key, buffer, content_type)
