import pickle
from typing import Any

from io_collection.load.load_buffer import load_buffer


def load_pickle(location: str, key: str) -> Any:
    """
    Load key as pickled object from specified location.

    Method will load from the S3 bucket if the location begins with the
    **s3://** protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.pkl`.

    Returns
    -------
    :
        Loaded object.
    """

    if not key.endswith(".pkl"):
        raise ValueError(f"key [ {key} ] must have [ pkl ] extension")

    buffer = load_buffer(location, key)
    return pickle.loads(buffer.read())
