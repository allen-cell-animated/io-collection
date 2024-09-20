import io
import pickle

from io_collection.save.save_buffer import save_buffer


def save_pickle(location: str, key: str, obj: object) -> None:
    """
    Save pickled object to key at specified location.

    Method will save to the S3 bucket if the location begins with the **s3://**
    protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.pkl`.
    obj
        Object to save.
    """

    if not key.endswith(".pkl"):
        message = f"key [ {key} ] must have [ pkl ] extension"
        raise ValueError(message)

    with io.BytesIO() as buffer:
        pickle.dump(obj, buffer)
        save_buffer(location, key, buffer)
