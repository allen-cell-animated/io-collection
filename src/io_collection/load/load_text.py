from pathlib import Path

from io_collection.load.load_buffer import _load_buffer_from_s3


def load_text(location: str, key: str) -> str:
    """
    Load key as string from specified location.

    Method will load from the S3 bucket if the location begins with the
    **s3://** protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in valid text extension.

    Returns
    -------
    :
        Loaded text.
    """

    if location[:5] == "s3://":
        return _load_text_from_s3(location[5:], key)
    return _load_text_from_fs(location, key)


def _load_text_from_fs(path: str, key: str) -> str:
    """
    Load key as dict or list from local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key ending in valid text extension.

    Returns
    -------
    :
        Loaded text.
    """

    full_path = Path(path) / key
    with full_path.open("r") as fileobj:
        return fileobj.read()


def _load_text_from_s3(bucket: str, key: str) -> str:
    """
    Load key as dict or list from AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key ending in valid text extension.

    Returns
    -------
    :
        Loaded text.
    """

    buffer = _load_buffer_from_s3(bucket, key)
    return buffer.getvalue().decode("utf-8")
