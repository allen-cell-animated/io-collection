import tarfile
from pathlib import Path

from io_collection.load.load_buffer import _load_buffer_from_s3


def load_tar(location: str, key: str) -> tarfile.TarFile:
    """
    Load key as tar archive from specified location.

    Method will load from the S3 bucket if the location begins with the
    **s3://** protocol, otherwise it assumes the location is a local path.

    Method currently only supports `xz` compression.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.tar.xz`.

    Returns
    -------
    :
        Loaded tar archive.
    """

    if not key.endswith(".tar.xz"):
        message = f"key [ {key} ] must have [ tar.xz ] extension"
        raise ValueError(message)

    if location[:5] == "s3://":
        return _load_tar_from_s3(location[5:], key)
    return _load_tar_from_fs(location, key)


def _load_tar_from_fs(path: str, key: str) -> tarfile.TarFile:
    """
    Load key as tar archive from local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key ending in `.tar.xz`.

    Returns
    -------
    :
        Loaded tar archive.
    """

    full_path = Path(path) / key
    return tarfile.open(full_path, mode="r:xz")


def _load_tar_from_s3(bucket: str, key: str) -> tarfile.TarFile:
    """
    Load key as tar archive from AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key ending in `.tar.xz`.

    Returns
    -------
    :
        Loaded tar archive.
    """

    buffer = _load_buffer_from_s3(bucket, key)
    return tarfile.open(fileobj=buffer, mode="r:xz")
