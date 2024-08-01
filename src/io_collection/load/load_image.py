import os
from typing import Optional

import bioio_ome_tiff
import bioio_tifffile
from bioio import BioImage

EXTENSIONS = (".tif", ".tiff", ".ome.tif", ".ome.tiff")


def load_image(location: str, key: str, dim_order: Optional[str] = None) -> BioImage:
    """
    Load key as BioIO image from specified location.

    Method will load from the S3 bucket if the location begins with the
    **s3://** protocol, otherwise it assumes the location is a local path.

    Method currently only supports `.tiff` and `ome.tiff` images.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.tiff` or `ome.tiff`.

    Returns
    -------
    :
        Loaded image.
    """

    if not key.endswith(EXTENSIONS):
        raise ValueError(
            f"key [ {key} ] must have [ {' | '.join([ext[1:] for ext in EXTENSIONS])} ] extension"
        )

    if location[:5] == "s3://":
        return load_image_from_s3(location[5:], key, dim_order)
    return load_image_from_fs(location, key, dim_order)


def load_image_from_fs(path: str, key: str, dim_order: Optional[str] = None) -> BioImage:
    """
    Load key as BioIO image from local file system.

    Parameters
    ----------
    path
        Local object path.
    key
        Object key ending in `.tiff` or `ome.tiff`.

    Returns
    -------
    :
        Loaded image.
    """

    full_path = os.path.join(path, key)
    return BioImage(
        full_path,
        reader=bioio_ome_tiff.Reader if ".ome.tif" in key else bioio_tifffile.Reader,
        dim_order=dim_order,
    )


def load_image_from_s3(bucket: str, key: str, dim_order: Optional[str] = None) -> BioImage:
    """
    Load key as BioIO image from AWS S3 bucket.

    Parameters
    ----------
    bucket
        AWS S3 bucket name.
    key
        Object key ending in `.tiff` or `ome.tiff`.

    Returns
    -------
    :
        Loaded image.
    """

    full_key = f"s3://{bucket}/{key}"
    return BioImage(
        full_key,
        reader=bioio_ome_tiff.Reader if ".ome.tif" in key else bioio_tifffile.Reader,
        dim_order=dim_order,
    )
