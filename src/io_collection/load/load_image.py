import os
from typing import Optional

import bioio_ome_tiff
import bioio_tifffile
from bioio import BioImage


def load_image(location: str, key: str, dim_order: Optional[str] = None) -> BioImage:
    if location[:5] == "s3://":
        return load_image_from_s3(location[5:], key, dim_order)
    else:
        return load_image_from_fs(location, key, dim_order)


def load_image_from_fs(path: str, key: str, dim_order: Optional[str] = None) -> BioImage:
    full_path = os.path.join(path, key)
    return BioImage(
        full_path,
        reader=bioio_ome_tiff.Reader if ".ome.tif" in key else bioio_tifffile.Reader,
        dim_order=dim_order,
    )


def load_image_from_s3(bucket: str, key: str, dim_order: Optional[str] = None) -> BioImage:
    full_key = f"s3://{bucket}/{key}"
    return BioImage(
        full_key,
        reader=bioio_ome_tiff.Reader if ".ome.tif" in key else bioio_tifffile.Reader,
        dim_order=dim_order,
    )
