import os
from typing import Optional

from aicsimageio import AICSImage
from aicsimageio.readers import OmeTiffReader, TiffReader


def load_image(location: str, key: str, dim_order: Optional[str] = None) -> AICSImage:
    if location[:5] == "s3://":
        return load_image_from_s3(location[5:], key, dim_order)
    else:
        return load_image_from_fs(location, key, dim_order)


def load_image_from_fs(path: str, key: str, dim_order: Optional[str] = None) -> AICSImage:
    full_path = os.path.join(path, key)
    return AICSImage(
        full_path,
        reader=OmeTiffReader if key.endswith(".ome.tiff") else TiffReader,
        dim_order=dim_order,
    )


def load_image_from_s3(bucket: str, key: str, dim_order: Optional[str] = None) -> AICSImage:
    full_key = f"s3://{bucket}/{key}"
    return AICSImage(
        full_key,
        reader=OmeTiffReader if key.endswith(".ome.tiff") else TiffReader,
        dim_order=dim_order,
    )
