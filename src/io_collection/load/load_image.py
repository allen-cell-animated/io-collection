import os

from prefect import task
from aicsimageio import AICSImage
from aicsimageio.readers import OmeTiffReader, TiffReader


@task
def load_image(location: str, key: str) -> AICSImage:
    if location[:5] == "s3://":
        return load_image_from_s3(location[5:], key)
    else:
        return load_image_from_fs(location, key)


def load_image_from_fs(path: str, key: str) -> AICSImage:
    full_path = os.path.join(path, key)
    return AICSImage(full_path, reader=OmeTiffReader if key.endswith(".ome.tiff") else TiffReader)


def load_image_from_s3(bucket: str, key: str) -> AICSImage:
    full_key = f"s3://{bucket}/{key}"
    return AICSImage(full_key, reader=OmeTiffReader if key.endswith(".ome.tiff") else TiffReader)
