import io

import quilt3

from io_collection.save.save_buffer import save_buffer


def save_quilt_item(location: str, key: str, package: quilt3.Package, item: str) -> None:
    """
    Save item from Quilt package to key at specified location.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key.
    package
        Quilt package.
    item
        Name of the item in the package.
    """

    contents = package[item].get_bytes()
    buffer = io.BytesIO(contents)
    save_buffer(location, key, buffer)
