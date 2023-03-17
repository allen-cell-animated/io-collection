import io

import quilt3

from io_collection.save.save_buffer import save_buffer


def save_quilt_item(location: str, key: str, package: quilt3.Package, item: str) -> None:
    contents = package[item].get_bytes()
    buffer = io.BytesIO(contents)
    save_buffer(location, key, buffer)
