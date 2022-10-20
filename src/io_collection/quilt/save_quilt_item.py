import io

import quilt3
from prefect import task

from io_collection.save import save_buffer


@task
def save_quilt_item(location: str, key: str, package: quilt3.Package, item: str) -> None:
    contents = package[item].get_bytes()
    buffer = io.BytesIO(contents)
    save_buffer.fn(location, key, buffer)
