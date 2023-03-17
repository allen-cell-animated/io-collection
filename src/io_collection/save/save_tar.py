import io
import tarfile

from io_collection.keys.check_key import check_key
from io_collection.load.load_buffer import load_buffer
from io_collection.load.load_tar import load_tar
from io_collection.save.save_buffer import save_buffer


def save_tar(location: str, key: str, contents: list[str]) -> None:
    if check_key(location, key):
        existing_tar = load_tar(location, key)
    else:
        existing_tar = None

    with io.BytesIO() as buffer:
        with tarfile.open(fileobj=buffer, mode="w:xz") as tar:
            if existing_tar is not None:
                for member in existing_tar.getmembers():
                    tar.addfile(member, existing_tar.extractfile(member.name))

            for content_key in contents:
                content = load_buffer(location, content_key)
                info = tarfile.TarInfo(content_key.split("/")[-1])
                info.size = content.getbuffer().nbytes
                tar.addfile(info, fileobj=content)

        save_buffer(location, key, buffer)
