import io
import tarfile

from io_collection.keys.check_key import check_key
from io_collection.load.load_buffer import load_buffer
from io_collection.load.load_tar import load_tar
from io_collection.save.save_buffer import save_buffer


def save_tar(location: str, key: str, contents: list[str]) -> None:
    """
    Save tar archive to key at specified location.

    Method will save to the S3 bucket if the location begins with the **s3://**
    protocol, otherwise it assumes the location is a local path.

    If the archive already exists, all objects in the existing archive will be
    copied into the new archive, along with the new contents.

    Method currently only supports `xz` compression.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.tar.xz`.
    contents
        List of content keys to include in the archive.
    """

    if not key.endswith(".tar.xz"):
        message = f"key [ {key} ] must have [ tar.xz ] extension"
        raise ValueError(message)

    existing_tar = load_tar(location, key) if check_key(location, key) else None

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
