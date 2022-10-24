import os
import tarfile

from prefect import task

from io_collection.load.load_buffer import load_buffer_from_s3


@task(persist_result=False)
def load_tar(location: str, key: str) -> tarfile.TarFile:
    if location[:5] == "s3://":
        return load_tar_from_s3(location[5:], key)
    else:
        return load_tar_from_fs(location, key)


def load_tar_from_fs(path: str, key: str) -> tarfile.TarFile:
    full_path = os.path.join(path, key)
    return tarfile.open(full_path, mode="r:xz")


def load_tar_from_s3(bucket: str, key: str) -> tarfile.TarFile:
    buffer = load_buffer_from_s3(bucket, key)
    return tarfile.open(fileobj=buffer, mode="r:xz")
