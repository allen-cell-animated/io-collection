import os

from prefect import task

from io_collection.load.load_buffer import load_buffer_from_s3


@task
def load_text(location: str, key: str) -> str:
    if location[:5] == "s3://":
        return load_text_from_s3(location[5:], key)
    else:
        return load_text_from_fs(location, key)


def load_text_from_fs(path: str, key: str) -> str:
    full_path = os.path.join(path, key)
    return open(full_path, "r", encoding="utf-8").read()


def load_text_from_s3(bucket: str, key: str) -> str:
    buffer = load_buffer_from_s3(bucket, key)
    return buffer.getvalue().decode("utf-8")
