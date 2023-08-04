import json
import os

from io_collection.load.load_buffer import load_buffer_from_s3


def load_json(location: str, key: str) -> str:
    if location[:5] == "s3://":
        return load_json_from_s3(location[5:], key)
    else:
        return load_json_from_fs(location, key)


def load_json_from_fs(path: str, key: str) -> str:
    full_path = os.path.join(path, key)
    return json.loads(open(full_path, "r", encoding="utf-8").read())


def load_json_from_s3(bucket: str, key: str) -> str:
    buffer = load_buffer_from_s3(bucket, key)
    return json.loads(buffer.getvalue().decode("utf-8"))
