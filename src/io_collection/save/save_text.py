import io
import os

from io_collection.save.save_buffer import save_buffer_to_s3


def save_text(
    location: str, key: str, text: str, content_type: str = "binary/octet-stream"
) -> None:
    if location[:5] == "s3://":
        save_text_to_s3(location[5:], key, text, content_type)
    else:
        save_text_to_fs(location, key, text)


def save_text_to_fs(path: str, key: str, text: str) -> None:
    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)
    with open(full_path, "w", encoding="utf-8") as file:
        file.write(text)


def save_text_to_s3(bucket: str, key: str, text: str, content_type: str) -> None:
    with io.BytesIO() as buffer:
        buffer.write(text.encode("utf-8"))
        save_buffer_to_s3(bucket, key, buffer, content_type)
