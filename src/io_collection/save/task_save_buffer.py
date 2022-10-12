import io
import os

import boto3
from prefect import task


@task
def save_buffer(location: str, key: str, buffer: io.BytesIO) -> None:
    if location[:5] == "s3://":
        save_buffer_to_s3(location[5:], key, buffer)
    else:
        save_buffer_to_fs(location, key, buffer)


def save_buffer_to_fs(path: str, key: str, buffer: io.BytesIO) -> None:
    full_path = os.path.join(path, key)
    os.makedirs(os.path.split(full_path)[0], exist_ok=True)
    with open(full_path, "wb") as file:
        file.write(buffer.getvalue())


def save_buffer_to_s3(bucket: str, key: str, buffer: io.BytesIO) -> None:
    """
    Saves buffer to bucket with given key.
    """
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
