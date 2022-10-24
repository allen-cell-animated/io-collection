import io
import os

import boto3
from prefect import task

MAX_CONTENT_LENGTH = 2**31 - 1


@task
def load_buffer(location: str, key: str) -> io.BytesIO:
    if location[:5] == "s3://":
        return load_buffer_from_s3(location[5:], key)
    else:
        return load_buffer_from_fs(location, key)


def load_buffer_from_fs(path: str, key: str) -> io.BytesIO:
    full_path = os.path.join(path, key)
    return io.BytesIO(open(full_path, "rb").read())


def load_buffer_from_s3(bucket: str, key: str) -> io.BytesIO:
    """
    Loads body from bucket for given key.
    """
    s3_client = boto3.client("s3")
    obj = s3_client.get_object(Bucket=bucket, Key=key)

    # Check if body needs to be loaded in chunks.
    content_length = obj["ContentLength"]

    if content_length > MAX_CONTENT_LENGTH:
        print("Loading chunks ...")
        body = bytearray()
        for chunk in obj["Body"].iter_chunks(chunk_size=MAX_CONTENT_LENGTH):
            body += chunk
        return io.BytesIO(body)
    else:
        return io.BytesIO(obj["Body"].read())
