import os

import boto3


def remove_key(location: str, key: str) -> None:
    if location[:5] == "s3://":
        remove_key_on_s3(location[5:], key)
    else:
        remove_key_on_fs(location, key)


def remove_key_on_fs(path: str, key: str) -> None:
    full_path = os.path.join(path, key)
    os.remove(full_path)


def remove_key_on_s3(bucket: str, key: str) -> None:
    s3_client = boto3.client("s3")
    s3_client.delete_object(Bucket=bucket, Key=key)
