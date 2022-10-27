import os

import boto3
from prefect import task


@task
def check_key(location: str, key: str) -> bool:
    if location[:5] == "s3://":
        return check_key_on_s3(location[5:], key)
    else:
        return check_key_on_fs(location, key)


def check_key_on_fs(path: str, key: str) -> bool:
    full_path = os.path.join(path, key)
    return os.path.isfile(full_path)


def check_key_on_s3(bucket: str, key: str) -> bool:
    s3_client = boto3.client("s3")
    result = s3_client.list_objects(Bucket=bucket, Prefix=key)
    return "Contents" in result
