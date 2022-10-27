import os

import boto3
from prefect import task


@task
def change_key(location: str, old_key: str, new_key: str) -> None:
    if location[:5] == "s3://":
        change_key_on_s3(location[5:], old_key, new_key)
    else:
        change_key_on_fs(location, old_key, new_key)


def change_key_on_fs(path: str, old_key: str, new_key: str) -> None:
    full_old_path = os.path.join(path, old_key)
    full_new_path = os.path.join(path, new_key)
    os.renames(full_old_path, full_new_path)


def change_key_on_s3(bucket: str, old_key: str, new_key: str) -> None:
    s3_client = boto3.client("s3")
    s3_client.copy_object(Bucket=bucket, CopySource=f"{bucket}/{old_key}", Key=new_key)
    s3_client.delete_object(Bucket=bucket, Key=old_key)
