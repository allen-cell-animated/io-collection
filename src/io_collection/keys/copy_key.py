import os
import shutil

import boto3


def copy_key(location: str, old_key: str, new_key: str) -> None:
    if location[:5] == "s3://":
        copy_key_on_s3(location[5:], old_key, new_key)
    else:
        copy_key_on_fs(location, old_key, new_key)


def copy_key_on_fs(path: str, old_key: str, new_key: str) -> None:
    full_old_path = os.path.join(path, old_key)
    full_new_path = os.path.join(path, new_key)
    os.makedirs(os.path.split(full_new_path)[0], exist_ok=True)
    shutil.copyfile(full_old_path, full_new_path)


def copy_key_on_s3(bucket: str, old_key: str, new_key: str) -> None:
    s3_client = boto3.client("s3")
    s3_client.copy_object(Bucket=bucket, CopySource=f"{bucket}/{old_key}", Key=new_key)
