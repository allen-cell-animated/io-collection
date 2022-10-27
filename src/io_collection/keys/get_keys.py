import os
from glob import glob

import boto3
from prefect import task


@task
def get_keys(location: str, prefix: str) -> list[str]:
    if location[:5] == "s3://":
        return get_keys_from_s3(location[5:], prefix)
    else:
        return get_keys_from_fs(location, prefix)


def get_keys_from_fs(path: str, prefix: str) -> list[str]:
    all_files = glob(f"{path}{prefix}/**/*", recursive=True)
    regular_files = [file for file in all_files if not os.path.isdir(file)]
    keys = [file.replace(path, "") for file in regular_files]
    return keys


def get_keys_from_s3(bucket: str, prefix: str) -> list[str]:
    s3_client = boto3.client("s3")

    bucket = bucket.replace("s3://", "")
    prefix = f"{prefix}/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    get_response = True
    keys: list[str] = []

    while get_response:
        if "Contents" not in response:
            break

        content_keys = [content["Key"] for content in response["Contents"]]
        keys = keys + content_keys

        if response["IsTruncated"]:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=response["NextContinuationToken"],
            )
        else:
            get_response = False

    return keys
