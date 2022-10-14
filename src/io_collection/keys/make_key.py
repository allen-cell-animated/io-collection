import os
from datetime import datetime

from prefect import task


@task
def make_key(*subkeys: str) -> str:
    key = os.path.join(*subkeys)

    timestamp = datetime.now().strftime("%Y-%m-%d")
    key = key.replace("{{timestamp}}", timestamp)

    return key
