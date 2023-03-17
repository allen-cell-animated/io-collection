import os
from datetime import datetime


def make_key(*subkeys: str) -> str:
    key = os.path.join(*subkeys)

    timestamp = datetime.now().strftime("%Y-%m-%d")
    key = key.replace("{{timestamp}}", timestamp)

    key = key.replace("__", "_")
    key = key.replace("_.", ".")

    return key
