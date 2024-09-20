from __future__ import annotations

import json
import re

from io_collection.save.save_text import save_text


def save_json(location: str, key: str, obj: dict | list, levels: int = 5) -> None:
    """
    Save dict or list as json to key at specified location.

    Method will save to the S3 bucket if the location begins with the **s3://**
    protocol, otherwise it assumes the location is a local path.

    The json is pretty printed with an indentation level of 2. For lists,
    default pretty printing places each item on a new line which can produce
    very long files. This method will attempt map lists up to the specified
    number of items onto a single line.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.json`.
    obj
        Dict or list to save.
    levels
        Maximum number of items for single line list.
    """

    if not key.endswith(".json"):
        message = f"key [ {key} ] must have [ json ] extension"
        raise ValueError(message)

    contents = json.dumps(obj, indent=2)

    for level in range(1, levels + 1):
        match_pattern = (
            r"\[" + ",".join([r"\n\s*([A-z\"\-\d\.]+)" for i in range(level)]) + r"\n\s*\]"
        )
        sub_pattern = "[" + ",".join([f"\\{i + 1}" for i in range(level)]) + "]"
        pattern = re.compile(match_pattern)
        contents = pattern.sub(sub_pattern, contents)

    save_text(location, key, contents, "application/json")
