import json
import re

from prefect import task

from io_collection.save.save_text import save_text


@task
def save_json(location: str, key: str, obj: dict, levels: int = 5) -> None:
    contents = json.dumps(obj, indent=2)

    for level in range(1, levels + 1):
        match_pattern = r"\[" + ",".join([r"\n\s*([\d\.]+)" for i in range(level)]) + r"\n\s*\]"
        sub_pattern = "[" + ",".join([f"\\{i + 1}" for i in range(level)]) + "]"
        pattern = re.compile(match_pattern)
        contents = pattern.sub(sub_pattern, contents)

    save_text.fn(location, key, contents)
