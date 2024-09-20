import datetime
from pathlib import Path


def make_key(*subkeys: str) -> str:
    """
    Combine given subkeys into a single key.

    If any subkeys include **{{timestamp}}**, it will be replaced with the
    current date formatted as **YYYY-MM-DD**. Any instances of double
    underscores (`__`) are replaced with a single underscore (`_`). Any
    instances of underscore followed by a period (`_.`) are replaced with a
    period (`.`).

    Returns
    -------
    :
        The key.
    """

    key = str(Path(subkeys[0], *subkeys[1:]))

    timestamp = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%d")
    key = key.replace("{{timestamp}}", timestamp)

    return key.replace("__", "_").replace("_.", ".")
