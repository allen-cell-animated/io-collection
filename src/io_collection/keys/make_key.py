import os
from datetime import datetime


def make_key(*subkeys: str) -> str:
    """
    Combines given subkeys into a single key.

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

    key = os.path.join(*subkeys)

    timestamp = datetime.now().strftime("%Y-%m-%d")
    key = key.replace("{{timestamp}}", timestamp)

    key = key.replace("__", "_")
    key = key.replace("_.", ".")

    return key
