import pickle
from typing import Any

from io_collection.load.load_buffer import load_buffer


def load_pickle(location: str, key: str) -> Any:
    buffer = load_buffer(location, key)
    return pickle.loads(buffer.read())
