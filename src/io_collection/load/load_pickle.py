import pickle
from typing import Any

from prefect import task

from io_collection.load.load_buffer import load_buffer


@task
def load_pickle(location: str, key: str) -> Any:
    buffer = load_buffer.fn(location, key)
    return pickle.loads(buffer.read())
