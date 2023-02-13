import io
import pickle
from typing import Any

from prefect import task

from io_collection.save.save_buffer import save_buffer


@task
def save_pickle(location: str, key: str, obj: Any) -> None:
    with io.BytesIO() as buffer:
        pickle.dump(obj, buffer)
        save_buffer.fn(location, key, buffer)
