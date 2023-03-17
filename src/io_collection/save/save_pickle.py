import io
import pickle
from typing import Any

from io_collection.save.save_buffer import save_buffer


def save_pickle(location: str, key: str, obj: Any) -> None:
    with io.BytesIO() as buffer:
        pickle.dump(obj, buffer)
        save_buffer(location, key, buffer)
