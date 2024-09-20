import io

from PIL import Image

from io_collection.load.load_buffer import load_buffer
from io_collection.save.save_buffer import save_buffer


def save_gif(location: str, key: str, frame_keys: list[str]) -> None:
    """
    Save series of images as gif to key at specified location.

    Method will save to the S3 bucket if the location begins with the **s3://**
    protocol, otherwise it assumes the location is a local path.

    Parameters
    ----------
    location
        Object location (local path or S3 bucket).
    key
        Object key ending in `.gif`.
    frame_keys
        List of frame keys to include in the gif.
    """

    if not key.endswith(".gif"):
        message = f"key [ {key} ] must have [ gif ] extension"
        raise ValueError(message)

    with io.BytesIO() as buffer:
        frames = [Image.open(load_buffer(location, frame_key)) for frame_key in frame_keys]
        frames[0].save(
            buffer, format="gif", save_all=True, append_images=frames[1:], duration=100, loop=0
        )
        save_buffer(location, key, buffer, "image/gif")
