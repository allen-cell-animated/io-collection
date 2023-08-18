"""Tasks for loading different object types from local file systems or S3 buckets."""

from prefect import task

from .load_buffer import load_buffer
from .load_dataframe import load_dataframe
from .load_image import load_image
from .load_json import load_json
from .load_pickle import load_pickle
from .load_tar import load_tar
from .load_text import load_text

load_buffer = task(load_buffer)
load_dataframe = task(load_dataframe)
load_image = task(load_image)
load_json = task(load_json)
load_pickle = task(load_pickle)
load_tar = task(load_tar)
load_text = task(load_text)
