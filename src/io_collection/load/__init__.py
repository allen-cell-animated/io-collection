import importlib
import sys

from prefect import task

from .load_buffer import load_buffer
from .load_dataframe import load_dataframe
from .load_image import load_image
from .load_pickle import load_pickle
from .load_tar import load_tar
from .load_text import load_text

TASK_MODULES = [
    load_buffer,
    load_dataframe,
    load_image,
    load_pickle,
    load_tar,
    load_text,
]

for task_module in TASK_MODULES:
    MODULE_NAME = task_module.__name__
    module = importlib.import_module(f".{MODULE_NAME}", package=__name__)
    setattr(sys.modules[__name__], MODULE_NAME, task(getattr(module, MODULE_NAME)))
