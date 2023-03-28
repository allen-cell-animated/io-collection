import importlib
import sys

from prefect import task

from .save_buffer import save_buffer
from .save_dataframe import save_dataframe
from .save_figure import save_figure
from .save_gif import save_gif
from .save_image import save_image
from .save_json import save_json
from .save_pickle import save_pickle
from .save_tar import save_tar
from .save_text import save_text

TASK_MODULES = [
    save_buffer,
    save_dataframe,
    save_figure,
    save_gif,
    save_image,
    save_json,
    save_pickle,
    save_tar,
    save_text,
]

for task_module in TASK_MODULES:
    MODULE_NAME = task_module.__name__
    module = importlib.import_module(f".{MODULE_NAME}", package=__name__)
    setattr(sys.modules[__name__], MODULE_NAME, task(getattr(module, MODULE_NAME)))
