import importlib
import sys

from prefect import task

from .load_quilt_package import load_quilt_package
from .save_quilt_item import save_quilt_item

TASK_MODULES = [
    load_quilt_package,
    save_quilt_item,
]

for task_module in TASK_MODULES:
    MODULE_NAME = task_module.__name__
    module = importlib.import_module(f".{MODULE_NAME}", package=__name__)
    setattr(sys.modules[__name__], MODULE_NAME, task(getattr(module, MODULE_NAME)))
