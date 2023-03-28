import importlib
import sys

from prefect import task

from .change_key import change_key
from .check_key import check_key
from .copy_key import copy_key
from .get_keys import get_keys
from .make_key import make_key
from .remove_key import remove_key

TASK_MODULES = [
    change_key,
    check_key,
    copy_key,
    get_keys,
    make_key,
    remove_key,
]

for task_module in TASK_MODULES:
    MODULE_NAME = task_module.__name__
    module = importlib.import_module(f".{MODULE_NAME}", package=__name__)
    setattr(sys.modules[__name__], MODULE_NAME, task(getattr(module, MODULE_NAME)))
