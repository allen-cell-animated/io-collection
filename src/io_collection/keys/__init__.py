"""Tasks for working with object keys."""

from prefect import task

from .change_key import change_key
from .check_key import check_key
from .copy_key import copy_key
from .get_keys import get_keys
from .make_key import make_key
from .remove_key import remove_key

change_key = task(change_key)
check_key = task(check_key)
copy_key = task(copy_key)
get_keys = task(get_keys)
make_key = task(make_key)
remove_key = task(remove_key)
