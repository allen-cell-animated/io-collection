"""Tasks for loading and saving objects via Quilt."""

from prefect import task

from .load_quilt_package import load_quilt_package
from .save_quilt_item import save_quilt_item

load_quilt_package = task(load_quilt_package)
save_quilt_item = task(save_quilt_item)
