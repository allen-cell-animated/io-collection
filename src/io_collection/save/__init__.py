"""Tasks for saving different object types to local file systems or S3 buckets."""

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

save_buffer = task(save_buffer)
save_dataframe = task(save_dataframe)
save_figure = task(save_figure)
save_gif = task(save_gif)
save_image = task(save_image)
save_json = task(save_json)
save_pickle = task(save_pickle)
save_tar = task(save_tar)
save_text = task(save_text)
