import xarray
from typing import List
import pandas as pd


def location_bbox(
    dataset: xarray.Dataset, bounding_box: List[float], fields=["lat", "lon"]
):
    return (
        dataset.where(dataset[fields[0]] > bounding_box[0])
        .where(dataset[fields[0]] < bounding_box[1])
        .where(dataset[fields[1]] > bounding_box[2])
        .where(dataset[fields[1]] < bounding_box[4])
    )


def timestamps(dataset: xarray.Dataset, timestamps: List[str], field="time"):
    pass


def divide_by(dataset):
    pass
