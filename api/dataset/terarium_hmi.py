import xarray
from api.dataset.models import DatasetSubsetOptions
from api.dataset.metadata import extract_metadata, extract_esgf_specific_fields
from api.search.providers.era5 import ERA5SearchData
from api.settings import default_settings
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import numpy
from api.preview.render import render
from typing import Dict, Any

def generate_description(
    ds: xarray.Dataset, dataset_id: str, opts: DatasetSubsetOptions
):
    string = f"""Dataset Subset: {dataset_id}
  Created with options:\n"""

    if opts.temporal is not None:
        string += f"""    Temporal Range:
      Start: {opts.temporal.timestamp_range[0]}
      End: {opts.temporal.timestamp_range[1]}\n"""

    if opts.geospatial is not None:
        string += f"""    Geographic Envelope:
      Bounds: {opts.geospatial.envelope}\n"""

    if opts.thinning is not None:
        string += f"""    Thinning:
      Factor: {opts.thinning.factor}
      Fields: {opts.thinning.fields} (blank is all fields)"""

    return string

