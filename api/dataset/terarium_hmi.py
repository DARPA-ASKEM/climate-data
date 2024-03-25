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

HMIDataset = Dict[str, Any]


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


def enumerate_dataset_skeleton(
    ds: xarray.Dataset, parent_id: str, variable_id: str = ""
) -> HMIDataset:
    """
    generates the generic body of the metadata field from a given dataset.
    this function should remain as broadly applicable as possible with the only difference
    being in data provider specialization functions below.

    important omissions (not a comprehensive list, only example):
      name, description, subsetDetails, metadata.subsetDetails

    note: continues on preview not working with an exception!
    """
    try:
        start = ds.isel(time=0).time.item().year
        end = ds.isel(time=-1).time.item().year
        preview = render(ds, timestamps=f"{start},{end}", variable_index=variable_id)
    except Exception as e:
        preview = f"error creating preview: {e}"
        print(e, flush=True)
    hmi_dataset = {
        "userId": "",
        "fileNames": [],
        "columns": [],
        "metadata": extract_metadata(ds)
        | {
            "parentDatasetId": parent_id,
            "preview": preview,
        },
        "grounding": {},
    }
    return hmi_dataset


def construct_hmi_dataset(
    ds: xarray.Dataset,
    dataset_id: str,
    parent_dataset_id: str,
    subset_uuid: str,
    opts: DatasetSubsetOptions,
    variable_id: str = "",
) -> HMIDataset:
    """
    generic function for turning a given subset dataset into a terarium-postable request body.
    this is for anything that can use DatasetSubsetOptions and the standard search->subset workflow.
    """
    hmi_dataset = enumerate_dataset_skeleton(ds, parent_dataset_id)

    dataset_name = dataset_id.split("|")[0]
    additional_fields = {
        "name": f"{dataset_name}-subset-{subset_uuid}",
        "description": generate_description(ds, dataset_id, opts),
    } | extract_esgf_specific_fields(ds)
    additional_metadata = {
        "parentDatasetId": parent_dataset_id,
        "subsetDetails": repr(opts),
    }

    hmi_dataset |= additional_fields
    hmi_dataset["metadata"] |= additional_metadata

    print(f"dataset: {dataset_name}-subset-{subset_uuid}", flush=True)
    return hmi_dataset


def construct_hmi_dataset_era5(
    ds: xarray.Dataset,
    dataset_id: str,
    parent_dataset_id: str,
    subset_uuid: str,
    data: ERA5SearchData,
) -> HMIDataset:
    """
    construct dataset - ERA5 specific version due to difference in subsetting and dataset information.
    """
    hmi_dataset = enumerate_dataset_skeleton(ds, parent_dataset_id)

    dataset_name = dataset_id
    additional_fields = {
        "name": f"{dataset_name}-subset-{subset_uuid}",
        "description": "",
        "dataSourceDate": "",
        "datasetUrl": "",
        "source": "",
    }
    additional_metadata = {
        "parentDatasetId": parent_dataset_id,
        "subsetDetails": "",
    }

    hmi_dataset |= additional_fields
    hmi_dataset["metadata"] |= additional_metadata

    print(f"dataset: {dataset_name}-subset-{subset_uuid}", flush=True)
    return hmi_dataset


def post_hmi_dataset(hmi_dataset: HMIDataset, filepath: str) -> str:
    terarium_auth = (default_settings.terarium_user, default_settings.terarium_pass)

    r = requests.post(
        f"{default_settings.terarium_url}/datasets",
        json=hmi_dataset,
        auth=terarium_auth,
    )

    if r.status_code != 201:
        raise Exception(
            f"failed to create dataset: POST /datasets: {r.status_code} {r.content}"
        )
    response = r.json()
    hmi_id = response.get("id", "")
    print(f"created dataset {hmi_id}")
    if hmi_id == "":
        raise Exception(f"failed to create dataset: id not found: {response}")

    ds_url = f"{default_settings.terarium_url}/datasets/{hmi_id}/upload-file"
    m = MultipartEncoder(fields={"file": ("filename", open(filepath, "rb"))})
    r = requests.put(
        ds_url,
        data=m,
        params={"filename": filepath},
        headers={"Content-Type": m.content_type},
        auth=terarium_auth,
    )
    if r.status_code != 200:
        raise Exception(f"failed to upload file: {ds_url}: {r.status_code}")

    return hmi_id
