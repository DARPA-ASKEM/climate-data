from uuid import uuid4
import xarray
from datetime import datetime, timezone
from api.dataset.models import DatasetSubsetOptions
from api.settings import default_settings
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import numpy


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


def construct_hmi_dataset(
    ds: xarray.Dataset,
    dataset_id: str,
    subset_uuid: str,
    opts: DatasetSubsetOptions,
    username: str,
    netcdf_path: str,
) -> str:
    terarium_auth = (default_settings.terarium_user, default_settings.terarium_pass)
    dataset_name = dataset_id.split("|")[0]
    hmi_dataset = {
        "userId": "",
        "name": f"{dataset_name}-subset-{subset_uuid}",
        "description": generate_description(ds, dataset_id, opts),
        "dataSourceDate": ds.attrs.get("creation_date", "UNKNOWN"),
        "fileNames": [],
        "datasetUrl": ds.attrs.get("further_info_url", "UNKNOWN"),
        "columns": [
            {
                "name": k,
                "metadata": {
                    "attrs": {
                        ak: ds[k].attrs[ak].item()
                        if isinstance(ds[k].attrs[ak], numpy.generic)
                        else ds[k].attrs[ak]
                        for ak in ds[k].attrs
                    },
                    "indexes": [i for i in ds[k].indexes.keys()],
                    "coordinates": [i for i in ds[k].coords.keys()],
                },
            }
            for k in ds.variables.keys()
        ],
        "metadata": {
            k: ds.attrs[k].item()
            if isinstance(ds.attrs[k], numpy.generic)
            else ds.attrs[k]
            for k in ds.attrs.keys()
        },
        "source": ds.attrs.get("source", "UNKNOWN"),
        "grounding": {},
    }
    print(f"dataset: {dataset_name}-subset-{subset_uuid}", flush=True)
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
    m = MultipartEncoder(fields={"file": ("filename", open(netcdf_path, "rb"))})
    r = requests.put(
        ds_url,
        data=m,
        params={"filename": netcdf_path},
        headers={"Content-Type": m.content_type},
        auth=terarium_auth,
    )
    if r.status_code != 200:
        raise Exception(f"failed to upload file: {ds_url}: {r.status_code}")

    return hmi_id
