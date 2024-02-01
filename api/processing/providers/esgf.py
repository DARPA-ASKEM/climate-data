import base64
from .. import filters
from api.settings import default_settings
import xarray
from typing import Any, Dict, List
from api.dataset.terarium_hmi import construct_hmi_dataset
from api.dataset.remote import open_remote_dataset


def slice_esgf_dataset(
    urls: List[str], dataset_id: str, params: Dict[str, Any]
) -> xarray.Dataset:
    ds = open_remote_dataset(urls)
    options = filters.options_from_url_parameters(params)
    print(f"original size: {ds.nbytes}\nslicing with options {options}", flush=True)
    return filters.subset_with_options(ds, options)


def slice_and_store_dataset(
    urls: List[str], dataset_id: str, params: Dict[str, Any], **kwargs
):
    job_id = kwargs["job_id"]
    filename = f"cmip6-{job_id}.nc"
    parent_dataset_id = params.get("parent_dataset_id", "")
    print(f"running job esgf subset job for: {job_id}", flush=True)
    ds = slice_esgf_dataset(urls, dataset_id, params)
    print(f"bytes: {ds.nbytes}", flush=True)
    try:
        print("pulling sliced dataset from remote", flush=True)
        ds.load()
        print("done", flush=True)
        ds.to_netcdf(filename)
    except Exception:
        return "Upstream OPENDAP server rejected the request for being too large."
    # minio s3 -> do terarium for now instead
    # s3 = initialize_client()
    # s3.upload_file(filename, default_settings.bucket_name, filename)
    # return {"url": f"s3://{default_settings.bucket_name}/{filename}"}
    try:
        hmi_id = construct_hmi_dataset(
            ds,
            dataset_id,
            parent_dataset_id,
            job_id,
            filters.options_from_url_parameters(params),
            "dataset-netcdf-testuser",
            filename,
        )
        return {"status": "ok", "dataset_id": hmi_id}
    except Exception as e:
        return {"status": "failed", "error": str(e), "dataset_id": ""}
