from api.dataset.models import DatasetType
from api.search.provider import AccessURLs
from .. import filters
import xarray
from typing import Any, Dict
from api.dataset.terarium_hmi import construct_hmi_dataset, post_hmi_dataset
from api.dataset.remote import cleanup_potential_artifacts, open_dataset
import os


def slice_esgf_dataset(
    urls: AccessURLs, dataset_id: str, params: Dict[str, Any]
) -> xarray.Dataset:
    ds = open_dataset(urls)
    options = filters.options_from_url_parameters(params)
    print(f"original size: {ds.nbytes}\nslicing with options {options}", flush=True)
    return filters.subset_with_options(ds, options)


def slice_and_store_dataset(
    urls: AccessURLs,
    parent_id: str,
    dataset_id: str,
    params: Dict[str, Any],
    **kwargs,
):
    job_id = kwargs["job_id"]
    filename = f"cmip6-{job_id}.nc"
    print(f"running job esgf subset job for: {job_id}", flush=True)
    try:
        ds = slice_esgf_dataset(urls, dataset_id, params)
    except IOError as e:
        return {
            "status": "failed",
            "error": f"upstream is likely having a problem. {e}",
        }
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
        dataset = construct_hmi_dataset(
            ds,
            dataset_id,
            parent_id,
            job_id,
            filters.options_from_url_parameters(params),
        )
        hmi_id = post_hmi_dataset(dataset, filename)
        return {"status": "ok", "dataset_id": hmi_id, "filename": filename}
    except Exception as e:
        return {"status": "failed", "error": str(e), "dataset_id": ""}
    finally:
        cleanup_potential_artifacts(job_id)
        os.remove(filename)
