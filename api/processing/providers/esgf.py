from .. import filters
from api.settings import default_settings
import xarray
from typing import Any, Dict, List
from api.dataset.storage import initialize_client


def slice_esgf_dataset(urls: List[str], dataset_id: str, params: Dict[str, Any]):
    ds = xarray.open_mfdataset(urls)
    options = filters.options_from_url_parameters(params)
    print(f"original size: {ds.nbytes}\nslicing with options {options}")
    return filters.subset_with_options(ds, options)


def slice_and_store_dataset(
    urls: List[str], dataset_id: str, params: Dict[str, Any], **kwargs
):
    job_id = kwargs["job_id"]
    filename = f"cmip6-{job_id}.nc"
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
    s3 = initialize_client()
    s3.upload_file(filename, default_settings.bucket_name, filename)
    return {"url": f"s3://{default_settings.bucket_name}/{filename}"}
