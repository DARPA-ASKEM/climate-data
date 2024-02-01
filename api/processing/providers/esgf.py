import base64
from .. import filters
from api.settings import default_settings
import xarray
from typing import Any, Dict, List
from api.dataset.storage import initialize_client
from api.dataset.terarium_hmi import construct_hmi_dataset
import s3fs
import matplotlib.pyplot as plt
import numpy as np
import io
import cartopy.crs as ccrs

# we have to operate on urls / dataset_ids due to the fact that
# rq jobs can't pass the context of a loaded xarray dataset in memory (json serialization)


def open_remote_dataset(urls: List[str]) -> xarray.Dataset:
    try:
        ds = xarray.open_mfdataset(
            urls,
            chunks={"time": 10},
            concat_dim="time",
            combine="nested",
            parallel=True,
        )
    except IOError as e:
        print(f"failed to open parallel: {e}")
        try:
            ds = xarray.open_mfdataset(urls, concat_dim="time", combine="nested")
        except IOError as e:
            print(f"failed to open sequentially, falling back to s3: {e}")
            return open_remote_dataset_s3(urls)
    return ds


def open_remote_dataset_s3(urls: List[str]) -> xarray.Dataset:
    fs = s3fs.S3FileSystem(anon=True)
    urls = ["s3://esgf-world" + url[url.find("/CMIP6") :] for url in urls]
    print(urls, flush=True)
    files = [xarray.open_dataset(fs.open(url), chunks={"time": 10}) for url in urls]
    return xarray.merge(files)


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


def buffer_to_b64_png(buffer: io.BytesIO) -> str:
    buffer.seek(0)
    content = buffer.read()
    payload = base64.b64encode(content).decode("utf-8")
    return f"data:image/png;base64,{payload}"


def render_preview_for_dataset(
    urls: List[str],
    variable_index: str = "",
    time_index: str = "",
    timestamps: str = "",
    **kwargs,
):
    ds = open_remote_dataset(urls)
    axes = {}
    for v in ds.variables.keys():
        if "axis" in ds[v].attrs:
            axes[ds[v].attrs["axis"]] = v
    if variable_index == "":
        variable_index = ds.attrs.get("variable_id", "")
    if time_index == "":
        if "time" in ds.variables:
            time_index = "time"
        else:
            if "T" in axes:
                time_index = axes["T"]
            else:
                raise IOError("Dataset has no time axis, please provide time index")
    if timestamps == "":
        ds = ds.sel({time_index: ds[time_index][0]})
    else:
        ds = ds.sel({time_index: slice(timestamps.split(","))})

    # we're plotting x, y, time - others need to be shortened to the first element
    other_axes = [axis for axis in axes if axis not in ["X", "Y", "T"]]
    for axis in other_axes:
        ds = ds.sel({axes[axis]: ds[axes[axis]][0]})

    ds = ds[variable_index]

    fig, ax = plt.subplots(subplot_kw={"projection": ccrs.PlateCarree()})
    ds.plot(transform=ccrs.PlateCarree(), x=axes["X"], y=axes["Y"], add_colorbar=True)
    ax.coastlines()

    buffer = io.BytesIO()
    plt.savefig(buffer, format="png")

    return {"png": buffer_to_b64_png(buffer)}
