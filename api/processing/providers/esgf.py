from .. import filters
from api.settings import default_settings
import xarray
from typing import Any, Dict, List
from api.dataset.storage import initialize_client
from api.dataset.terarium_hmi import construct_hmi_dataset
import s3fs
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.basemap import Basemap

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
            ds = xarray.open_mfdataset(urls)
        except IOError as e:
            print(f"failed to open sequentially: {e}")
            raise IOError(e)
    return ds


def open_remote_dataset_s3(urls: List[str]) -> xarray.Dataset:
    fs = s3fs.S3FileSystem(anon=True)
    urls = ["s3://esgf-world" + url[url.find("/CMIP6") :] for url in urls]
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


def render_preview_for_dataset(
    urls: List[str],
    dataset_id: str,
    variable_index: str,
    time_index: str,
    timestamps: str,
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
    ds = ds[variable_index]
    x_points = ds[axes["X"]][:]
    y_points = ds[axes["Y"]][:]
    units = ds.units
    name = ds.long_name
    center_x = x_points.mean()
    center_y = y_points.mean()

    m = Basemap(
        width=5000000,
        height=3500000,
        resolution="l",
        projection="merc",
        llcrnrlat=-80,
        urcrnrlat=80,
        llcrnrlon=0,
        urcrnrlon=360,
    )
    lon, lat = np.meshgrid(x_points, y_points)
    xi, yi = m(lon, lat)
    cs = m.pcolor(xi, yi, np.squeeze(ds))
    m.drawparallels(np.arange(-80.0, 81.0, 10.0), labels=[1, 0, 0, 0], fontsize=10)
    m.drawmeridians(np.arange(-180.0, 181.0, 10.0), labels=[0, 0, 0, 1], fontsize=10)

    # Add Coastlines, States, and Country Boundaries
    m.drawcoastlines()
    m.drawstates()
    m.drawcountries()

    # Add Colorbar
    cbar = m.colorbar(cs, location="bottom", pad="10%")
    cbar.set_label(units)

    # Add Title
    plt.title(name)
    plt.savefig("x.png")
