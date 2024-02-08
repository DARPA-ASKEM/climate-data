import io
import base64
from api.search.provider import AccessURLs
import cartopy.crs as ccrs
import xarray
from matplotlib import pyplot as plt
from typing import List
from api.dataset.remote import (
    cleanup_potential_artifacts,
    open_dataset,
    open_remote_dataset_hmi,
)


def buffer_to_b64_png(buffer: io.BytesIO) -> str:
    buffer.seek(0)
    content = buffer.read()
    payload = base64.b64encode(content).decode("utf-8")
    return f"data:image/png;base64,{payload}"


# handles loading as to not share xarray over rq-worker boundaries
def render_preview_for_dataset(
    urls: AccessURLs,
    variable_index: str = "",
    time_index: str = "",
    timestamps: str = "",
    **kwargs,
):
    job_id = kwargs["job_id"]
    try:
        ds = open_dataset(urls, job_id)
        png = render(ds, variable_index, time_index, timestamps)
        cleanup_potential_artifacts(job_id)
        return {"png": png}
    except IOError as e:
        return {"error": f"upstream hosting is likely having a problem. {e}"}


def render_preview_for_hmi(uuid: str, **kwargs):
    job_id = kwargs["job_id"]
    try:
        ds = open_remote_dataset_hmi(uuid, job_id)
        png = render(ds=ds)
        cleanup_potential_artifacts(job_id)
        return {"png": png}
    except IOError as e:
        return {"error": f"failed with error {e}"}


def render(
    ds,
    variable_index: str = "",
    time_index: str = "",
    timestamps: str = "",
    **kwargs,
):
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
    print(axes, flush=True)
    other_axes = [axis for axis in axes if axis not in ["X", "Y", "T"]]
    for axis in other_axes:
        try:
            ds = ds.sel({axes[axis]: ds[axes[axis]][0]})
        except Exception as e:
            print(
                f"failed to trim non-relevant axis {axis}: {ds[axes[axis]]}: {e}: (this can be safely ignored if expected)"
            )

    ds = ds[variable_index]

    fig, ax = plt.subplots(subplot_kw={"projection": ccrs.PlateCarree()})
    ds.plot(transform=ccrs.PlateCarree(), x=axes["X"], y=axes["Y"], add_colorbar=True)
    ax.coastlines()

    buffer = io.BytesIO()
    plt.savefig(buffer, format="png")

    return buffer_to_b64_png(buffer)
