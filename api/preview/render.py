import datetime
import io
import base64
from api.search.provider import AccessURLs
import cartopy.crs as ccrs
import xarray
from matplotlib import pyplot as plt
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
    dataset: AccessURLs | str,
    variable_index: str = "",
    time_index: str = "",
    timestamps: str = "",
    **kwargs,
):
    job_id = kwargs["job_id"]
    try:
        ds: xarray.Dataset | None = None
        # AccessURLs list or UUID str -- UUID str is terarium handle.
        if isinstance(dataset, list):
            ds = open_dataset(dataset, job_id)
        elif isinstance(dataset, str):
            ds = open_remote_dataset_hmi(dataset, job_id)
        if timestamps != "":
            if len(timestamps.split(",")) != 2:
                return {
                    "error": f"invalid timestamps '{timestamps}'. ensure it is two timestamps, comma separated"
                }
        png = render(ds, variable_index, time_index, timestamps)
        cleanup_potential_artifacts(job_id)
        return {"previews": png}
    except IOError as e:
        return {"error": f"upstream hosting is likely having a problem. {e}"}


def render(
    ds,
    variable_index: str = "",
    time_index: str = "",
    timestamps: str = "",
    **kwargs,
) -> list[dict[str, str]]:
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
        ts = [t.strip() for t in timestamps.split(",")]
        ds = ds.sel({time_index: slice(*ts)})

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

    preview_buffers: list[tuple[str, io.BytesIO]] = []

    def make_plot(data: xarray.Dataset) -> io.BytesIO:
        fig, ax = plt.subplots(subplot_kw={"projection": ccrs.PlateCarree()})
        data.plot(
            ax=ax,
            transform=ccrs.PlateCarree(),
            x=axes["X"],
            y=axes["Y"],
            add_colorbar=True,
        )
        ax.coastlines()
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png")
        return buffer

    if axes["T"] in ds.dims:
        # get delta of first two elements to see if it's yearly / monthly / daily
        delta = ds[axes["T"]][1].item() - ds[axes["T"]][0].item()
        steps = 0
        if delta > datetime.timedelta(days=32):
            steps = 1
        elif delta > datetime.timedelta(days=1):
            steps = 12
        else:
            steps = 365

        leap_offset = 0
        last_year = 0
        # skip by frequency such that index points to head of year
        for time_i in range(0, len(ds[axes["T"]]), steps):
            # handle leap years
            index = time_i + leap_offset
            if index >= len(ds[axes["T"]]):
                break
            year_check = ds.isel({axes["T"]: index})[axes["T"]].item().year
            if year_check == last_year:
                leap_offset += 1
                index += 1

            data = ds.isel({axes["T"]: index})
            date = data[axes["T"]].item()
            print(f"rendering: {date}")
            last_year = date.year
            preview_buffers.append((date.year, make_plot(data)))
    else:
        # single element rather than list
        year = ds[axes["T"]].item().year
        print(f"rendering: {year}")
        preview_buffers.append((year, make_plot(ds)))
    renders = [{"year": y, "image": buffer_to_b64_png(b)} for (y, b) in preview_buffers]
    print(f"created {len(renders)} previews", flush=True)
    return renders
