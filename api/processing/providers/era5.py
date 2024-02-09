from api.search.providers.era5 import ERA5SearchData
import xarray
from typing import Dict, List
from api.dataset.terarium_hmi import construct_hmi_dataset_era5
from api.dataset.remote import cleanup_potential_artifacts
import os
import cdsapi

# take just about anything we can parse and format into an ERA5 request data payload
# such as ranges or single values
Coercible = List[str] | str | int


# [lon0, lon1, lat0, lat1]
def generate_geographical_envelope(e: List[float]) -> str:
    north = max(e[2], e[3])
    south = min(e[2], e[3])
    east = max(e[0], e[1])
    west = min(e[0], e[1])
    return f"{north}/{west}/{south}/{east}"


def generate_era5_range(
    days: Coercible,
    months: Coercible,
    years: Coercible,
    hours: Coercible,
) -> Dict[str, List[str]]:
    """
    Creates an ERA5 formatted date range according to the following conventions:
    single values are passed directly
    lists of values are passed directly
    ranges are in the form of "a...b" where b is inclusive.

    examples:
        "1"             -> ["01"]
        ["3", "4", "5"] -> ["03", "04", "05"]
        "00:00...23:00" -> all hours between 0 and 23, 23 inclusive
    """

    def gen_string_range_inclusive(a: int | str, b: int | str) -> List[int]:
        def convert(x: int | str) -> int:
            if isinstance(x, int):
                return x
            # remove empty minutes tag when year/day can be different lengths
            return int(x.split(":")[0])

        a = convert(a)
        b = convert(b)
        return [i for i in range(a, b + 1)]

    def handle(v: Coercible) -> List[int]:
        if isinstance(v, str):
            split = v.split("...")
            if len(split) == 1:
                return [int(v.split(":")[0])]
            else:
                return gen_string_range_inclusive(split[0], split[1])
        if isinstance(v, int):
            return [v]
        if isinstance(v, list):
            return [int(i) for i in v]

    formatted = {
        "day": [f"{d:02}" for d in handle(days)],
        "month": [f"{m:02}" for m in handle(months)],
        "year": [f"{y:04}" for y in handle(years)],
        "time": [f"{t:02}:00" for t in handle(hours)],
    }
    return formatted


# era5 uses entirely different subsetting options than generic xarray ones
# as it is provided during the api call rather than outside of it


def download_era5_subset(
    filename: str,
    data: ERA5SearchData,
    days: str | List[str],
    months: str | List[str],
    years: str | List[str],
    hours: str | List[str],
) -> xarray.Dataset:
    generate_era5_range(days, months, years, hours)
    request = {
        "product_type": data.product_type,
        "variable": data.variable,
        "year": years,
        "month": months,
        "day": days,
        "time": hours,
        "format": "netcdf",
    }
    print(f"making CDS request: {request}")
    c = cdsapi.Client()
    c.retrieve(
        data.dataset_name,
        request,
        filename,
    )
    return xarray.open_dataset(filename)


def era5_subset_job(
    data: ERA5SearchData,
    parent_id: str,
    days: List[str] | str,
    months: List[str] | str,
    years: List[str] | str,
    hours: List[str] | str,
    **kwargs,
):
    job_id = kwargs["job_id"]
    filename = f"era5-{job_id}.nc"
    print(f"running ERA5 subset job for: {job_id}", flush=True)
    try:
        ds = download_era5_subset(filename, data, days, months, years, hours)
    except IOError as e:
        return {
            "status": "failed",
            "error": f"failed to download era5 dataset. {e}",
        }
    print(f"bytes: {ds.nbytes}", flush=True)
    try:
        hmi_id = construct_hmi_dataset_era5(
            ds,
            "",
            parent_id,
            job_id,
            data,
        )
        return {"status": "ok", "dataset_id": hmi_id}
    except Exception as e:
        return {"status": "failed", "error": str(e), "dataset_id": ""}
    finally:
        cleanup_potential_artifacts(job_id)
        os.remove(filename)
