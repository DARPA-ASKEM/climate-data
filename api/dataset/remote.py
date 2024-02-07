from concurrent.futures import ThreadPoolExecutor
import glob
import xarray
from typing import List
from api.search.provider import AccessURLs
from api.settings import default_settings
import os
import s3fs
import requests

# we have to operate on urls, paths / dataset_ids due to the fact that
# rq jobs can't pass the context of a loaded xarray dataset in memory (json serialization)

# list of ordered download priorities:
#     all mirrors are checked in each method
# ------------------------------------
# opendap [parallel]
# opendap [sequential]
# s3 mirror - s3://esgf-world netcdf4 bucket
# plain http
# s3 mirror - zarr format


def open_dataset(paths: AccessURLs, job_id=None) -> xarray.Dataset:
    if len(paths) == 0:
        raise IOError(
            "paths was provided an empty list - does the dataset exist? no URLs found."
        )

    for mirror in paths:
        opendap_urls = mirror["opendap"]
        if len(opendap_urls) == 0:
            continue
        try:
            ds = xarray.open_mfdataset(
                opendap_urls,
                chunks={"time": 10},
                concat_dim="time",
                combine="nested",
                parallel=True,
                use_cftime=True,
            )
            return ds
        except IOError as e:
            print(f"failed to open parallel: {e}")
        try:
            ds = xarray.open_mfdataset(
                opendap_urls,
                concat_dim="time",
                combine="nested",
                use_cftime=True,
            )
            return ds
        except IOError as e:
            print(f"failed to open sequentially {e}")

    print("failed to find dataset in all mirrors.")
    try:
        # function handles stripping out url part, so any mirror will have the same result
        ds = open_remote_dataset_s3(paths[0]["opendap"])
        return ds
    except IOError as e:
        print(f"file not found in s3 mirroring: {e}")

    for mirror in paths:
        http_urls = mirror["http"]
        if len(http_urls) == 0:
            continue
        try:
            if job_id is None:
                raise IOError(
                    "http downloads must have an associated job id for cleanup purposes"
                )
            ds = open_remote_dataset_http(http_urls, job_id)
            return ds
        except IOError as e:
            print(f"failed to download via plain http: {e}")

    raise IOError(
        f"Failed to download dataset via parallel dap, sequential dap, s3 mirror, and http: {paths}"
    )


def open_remote_dataset_s3(urls: List[str]) -> xarray.Dataset:
    fs = s3fs.S3FileSystem(anon=True)
    urls = ["s3://esgf-world" + url[url.find("/CMIP6") :] for url in urls]
    print(urls, flush=True)
    files = [
        xarray.open_dataset(
            fs.open(url),
            chunks={"time": 10},
            use_cftime=True,
        )
        for url in urls
    ]
    return xarray.merge(files)


def download_file_http(url: str, dir: str):
    rs = requests.get(url, stream=True)
    if rs.status_code == 401:
        rs = requests.get(url, stream=True, auth=default_settings.esgf_openid)
    filename = url.split("/")[-1]
    print("writing ", os.path.join(dir, filename))
    with open(os.path.join(dir, filename), mode="wb") as file:
        for chunk in rs.iter_content(chunk_size=10 * 1024):
            file.write(chunk)


def open_remote_dataset_http(urls: List[str], job_id) -> xarray.Dataset:
    temp_directory = os.path.join(".", str(job_id))
    if not os.path.exists(temp_directory):
        os.makedirs(temp_directory)
    with ThreadPoolExecutor() as executor:
        executor.map(lambda url: download_file_http(url, temp_directory), urls)
    files = [os.path.join(temp_directory, f) for f in os.listdir(temp_directory)]
    ds = xarray.open_mfdataset(
        files,
        parallel=True,
        concat_dim="time",
        combine="nested",
        use_cftime=True,
        chunks={"time": 10},
    )
    return ds


def cleanup_potential_artifacts(job_id):
    temp_directory = os.path.join(".", str(job_id))
    if os.path.exists(temp_directory):
        print(f"cleaning http artifact: {temp_directory}")
        for file in glob.glob(os.path.join(temp_directory, "*.nc")):
            os.remove(file)
        os.removedirs(temp_directory)
