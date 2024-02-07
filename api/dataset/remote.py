import xarray
from typing import List
import s3fs

# we have to operate on urls, paths / dataset_ids due to the fact that
# rq jobs can't pass the context of a loaded xarray dataset in memory (json serialization)


def open_dataset(paths: List[List[str]]) -> xarray.Dataset:
    for mirror in paths:
        try:
            ds = xarray.open_mfdataset(
                mirror,
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
                mirror,
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
        ds = open_remote_dataset_s3(paths[0])
        return ds
    except IOError as e:
        print(f"file not found in s3 mirroring: {e}")

    for mirror in paths:
        try:
            ds = open_remote_dataset_http(mirror)
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


def open_remote_dataset_http(urls: List[str]) -> xarray.Dataset:
    raise IOError(
        "failed to attempt http downloading: dataset requires authorization when in plain http download"
    )
