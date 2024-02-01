import xarray
from typing import List
import s3fs

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
            use_cftime=True,
        )
    except IOError as e:
        print(f"failed to open parallel: {e}")
        try:
            ds = xarray.open_mfdataset(
                urls,
                concat_dim="time",
                combine="nested",
                use_cftime=True,
            )
        except IOError as e:
            print(f"failed to open sequentially, falling back to s3: {e}")
            return open_remote_dataset_s3(urls)
    return ds


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
