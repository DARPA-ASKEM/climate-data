import xarray as xr
from api.search.provider import DatasetSearchResults
from typing import List


def get_dataset_sizes(results: DatasetSearchResults) -> List[str]:
    return [xr.open_mfdataset(ds).nbytes for ds in results]
