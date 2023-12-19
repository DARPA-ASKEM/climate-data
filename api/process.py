import xarray as xr
from api.search.provider import DatasetSearchResults
from typing import List


# stubbed example of processing lazy loaded datasets -- not useful for metadata,
# but useful in the final result -> TDS
def get_dataset_sizes(results: DatasetSearchResults) -> List[str]:
    return []
    return [xr.open_mfdataset(ds).nbytes for ds in results]
