from typing import List, Dict, Any
from dask.delayed import Delayed
import dask

# consistent interface for handling search results and paths
# across multiple sources.


class Dataset:
    metadata: Dict[str, Any]  # json

    def __init__(self, metadata):
        self.metadata = metadata


DatasetSearchResults = List[Dataset]


class BaseSearchProvider:
    def search(self, query: str, page: int) -> DatasetSearchResults:
        return []

    # [mirrors... [dataset urls...]]
    def get_access_paths(self, dataset: Dataset) -> List[List[str]]:
        return []
