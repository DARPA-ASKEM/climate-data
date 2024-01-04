from typing import List, Dict, Any
from dask.delayed import Delayed
import dask


class Dataset:
    metadata: Dict[str, Any]  # json

    def __init__(self, metadata):
        self.metadata = metadata


DatasetSearchResults = List[Dataset]


class BaseSearchProvider:
    def search(self, query: str, page: int) -> DatasetSearchResults:
        return []

    def get_access_urls(self, dataset: Dataset) -> List[str]:
        return []
