from typing import List, Dict, Any
from dask.delayed import Delayed
import dask


class Dataset:
    metadata: Dict[str, Any]  # json
    opendap_urls: Delayed  # List[str]

    def __init__(self, metadata, urls):
        self.metadata = metadata
        self.urls = urls


DatasetSearchResults = List[Dataset]


class BaseSearchProvider:
    def search(self, query: str, page: int) -> DatasetSearchResults:
        return []
