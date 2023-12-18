from typing import List

DatasetSearchResults = List[List[str]]


class BaseSearchProvider:
    def search(query: str, page: int) -> DatasetSearchResults:
        pass
