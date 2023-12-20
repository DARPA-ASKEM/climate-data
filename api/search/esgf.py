from api.settings import default_settings
from api.search.provider import BaseSearchProvider, DatasetSearchResults, Dataset
import requests
from urllib.parse import urlencode
from typing import List, Dict, Any
import itertools
import dask

NATURAL_LANGUAGE_PROCESSING_CONTEXT = """
Split the given input text into key search terms and separate them with `AND`. 
Surround measures of distance in escaped double quotes. 

Respond according to the given examples provided in a mapping.

"6hr 100 km air_temperature" => "6hr AND \"100 km\" AND air_temperature"
"6 hours 100 km air_temperature" => "6hr AND \"100 km\" AND air_temperature"
"6 hour 100 km air_temperature" => "6hr AND \"100 km\" AND air_temperature"
"6hr 100km air_temperature" => "6hr AND \"100 km\" AND air_temperature"
"day experiment_name 200 km historical" => "day AND experiment_name AND \"200 km\" AND historical"
"daily humidity 200km historical" => "day AND humidity AND \"200 km\" AND historical"
"1 day 100km experiment_name" => "day AND \"100 km\" AND experiment_name"
"20 km 3hr piControl nRoot" => "\"20 km\" AND 3hr AND piControl AND nRoot" 
"20km 3hr piControl nRoot" => "\"20 km\" AND 3hr AND piControl AND nRoot" 
"20 km 3 hours piControl nRoot" => "\"20 km\" AND 3hr AND piControl AND nRoot" 
"20km three hours piControl nRoot" => "\"20 km\" AND 3hr AND piControl AND nRoot" 

Do not provide any other words except the converted search terms.
"""


class ESGFProvider(BaseSearchProvider):
    def __init__(self, openai_client):
        self.client = openai_client

    def search(self, query: str, page: int) -> DatasetSearchResults:
        return self.natural_language_search(query, page)

    def natural_language_search(
        self, search_query: str, page: int
    ) -> DatasetSearchResults:
        query_string = self.process_natural_language(search_query)
        print(query_string)
        print(urlencode({"query": query_string}))
        return self.run_esgf_query(query_string, page)

    def build_natural_language_prompt(self, search_query: str) -> str:
        return "Convert the following input text: {}".format(search_query)

    def process_natural_language(self, search_query: str) -> str:
        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": NATURAL_LANGUAGE_PROCESSING_CONTEXT},
                {
                    "role": "user",
                    "content": self.build_natural_language_prompt(search_query),
                },
            ],
            temperature=0.7,
        )
        query = response.choices[0].message.content
        if query[0] == '"' and query[-1] == '"':
            query = query[1:-1]
        return query  # .replace('"', '\\"')

    def _extract_files_from_dataset(self, dataset: Dict[str, Any]) -> List[str]:
        dataset_id = dataset["id"]
        params = urlencode(
            {
                "type": "File",
                "format": "application/solr+json",
                "dataset_id": dataset_id,
            }
        )
        full_url = f"{default_settings.esgf_url}/search?{params}"
        r = requests.get(full_url)
        response = r.json()
        if r.status_code != 200:
            raise ConnectionError(
                f"Failed to extract files from dataset via file search: {full_url} {response}"
            )
        files = response["response"]["docs"]
        if len(files) == 0:
            raise ConnectionError(
                f"Failed to extract files from dataset: empty list {full_url}"
            )

        # file url responses are lists of strings with their protocols separated by |
        # e.x. https://esgf-node.example|mimetype|OPENDAP
        opendap_urls = [
            url.split("|")[0]
            for url in itertools.chain.from_iterable([f["url"] for f in files])
            if "OPENDAP" in url
        ]
        return opendap_urls

    def run_esgf_query(self, query_string: str, page: int) -> DatasetSearchResults:
        encoded_string = urlencode(
            {
                "query": query_string,
                "project": "CMIP6",
                "fields": "*",
                "latest": "true",
                "sort": "true",
                "limit": f"{default_settings.entries_per_page}",
                "offset": "{}".format(default_settings.entries_per_page * page),
                "format": "application/solr+json",
            }
        )

        full_url = f"{default_settings.esgf_url}/search?{encoded_string}"
        r = requests.get(full_url)
        response = r.json()
        if r.status_code != 200:
            raise ConnectionError(
                f"Failed to search against ESGF node: {full_url} {r.status_code} {response}"
            )
        # parallel over datasets, but delay fetching url until needed
        return dask.compute(
            [
                dask.delayed(Dataset)(
                    dataset, dask.delayed(self._extract_files_from_dataset)(dataset)
                )
                for dataset in response["response"]["docs"]
            ]
        )[0]
