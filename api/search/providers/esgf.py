import re

from api.settings import default_settings
from api.search.provider import (
    AccessURLs,
    BaseSearchProvider,
    DatasetSearchResults,
    Dataset,
)
import requests
from urllib.parse import urlencode
from typing import Any, List, Dict
import itertools
import dask
from openai import OpenAI
import json


def generate_natural_language_system_prompt(facets: dict[str, list[str]]) -> str:
    return f"""
You are an assistant trying to help a user determine which variables, sources, experiments, resolutions,
variants, institutions, and frequencies from ESGF's CMIP6 are being referenced in their natural language query.

Here is a list of variable_descriptions: {facets['variable_long_name']}
Here is a list of variables: {facets['variable_id']}
Here is a list of source_ids: {facets['source_id']}
Here is a list of experiment_ids: {facets['experiment_id']}
Here is a list of nominal_resolutions: {facets['nominal_resolution']}
Here is a list of institution_ids: {facets['institution_id']}
Here is a list of variant_labels: {facets['variant_label']}
Here is a list of frequencies: {facets['frequency']}

You should respond by building a dictionary that has the following keys: 
  [variable_descriptions, variable, source_id, experiment_id, nominal_resolution, institution_id, variant_label, frequency]

Please select up to three variable_descriptions from the variable_descriptions list that most closely matches the user's query and assign those variable_descriptions to the variable_descriptions key.
If none clearly and obviously match, assign an empty string ''.

Please select up to three variables from the variables list that most closely matches the user's query and assign those variables to the variable key.
If none clearly and obviously match, assign an empty string ''.

Please select one and ONLY ONE source_id from the source_ids list that most closely matches the user's query and assign ONLY that source_id to the source_id key.
If none clearly and obviously match, assign an empty string ''."

Please select one and ONLY ONE experiment_id from the experiment_ids list that most closely matches the user's query and assign ONLY that experiment_id to the experiment_id key." \
If none clearly and obviously match, assign an empty string ''.

Please select one and ONLY ONE nominal_resolution from the nominal_resolutions list that most closely matches the user's query and assign ONLY that nominal_resolution to the nominal_resolution key." \
If none clearly and obviously match, assign an empty string ''.

Please select one and ONLY ONE institution_id from the institution_ids list that most closely matches the user's query and assign ONLY that institution_id to the institution_id key." \
If none clearly and obviously match, assign an empty string ''.

Please select one and ONLY ONE variant_label from the variant_labels list that most closely matches the user's query and assign ONLY that variant_label to the variant_label key." \
If none clearly and obviously match, assign an empty string ''.

Please select one and ONLY ONE frequency from the frequencies list that most closely matches the user's query and assign ONLY that frequency to the frequency key." \
If none clearly and obviously match, assign an empty string ''.

Ensure that your response is properly formatted JSON please.

Also, when you are selecting variable, source_id, experiment_id, nominal_resolution, institution_id, variant_label, and frequency make sure to select" \
the most simple and obvious choice--no fancy footwork here please.
"""


SEARCH_FACETS = [
    "experiment_title",
    "cf_standard_name",
    "variable_long_name",
    "variable_id",
    "table_id",
    "source_type",
    "source_id",
    "activity_id",
    "nominal_resolution",
    "frequency",
    "realm",
    "institution_id",
    "variant_label",
    "experiment_id",
    "grid_label",
    "nominal_resolution",
    "frequency",
]


class ESGFProvider(BaseSearchProvider):
    def __init__(self, openai_client):
        print("initializing esgf search provider")
        self.client: OpenAI = openai_client
        self.search_mirrors = [
            default_settings.esgf_url,
            *default_settings.esgf_fallbacks.split(","),
        ]
        self.current_mirror_index = 0
        self.retries = 0
        self.max_retries = len(self.search_mirrors)

        self.with_all_available_mirrors(self.get_facet_possiblities)

    def increment_mirror(self):
        self.current_mirror_index += 1
        self.current_mirror_index = self.current_mirror_index % len(self.search_mirrors)

    def with_all_available_mirrors(self, func, *args, **kwargs) -> Any:
        self.retries = 0
        return_value = None
        while self.retries < self.max_retries:
            try:
                return_value = func(*args, **kwargs)
                break
            except Exception as e:
                print(
                    f"failed to run: retry {self.retries}, mirror: {self.search_mirrors[self.current_mirror_index]}",
                    flush=True,
                )
                self.increment_mirror()
                self.retries += 1
                if self.retries >= self.max_retries:
                    raise Exception(f"failed after {self.retries} retries: {e}")
        return return_value

    def get_esgf_url_with_current_mirror(self) -> str:
        mirror = self.search_mirrors[self.current_mirror_index]
        return f"{mirror}/search"

    def get_facet_possiblities(self):
        query = {
            "project": "CMIP6",
            "facets": ",".join(SEARCH_FACETS),
            "limit": "0",
            "format": "application/solr+json",
        }
        base_url = self.get_esgf_url_with_current_mirror()
        response = requests.get(base_url, params=query)
        if response.status_code >= 300:
            msg = f"failed to fetch available facets: {response.status_code}, {response.content}"
            raise Exception(msg)
        facets = response.json()
        self.facet_possibilities = facets["facet_counts"]["facet_fields"]
        for facet, terms in self.facet_possibilities.items():
            self.facet_possibilities[facet] = terms[0::2]

    def is_terarium_hmi_dataset(self, dataset_id: str) -> bool:
        """
        checks if a dataset id is HMI or ESGF - uuid regex
        """
        p = re.compile(r"^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$")
        return bool(p.match(dataset_id.lower()))

    def search(self, query: str, page: int, keywords: bool) -> dict[str, Any]:
        """
        converts a natural language query to a list of ESGF dataset
        metadata dictionaries by running a lucene query against the given
        ESGF node in settings.

        keywords: pass keywords directly to ESGF with no LLM in the middle
        """
        if keywords:
            print(f"keyword searching for {query}", flush=True)
            return self.keyword_search(query, page)
        return self.natural_language_search(query, page)

    def get_all_access_paths_by_id(self, dataset_id: str) -> AccessURLs:
        return [
            self.with_all_available_mirrors(self.get_access_paths_by_id, id)
            for id in self.with_all_available_mirrors(
                self.get_mirrors_for_dataset, dataset_id
            )
        ]

    def get_mirrors_for_dataset(self, dataset_id: str) -> List[str]:
        # strip vert bar if provided with example mirror attached
        dataset_id = dataset_id.split("|")[0]
        response = self.run_esgf_dataset_query(f"id:{dataset_id}*", 1, {})
        full_ids = [d.metadata["id"] for d in response]
        return full_ids

    def get_datasets_from_id(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        returns a list of datasets for a given ID. includes mirrors.
        """
        if dataset_id == "":
            return []
        params = urlencode(
            {
                "type": "File",
                "format": "application/solr+json",
                "dataset_id": dataset_id,
                "limit": 200,
            }
        )
        base_url = self.get_esgf_url_with_current_mirror()
        full_url = f"{base_url}?{params}"
        r = requests.get(full_url)
        response = r.json()
        if r.status_code != 200:
            raise ConnectionError(
                f"Failed to extract files from dataset via file search: {full_url} {response}"
            )
        datasets = response["response"]["docs"]
        if len(datasets) == 0:
            raise ConnectionError(
                f"Failed to extract files from dataset: empty list {full_url}"
            )
        return datasets

    def get_access_paths_by_id(self, dataset_id: str) -> Dict[str, List[str]]:
        """
        returns a list of OPENDAP URLs for use in processing given a dataset.
        """
        files = self.get_datasets_from_id(dataset_id)

        # file url responses are lists of strings with their protocols separated by |
        # e.x. https://esgf-node.example|mimetype|OPENDAP
        def select(files, selector):
            return [
                url.split("|")[0]
                for url in itertools.chain.from_iterable([f["url"] for f in files])
                if selector in url
            ]

        http_urls = select(files, "HTTP")
        # sometimes the opendap request form is returned. we strip the trailing suffix if needed
        opendap_urls = select(files, "OPENDAP")
        opendap_urls = [u[:-5] if u.endswith(".nc.html") else u for u in opendap_urls]

        return {"opendap": opendap_urls, "http": http_urls}

    def get_metadata_for_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """
        returns a list of OPENDAP URLs for use in processing given a dataset.
        """
        datasets = self.get_datasets_from_id(dataset_id)
        if len(datasets) == 0:
            msg = "no datasets found for given ID"
            raise ValueError(msg)
        return datasets[0]

    def get_access_paths(self, dataset: Dataset) -> AccessURLs:
        return self.get_all_access_paths_by_id(dataset.metadata["id"])

    def keyword_search(self, query: str, page: int) -> dict[str, Any]:
        """
        converts a list of keywords to an ESGF query and runs it against the node.
        """
        lucene_query_statements = ["AND", "OR", "(", ")"]
        if any([query.find(substring) != -1 for substring in lucene_query_statements]):
            datasets = self.run_esgf_dataset_query(query, page, options={})
            return {"query": {"raw": query}, "results": datasets}
        else:
            stripped_query = re.sub(r"[^A-Za-z0-9 ]+", "", query)
            lucene_query = " AND ".join(stripped_query.split(" "))
            datasets = self.run_esgf_dataset_query(lucene_query, page, options={})
            return {
                "query": {
                    "original": query,
                    "raw": lucene_query,
                },
                "results": datasets,
            }

    def natural_language_search(
        self, search_query: str, page: int, retries=0
    ) -> dict[str, Any]:
        """
        converts to natural language and runs the result against the ESGF node, returning a list of datasets.
        """
        search_terms_json = self.process_natural_language(search_query)
        try:
            search_terms = json.loads(search_terms_json)
        except ValueError as e:
            print(
                f"openAI returned more than just json, retrying query... \n {e} {search_terms_json}"
            )
            if retries >= 3:
                print("openAI returned non-json in multiple retries, exiting")
                return {
                    "error": f"openAI returned non-json in multiple retries. raw text: {search_terms_json}"
                }
            return self.natural_language_search(search_query, page, retries + 1)
        query = " AND ".join(
            [
                (
                    search_term.strip()
                    if isinstance(search_term, str)
                    else "({})".format(
                        " OR ".join(
                            filter(lambda term: term.strip() != "", search_term)
                        )
                    )
                )
                for search_term in filter(
                    lambda element: element != "", search_terms.values()
                )
            ]
        )
        datasets = self.with_all_available_mirrors(
            self.run_esgf_dataset_query, query, page, options={}
        )
        return {
            "query": {"raw": query, "search_terms": search_terms},
            "results": datasets,
        }

    def build_natural_language_prompt(self, search_query: str) -> str:
        """
        wraps user input given to the LLM after the context.
        """
        return "Convert the following input text: {}".format(search_query)

    def process_natural_language(self, search_query: str) -> str:
        """
        runs query against LLM and returns the result string.
        """
        response = self.client.chat.completions.create(
            model="gpt-4-0125-preview",
            messages=[
                {
                    "role": "system",
                    "content": generate_natural_language_system_prompt(
                        self.facet_possibilities
                    ),
                },
                {
                    "role": "user",
                    "content": self.build_natural_language_prompt(search_query),
                },
            ],
            temperature=0.7,
        )
        keywords_json = response.choices[0].message.content
        return keywords_json

    def run_esgf_dataset_query(
        self, query_string: str, page: int, options: Dict[str, str]
    ) -> DatasetSearchResults:
        """
        runs the formatted apache lucene query against the ESGF node and returns the metadata in datasets.
        """
        encoded_string = urlencode(
            {
                "query": query_string,
                "project": "CMIP6",
                "fields": "*",
                "latest": "true",
                "sort": "true",
                "limit": f"{default_settings.entries_per_page}",
                "offset": "{}".format(default_settings.entries_per_page * (page - 1)),
                "format": "application/solr+json",
                "distrib": "true",
            }
            | options
        )

        base_url = self.get_esgf_url_with_current_mirror()
        full_url = f"{base_url}?{encoded_string}"
        r = requests.get(full_url)
        if r.status_code != 200:
            error = str(r.content)
            raise ConnectionError(
                f"Failed to search against ESGF node: {full_url}: error from node upstream is: {r.status_code} {error}"
            )
        response = r.json()

        # parallel over datasets, but delay fetching url until needed
        return dask.compute(
            [
                dask.delayed(Dataset)(
                    metadata,
                )
                for metadata in response["response"]["docs"]
            ]
        )[0]
