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
from numpy import dot
import json
import numpy as np
import pandas as pd
from pathlib import Path
import pickle

NATURAL_LANGUAGE_PROCESSING_CONTEXT = """
You are a tool to extract keyword search terms by category from a given search request. 

The given keyword fields are: frequency, nominal_resolution, lower_time_bound, upper_time_bound, and description.

The definitions of the keyword fields are as follows. 

frequency is a duration. 
Possible example values for frequency values are: 6 hours, 6hrs, 3hr, daily, day, yearly, 12 hours, 12 hr

nominal_resolution is a measure of distance. 
Possible example values for resolution are: 100 km, 100km, 200km, 200 km, 2x2 degrees, 1x1 degrees, 1x1, 2x2, 20000 km

lower_time_bound and upper_time_bound are measures of time. 
When extracted from user input, convert them to UTC ISO 8601 format.
Possible example values include:

"after 2022" = lower_time_bound: 2022-00-00T00:00:00Z
"between march 2021 and april 2023" = lower_time_bound: 2021-03-00T00:00:00Z ; upper_time_bound: 2023-04-00T00:00:00Z
"before september 1995" = upper_time_bound: 1995-09-00T00:00:00Z

description is a text field and contains all other unprocessed information. 

Return the fields as a JSON object and include no other information. 

Examples of full processing are as follows. 

Input:
100km before 2023 daily air temperature
Output:
{
    "frequency": "daily",
    "nominal_resolution": "100km",
    "upper_time_bound": "2023-00-00T00:00:00Z",
    "description": "air temperature"
}

Input: 2x2 degree relative humidity between june 1997 and july 1999 6hr
Output:
{
    "frequency": "6hr",
    "nominal_resolution": "2x2 degree",
    "lower_time_bound": "1997-06-00T00:00:00Z",
    "upper_time_bound": "1999-07-00T00:00:00Z",
    "description": "relative humidity"
}

Input: ts
Output: {
    "description": "ts"
}

Input: Find me datasets with the variable relative humidity 
Output: {
    "description": "relative humidity"
}

Input: datasets before june 1995 the variable surface temperature model BCC-ESM1
Output: {
    "upper_time_bound": "1995-06-00T00:00:00Z",
    "description": "surface temperature BCC-ESM1"
}
Only return JSON.
"""

# cosine matching threshold to greedily take term
GREEDY_EXTRACTION_THRESHOLD = 0.93

SEARCH_FACETS = {
    # match by cosine similarity
    "similar": [
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
    ],
    # only take exact matches
    "exact": [
        "institution_id",
        "variant_label",
        "experiment_id",
        "grid_label",
    ],
    # create embeddings, but handle manually elsewhere - optimization on a specific field
    "other": ["nominal_resolution", "frequency"],
}


class ESGFProvider(BaseSearchProvider):
    def __init__(self, openai_client):
        print("initializing esgf search provider")
        self.client: OpenAI = openai_client
        self.embeddings = {}

    def initialize_embeddings(self, force_refresh=False):
        """
        creates string embeddings if needed, otherwise reloads from cache.
        force_refresh is needed if the list of facets changes.
        """
        cache = Path("./embedding_cache")
        if cache.exists() and not force_refresh:
            print("embedding cache exists", flush=True)
            with cache.open("rb") as f:
                self.embeddings = pickle.load(f)
        else:
            print("no embedding cache, generating new", flush=True)
            with cache.open(mode="wb") as f:
                try:
                    self.embeddings = self.extract_embedding_strings()
                except Exception as e:
                    raise IOError(
                        f"failed to access OpenAI: is OPENAI_API_KEY set in env?: {e}"
                    )
                pickle.dump(self.embeddings, f)

    def is_terarium_hmi_dataset(self, dataset_id: str) -> bool:
        """
        checks if a dataset id is HMI or ESGF - uuid regex
        """
        p = re.compile(r"^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$")
        return bool(p.match(dataset_id.lower()))

    def search(
        self, query: str, page: int, force_refresh_cache: bool = False
    ) -> DatasetSearchResults:
        """
        converts a natural language query to a list of ESGF dataset
        metadata dictionaries by running a lucene query against the given
        ESGF node in settings.
        """
        if len(self.embeddings.keys()) == 0 or force_refresh_cache:
            self.initialize_embeddings(force_refresh_cache)
        return self.natural_language_search(query, page)

    def get_all_access_paths_by_id(self, dataset_id: str) -> AccessURLs:
        return [
            self.get_access_paths_by_id(id)
            for id in self.get_mirrors_for_dataset(dataset_id)
        ]

    def get_mirrors_for_dataset(self, dataset_id: str) -> List[str]:
        # strip vert bar if provided with example mirror attached
        dataset_id = dataset_id.split("|")[0]
        response = self.run_esgf_query(f"id:{dataset_id}*", 1, {})
        full_ids = [d.metadata["id"] for d in response]
        return full_ids

    def get_datasets_from_id(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        returns a list of datasets for a given ID. includes mirrors.
        """
        if dataset_id == "":
            return {}
        params = urlencode(
            {
                "type": "File",
                "format": "application/solr+json",
                "dataset_id": dataset_id,
                "limit": 200,
            }
        )
        full_url = f"{default_settings.esgf_url}/search?{params}"
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

    def natural_language_search(
        self, search_query: str, page: int, retries=0
    ) -> DatasetSearchResults:
        """
        converts to natural language and runs the result against the ESGF node, returning a list of datasets.
        """
        search_terms_json = self.process_natural_language(search_query)
        print(search_terms_json, flush=True)
        try:
            search_terms = json.loads(search_terms_json)
        except ValueError as e:
            print(
                f"openAI returned more than just json, retrying query... \n {e} {search_terms_json}"
            )
            if retries >= 3:
                print("openAI returned non-json in multiple retries, exiting")
                return []
            return self.natural_language_search(search_query, page, retries + 1)
        query = self.generate_query_string(search_terms)
        options = self.generate_temporal_coverage_query(search_terms)

        print(query, flush=True)
        if query == "":
            return []
        return self.run_esgf_query(query, page, options)

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
            model="gpt-4",
            messages=[
                {"role": "system", "content": NATURAL_LANGUAGE_PROCESSING_CONTEXT},
                {
                    "role": "user",
                    "content": self.build_natural_language_prompt(search_query),
                },
            ],
            temperature=0.7,
        )
        query = response.choices[0].message.content or ""
        print(query)
        query = query[query.find("{") :]
        return query

    def run_esgf_query(
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
                    metadata,
                )
                for metadata in response["response"]["docs"]
            ]
        )[0]

    def get_embedding(self, text):
        """returns an embedding for a single string."""
        return (
            self.client.embeddings.create(input=[text], model="text-embedding-ada-002")
            .data[0]
            .embedding
        )

    def get_embeddings(self, text):
        """returns a list of embeddings for a list of strings."""
        return [
            e.embedding
            for e in self.client.embeddings.create(
                input=text, model="text-embedding-ada-002"
            ).data
        ]

    def cosine_similarity(self, a, b):
        return dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

    def extract_embedding_strings(self) -> Dict[str, pd.DataFrame]:
        """
        builds embeddings dictionary given the desired SEARCH_FACETS. finds possible
        values for given facets from ESGF node and then gets string embeddings of those
        enumerated values.
        """
        desired_facets = [item for inner in SEARCH_FACETS.values() for item in inner]
        print(desired_facets)
        encoded_string = urlencode(
            {
                "project": "CMIP6",
                "facets": ",".join(desired_facets),
                "limit": "0",
                "format": "application/solr+json",
            }
        )
        facet_possibilities = (
            f"https://esgf-node.llnl.gov/esg-search/search?{encoded_string}"
        )

        print("querying fields", flush=True)
        r = requests.get(facet_possibilities)
        if r.status_code != 200:
            raise ConnectionError(
                f"Failed to get facet potential values from ESGF node: {facet_possibilities} {r.status_code}"
            )
        response = r.json()
        fields = response["facet_counts"]["facet_fields"]
        print("aggregating fields", flush=True)
        embeddings = {
            f: pd.DataFrame({"string": fields[f][::2]}) for f in desired_facets
        }
        print("creating embeddings...", flush=True)
        for k in embeddings.keys():
            print(f"  embeddings for: {k}", flush=True)
            # drop '' and other falsy strings
            embeddings[k] = embeddings[k][embeddings[k].string.astype(bool)]
            embeddings[k]["embed"] = self.get_embeddings(embeddings[k].string.to_list())

        return embeddings

    def get_single_best_match(self, text, similar_fields):
        @dask.delayed
        def get_best_match_from_field(self, text, field):
            embedding = (
                self.get_embedding(text)
                if field != "source_id"
                else self.get_embedding(text.upper())
            )
            self.embeddings[field]["similarities"] = self.embeddings[field][
                "embed"
            ].apply(lambda e: self.cosine_similarity(e, embedding))
            best_match = (
                self.embeddings[field]
                .sort_values("similarities", ascending=False)
                .head(3)
            )
            string = best_match.string.values[0]
            similarity = best_match.similarities.values[0]
            print(f"    {string} => {similarity}")
            return (string, similarity)

        results = map(
            lambda x: get_best_match_from_field(self, text, x), similar_fields
        )
        computed: list = list(dask.compute(results))[0]
        # sort by similarity value, descending
        computed.sort(key=lambda x: x[1], reverse=True)
        print(computed)
        return computed[0] or ("", 0.00)

    def extract_relevant_description(self, description: str) -> List[str]:
        """
        takes the LLM-extracted description field and parses it into meaningful
        terms to build into the formatted apache lucene query.
        """
        # experiment id and variant id are best taken as exact match rather than assumed by cosine
        # general idea:
        #   break on word boundary and take...
        #     exact matches to experiment id and variant_label
        #     anything that's over GREEDY_EXTRACTION_THRESHOLD
        #   otherwise...
        #     take non-matching inputs and conjoin them back into a phrase to take highest match across all categories
        #     take the most relevant between averaged individual token similarities and the whole phrase
        tokens = description.replace(",", " ").split()

        # looking for exact match on an ESGF dataset full ID would be a dict of 10+M entries
        # so we can leverage breaking apart the longform id into each component period-separated
        # as individual tokens. much faster and cleaner.
        tokens = [
            t for exploded in [token.split(".") for token in tokens] for t in exploded
        ]
        # after the above, date stamps aren't in the same format in the version field,
        # so we strip according to the format if it perfectly matches, then use it as free-text
        # rather than a field to check. this happens below, during exact match

        matched = []
        exact_match_values = [
            match
            for nested_list in [
                self.embeddings[field].string.values for field in SEARCH_FACETS["exact"]
            ]
            for match in nested_list
        ]

        fallback_similarities = []

        print(f"finding best terms for {tokens}")

        # first check all as a phrase, kick back to individual tokens if nothing fits well (greedy threshold)
        conjoined_phrase, conjoined_similarity = self.get_single_best_match(
            " ".join(tokens), SEARCH_FACETS["similar"]
        )
        if conjoined_similarity > 0.935:
            print(
                f"  greedily taking full phrase: {conjoined_phrase} at {conjoined_similarity}"
            )
            matched.append(conjoined_phrase)
            tokens = []

        # parallel inner iterator for tokens - refactor of "remove from leftover tokens,
        # append to matched" workflow. returns (matched, fallback) to be zipped over;
        # if matched, return (token, None), if fallback, return (None, (phrase, similarity))
        @dask.delayed
        def inner_iterator(t):
            if len(t) == 9 and t[0] == "v" and t[1:].isdigit():
                print(f"  date match: {t}")
                return (t[1:], None)
            if t in exact_match_values:
                print(f"  exact match: {t}")
                return (t, None)
            else:
                print(f"  approximate matching for {t}")
                phrase, similarity = self.get_single_best_match(
                    t, SEARCH_FACETS["similar"]
                )
                if similarity >= GREEDY_EXTRACTION_THRESHOLD:
                    print(
                        f"    matched word {t} -> {phrase} over threshold {GREEDY_EXTRACTION_THRESHOLD}: {similarity}"
                    )
                    return (phrase, (phrase, similarity))
                else:
                    print(
                        f"    closest match {t} -> {phrase} is under threshold {GREEDY_EXTRACTION_THRESHOLD}: {similarity}"
                    )
                return (None, (phrase, similarity))

        # zip(*x) is inverse to zip(x) - filter nones, split the two lists that were done in parallel
        results = list(list(dask.compute(map(inner_iterator, tokens[:])))[0])
        if len(results) == 0:
            return matched
        matched, fallback_similarities = list(
            map(lambda x: list(filter(lambda y: y is not None, x)), zip(*results))
        )
        # removed matched tokens. some require transformations, e.g. upper()
        tokens = [t for t in tokens if t not in matched and t.upper() not in matched]

        if len(tokens) == 0:
            print(f"finalized search terms are {matched}")
            return matched

        print(f"  leftover tokens: {tokens}\nmatching for whole phrase")

        conjoined_phrase, conjoined_similarity = self.get_single_best_match(
            " ".join(tokens), SEARCH_FACETS["similar"]
        )

        fallback_similarities = [f for f in fallback_similarities if f[0] in tokens]
        if len(fallback_similarities) == 0:
            if conjoined_similarity >= 0.90:
                matched.append(conjoined_phrase)
            return matched

        avg_sim = sum((map(lambda f: f[1], fallback_similarities))) / len(
            fallback_similarities
        )
        print(f"  conjoined similarity {conjoined_similarity} - avg by parts {avg_sim}")
        if conjoined_similarity >= avg_sim:
            print(f"    using conjoined phrase {conjoined_phrase}")
            matched.append(conjoined_phrase)
        else:
            for part in fallback_similarities:
                matched += part[0]

        print(f"finalized search terms are {matched}")

        return matched

    def generate_query_string(self, search_terms: Dict[str, str]) -> str:
        """
        handles LLM-extracted fields separately as needed and returns the formatted lucene query.
        """
        desired_terms = ["nominal_resolution", "frequency"]
        best_matches = []

        for desired in desired_terms:
            if desired in search_terms:
                print(f"{search_terms[desired], desired}")
                phrase, sim = self.get_single_best_match(
                    search_terms[desired], [desired]
                )
                if sim >= GREEDY_EXTRACTION_THRESHOLD:
                    best_matches.append(phrase)
                else:
                    print(
                        f"  failed to find good candidate for {desired}: '{search_terms[desired]}'"
                    )
                    search_terms["description"] += f" {search_terms[desired]}"

        description = []
        if "description" in search_terms:
            description = self.extract_relevant_description(search_terms["description"])
        query_string = " AND ".join(map(lambda t: f'"{t}"', best_matches + description))
        print(f"lucene query: {query_string}")
        return query_string

    def generate_temporal_coverage_query(self, terms: Dict[str, str]) -> Dict[str, str]:
        """
        creates ESGF search time bound arguments.
        """
        query = {}
        if "upper_time_bound" in terms:
            query["end"] = terms["upper_time_bound"]
        if "lower_time_bound" in terms:
            query["start"] = terms["lower_time_bound"]
        return query
