import pyesgf.search
from api.settings import default_settings
from api.search.provider import BaseSearchProvider, DatasetSearchResults

NATURAL_LANGUAGE_PROCESSING_CONTEXT = """
Split the given input text into key search terms and separate them with `AND`. 
Surround measures of distance in escaped double quotes. 

Respond according to the given examples provided in a mapping.

"6hr 100 km air_temperature" => "6hr AND  \"100 km\" AND air_temperature"
"day experiment_name 200 km historical" => "day AND experiment_name AND \"200 km\" AND historical"
"20 km 3hr piControl nRoot" => "\"20 km\" AND 3hr AND piControl AND nRoot" 

Do not provide any other words except the converted search terms.

Additionally, convert measures of duration according to the following formatting examples.

"3 hours" => "3hr"
"One day" => "day"
"1 day" => "day"
"6 hours" => "6hr"
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
        return query.replace('"', '\\"')

    def run_esgf_query(
        self, query_string: str, page: int, node_url: str = "", facets: str = ""
    ) -> DatasetSearchResults:
        if page < 1:
            return ["Invalid page."]
        conn = pyesgf.search.SearchConnection(
            node_url or default_settings.esgf_url, distrib=True
        )
        print("running esgf query with string '{}'".format(query_string))
        query_string += "&sort=true"
        context = conn.new_context(
            project="CMIP6",
            query=query_string,
            facets=facets or default_settings.default_facets,
            latest=True,
        )

        slice_start = default_settings.entries_per_page * page
        slice_end = slice_start + default_settings.entries_per_page

        # context.search()[start:end] doesn't work due to overloading for caching in library
        datasets = [
            context.search()[i]
            for i in range(slice_start, min(slice_end, context.hit_count))
        ]

        files_list = [
            [f.opendap_url for f in files]
            for files in [d.file_context().search() for d in datasets]
        ]

        return files_list
