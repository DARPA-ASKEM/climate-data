from api.search.provider import BaseSearchProvider, DatasetSearchResults, Dataset
from api.settings import default_settings
import ast
from openai import OpenAI
from typing import List


class ERA5ApiCallNodeVisitor(ast.NodeVisitor):
    """
    walks the ast to strip out a GPT-4 generated ERA5 api call to get the important details from it
    for storage, rather than keeping a chunk to eval - easier to reconstitute and make changes from
    frontend arguments for subsetting.
    """

    def visit_Call(self, node):
        if isinstance(node.func.value, ast.Name) and node.func.attr == "retrieve":
            args = [arg for arg in node.args]
            expected = [ast.Constant, ast.Dict, ast.Constant]
            type_match = [isinstance(args[i], expected[i]) for i in range(len(args))]
            if not all(type_match):
                raise IOError(
                    f"malformed API call: type match failed for args: {type_match}; expected {expected}; got {args}"
                )
            dataset_name = args[0].value
            api_arguments = eval(
                compile(ast.Expression(args[1]), "<ast dictionary>", "eval")
            )
            filename = args[2].value
            return (dataset_name, api_arguments, filename)
        self.generic_visit(node)


class ERA5(BaseSearchProvider):
    def __init__(self, openai_client):
        print("initializing ERA5 search provider")
        self.client: OpenAI = openai_client

    def search(self, query: str, *_) -> DatasetSearchResults:
        """
        unpaginated - search ERA5 datasets and download file. the ERA5 api downloads to disk
        as the only API call rather than search / subset / fetch as two operations.

        this generates the *api call* for preview and extracts the information - subsetting it
        will construct it from that data. not quite ideal, but ERA5 is not fun to work with
        """
        code_output = self.natural_language_search(query)
        retrieve_call = code_output[code_output.find("c.retrieve(") :]
        visitor = ERA5ApiCallNodeVisitor()
        (name, args, filename) = visitor.visit(ast.parse(retrieve_call))
        return [Dataset(dict(dataset_name=name, arguments=args, filename=filename))]

    def generate_natural_language_context(self, search: str):
        return f"""
construct a python api call for the CDS ERA5 climate data store to retrieve {search} in netcdf format. 
return only the python code with no additional explanation. 
    """

    def natural_language_search(self, query: str) -> str:
        context = self.generate_natural_language_context(query)
        response = self.client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "user", "content": context},
            ],
            temperature=0.7,
        )
        return response.choices[0].message.content or ""

    def get_access_paths(self, dataset: Dataset) -> List[str]:
        """
        era5 metadata is less useful than other sources with a search / fetch workflow.
        additionally, the loaded dataset used for subsetting / renders must be done in one step.
        """
        return []
