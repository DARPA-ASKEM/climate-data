from api.search.provider import BaseSearchProvider, DatasetSearchResults, Dataset
import ast
from openai import OpenAI
from typing import List
from pydantic import BaseModel, Field


class ERA5SearchData(BaseModel):
    dataset_name: str = Field()
    product_type: str = Field()
    variable: str = Field()


class ERA5ApiCallNodeVisitor(ast.NodeVisitor):
    """
    walks the ast to strip out a GPT-4 generated ERA5 api call to get the important details from it
    for storage, rather than keeping a chunk to eval - easier to reconstitute and make changes from
    frontend arguments for subsetting.
    """

    output: ERA5SearchData | None

    def __init__(self):
        self.output = None
        super()

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
            self.output = ERA5SearchData(
                dataset_name=dataset_name,
                product_type=api_arguments["product_type"],
                variable=api_arguments["variable"],
            )
            # break and stop traversal - return != return None here! likely a misuse of the NodeVisitor api.
            return None
        self.generic_visit(node)


class ERA5Provider(BaseSearchProvider):
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
        retrieve_call = code_output[code_output.find("c.retrieve(") :].replace(
            "```", ""
        )
        print(retrieve_call, flush=True)
        visitor = ERA5ApiCallNodeVisitor()
        visitor.visit(ast.parse(retrieve_call))
        data = visitor.output
        if data is None:
            raise IOError("failed to walk and get data from ast: None in result")
        return [Dataset(data.dict())]

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
