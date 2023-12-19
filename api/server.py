from fastapi import FastAPI
from api.search.esgf import ESGFProvider
from api.search.provider import BaseSearchProvider, DatasetSearchResults
from api.process import get_dataset_sizes
from openai import OpenAI
import dask

app = FastAPI(docs_url="/")
client = OpenAI()


@app.get("/search/esgf")
async def esgf_search(query: str = "", page: int = 1):
    esgf = ESGFProvider(client)
    datasets = esgf.search(query, page)
    return {"results": datasets}