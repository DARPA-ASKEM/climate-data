from fastapi import FastAPI, Request
from api.search.providers.esgf import ESGFProvider
from api.processing.providers.esgf import slice_esgf_dataset
from openai import OpenAI
from urllib.parse import parse_qs

app = FastAPI(docs_url="/")
client = OpenAI()

esgf = ESGFProvider(client)


def params_to_dict(request: Request):
    return parse_qs(request.url.query, keep_blank_values=True)


@app.get("/search/esgf")
async def esgf_search(query: str = "", page: int = 1, refresh_cache=False):
    datasets = esgf.search(query, page, refresh_cache)
    return {"results": datasets}


@app.get(path="/subset/esgf")
async def esgf_subset(request: Request, dataset_id: str = ""):
    params = params_to_dict(request)
    print(params)
    ds = slice_esgf_dataset(esgf, dataset_id, params)
    ds.to_netcdf("sliced.nc")
