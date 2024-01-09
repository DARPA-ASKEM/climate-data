from fastapi import FastAPI, Request
from api.search.providers.esgf import ESGFProvider
from api.processing.providers.esgf import slice_esgf_dataset
from openai import OpenAI
from urllib.parse import parse_qs
from typing import List, Dict

app = FastAPI(docs_url="/")
client = OpenAI()

esgf = ESGFProvider(client)


def params_to_dict(request: Request) -> Dict[str, str | List[str]]:
    lists = parse_qs(request.url.query, keep_blank_values=True)
    return {k: v[0] if len(v) == 1 else v for k, v in lists.items()}


@app.get("/search/esgf")
async def esgf_search(query: str = "", page: int = 1, refresh_cache=False):
    datasets = esgf.search(query, page, refresh_cache)
    return {"results": datasets}


@app.get(path="/subset/esgf")
async def esgf_subset(request: Request, dataset_id: str = ""):
    params = params_to_dict(request)
    print(params)
    ds = slice_esgf_dataset(esgf, dataset_id, params)
    print("finished")
    return {"ds": {"nbytes": ds.nbytes}}
    # ds.to_netcdf("sliced.nc")
