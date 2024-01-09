from venv import create
from fastapi import FastAPI, Request, Depends
from api.search.providers.esgf import ESGFProvider
from api.processing.providers.esgf import slice_and_store_dataset, slice_esgf_dataset
from api.dataset.job_queue import create_job, fetch_job_status, get_redis
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


@app.get("/fetch/esgf")
async def esgf_fetch(dataset_id: str = ""):
    urls = esgf.get_access_urls_by_id(dataset_id)
    return {"dataset": dataset_id, "urls": urls}


@app.get(path="/subset/esgf")
async def esgf_subset(request: Request, redis=Depends(get_redis), dataset_id: str = ""):
    params = params_to_dict(request)
    urls = esgf.get_access_urls_by_id(dataset_id)
    job = create_job(
        func=slice_and_store_dataset, args=[urls, dataset_id, params], redis=redis
    )
    return job


@app.get(path="/status/{job_id}")
async def job_status(job_id: str, redis=Depends(get_redis)):
    return fetch_job_status(job_id, redis=redis)
