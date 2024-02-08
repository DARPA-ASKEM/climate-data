from fastapi import FastAPI, Request, Depends
from api.search.providers.esgf import ESGFProvider
from api.processing.providers.esgf import (
    slice_and_store_dataset,
)
from api.dataset.job_queue import create_job, fetch_job_status, get_redis
from openai import OpenAI
from urllib.parse import parse_qs
from typing import List, Dict
from api.preview.render import render_preview_for_dataset, render_preview_for_hmi

app = FastAPI(docs_url="/")
client = OpenAI()

esgf = ESGFProvider(client)
esgf.initialize_embeddings()


def params_to_dict(request: Request) -> Dict[str, str | List[str]]:
    lists = parse_qs(request.url.query, keep_blank_values=True)
    return {k: v[0] if len(v) == 1 else v for k, v in lists.items()}


@app.get("/search/esgf")
async def esgf_search(query: str = "", page: int = 1, refresh_cache=False):
    datasets = esgf.search(query, page, refresh_cache)
    return {"results": datasets}


@app.get("/fetch/esgf")
async def esgf_fetch(dataset_id):
    urls = esgf.get_all_access_paths_by_id(dataset_id)
    return {"dataset": dataset_id, "urls": urls}


@app.get(path="/subset/esgf")
async def esgf_subset(
    request: Request, parent_id, dataset_id, redis=Depends(get_redis)
):
    params = params_to_dict(request)
    urls = esgf.get_all_access_paths_by_id(dataset_id)
    job = create_job(
        func=slice_and_store_dataset,
        args=[urls, parent_id, dataset_id, params],
        redis=redis,
        queue="subset",
    )
    return job


@app.get(path="/preview/esgf")
async def esgf_preview(dataset_id: str, redis=Depends(get_redis)):
    if esgf.is_terarium_hmi_dataset(dataset_id):
        print("terarium uuid found", flush=True)
        job = create_job(
            func=render_preview_for_hmi, args=[dataset_id], redis=redis, queue="preview"
        )
        return job
    else:
        urls = esgf.get_all_access_paths_by_id(dataset_id)
        job = create_job(
            func=render_preview_for_dataset, args=[urls], redis=redis, queue="preview"
        )
        return job


@app.get(path="/status/{job_id}")
async def job_status(job_id: str, redis=Depends(get_redis)):
    return fetch_job_status(job_id, redis=redis)
