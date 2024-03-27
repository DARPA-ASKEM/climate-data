from fastapi import FastAPI, Request, Depends
from api.processing.providers.era5 import download_era5_subset, era5_subset_job
from api.search.providers.era5 import ERA5Provider, ERA5SearchData
from api.search.providers.esgf import ESGFProvider
from api.processing.providers.esgf import (
    slice_and_store_dataset,
)
from api.dataset.job_queue import create_job, fetch_job_status, get_redis
from openai import OpenAI
from urllib.parse import parse_qs
from typing import List, Dict
from api.preview.render import render_preview_for_dataset

app = FastAPI(docs_url="/")
client = OpenAI()

esgf = ESGFProvider(client)
esgf.initialize_embeddings()

era5 = ERA5Provider(client)


def params_to_dict(request: Request) -> Dict[str, str | List[str]]:
    lists = parse_qs(request.url.query, keep_blank_values=True)
    return {k: v[0] if len(v) == 1 else v for k, v in lists.items()}


@app.get(path="/status/{job_id}")
async def job_status(job_id: str, redis=Depends(get_redis)):
    return fetch_job_status(job_id, redis=redis)


@app.get("/search/esgf")
async def esgf_search(query: str = "", page: int = 1, refresh_cache=False):
    try:
        datasets = esgf.search(query, page, refresh_cache)
    except Exception as e:
        return {"error": f"failed to fetch datasets: {e}"}
    return {"results": datasets}


@app.get("/search/era5")
async def era5_search(query: str = ""):
    datasets = era5.search(query)
    return {"results": datasets}


@app.get("/fetch/esgf")
async def esgf_fetch(dataset_id: str):
    urls = esgf.get_all_access_paths_by_id(dataset_id)
    metadata = esgf.get_metadata_for_dataset(dataset_id)
    return {"dataset": dataset_id, "urls": urls, "metadata": metadata}


@app.get(path="/subset/esgf")
async def esgf_subset(
    request: Request,
    parent_id: str,
    dataset_id: str,
    variable_id: str = "",
    redis=Depends(get_redis),
):
    params = params_to_dict(request)
    urls = esgf.get_all_access_paths_by_id(dataset_id)
    job = create_job(
        func=slice_and_store_dataset,
        args=[urls, parent_id, dataset_id, params, variable_id],
        redis=redis,
        queue="subset",
    )
    return job


@app.get(path="/subset/era5")
async def era5_subset(
    parent_id: str,
    dataset_name: str,
    product_type: str,
    variable: str,
    days: str,
    months: str,
    years: str,
    hours: str,
    redis=Depends(get_redis),
):
    sd = ERA5SearchData(
        dataset_name=dataset_name, product_type=product_type, variable=variable
    )
    job = create_job(
        func=era5_subset_job,
        args=[sd, parent_id, days, months, years, hours],
        redis=redis,
        queue="subset",
    )
    return job


@app.get(path="/preview/esgf")
async def esgf_preview(
    dataset_id: str,
    variable_id: str = "",
    time_index: str = "",
    timestamps: str = "",
    analyze: bool = False,
    redis=Depends(get_redis),
):
    dataset = (
        dataset_id
        if esgf.is_terarium_hmi_dataset(dataset_id)
        else esgf.get_all_access_paths_by_id(dataset_id)
    )
    job = create_job(
        func=render_preview_for_dataset,
        args=[dataset, variable_id, time_index, timestamps, analyze],
        redis=redis,
        queue="preview",
    )
    return job
