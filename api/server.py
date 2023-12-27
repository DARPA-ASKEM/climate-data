from fastapi import FastAPI
from api.search.esgf import ESGFProvider
from openai import OpenAI

# 6hr between march 8 1800 and july 10 1999 historical atmospheric_co2 piControl 2x2

app = FastAPI(docs_url="/")
client = OpenAI()

esgf = ESGFProvider(client)


@app.get("/search/esgf")
async def esgf_search(query: str = "", page: int = 1):
    datasets = esgf.search(query, page)
    return {"results": datasets}
