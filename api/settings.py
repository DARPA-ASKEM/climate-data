from typing import Tuple
from pydantic import Field
from pydantic_settings import BaseSettings
import os

DEFAULT_ESGF_FALLBACKS = [
    "https://esgf-node.ornl.gov/esg-search",
    "https://ds.nccs.nasa.gov/esg-search",
    "https://dpesgf03.nccs.nasa.gov/esg-search",
    "https://esg-dn1.nsc.liu.se/esg-search",
    "https://esg-dn2.nsc.liu.se/esg-search",
    "https://esg-dn3.nsc.liu.se/esg-search",
    "https://cmip.bcc.cma.cn/esg-search",
    "http://cmip.fio.org.cn/esg-search",
    "http://cordexesg.dmi.dk/esg-search",
    "http://data.meteo.unican.es/esg-search",
    "http://esg-cccr.tropmet.res.in/esg-search",
]


class Settings(BaseSettings):
    esgf_url: str = Field(
        os.environ.get("ESGF_URL", "https://esgf-node.lln.gov/esg-search")
    )
    esgf_fallbacks: str = Field(
        os.environ.get("ESGF_FALLBACKS", ",".join(DEFAULT_ESGF_FALLBACKS))
    )
    esgf_openid: Tuple[str, str] = Field(
        (os.environ.get("ESGF_OPENID_USER", ""), os.environ.get("ESGF_OPENID_PASS", ""))
    )
    default_facets: str = Field("project,experiment_family")
    entries_per_page: int = Field(20)

    redis_host: str = Field(os.environ.get("REDIS_HOST", "redis-climate-data"))
    redis_port: int = Field(os.environ.get("REDIS_PORT", 6379))

    minio_url: str = Field(os.environ.get("MINIO_URL", "http://minio:9000"))
    minio_user: str = Field(os.environ.get("MINIO_USER", "miniouser"))
    minio_pass: str = Field(os.environ.get("MINIO_PASS", "miniopass"))
    bucket_name: str = Field(
        os.environ.get("MINIO_BUCKET_NAME", "climate-data-test-bucket")
    )

    terarium_url: str = Field(
        os.environ.get("TERARIUM_URL", "https://server.staging.terarium.ai")
    )
    terarium_user: str = Field(os.environ.get("TERARIUM_USER", ""))
    terarium_pass: str = Field(os.environ.get("TERARIUM_PASS", ""))


default_settings = Settings()
