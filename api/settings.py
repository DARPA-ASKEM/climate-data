from typing import Dict, Any
from pydantic import Field
from pydantic_settings import BaseSettings
import os


class Settings(BaseSettings):
    esgf_url: str = Field(os.environ.get("ESGF_URL", "https://esgf-node.llnl.gov/esg-search"))
    default_facets: str = Field("project,experiment_family")
    entries_per_page: int = Field(20)

    redis_host: Field(os.environ.get("REDIS_HOST", "redis-climate-data"))
    redis_port: Field(os.environ.get("REDIS_HOST", 6379))

    minio_url: str = Field(os.environ.get("MINIO_URL", "http://minio:9000"))
    minio_user: str = Field(os.environ.get("MINIO_USER", "miniouser"))
    minio_pass: str = Field(os.environ.get("MINIO_PASS", "miniopass"))
    bucket_name: str = Field(os.environ.get("MINIO_BUCKET_NAME", "climate-data-test-bucket"))

    terarium_url: str = Field(
        os.environ.get("TERARIUM_URL", "https://server.staging.terarium.ai")
    )
    terarium_user: str = Field(os.environ.get("TERARIUM_USER", ""))
    terarium_pass: str = Field(os.environ.get("TERARIUM_PASS", ""))


default_settings = Settings()
