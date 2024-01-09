from typing import Dict
from pydantic import Field
from pydantic_settings import BaseSettings
import os


class Settings(BaseSettings):
    esgf_url: str = Field("https://esgf-node.llnl.gov/esg-search")
    default_facets: str = Field("project,experiment_family")
    entries_per_page: int = Field(20)
    redis: Dict[str, str] = Field({"host": "localhost", "port": "port"})


default_settings = Settings()
