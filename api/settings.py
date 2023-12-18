from pydantic import Field
from pydantic_settings import BaseSettings
import os


class Settings(BaseSettings):
    esgf_url: str = Field("https://esgf-node.llnl.gov/esg-search")
    default_facets: str = Field("project,experiment_family")
    entries_per_page: int = 5


default_settings = Settings()
