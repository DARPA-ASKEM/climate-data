from datetime import datetime
from enum import Enum
from typing import Dict, List

from pydantic import BaseModel, Field


class Status(Enum):
    started = "started"
    finished = "finished"
    cancelled = "cancelled"
    complete = "complete"
    error = "error"
    queued = "queued"
    running = "running"
    failed = "failed"


class Result(BaseModel):
    created_at: datetime
    enqueued_at: datetime
    started_at: datetime | None
    job_result: Dict | None
    job_error: str | None


class SliceJob(BaseModel):
    id: str
    status: Status
    result: Result | None


class TemporalSubsetOptions(BaseModel):
    timestamp_range: List[str]
    field: str = "time"


class GeospatialSubsetOptions(BaseModel):
    envelope: List[float]
    fields: List[str] = ["lat", "lon"]


class ThinningSubsetOptions(BaseModel):
    factor: int = 1
    fields: List[str] | None = None  # none implies all fields
    negated: bool = False
    squared: bool = False


class CustomSubsetOptions(BaseModel):
    payload: Dict[str, str] = Field({})


class DatasetSubsetOptions(BaseModel):
    geospatial: GeospatialSubsetOptions | None = None
    temporal: TemporalSubsetOptions | None = None
    thinning: ThinningSubsetOptions | None = None
    custom: CustomSubsetOptions | None = None


class DatasetQueryParameters(Enum):
    envelope = "envelope"
    timestamps = "timestamps"
    thin_factor = "thin_factor"
    thin_fields = "thin_fields"
    thin_square = "thin_squared"
    custom = "custom"
