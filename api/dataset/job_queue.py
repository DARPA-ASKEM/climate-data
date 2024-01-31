import time
import uuid
from typing import Any, Dict, Optional
from fastapi import Response, status
from redis import Redis
from rq import Queue
from rq.exceptions import NoSuchJobError
from rq.job import Job
from api.dataset.models import SliceJob
from api.settings import default_settings


def get_redis():
    return Redis(default_settings.redis["host"], default_settings.redis["port"])


# from knowledge-middleware/api/utils.py:37
def create_job(*, func, args, redis, queue="default"):
    q = Queue(name=queue, connection=redis, default_timeout=-1)
    job_id = str(uuid.uuid4())
    job = q.enqueue(func, args=args, kwargs={"job_id": job_id}, job_id=job_id)
    status = job.get_status()
    if status in ("finished", "failed"):
        job_result = job.return_value()
        job_error = job.exc_info
        job.cleanup(ttl=0)  # Cleanup/remove data immediately
    else:
        job_result = None
        job_error = None
    result = {
        "created_at": job.created_at,
        "enqueued_at": job.enqueued_at,
        "started_at": job.started_at,
        "job_error": job_error,
        "job_result": job_result,
    }
    return SliceJob(id=job_id, status=status, result=result)


def fetch_job_status(job_id, redis):
    """Fetch a job's results from RQ.

    Args:
        job_id (str): The id of the job being run in RQ. Comes from the subset/{provider} endpoint.

    Returns:
        Response:
            status_code: 200 if successful, 404 if job does not exist.
            content: contains the job's results.
    """
    try:
        job = Job.fetch(job_id, connection=redis)
        result = {
            "created_at": job.created_at,
            "enqueued_at": job.enqueued_at,
            "started_at": job.started_at,
            "job_error": job.exc_info,
            "job_result": job.return_value(),
        }
        return SliceJob(id=job_id, status=job.get_status(), result=result)
    except NoSuchJobError:
        return status.HTTP_404_NOT_FOUND
