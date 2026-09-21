"""FastAPI dependencies that hand out the services built at startup (see `app.main`).

`app.state` is untyped, so each getter uses `cast` to state what `lifespan` put there.
"""

from typing import Annotated, cast

from fastapi import Depends, HTTPException, Request, status

from app.config import Settings, get_settings
from app.inbox import WebhookInbox
from app.models import Job
from app.runner import JobRunner
from app.store import JobStore


def get_store(request: Request) -> JobStore:
    """Return the job store."""
    return cast(JobStore, request.app.state.store)


def get_runner(request: Request) -> JobRunner:
    """Return the background job runner."""
    return cast(JobRunner, request.app.state.runner)


def get_inbox(request: Request) -> WebhookInbox:
    """Return the demo webhook inbox."""
    return cast(WebhookInbox, request.app.state.inbox)


StoreDep = Annotated[JobStore, Depends(get_store)]
RunnerDep = Annotated[JobRunner, Depends(get_runner)]
InboxDep = Annotated[WebhookInbox, Depends(get_inbox)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def get_job(job_id: str, store: StoreDep) -> Job:
    """Look up the job named in the path, or respond with 404."""
    job = store.get(job_id)
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Job {job_id} not found")
    return job


JobDep = Annotated[Job, Depends(get_job)]
