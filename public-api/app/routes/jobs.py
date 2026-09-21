from collections.abc import AsyncIterable
from typing import Annotated

from fastapi import APIRouter, Header, Request, status
from fastapi.sse import EventSourceResponse, ServerSentEvent

from app.dependencies import JobDep, RunnerDep, StoreDep
from app.models import CreateJobRequest, Job, JobAccepted

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def create_job(
    body: CreateJobRequest, request: Request, store: StoreDep, runner: RunnerDep
) -> JobAccepted:
    """Accept a job and return 202 right away; the upstream call runs in the background."""
    job = store.create(body)
    runner.start(job)
    return JobAccepted(
        job_id=job.id,
        status=job.status,
        status_url=request.app.url_path_for("get_job_status", job_id=job.id),
        events_url=request.app.url_path_for("stream_job_events", job_id=job.id),
    )


@router.get("/{job_id}")  # noqa: FAST003 - job_id is resolved by JobDep
async def get_job_status(job: JobDep) -> Job:
    """Return a job's current state and full timeline (for polling clients)."""
    return job


@router.get("/{job_id}/events", response_class=EventSourceResponse)  # noqa: FAST003
async def stream_job_events(
    job: JobDep,
    store: StoreDep,
    last_event_id: Annotated[int, Header(ge=0)] = 0,
) -> AsyncIterable[ServerSentEvent]:
    """Stream a job's timeline to the browser, then send `end` and finish.

    This is the "tell the user" half of the demo. `Last-Event-ID` is honoured, so
    `EventSource` resumes without duplicates when it reconnects.
    """
    async for event in store.watch(job.id, after_seq=last_event_id):
        yield ServerSentEvent(data=event, event=event.kind, id=str(event.seq))
    yield ServerSentEvent(data={"job_id": job.id, "status": job.status}, event="end")
