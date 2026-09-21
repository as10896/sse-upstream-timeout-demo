import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any, Protocol

from app.models import CreateJobRequest, Job, JobEvent, JobEventKind, JobStatus


class JobReader(Protocol):
    """Read side of the job store, used by the HTTP routes."""

    def get(self, job_id: str) -> Job | None:
        """Return the job, or None if it doesn't exist."""
        ...

    def watch(self, job_id: str, after_seq: int = 0) -> AsyncIterator[JobEvent]:
        """Yield events after `after_seq`, then new ones as they happen, until the job closes."""
        ...


class JobWriter(Protocol):
    """Write side of the job store, used by the background runner."""

    def create(self, request: CreateJobRequest) -> Job:
        """Register a new queued job."""
        ...

    async def record(
        self, job_id: str, kind: JobEventKind, message: str, data: dict[str, Any] | None = None
    ) -> JobEvent:
        """Append an event to the job's timeline."""
        ...

    async def mark_running(self, job_id: str) -> None:
        """Move the job to `running`."""
        ...

    async def mark_succeeded(self, job_id: str, result: dict[str, Any]) -> None:
        """Move the job to `succeeded` and store its result."""
        ...

    async def mark_failed(self, job_id: str, error: str) -> None:
        """Move the job to `failed` and store the reason."""
        ...

    async def close(self, job_id: str) -> None:
        """Mark the timeline complete; watchers stop after the last event."""
        ...


class JobStore(JobReader, JobWriter, Protocol):
    """A store with both the read and the write side."""


class InMemoryJobStore(JobStore):
    """Keeps jobs in process memory.

    Fine for a single-process demo. Jobs are lost on restart and aren't shared
    between replicas, so a real deployment would use Redis or a database.
    """

    def __init__(self) -> None:
        """Create an empty store."""
        self._jobs: dict[str, Job] = {}
        self._changed = asyncio.Condition()

    def get(self, job_id: str) -> Job | None:
        """Return the job, or None if it doesn't exist."""
        return self._jobs.get(job_id)

    def create(self, request: CreateJobRequest) -> Job:
        """Register a new queued job."""
        job = Job(
            id=uuid.uuid4().hex[:12],
            mode=request.mode,
            query=request.query,
            duration_seconds=request.duration_seconds,
            callback_url=str(request.callback_url) if request.callback_url else None,
        )
        self._jobs[job.id] = job
        return job

    async def record(
        self, job_id: str, kind: JobEventKind, message: str, data: dict[str, Any] | None = None
    ) -> JobEvent:
        """Append an event to the job's timeline and wake up watchers."""
        job = self._jobs[job_id]
        event = JobEvent(
            seq=len(job.events) + 1,
            elapsed_seconds=job.seconds_since_start(),
            kind=kind,
            message=message,
            data=data,
        )
        async with self._changed:
            job.events.append(event)
            self._changed.notify_all()
        return event

    async def mark_running(self, job_id: str) -> None:
        """Move the job to `running`."""
        await self._set_status(job_id, JobStatus.RUNNING)

    async def mark_succeeded(self, job_id: str, result: dict[str, Any]) -> None:
        """Move the job to `succeeded` and store its result."""
        self._jobs[job_id].result = result
        await self._set_status(job_id, JobStatus.SUCCEEDED)

    async def mark_failed(self, job_id: str, error: str) -> None:
        """Move the job to `failed` and store the reason."""
        self._jobs[job_id].error = error
        await self._set_status(job_id, JobStatus.FAILED)

    async def close(self, job_id: str) -> None:
        """Mark the timeline complete; watchers stop after the last event."""
        async with self._changed:
            self._jobs[job_id].closed = True
            self._changed.notify_all()

    async def watch(self, job_id: str, after_seq: int = 0) -> AsyncIterator[JobEvent]:
        """Yield the job's events after `after_seq`, then new ones as they happen, until it closes.

        Events are numbered from 1, so `after_seq` works with the SSE `Last-Event-ID`
        header: a reconnecting browser picks up where it left off.
        """
        job = self._jobs[job_id]
        cursor = after_seq
        while True:

            def has_news(seen: int = cursor) -> bool:
                return len(job.events) > seen or job.closed

            async with self._changed:
                await self._changed.wait_for(has_news)
                fresh = job.events[cursor:]
                finished = job.closed
            for event in fresh:
                yield event
            cursor += len(fresh)
            if finished:
                return

    async def _set_status(self, job_id: str, status: JobStatus) -> None:
        self._jobs[job_id].status = status
        await self.record(job_id, JobEventKind.STATUS, f"Job is {status}", {"status": status})
