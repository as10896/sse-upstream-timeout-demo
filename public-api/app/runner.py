import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from app.models import CallbackPayload, Job, JobEventKind, JobMode, JobStatus
from app.notifier import ResultNotifier
from app.store import JobWriter
from app.upstream import UpstreamClient, UpstreamError

logger = logging.getLogger("public-api.runner")


@dataclass(frozen=True, slots=True)
class _Outcome:
    status: JobStatus
    result: dict[str, Any] | None = None
    error: str | None = None


class JobRunner:
    """Runs accepted jobs in the background, after the 202 has already gone out.

    For each job it calls the upstream with the client for the job's mode,
    records what happens, and sends the callback at the end. The runner doesn't
    know how any mode works; adding a mode means registering another
    `UpstreamClient`.
    """

    def __init__(
        self,
        store: JobWriter,
        clients: Mapping[JobMode, UpstreamClient],
        notifier: ResultNotifier,
    ) -> None:
        """Set up the runner with its collaborators."""
        self._store = store
        self._clients = clients
        self._notifier = notifier
        self._tasks: set[asyncio.Task[None]] = set()

    def start(self, job: Job) -> None:
        """Start processing `job` in the background and return right away."""
        task = asyncio.create_task(self._run(job), name=f"job-{job.id}")
        # asyncio keeps only weak references to tasks, so hold on to them here.
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def shutdown(self) -> None:
        """Cancel jobs still in flight (called on application shutdown)."""
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _run(self, job: Job) -> None:
        try:
            outcome = await self._execute(job)
            if job.callback_url:
                await self._deliver_callback(job, job.callback_url, outcome)
        finally:
            await self._store.close(job.id)

    async def _execute(self, job: Job) -> _Outcome:
        await self._store.mark_running(job.id)
        client = self._clients[job.mode]
        result: dict[str, Any] | None = None

        try:
            async for event in client.run(job.query, job.duration_seconds):
                await self._store.record(job.id, event.kind, event.message, event.data)
                if event.kind is JobEventKind.RESULT:
                    result = event.data
        except UpstreamError as exc:
            return await self._fail(job, str(exc))
        except Exception as exc:
            logger.exception("Job %s crashed", job.id)
            return await self._fail(job, f"Unexpected error: {exc!r}")

        if result is None:
            return await self._fail(job, "Upstream finished without a result")

        await self._store.mark_succeeded(job.id, result)
        return _Outcome(JobStatus.SUCCEEDED, result=result)

    async def _fail(self, job: Job, error: str) -> _Outcome:
        await self._store.record(job.id, JobEventKind.ERROR, error)
        await self._store.mark_failed(job.id, error)
        return _Outcome(JobStatus.FAILED, error=error)

    async def _deliver_callback(self, job: Job, url: str, outcome: _Outcome) -> None:
        payload = CallbackPayload(
            job_id=job.id,
            mode=job.mode,
            status=outcome.status,
            result=outcome.result,
            error=outcome.error,
        )
        try:
            status_code = await self._notifier.notify(url, payload)
        except httpx.HTTPError as exc:
            await self._store.record(
                job.id, JobEventKind.CALLBACK, f"Callback to {url} failed: {exc!r}"
            )
            return
        await self._store.record(
            job.id,
            JobEventKind.CALLBACK,
            f"Callback delivered to {url} (HTTP {status_code})",
            {"url": url, "status_code": status_code},
        )
