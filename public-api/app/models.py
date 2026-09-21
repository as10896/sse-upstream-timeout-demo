import time
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import AnyHttpUrl, BaseModel, Field, PrivateAttr


class JobMode(StrEnum):
    """How the public API talks to the upstream for a given job."""

    BLOCKING = "blocking"
    STREAMING_KEEPALIVE = "streaming_keepalive"
    """Streaming from an upstream that sends keep-alive pings."""
    STREAMING_NO_KEEPALIVE = "streaming_no_keepalive"
    """Streaming from an upstream that sends nothing while it's busy."""
    STREAMING_PROGRESS = "streaming_progress"
    """Same upstream without keep-alive, but the caller turns on progress events."""


class JobStatus(StrEnum):
    """Lifecycle of a job."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class JobEventKind(StrEnum):
    """Categories of entries in a job's timeline."""

    STATUS = "status"
    INFO = "info"
    STEP = "step"
    HEARTBEAT = "heartbeat"
    PROGRESS = "progress"
    RESULT = "result"
    ERROR = "error"
    CALLBACK = "callback"


class JobEvent(BaseModel):
    """One entry in a job's timeline."""

    seq: int
    elapsed_seconds: float
    kind: JobEventKind
    message: str
    data: dict[str, Any] | None = None


class Job(BaseModel):
    """A request the public API accepted and is working on in the background."""

    id: str
    mode: JobMode
    query: str
    duration_seconds: float
    callback_url: str | None
    status: JobStatus = JobStatus.QUEUED
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    result: dict[str, Any] | None = None
    error: str | None = None
    closed: bool = Field(
        default=False,
        description="True once the job is finished and its callback (if any) has been sent.",
    )
    events: list[JobEvent] = Field(default_factory=list)

    _started_at: float = PrivateAttr(default_factory=time.monotonic)

    def seconds_since_start(self) -> float:
        """Seconds since the job was accepted."""
        return round(time.monotonic() - self._started_at, 2)


class CreateJobRequest(BaseModel):
    """Body of `POST /jobs`."""

    mode: JobMode
    query: str = Field(default="Summarise this quarter's incidents", min_length=1, max_length=500)
    duration_seconds: float = Field(default=30.0, gt=0, le=600)
    callback_url: AnyHttpUrl | None = Field(
        default=None, description="If set, the final result is POSTed here."
    )


class JobAccepted(BaseModel):
    """Response to `POST /jobs`: sent immediately with HTTP 202."""

    job_id: str
    status: JobStatus
    status_url: str
    events_url: str


class CallbackPayload(BaseModel):
    """What the public API POSTs to a job's `callback_url` when it finishes."""

    job_id: str
    mode: JobMode
    status: JobStatus
    result: dict[str, Any] | None = None
    error: str | None = None


class WebhookDelivery(BaseModel):
    """A callback received by the built-in webhook sink."""

    received_at: datetime
    payload: dict[str, Any]
