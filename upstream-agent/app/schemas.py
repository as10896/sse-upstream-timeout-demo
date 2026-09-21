from enum import StrEnum
from typing import ClassVar

from pydantic import BaseModel, Field


class ResponseMode(StrEnum):
    """How the caller wants to receive the result (mirrors Dify's `response_mode`)."""

    BLOCKING = "blocking"
    STREAMING = "streaming"


class RunRequest(BaseModel):
    """Body of `POST /v1/runs`."""

    query: str = Field(min_length=1, max_length=500)
    duration_seconds: float = Field(
        default=30.0,
        gt=0,
        le=600,
        description="Total simulated processing time.",
    )
    response_mode: ResponseMode = ResponseMode.STREAMING
    stream_progress: bool = Field(
        default=False,
        description=(
            "Send `step_progress` events during long steps, the way some APIs can stream "
            "reasoning summaries or workflow node events. A real caller can turn this on."
        ),
    )
    demo_keepalive: bool = Field(
        default=True,
        description=(
            "Demo-only: pretend to be an upstream that does (true) or doesn't (false) send "
            "`: ping` keep-alives. Real APIs don't let the caller choose this; it's part of "
            "how the provider built its stream."
        ),
    )


class RunResult(BaseModel):
    """The final answer, returned as JSON (blocking) or in the `run_finished` event."""

    run_id: str
    query: str
    answer: str
    steps: list[str]
    elapsed_seconds: float


class AgentEvent(BaseModel):
    """Base class for everything the agent emits while it runs."""

    event_name: ClassVar[str]


class RunStarted(AgentEvent):
    """Sent once, as soon as the run begins."""

    event_name: ClassVar[str] = "run_started"

    run_id: str
    total_steps: int


class StepStarted(AgentEvent):
    """Sent when the agent moves on to its next step."""

    event_name: ClassVar[str] = "step_started"

    run_id: str
    index: int
    total_steps: int
    name: str
    description: str
    expected_seconds: float


class StepProgress(AgentEvent):
    """Intermediate output during a long step (only when `stream_progress` is on)."""

    event_name: ClassVar[str] = "step_progress"

    run_id: str
    index: int
    message: str


class RunFinished(AgentEvent):
    """Sent once, carrying the final result."""

    event_name: ClassVar[str] = "run_finished"

    result: RunResult
