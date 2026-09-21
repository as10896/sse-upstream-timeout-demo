import asyncio
import itertools
import time
import uuid
from collections.abc import AsyncIterator, Iterator, Sequence
from dataclasses import dataclass
from typing import Protocol

from app.schemas import AgentEvent, RunFinished, RunResult, RunStarted, StepProgress, StepStarted


class Agent(Protocol):
    """Anything that turns a query into a stream of agent events."""

    def run(
        self,
        query: str,
        duration_seconds: float,
        *,
        progress_interval_seconds: float | None = None,
    ) -> AsyncIterator[AgentEvent]:
        """Run the agent, yielding events as it goes and a `RunFinished` event last.

        With `progress_interval_seconds` set, long steps also report intermediate
        progress at that interval; with None, they run without any output.
        """
        ...


@dataclass(frozen=True, slots=True)
class StepSpec:
    """One step of the simulated agent and its share of the total run time."""

    name: str
    description: str
    weight: float


DEFAULT_PLAN: tuple[StepSpec, ...] = (
    StepSpec("plan", "Planning how to answer", 0.05),
    StepSpec("retrieve", "Retrieving context", 0.05),
    # The long step. 80% of the run happens here, silently unless progress
    # streaming is on, so it outlasts the edge proxy's idle timeout in both the
    # quick demo (0.8 * 30s = 24s > 15s) and the realistic setup (0.8 * 130s = 104s > 100s).
    StepSpec("reason", "Reasoning (long, like an LLM thinking)", 0.80),
    StepSpec("draft", "Drafting the final answer", 0.10),
)

# Stand-ins for the reasoning summaries a real model could stream.
PROGRESS_MESSAGES: tuple[str, ...] = (
    "Comparing the incident timelines",
    "Grouping incidents by root cause",
    "Checking which fixes are still open",
    "Estimating customer impact",
    "Looking for incidents that keep coming back",
    "Deciding what belongs in the summary",
)


class SimulatedAgent(Agent):
    """Stand-in for an AI agent: sleeps through a fixed plan of weighted steps."""

    def __init__(
        self,
        plan: Sequence[StepSpec] = DEFAULT_PLAN,
        progress_messages: Sequence[str] = PROGRESS_MESSAGES,
    ) -> None:
        """Create an agent that follows `plan` and reports `progress_messages` when asked."""
        self._plan = tuple(plan)
        self._total_weight = sum(step.weight for step in plan)
        self._progress_messages = tuple(progress_messages)

    async def run(
        self,
        query: str,
        duration_seconds: float,
        *,
        progress_interval_seconds: float | None = None,
    ) -> AsyncIterator[AgentEvent]:
        """Walk through the plan, spreading `duration_seconds` across the steps."""
        run_id = uuid.uuid4().hex[:12]
        started = time.monotonic()
        total = len(self._plan)
        messages = itertools.cycle(self._progress_messages)

        yield RunStarted(run_id=run_id, total_steps=total)

        for index, step in enumerate(self._plan, start=1):
            step_seconds = duration_seconds * step.weight / self._total_weight
            yield StepStarted(
                run_id=run_id,
                index=index,
                total_steps=total,
                name=step.name,
                description=step.description,
                expected_seconds=round(step_seconds, 2),
            )
            async for event in self._work(
                run_id, index, step_seconds, progress_interval_seconds, messages
            ):
                yield event

        yield RunFinished(
            result=RunResult(
                run_id=run_id,
                query=query,
                answer=f"Simulated answer to {query!r}, produced after a long run.",
                steps=[step.name for step in self._plan],
                elapsed_seconds=round(time.monotonic() - started, 2),
            )
        )

    async def _work(
        self,
        run_id: str,
        index: int,
        seconds: float,
        progress_interval: float | None,
        messages: Iterator[str],
    ) -> AsyncIterator[StepProgress]:
        """Spend `seconds` on one step, reporting progress every `progress_interval` if set."""
        remaining = seconds
        if progress_interval is not None:
            while remaining > progress_interval:
                await asyncio.sleep(progress_interval)
                remaining -= progress_interval
                yield StepProgress(run_id=run_id, index=index, message=next(messages))
        await asyncio.sleep(remaining)


async def collect_result(events: AsyncIterator[AgentEvent]) -> RunResult:
    """Drain an agent run and return its final result (used by blocking mode)."""
    async for event in events:
        if isinstance(event, RunFinished):
            return event.result
    raise RuntimeError("Agent finished without producing a result")
