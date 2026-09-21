import logging
from typing import Annotated

from fastapi import Depends, FastAPI

from app.agent import Agent, SimulatedAgent, collect_result
from app.config import Settings, get_settings
from app.schemas import ResponseMode, RunRequest, RunResult
from app.sse import EventStreamResponse, encode_events, with_heartbeat

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("upstream-agent")

app = FastAPI(
    title="Upstream Agent",
    summary="A slow, HTTP-only upstream that can answer in blocking or streaming mode.",
)


def get_agent() -> Agent:
    """Provide the agent implementation used by the endpoints."""
    return SimulatedAgent()


AgentDep = Annotated[Agent, Depends(get_agent)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


@app.post(
    "/v1/runs",
    response_model=None,
    responses={
        200: {
            "description": "JSON in blocking mode, an SSE stream in streaming mode.",
            "model": RunResult,
            "content": {"text/event-stream": {}},
        }
    },
)
async def create_run(
    request: RunRequest, agent: AgentDep, settings: SettingsDep
) -> RunResult | EventStreamResponse:
    """Run the agent and return its result, either all at once or as a stream.

    - `blocking`: nothing is sent until the run finishes. A gateway with an idle
      timeout shorter than the run will cut the connection first.
    - `streaming`: events are sent as they happen. Whether the connection survives
      the long silent step depends on whether bytes keep flowing: keep-alive pings
      (`demo_keepalive`, the provider's choice) or progress events
      (`stream_progress`, the caller's choice).
    """
    if request.response_mode is ResponseMode.BLOCKING:
        result = await collect_result(agent.run(request.query, request.duration_seconds))
        logger.info(
            "Blocking run %s finished after %.1fs; the caller may already have given up",
            result.run_id,
            result.elapsed_seconds,
        )
        return result

    events = agent.run(
        request.query,
        request.duration_seconds,
        progress_interval_seconds=(
            settings.progress_interval_seconds if request.stream_progress else None
        ),
    )
    frames = encode_events(events)
    if request.demo_keepalive:
        frames = with_heartbeat(frames, settings.heartbeat_interval_seconds)
    return EventStreamResponse(frames)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok"}
