"""Server-Sent Events encoding with an explicit, optional heartbeat.

FastAPI's `EventSourceResponse` already sends a `: ping` every 15 seconds, and
that interval can't be changed or turned off. This demo needs both (to show
what happens *without* pings), so it builds the stream itself.
"""

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Mapping

from fastapi.sse import format_sse_event
from starlette.background import BackgroundTask
from starlette.responses import StreamingResponse

from app.schemas import AgentEvent

HEARTBEAT = b": ping\n\n"


class EventStreamResponse(StreamingResponse):
    """A `text/event-stream` response that proxies must not buffer or cache."""

    media_type = "text/event-stream"

    def __init__(
        self,
        content: AsyncIterable[bytes],
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        """Wrap `content` and add the headers that keep proxies from buffering it."""
        stream_headers = {
            "Cache-Control": "no-cache",
            # Tells nginx (and nginx-based gateways) to pass each chunk through right away.
            "X-Accel-Buffering": "no",
            **(headers or {}),
        }
        super().__init__(content, status_code, stream_headers, self.media_type, background)


async def encode_events(events: AsyncIterable[AgentEvent]) -> AsyncIterator[bytes]:
    """Turn agent events into SSE frames: `event: <name>` plus a JSON `data:` line."""
    async for event in events:
        yield format_sse_event(data_str=event.model_dump_json(), event=event.event_name)


async def with_heartbeat(
    frames: AsyncIterable[bytes], interval_seconds: float
) -> AsyncIterator[bytes]:
    """Pass `frames` through, and add a `: ping` comment whenever the source is quiet.

    The pending `__anext__()` call runs as a task that survives across timeouts,
    so a heartbeat never cancels the work the source is doing.
    """
    iterator = aiter(frames)
    pending: asyncio.Future[bytes] = asyncio.ensure_future(anext(iterator))
    try:
        while True:
            done, _ = await asyncio.wait({pending}, timeout=interval_seconds)
            if not done:
                yield HEARTBEAT
                continue
            try:
                frame = pending.result()
            except StopAsyncIteration:
                return
            yield frame
            pending = asyncio.ensure_future(anext(iterator))
    finally:
        pending.cancel()
