"""Clients for the upstream agent, one per way of calling it.

Each client turns a single upstream call into `UpstreamEvent`s, so the job
runner can treat every mode the same way.
"""

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from app.models import JobEventKind
from app.sse_parser import SSEMessage, iter_sse

RUNS_PATH = "/v1/runs"


class UpstreamError(Exception):
    """The upstream call ended without a final result."""


@dataclass(frozen=True, slots=True)
class UpstreamEvent:
    """Something that happened while talking to the upstream."""

    kind: JobEventKind
    message: str
    data: dict[str, Any] | None = None


class UpstreamClient(Protocol):
    """One way of calling the upstream agent."""

    def run(self, query: str, duration_seconds: float) -> AsyncIterator[UpstreamEvent]:
        """Call the upstream and yield what happens, ending with a `RESULT` event.

        Raises `UpstreamError` if no result arrives.
        """
        ...


class BlockingUpstreamClient(UpstreamClient):
    """Classic request/response: send the request, wait for the whole JSON body."""

    def __init__(self, http: httpx.AsyncClient) -> None:
        """Use `http` (already set up with base URL and timeouts) for requests."""
        self._http = http

    async def run(self, query: str, duration_seconds: float) -> AsyncIterator[UpstreamEvent]:
        """Send one blocking request. Nothing arrives until the upstream is completely done."""
        yield UpstreamEvent(
            JobEventKind.INFO,
            f"POST {RUNS_PATH} (response_mode=blocking), waiting for the full response",
        )
        try:
            response = await self._http.post(
                RUNS_PATH,
                json={
                    "query": query,
                    "duration_seconds": duration_seconds,
                    "response_mode": "blocking",
                },
            )
        except httpx.HTTPError as exc:
            raise UpstreamError(f"Request failed: {exc!r}") from exc

        if response.status_code != httpx.codes.OK:
            raise UpstreamError(
                f"Upstream answered HTTP {response.status_code}: {response.text.strip()[:200]}"
            )
        yield UpstreamEvent(JobEventKind.RESULT, "Received the final answer", response.json())


class StreamingUpstreamClient(UpstreamClient):
    """Streaming call: read the SSE body as it arrives, pings included."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        upstream_keepalive: bool = True,
        stream_progress: bool = False,
    ) -> None:
        """Set up a streaming client.

        Args:
            http: Client already set up with base URL and timeouts.
            upstream_keepalive: Which kind of upstream to simulate: one that sends
                keep-alive pings or one that doesn't. With a real provider this isn't
                a caller setting; it's how the provider built its stream.
            stream_progress: Ask the upstream for intermediate progress events. This
                *is* a caller setting, like turning on streamed reasoning summaries.

        """
        self._http = http
        self._upstream_keepalive = upstream_keepalive
        self._stream_progress = stream_progress

    async def run(self, query: str, duration_seconds: float) -> AsyncIterator[UpstreamEvent]:
        """Open the stream and pass on each event until `run_finished` arrives."""
        yield UpstreamEvent(
            JobEventKind.INFO,
            f"POST {RUNS_PATH} (response_mode=streaming, stream_progress="
            f"{str(self._stream_progress).lower()}); simulated upstream "
            f"{'sends' if self._upstream_keepalive else 'does not send'} keep-alives",
        )
        payload = {
            "query": query,
            "duration_seconds": duration_seconds,
            "response_mode": "streaming",
            "stream_progress": self._stream_progress,
            "demo_keepalive": self._upstream_keepalive,
        }
        try:
            async with self._http.stream(
                "POST", RUNS_PATH, json=payload, headers={"Accept": "text/event-stream"}
            ) as response:
                if response.status_code != httpx.codes.OK:
                    body = (await response.aread()).decode(errors="replace").strip()
                    raise UpstreamError(
                        f"Upstream answered HTTP {response.status_code}: {body[:200]}"
                    )
                yield UpstreamEvent(JobEventKind.INFO, "Response headers received, stream is open")
                async for message in iter_sse(response.aiter_lines()):
                    event = _translate(message)
                    yield event
                    if event.kind is JobEventKind.RESULT:
                        return
        except httpx.RemoteProtocolError as exc:
            raise UpstreamError(
                "Connection dropped mid-stream: the gateway closed it after it sat idle "
                f"for too long ({exc})"
            ) from exc
        except httpx.HTTPError as exc:
            raise UpstreamError(f"Request failed: {exc!r}") from exc

        raise UpstreamError("Stream ended without a run_finished event")


def _translate(message: SSEMessage) -> UpstreamEvent:
    """Convert a raw upstream SSE message into an `UpstreamEvent`."""
    if message.is_comment:
        return UpstreamEvent(JobEventKind.HEARTBEAT, f"Heartbeat ({message.data or 'comment'})")

    data: dict[str, Any] = json.loads(message.data) if message.data else {}
    match message.event:
        case "run_started":
            return UpstreamEvent(JobEventKind.STEP, f"Run {data.get('run_id')} started", data)
        case "step_started":
            return UpstreamEvent(
                JobEventKind.STEP,
                f"Step {data['index']}/{data['total_steps']}: {data['description']} "
                f"(~{data['expected_seconds']}s)",
                data,
            )
        case "step_progress":
            return UpstreamEvent(JobEventKind.PROGRESS, data["message"], data)
        case "run_finished":
            return UpstreamEvent(JobEventKind.RESULT, "Received the final answer", data["result"])
        case other:
            return UpstreamEvent(JobEventKind.INFO, f"Unrecognised event {other!r}", data)
