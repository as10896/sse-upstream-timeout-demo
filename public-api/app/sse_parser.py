"""A small Server-Sent Events parser that also reports comment lines.

Libraries such as `httpx-sse` drop comments, but here the `: ping` heartbeats
are the point of the demo, so we want to see them.
"""

from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class SSEMessage:
    """A dispatched event (`is_comment=False`) or a single comment line (`is_comment=True`)."""

    event: str = "message"
    data: str = ""
    id: str | None = None
    is_comment: bool = False


@dataclass(slots=True)
class _Pending:
    event: str = ""
    data: list[str] = field(default_factory=list)
    id: str | None = None

    def is_empty(self) -> bool:
        return not self.data and not self.event

    def build(self) -> SSEMessage:
        return SSEMessage(event=self.event or "message", data="\n".join(self.data), id=self.id)


async def iter_sse(lines: AsyncIterable[str]) -> AsyncIterator[SSEMessage]:
    """Parse lines of a `text/event-stream` body into messages.

    See https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
    """
    pending = _Pending()
    async for raw_line in lines:
        line = raw_line.rstrip("\r\n")

        if not line:
            if not pending.is_empty():
                yield pending.build()
            pending = _Pending()
            continue

        if line.startswith(":"):
            yield SSEMessage(data=line[1:].strip(), is_comment=True)
            continue

        name, _, value = line.partition(":")
        value = value.removeprefix(" ")
        match name:
            case "event":
                pending.event = value
            case "data":
                pending.data.append(value)
            case "id":
                pending.id = value
            case _:
                pass  # `retry` and unknown fields don't matter here.

    # Per the spec, an event not terminated by a blank line is incomplete and dropped.
