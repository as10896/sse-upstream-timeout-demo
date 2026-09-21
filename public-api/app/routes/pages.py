from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.dependencies import SettingsDep
from app.models import JobMode

TEMPLATES = Jinja2Templates(directory=Path(__file__).resolve().parent.parent / "templates")

router = APIRouter(tags=["pages"], include_in_schema=False)

SCENARIOS: tuple[dict[str, str], ...] = (
    {
        "mode": JobMode.BLOCKING,
        "title": "Blocking call",
        "summary": "One request, then nothing until the whole run finishes.",
        "control": "Caller's choice: response_mode=blocking",
    },
    {
        "mode": JobMode.STREAMING_KEEPALIVE,
        "title": "Streaming, upstream sends keep-alives",
        "summary": (
            "The upstream pings during quiet stretches "
            "(Dify Cloud, for example, pings about every 10 s)."
        ),
        "control": "Up to the provider: pings are built into its stream",
    },
    {
        "mode": JobMode.STREAMING_NO_KEEPALIVE,
        "title": "Streaming, upstream sends no keep-alives",
        "summary": (
            "Same stream, but the upstream says nothing during the long step. "
            "Nothing the caller sends can reset the gateway's timer."
        ),
        "control": "Up to the provider: no pings, and the caller can't add any",
    },
    {
        "mode": JobMode.STREAMING_PROGRESS,
        "title": "No keep-alives, progress events turned on",
        "summary": (
            "Same upstream without pings, but the caller asks for intermediate progress "
            "(like streamed reasoning summaries), so data keeps flowing."
        ),
        "control": "Caller's choice: stream_progress=true",
    },
)


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, settings: SettingsDep) -> HTMLResponse:
    """Render the demo page."""
    return TEMPLATES.TemplateResponse(
        request,
        "index.html",
        {
            "scenarios": SCENARIOS,
            "edge_timeout": settings.edge_read_timeout_seconds,
            "heartbeat_interval": settings.heartbeat_interval_seconds,
            "progress_interval": settings.progress_interval_seconds,
            "default_duration": settings.default_duration_seconds,
            "default_webhook_url": settings.default_webhook_url,
        },
    )
