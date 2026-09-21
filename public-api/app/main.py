import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import Settings, get_settings
from app.inbox import WebhookInbox
from app.models import JobMode
from app.notifier import WebhookNotifier
from app.routes import jobs, pages, webhooks
from app.runner import JobRunner
from app.store import InMemoryJobStore
from app.upstream import BlockingUpstreamClient, StreamingUpstreamClient, UpstreamClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

STATIC_DIR = Path(__file__).resolve().parent / "static"


def build_upstream_clients(http: httpx.AsyncClient) -> dict[JobMode, UpstreamClient]:
    """Map each job mode to the client that implements it."""
    return {
        JobMode.BLOCKING: BlockingUpstreamClient(http),
        JobMode.STREAMING_KEEPALIVE: StreamingUpstreamClient(http, upstream_keepalive=True),
        JobMode.STREAMING_NO_KEEPALIVE: StreamingUpstreamClient(http, upstream_keepalive=False),
        JobMode.STREAMING_PROGRESS: StreamingUpstreamClient(
            http, upstream_keepalive=False, stream_progress=True
        ),
    }


def upstream_timeout(settings: Settings) -> httpx.Timeout:
    """Build our own client's timeouts: short to connect, very long to read.

    With a read timeout this long, any cut-off comes from the gateway in between.
    """
    return httpx.Timeout(
        connect=5.0, read=settings.upstream_read_timeout_seconds, write=10.0, pool=5.0
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Create shared HTTP clients and services, and clean them up on shutdown."""
    settings = get_settings()
    async with (
        httpx.AsyncClient(
            base_url=settings.upstream_base_url, timeout=upstream_timeout(settings)
        ) as upstream_http,
        httpx.AsyncClient(timeout=settings.webhook_timeout_seconds) as webhook_http,
    ):
        store = InMemoryJobStore()
        runner = JobRunner(
            store=store,
            clients=build_upstream_clients(upstream_http),
            notifier=WebhookNotifier(webhook_http),
        )
        app.state.store = store
        app.state.runner = runner
        app.state.inbox = WebhookInbox()
        yield
        await runner.shutdown()


app = FastAPI(
    title="Public API",
    summary="Accepts jobs with 202 and calls a slow HTTP-only upstream in the background.",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
app.include_router(pages.router)
app.include_router(jobs.router)
app.include_router(webhooks.router)


@app.get("/healthz", tags=["ops"])
async def healthz() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok"}
