from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Runtime configuration, read from environment variables."""

    upstream_base_url: str = Field(
        default="http://edge-proxy:8080",
        description="Where the upstream is reached. In this demo it's the edge proxy.",
    )
    upstream_read_timeout_seconds: float = Field(
        default=600.0,
        gt=0,
        description=(
            "Our own HTTP client's read timeout. It's deliberately generous, so any "
            "cut-off you see comes from the gateway in between, not from this client."
        ),
    )
    webhook_timeout_seconds: float = Field(default=10.0, gt=0)
    default_webhook_url: str = "http://public-api:8000/webhook-sink"
    default_duration_seconds: float = Field(default=30.0, gt=0)

    # Only displayed on the demo page; they're enforced by other containers.
    edge_read_timeout_seconds: float = 15.0
    heartbeat_interval_seconds: float = 5.0
    progress_interval_seconds: float = 4.0


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings instance."""
    return Settings()
