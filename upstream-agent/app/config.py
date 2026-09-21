from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Runtime configuration, read from environment variables."""

    heartbeat_interval_seconds: float = Field(
        default=5.0,
        gt=0,
        description="How often a `: ping` comment is sent while the agent is silent.",
    )
    progress_interval_seconds: float = Field(
        default=4.0,
        gt=0,
        description="How often a `step_progress` event is sent when progress streaming is on.",
    )


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings instance."""
    return Settings()
