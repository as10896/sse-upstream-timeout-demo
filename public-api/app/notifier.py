from typing import Protocol

import httpx

from app.models import CallbackPayload


class ResultNotifier(Protocol):
    """Tells the original caller that their job is finished."""

    async def notify(self, url: str, payload: CallbackPayload) -> int:
        """Deliver `payload` to `url` and return the receiver's HTTP status code."""
        ...


class WebhookNotifier(ResultNotifier):
    """Delivers results by POSTing JSON to the caller's callback URL."""

    def __init__(self, http: httpx.AsyncClient) -> None:
        """Use `http` (already set up with timeouts) for deliveries."""
        self._http = http

    async def notify(self, url: str, payload: CallbackPayload) -> int:
        """POST the payload; raise `httpx.HTTPError` on network errors or non-2xx responses."""
        response = await self._http.post(url, json=payload.model_dump(mode="json"))
        response.raise_for_status()
        return response.status_code
