from collections import deque
from datetime import UTC, datetime
from typing import Any

from app.models import WebhookDelivery


class WebhookInbox:
    """Keeps the most recent callbacks received by the demo webhook sink."""

    def __init__(self, capacity: int = 50) -> None:
        """Keep at most `capacity` deliveries; older ones are dropped."""
        self._deliveries: deque[WebhookDelivery] = deque(maxlen=capacity)

    def add(self, payload: dict[str, Any]) -> WebhookDelivery:
        """Store a received payload."""
        delivery = WebhookDelivery(received_at=datetime.now(UTC), payload=payload)
        self._deliveries.appendleft(delivery)
        return delivery

    def list(self) -> list[WebhookDelivery]:
        """Return deliveries, newest first."""
        return list(self._deliveries)
