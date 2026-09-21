from typing import Any

from fastapi import APIRouter, status

from app.dependencies import InboxDep
from app.models import WebhookDelivery

router = APIRouter(prefix="/webhook-sink", tags=["webhook sink"])


@router.post("", status_code=status.HTTP_204_NO_CONTENT)
async def receive_webhook(payload: dict[str, Any], inbox: InboxDep) -> None:
    """Stand-in for the caller's webhook endpoint: store whatever is POSTed."""
    inbox.add(payload)


@router.get("")
async def list_webhooks(inbox: InboxDep) -> list[WebhookDelivery]:
    """List the callbacks received so far, newest first."""
    return inbox.list()
