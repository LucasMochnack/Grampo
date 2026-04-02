from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class WebhookReceived(BaseModel):
    status: str
    event_id: UUID


class EventDetail(BaseModel):
    id: UUID
    received_at: datetime
    raw_payload: Any
    raw_headers: Any
    zenvia_event_id: str | None
    zenvia_event_type: str | None
    zenvia_channel: str | None
    zenvia_timestamp: str | None
    source_ip: str | None
    user_agent: str | None
    content_type: str | None

    model_config = {"from_attributes": True}


class EventListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[EventDetail]
