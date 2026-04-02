from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from app.models import WebhookEvent


def create_event(
    db: Session,
    *,
    raw_payload: dict[str, Any],
    raw_headers: dict[str, str] | None,
    source_ip: str | None,
    user_agent: str | None,
    content_type: str | None,
) -> WebhookEvent:
    event = WebhookEvent(
        raw_payload=raw_payload,
        raw_headers=raw_headers,
        source_ip=source_ip,
        user_agent=user_agent,
        content_type=content_type,
        # Extract top-level Zenvia fields only when present; never default.
        zenvia_event_id=raw_payload.get("id"),
        zenvia_event_type=raw_payload.get("type"),
        zenvia_channel=raw_payload.get("channel"),
        zenvia_timestamp=raw_payload.get("timestamp"),
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event


def get_events(
    db: Session, *, limit: int, offset: int
) -> tuple[list[WebhookEvent], int]:
    query = db.query(WebhookEvent).order_by(WebhookEvent.received_at.desc())
    total = query.count()
    items = query.offset(offset).limit(limit).all()
    return items, total


def get_event(db: Session, event_id: UUID) -> WebhookEvent | None:
    return db.query(WebhookEvent).filter(WebhookEvent.id == str(event_id)).first()
