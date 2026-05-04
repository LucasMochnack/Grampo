from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from app.models import AgentMapping, AppSetting, WebhookEvent


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


def get_events_only(
    db: Session, *, limit: int
) -> list[WebhookEvent]:
    """Fetch events without the extra COUNT(*) query. Use when total is not needed."""
    return (
        db.query(WebhookEvent)
        .order_by(WebhookEvent.received_at.desc())
        .limit(limit)
        .all()
    )


def get_events_since(
    db: Session, *, since, limit: int = 20000
) -> list[WebhookEvent]:
    """Fetch events received on or after `since` (datetime). Filters at SQL level."""
    return (
        db.query(WebhookEvent)
        .filter(WebhookEvent.received_at >= since)
        .order_by(WebhookEvent.received_at.desc())
        .limit(limit)
        .all()
    )


def get_event(db: Session, event_id: UUID) -> WebhookEvent | None:
    return db.query(WebhookEvent).filter(WebhookEvent.id == str(event_id)).first()


# --- Agent Mappings ---

def get_agent_mappings(db: Session) -> dict[str, str]:
    rows = db.query(AgentMapping).all()
    return {r.phone: r.agent_name for r in rows}


def get_client_names(db: Session) -> dict[str, str]:
    rows = db.query(AgentMapping).all()
    return {r.phone: r.client_name for r in rows if r.client_name}


def replace_agent_mappings(db: Session, mappings: dict[str, dict[str, str]]) -> int:
    """Merge new mappings: update existing phones, add new ones, keep old ones intact."""
    updated = 0
    added = 0
    for phone, data in mappings.items():
        existing = db.query(AgentMapping).filter(AgentMapping.phone == phone).first()
        if existing:
            existing.agent_name = data["agent_name"]
            if data.get("client_name"):
                existing.client_name = data["client_name"]
            updated += 1
        else:
            db.add(AgentMapping(
                phone=phone,
                agent_name=data["agent_name"],
                client_name=data.get("client_name", ""),
            ))
            added += 1
    db.commit()
    return updated + added


# --- App Settings ---

def get_setting(db: Session, key: str) -> str | None:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    return row.value if row else None


def set_setting(db: Session, key: str, value: str) -> None:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    if row:
        row.value = value
    else:
        db.add(AppSetting(key=key, value=value))
    db.commit()


