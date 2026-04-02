from typing import Any

from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db, sanitize_headers, verify_webhook_token
from app.schemas import WebhookReceived

router = APIRouter(tags=["webhook"])


@router.post("/webhook/zenvia", response_model=WebhookReceived, status_code=200)
def receive_webhook(
    request: Request,
    payload: dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    _: None = Depends(verify_webhook_token),
) -> WebhookReceived:
    """
    Receives a Zenvia webhook event, stores it verbatim, and acknowledges immediately.

    The raw payload is stored without transformation.  Top-level fields
    (`id`, `type`, `channel`, `timestamp`) are extracted when present for
    easier querying, but their absence is never an error.
    """
    event = crud.create_event(
        db=db,
        raw_payload=payload,
        raw_headers=sanitize_headers(dict(request.headers)),
        source_ip=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
        content_type=request.headers.get("content-type"),
    )
    return WebhookReceived(status="received", event_id=event.id)
