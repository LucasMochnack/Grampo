"""
Bulletproof Zenvia webhook handler.

Goals:
- ALWAYS return 200 to Zenvia. Never let an exception propagate (Zenvia
  disables webhooks after repeated 5xx responses).
- Use a dedicated DB pool so webhook writes don't compete with dashboard
  reads for connections.
- If the DB write fails for ANY reason, log the full payload to stdout
  (Railway captures it) so the event can be replayed manually later.
"""
import json
import logging
import traceback
from typing import Any

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app import crud
from app.database import WebhookSessionLocal
from app.dependencies import sanitize_headers, verify_webhook_token
from fastapi import Depends

router = APIRouter(tags=["webhook"])
logger = logging.getLogger("webhook")


def _persist_event(payload: dict, request: Request) -> str | None:
    """Try to persist the event using the dedicated webhook session.
    Returns the new event_id on success, or None on failure (already logged)."""
    db: Session | None = None
    try:
        db = WebhookSessionLocal()
        event = crud.create_event(
            db=db,
            raw_payload=payload,
            raw_headers=sanitize_headers(dict(request.headers)),
            source_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            content_type=request.headers.get("content-type"),
        )
        return str(event.id)
    except Exception:
        # Log the full payload so the event is recoverable from Railway logs.
        # Never re-raise — Zenvia must always get a 200.
        try:
            logger.error(
                "WEBHOOK_DB_FAIL — payload could not be persisted, "
                "logging here for replay: %s",
                json.dumps(payload, ensure_ascii=False)[:8000],
            )
            logger.error("WEBHOOK_DB_FAIL traceback: %s", traceback.format_exc())
        except Exception:
            pass
        try:
            if db is not None:
                db.rollback()
        except Exception:
            pass
        return None
    finally:
        if db is not None:
            try:
                db.close()
            except Exception:
                pass


@router.post("/webhook/zenvia", status_code=200)
def receive_webhook(
    request: Request,
    payload: dict[str, Any] = Body(...),
    _: None = Depends(verify_webhook_token),
):
    """
    Receives a Zenvia webhook event and acknowledges immediately.

    The handler is resilient by design: any failure during persistence is
    swallowed (logged to stdout) and a 200 response is always returned so
    Zenvia never disables the webhook subscription.
    """
    event_id = _persist_event(payload, request)
    if event_id:
        return JSONResponse({"status": "received", "event_id": event_id}, status_code=200)
    # Fallback: persistence failed but we already logged the payload.
    # Acknowledge anyway so Zenvia doesn't retry / disable the webhook.
    return JSONResponse({"status": "accepted"}, status_code=200)
