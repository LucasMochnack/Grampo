import hmac
from typing import Generator

from fastapi import Depends, Header, HTTPException
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal

_SENSITIVE_HEADER_PREFIXES = (
    "authorization",
    "x-zenvia-token",
    "x-hub-signature",
)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def sanitize_headers(headers: dict[str, str]) -> dict[str, str]:
    """Redact authentication / signature headers before storing."""
    return {
        k: "[REDACTED]"
        if any(k.lower().startswith(p) for p in _SENSITIVE_HEADER_PREFIXES)
        else v
        for k, v in headers.items()
    }


def verify_webhook_token(
    x_zenvia_token: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> None:
    """
    Validates the webhook bearer token sent by Zenvia.

    When WEBHOOK_SECRET_TOKEN is empty (default), validation is disabled —
    this is intentional for local development.  Set the env var in production.
    """
    expected = settings.WEBHOOK_SECRET_TOKEN
    if not expected:
        return

    received = x_zenvia_token or authorization
    if not received:
        raise HTTPException(status_code=401, detail="Missing webhook authentication token")

    # Strip optional "Bearer " prefix
    if received.startswith("Bearer "):
        received = received[7:]

    if not hmac.compare_digest(expected.encode(), received.encode()):
        raise HTTPException(status_code=403, detail="Invalid webhook token")
