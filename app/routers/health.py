"""
Health endpoints.

- /health        — fast liveness probe used by Railway healthcheck. Always 200.
- /health/deep   — observability probe with DB connectivity, last webhook
                   timestamp, recent webhook count, and cache stats.
                   Returns 503 if anything looks broken so external monitors
                   (UptimeRobot, etc.) can alert.
"""
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.crud import count_events_since, get_last_event_received_at
from app.database import engine, webhook_engine
from app.dependencies import get_db
from app.services import cache as _cache

router = APIRouter(tags=["health"])


@router.get("/health")
def health_check() -> dict:
    """Fast liveness probe (used by Railway). Always 200."""
    return {"status": "ok"}


def _ping_engine(eng) -> dict:
    """Return {ok: bool, error: str|None} after a SELECT 1."""
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


@router.get("/health/deep")
def health_deep(db: Session = Depends(get_db)):
    """Detailed health: DB pools + recent webhook activity + cache stats.

    Status code:
      200 if everything is healthy
      503 if a DB pool is down
      200 with status="degraded" if webhook hasn't received in a while
    """
    main_db = _ping_engine(engine)
    webhook_db = _ping_engine(webhook_engine)

    last_event_at = None
    events_5m = None
    events_1h = None
    if main_db["ok"]:
        try:
            last_event_at = get_last_event_received_at(db)
            now_utc = datetime.now(timezone.utc)
            events_5m = count_events_since(db, since=now_utc - timedelta(minutes=5))
            events_1h = count_events_since(db, since=now_utc - timedelta(hours=1))
        except Exception:
            pass

    # Health logic:
    # - main DB down       → critical (503)
    # - webhook DB down    → critical (503) — can't persist new events
    # - no event in 1h during business hours → degraded (still 200)
    is_critical = (not main_db["ok"]) or (not webhook_db["ok"])
    is_business_hours = 6 <= datetime.now(timezone(timedelta(hours=-3))).hour <= 19
    is_degraded = bool(
        is_business_hours
        and last_event_at is not None
        and (datetime.now(timezone.utc) - last_event_at.astimezone(timezone.utc)).total_seconds() > 3600
    )

    if is_critical:
        status = "down"
    elif is_degraded:
        status = "degraded"
    else:
        status = "ok"

    payload = {
        "status": status,
        "checks": {
            "main_db": main_db,
            "webhook_db": webhook_db,
        },
        "webhook_activity": {
            "last_received_at": last_event_at.isoformat() if last_event_at else None,
            "events_last_5m": events_5m,
            "events_last_1h": events_1h,
        },
        "cache": _cache.stats(),
    }
    http_status = 503 if is_critical else 200
    return JSONResponse(payload, status_code=http_status)
