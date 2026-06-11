"""
Weekly auto-score scheduler.

Runs INSIDE the app process (no external cron, no browser): an asyncio loop
started from the FastAPI lifespan checks every few minutes whether the weekly
window was reached and, if so, runs the score scan server-side in a worker
thread.

Schedule: every Sunday at AUTO_SCORE_HOUR (default 22h, Brasília). If the app
is down/restarting at that moment, the run is recovered until Monday 06:59 —
after that, it waits for the next Sunday. The last completed run is persisted
in the settings table ("auto_score_last_run" = the Sunday's ISO date), so
restarts never double-run and every replica/deploy agrees on what already ran.

Disable with AUTO_SCORE_ENABLED=0 (env, no deploy needed).
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.config import settings

logger = logging.getLogger("auto_score")

BRASILIA = ZoneInfo("America/Sao_Paulo")

_CHECK_EVERY_S = 600          # loop granularity (10 min)
_LAST_RUN_KEY = "auto_score_last_run"
_LAST_RESULT_KEY = "auto_score_last_result"


def _due_sunday(now_br: datetime) -> str | None:
    """Return the ISO date of the Sunday whose run is due NOW, else None.

    Window: Sunday >= AUTO_SCORE_HOUR  →  Monday < 07:00 (late recovery).
    """
    hour = settings.AUTO_SCORE_HOUR
    if now_br.weekday() == 6 and now_br.hour >= hour:        # Sunday night
        return now_br.date().isoformat()
    if now_br.weekday() == 0 and now_br.hour < 7:            # Monday early
        return (now_br.date() - timedelta(days=1)).isoformat()
    return None


def _run_job(due_key: str) -> None:
    """Blocking: run the scan and persist the marker. Runs in a worker thread."""
    from app.database import SessionLocal
    from app.crud import get_setting, set_setting
    from app.routers.dashboard import run_score_scan

    db = SessionLocal()
    try:
        # Re-check the marker inside the job (cheap double-run guard).
        if get_setting(db, _LAST_RUN_KEY) == due_key:
            return
        logger.info("auto-score: starting weekly scan (due %s)", due_key)
        result = run_score_scan(
            db, canal=settings.AUTO_SCORE_CANAL, days=settings.AUTO_SCORE_DAYS
        )
        result["ran_at"] = datetime.now(timezone.utc).isoformat()
        result["due"] = due_key
        # Persist the report either way (visible no Diagnóstico), but only mark
        # the week as done on SUCCESS — a failed run is retried on the next
        # tick while the window (domingo 22h → segunda 06:59) is still open.
        if result.get("ok"):
            set_setting(db, _LAST_RUN_KEY, due_key)
        set_setting(db, _LAST_RESULT_KEY, json.dumps(result, ensure_ascii=False))
        logger.info("auto-score: done — %s", result)
    except Exception:
        logger.exception("auto-score: weekly scan failed")
        db.rollback()
    finally:
        db.close()


async def weekly_score_loop() -> None:
    """Background task: wake up every few minutes; run when the window opens."""
    if str(settings.AUTO_SCORE_ENABLED).strip().lower() in ("0", "false", "no", ""):
        logger.info("auto-score: disabled via AUTO_SCORE_ENABLED")
        return
    logger.info(
        "auto-score: scheduler armed (domingo %dh Brasília, janela %dd, canal %s)",
        settings.AUTO_SCORE_HOUR, settings.AUTO_SCORE_DAYS, settings.AUTO_SCORE_CANAL,
    )
    while True:
        try:
            due = _due_sunday(datetime.now(BRASILIA))
            if due:
                # Quick marker check without holding a session across the sleep.
                from app.database import SessionLocal
                from app.crud import get_setting
                db = SessionLocal()
                try:
                    already = get_setting(db, _LAST_RUN_KEY) == due
                finally:
                    db.close()
                if not already:
                    await asyncio.to_thread(_run_job, due)
        except Exception:
            logger.exception("auto-score: scheduler tick failed")
        await asyncio.sleep(_CHECK_EVERY_S)
