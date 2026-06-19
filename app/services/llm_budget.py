"""
Hard daily cap on Anthropic API calls (cost safeguard).

A single global counter per Brasília day, persisted in the settings table,
shared across every AI feature (scoring, opportunities, sem-resposta,
suggestion). Each feature calls ``try_consume(db)`` right before an LLM call;
once the day's count reaches ``LLM_DAILY_CAP`` it returns False and the feature
skips the call (degrading to cache/keyword) until the next day.

Set ``LLM_DAILY_CAP=0`` to disable the cap entirely.
"""
from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from app.config import settings
from app.crud import get_setting, set_setting

BRASILIA = ZoneInfo("America/Sao_Paulo")
_KEY = "llm_budget"


def _cap() -> int:
    try:
        return max(0, int(settings.LLM_DAILY_CAP))
    except (TypeError, ValueError):
        return 1500


def _today() -> str:
    return datetime.now(BRASILIA).strftime("%Y-%m-%d")


def _load(db) -> dict:
    try:
        data = json.loads(get_setting(db, _KEY) or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def usage_today(db) -> tuple[int, int]:
    """Return (chamadas_hoje, teto). teto 0 = sem limite."""
    data = _load(db)
    cnt = int(data.get("count", 0)) if data.get("day") == _today() else 0
    return cnt, _cap()


def try_consume(db, n: int = 1) -> bool:
    """Reserva n chamadas contra o teto de hoje. False se estourar o teto
    (a feature deve então pular a chamada à IA)."""
    cap = _cap()
    if cap <= 0:
        return True   # sem limite
    today = _today()
    data = _load(db)
    cnt = int(data.get("count", 0)) if data.get("day") == today else 0
    if cnt + n > cap:
        return False
    try:
        set_setting(db, _KEY, json.dumps({"day": today, "count": cnt + n}))
    except Exception:
        return True   # nunca bloquear por falha de persistência do contador
    return True
