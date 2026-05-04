"""
Daily aggregation of webhook events into the `daily_agent_stats` table.

Used by the heavy /agentes endpoint to avoid recomputing past days on
every page render. Today's row is refreshed on demand (cheap because
today is at most a few thousand events). Past days are computed once
and reused.

Idempotent: running refresh_day() on an existing date overwrites with
the latest computation.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date as date_cls, datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy.orm import Session

from app import crud
from app.models import DailyAgentStat


def _real_phone(key: str) -> str:
    return key.split("##agent##")[0] if "##agent##" in key else key


def compute_stats_from_groups(
    *,
    groups: dict,
    phone_learned: dict,
    client_agent_map: dict,
    canal: str,
    extract_direction,
    extract_client_number=None,
) -> dict[str, dict]:
    """Compute per-agent stats from already-grouped events.

    Args:
        groups: client_phone -> list of events (output of _group_events)
        phone_learned: client_phone -> agent_name (learned from events)
        client_agent_map: client_phone -> agent_name (from gabarito CSV)
        canal: company channel number, used only for filtering company self
        extract_direction: callable(payload) -> "IN" / "OUT" / ""

    Returns:
        dict[agent_name] -> {msgs_out, msgs_in, clients_count, waiting_count}
    """
    stats: dict[str, dict] = defaultdict(
        lambda: {
            "msgs_out": 0,
            "msgs_in": 0,
            "clients": set(),
            "waiting": set(),
        }
    )

    canal_phone = _real_phone(canal)

    for client_num, evs in groups.items():
        ph = _real_phone(client_num)
        if not ph or ph == canal_phone:
            continue
        agent = (
            phone_learned.get(client_num)
            or client_agent_map.get(ph)
            or phone_learned.get(ph)
            or "Sem atendente"
        )
        if agent == "Sem atendente":
            continue

        # Sort events chronologically to determine "last direction"
        sorted_evs = sorted(
            evs, key=lambda e: e.received_at or datetime.min.replace(tzinfo=timezone.utc)
        )
        last_direction = ""
        for ev in sorted_evs:
            p = ev.raw_payload or {}
            direction = extract_direction(p)
            if direction == "OUT":
                stats[agent]["msgs_out"] += 1
                last_direction = "OUT"
            elif direction == "IN":
                stats[agent]["msgs_in"] += 1
                last_direction = "IN"

        stats[agent]["clients"].add(ph)
        if last_direction == "IN":
            stats[agent]["waiting"].add(ph)

    # Convert sets to counts for storage
    return {
        agent: {
            "msgs_out": s["msgs_out"],
            "msgs_in": s["msgs_in"],
            "clients_count": len(s["clients"]),
            "waiting_count": len(s["waiting"]),
        }
        for agent, s in stats.items()
    }


def refresh_day(
    db: Session,
    *,
    target_date: date_cls,
    canal: str,
    groups: dict,
    phone_learned: dict,
    client_agent_map: dict,
    extract_direction,
) -> int:
    """Recompute and upsert stats for `target_date` / `canal`.
    Returns the number of agent rows written."""
    agent_stats = compute_stats_from_groups(
        groups=groups,
        phone_learned=phone_learned,
        client_agent_map=client_agent_map,
        canal=canal,
        extract_direction=extract_direction,
    )

    for agent_name, s in agent_stats.items():
        crud.upsert_daily_stat(
            db,
            date=target_date,
            canal=canal,
            agent_name=agent_name,
            msgs_out=s["msgs_out"],
            msgs_in=s["msgs_in"],
            clients_count=s["clients_count"],
            waiting_count=s["waiting_count"],
        )

    db.commit()
    return len(agent_stats)


def needs_refresh(
    db: Session, *, target_date: date_cls, canal: str, max_age_seconds: int = 3600
) -> bool:
    """Return True if no row exists for the given (date, canal) OR if the
    most recent refresh is older than `max_age_seconds`. Past days are
    refreshed at most once unless explicitly invalidated.
    """
    row = (
        db.query(DailyAgentStat)
        .filter(DailyAgentStat.date == target_date, DailyAgentStat.canal == canal)
        .first()
    )
    if row is None:
        return True
    if target_date >= datetime.now(timezone.utc).date():
        # today — refresh if stale
        age = (datetime.now(timezone.utc) - row.refreshed_at.replace(tzinfo=timezone.utc)).total_seconds()
        return age > max_age_seconds
    return False
