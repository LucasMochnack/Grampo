import uuid as uuid_module
from datetime import datetime, timezone

from sqlalchemy import Column, Date, DateTime, Integer, JSON, String, Text, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.types import CHAR, TypeDecorator

from app.database import Base


class _GUID(TypeDecorator):
    """
    Platform-independent UUID column.
    PostgreSQL: native UUID type.
    SQLite / others: CHAR(36) string representation.
    """

    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID())
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        return uuid_module.UUID(str(value))


class WebhookEvent(Base):
    __tablename__ = "webhook_events"

    id = Column(_GUID, primary_key=True, default=uuid_module.uuid4)

    received_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )

    # --- Source of truth: verbatim request body ---
    raw_payload = Column(JSON, nullable=False)

    # --- Sanitized HTTP headers (auth values redacted) ---
    raw_headers = Column(JSON, nullable=True)

    # --- Top-level fields extracted when present in the payload ---
    # These are NEVER invented; all nullable.
    zenvia_event_id = Column(String(255), nullable=True, index=True)
    zenvia_event_type = Column(String(100), nullable=True, index=True)
    zenvia_channel = Column(String(100), nullable=True)
    zenvia_timestamp = Column(String(64), nullable=True)  # kept as string, never parsed

    # --- Request metadata ---
    source_ip = Column(String(64), nullable=True)
    user_agent = Column(Text, nullable=True)
    content_type = Column(String(255), nullable=True)


class AgentMapping(Base):
    __tablename__ = "agent_mappings"

    phone = Column(String(32), primary_key=True)
    agent_name = Column(String(255), nullable=False)
    client_name = Column(String(255), nullable=True)


class AppSetting(Base):
    __tablename__ = "app_settings"

    key = Column(String(100), primary_key=True)
    value = Column(Text, nullable=True)


class DailyAgentStat(Base):
    """Pre-aggregated per-day, per-canal, per-agent stats.

    Populated by app.services.agent_stats.refresh_day(). Read by the heavy
    /agentes endpoint instead of recomputing from webhook_events for past
    days. Today's row is refreshed on every render; past days are computed
    once and reused.
    """
    __tablename__ = "daily_agent_stats"

    date = Column(Date, primary_key=True)
    canal = Column(String(32), primary_key=True)
    agent_name = Column(String(255), primary_key=True)

    msgs_out = Column(Integer, nullable=False, default=0)
    msgs_in = Column(Integer, nullable=False, default=0)
    clients_count = Column(Integer, nullable=False, default=0)
    waiting_count = Column(Integer, nullable=False, default=0)

    # Last refresh — used to decide if we need to recompute
    refreshed_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_daily_agent_stats_date_canal", "date", "canal"),
    )


