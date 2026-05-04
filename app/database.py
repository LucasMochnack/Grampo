from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from app.config import settings


def _build_engine(*, pool_size: int = 5, max_overflow: int = 15, pool_timeout: int = 30):
    url = settings.DATABASE_URL
    kwargs: dict = {}

    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    else:
        kwargs["pool_size"] = pool_size
        kwargs["max_overflow"] = max_overflow
        kwargs["pool_pre_ping"] = True
        kwargs["pool_timeout"] = pool_timeout
        kwargs["pool_recycle"] = 300

    return create_engine(url, **kwargs)


# Main engine — used by dashboard, API, etc.
engine = _build_engine(pool_size=5, max_overflow=15, pool_timeout=30)

# Dedicated webhook engine — small isolated pool so webhook writes NEVER
# starve when dashboard endpoints saturate the main pool. Short timeout so
# the handler fails fast and falls back to stdout logging instead of making
# Zenvia wait (which would cause Zenvia to retry / disable the webhook).
webhook_engine = _build_engine(pool_size=3, max_overflow=5, pool_timeout=5)

# Enable WAL mode for SQLite so concurrent reads don't block on writes
if settings.DATABASE_URL.startswith("sqlite"):

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
WebhookSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=webhook_engine)


class Base(DeclarativeBase):
    pass


def create_tables() -> None:
    from app import models  # noqa: F401 — ensures ORM models are registered with Base

    Base.metadata.create_all(bind=engine)
