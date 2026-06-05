import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import create_tables
from app.routers import events, health, webhook, dashboard

# Ensure webhook logger emits to stdout so Railway captures fallback payload
# logs when the DB is unavailable. Without this, logger.error() messages may
# be silently swallowed depending on uvicorn's logging config.
_webhook_logger = logging.getLogger("webhook")
_webhook_logger.setLevel(logging.INFO)
if not _webhook_logger.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    _webhook_logger.addHandler(_h)
    _webhook_logger.propagate = False


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        create_tables()
        yield

    app = FastAPI(
        title="Grampo — Zenvia Webhook Service",
        description=(
            "Phase 1: receives Zenvia/WhatsApp webhook events, "
            "stores them verbatim, and exposes simple inspection endpoints."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def _security_headers(request, call_next):
        resp = await call_next(request)
        resp.headers.setdefault("X-Content-Type-Options", "nosniff")
        resp.headers.setdefault("X-Frame-Options", "DENY")
        resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        resp.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        # Inline scripts/handlers are used throughout, so 'unsafe-inline' stays;
        # the value here still adds object-src/base-uri/frame-ancestors hardening
        # and constrains where assets/connections can come from.
        resp.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src 'self' data: https://fonts.gstatic.com; "
            "img-src 'self' data: https:; "
            "media-src 'self' https:; "
            "connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'",
        )
        return resp

    app.include_router(health.router)
    app.include_router(webhook.router)
    app.include_router(events.router)
    app.include_router(dashboard.router)

    return app


# Module-level instance consumed by Uvicorn: `uvicorn app.main:app`
app = create_app()
