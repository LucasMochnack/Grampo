from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import create_tables
from app.routers import events, health, webhook, dashboard


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

    app.include_router(health.router)
    app.include_router(webhook.router)
    app.include_router(events.router)
    app.include_router(dashboard.router)

    return app


# Module-level instance consumed by Uvicorn: `uvicorn app.main:app`
app = create_app()
