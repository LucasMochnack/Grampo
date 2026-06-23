import asyncio
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from app.database import create_tables, SessionLocal
from app.routers import events, health, webhook, dashboard

# Páginas que o perfil "compliance" PODE acessar (só a aba Alertas + auth + ação
# de marcar alerta). Qualquer outra rota /dashboard/* é redirecionada p/ Alertas.
_COMPLIANCE_ALLOWED = {
    "/dashboard/alertas",
    "/dashboard/ack-alert",
    "/dashboard/login",
    "/dashboard/logout",
}
from app.services.auto_score import weekly_score_loop

# Scheduler logs to stdout so Railway captures the weekly-run reports.
_autoscore_logger = logging.getLogger("auto_score")
_autoscore_logger.setLevel(logging.INFO)
if not _autoscore_logger.handlers:
    _ah = logging.StreamHandler(sys.stdout)
    _ah.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    _autoscore_logger.addHandler(_ah)
    _autoscore_logger.propagate = False

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
        # Avaliação automática semanal (domingo à noite) — roda no próprio app.
        scorer_task = asyncio.create_task(weekly_score_loop())
        yield
        scorer_task.cancel()

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

    @app.middleware("http")
    async def _compliance_gate(request, call_next):
        # O perfil "compliance" só enxerga a aba Alertas. Qualquer outra rota
        # /dashboard/* é redirecionada para lá. Só pagamos a consulta ao banco
        # quando o caminho NÃO está na lista permitida (rotas de Alertas/login
        # passam direto, sem custo). Não afeta /webhook nem /health.
        path = request.url.path
        if path.startswith("/dashboard") and path not in _COMPLIANCE_ALLOWED:
            access = None
            try:
                db = SessionLocal()
                try:
                    access = dashboard._get_access(request, db)
                finally:
                    db.close()
            except Exception:
                access = None
            if access and access.get("role") == "compliance":
                return RedirectResponse("/dashboard/alertas", status_code=303)
        return await call_next(request)

    app.include_router(health.router)
    app.include_router(webhook.router)
    app.include_router(events.router)
    app.include_router(dashboard.router)

    return app


# Module-level instance consumed by Uvicorn: `uvicorn app.main:app`
app = create_app()
