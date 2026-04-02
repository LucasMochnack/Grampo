# Grampo — Zenvia WhatsApp Webhook Service

Phase 1 MVP: receives Zenvia/WhatsApp webhook events, stores them verbatim, and exposes simple inspection endpoints.

---

## Quick start

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# The defaults work as-is for local SQLite development.
# Edit DATABASE_URL and WEBHOOK_SECRET_TOKEN as needed.

# 4. Run
uvicorn app.main:app --reload --port 8000
```

The service is now running at `http://localhost:8000`.
Interactive API docs: `http://localhost:8000/docs`

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check |
| `POST` | `/webhook/zenvia` | Receive a Zenvia webhook event |
| `GET` | `/events` | List received events (paginated) |
| `GET` | `/events/{id}` | Full detail of one event |

---

## Curl examples

### Health check
```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

### Send a fake webhook (no token required locally)
```bash
curl -X POST http://localhost:8000/webhook/zenvia \
  -H "Content-Type: application/json" \
  -d '{
    "id": "evt-001",
    "type": "MESSAGE",
    "channel": "whatsapp",
    "timestamp": "2024-01-15T10:30:00Z",
    "message": {
      "id": "msg-abc",
      "from": "5511999990001",
      "to": "5511999990002",
      "direction": "IN",
      "contents": [{"type": "text", "text": "Olá!"}]
    }
  }'
# {"status":"received","event_id":"<uuid>"}
```

### Send with a token (when WEBHOOK_SECRET_TOKEN is set)
```bash
curl -X POST http://localhost:8000/webhook/zenvia \
  -H "Content-Type: application/json" \
  -H "X-Zenvia-Token: your_secret_here" \
  -d '{"id": "evt-002", "type": "STATUS", "channel": "whatsapp"}'
```

### List events (newest first)
```bash
curl "http://localhost:8000/events/?limit=10&offset=0"
```

### Get one event by ID
```bash
curl http://localhost:8000/events/<uuid-from-webhook-response>
```

---

## Fake payload for local testing

Save as `test_payload.json` and use with curl:

```json
{
  "id": "evt-local-001",
  "type": "MESSAGE",
  "channel": "whatsapp",
  "timestamp": "2024-06-01T09:00:00Z",
  "message": {
    "id": "msg-local-001",
    "from": "5511900000001",
    "to": "5511900000002",
    "direction": "IN",
    "channel": "whatsapp",
    "contents": [
      {
        "type": "text",
        "text": "Quero informações sobre fundos de investimento."
      }
    ]
  }
}
```

```bash
curl -X POST http://localhost:8000/webhook/zenvia \
  -H "Content-Type: application/json" \
  -d @test_payload.json
```

---

## Running tests

```bash
pytest tests/                     # run all tests
pytest --cov=app tests/           # with coverage report
pytest tests/test_webhook.py -v   # specific file, verbose
```

---

## Production deployment (PostgreSQL)

1. Provision a PostgreSQL database.
2. Set `DATABASE_URL` in your environment:
   ```
   DATABASE_URL=postgresql+psycopg2://user:password@host:5432/grampo
   ```
3. Set `WEBHOOK_SECRET_TOKEN` to the value Zenvia will send.
4. Start with Uvicorn (or behind a reverse proxy like Nginx):
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 2
   ```

Tables are created automatically on startup via `Base.metadata.create_all()`.
Add Alembic for schema migrations when the model evolves.

---

## Security notes

- `WEBHOOK_SECRET_TOKEN` — Zenvia sends this in `X-Zenvia-Token` (or `Authorization: Bearer <token>`).  Compared with `hmac.compare_digest` to prevent timing attacks.  Leave empty only in local development.
- Authorization and signature headers are **redacted** (`[REDACTED]`) before being stored in `raw_headers`.
- When Zenvia's HMAC-SHA256 signing behaviour is confirmed, replace the token check with signature verification over the raw body using the `X-Hub-Signature-256` header (Meta WhatsApp standard).

---

## Project structure

```
app/
├── main.py          # FastAPI app factory + lifespan
├── config.py        # Settings (pydantic-settings, reads .env)
├── database.py      # SQLAlchemy engine, session, Base
├── models.py        # WebhookEvent ORM model
├── schemas.py       # Pydantic response/request schemas
├── crud.py          # All database operations
├── dependencies.py  # get_db, verify_webhook_token, sanitize_headers
└── routers/
    ├── health.py    # GET /health
    ├── webhook.py   # POST /webhook/zenvia
    └── events.py    # GET /events, GET /events/{id}

tests/
├── conftest.py      # Fixtures: in-memory SQLite, rollback-per-test session
├── test_health.py
├── test_webhook.py
└── test_events.py
```

---

## Database model

`webhook_events` table — all fields sourced from the actual request, nothing invented:

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `received_at` | timestamptz | UTC timestamp, indexed |
| `raw_payload` | JSON | Verbatim request body |
| `raw_headers` | JSON | Sanitized HTTP headers |
| `zenvia_event_id` | varchar | `payload["id"]` if present |
| `zenvia_event_type` | varchar | `payload["type"]` if present |
| `zenvia_channel` | varchar | `payload["channel"]` if present |
| `zenvia_timestamp` | varchar | `payload["timestamp"]` if present (stored as string) |
| `source_ip` | varchar | Caller IP |
| `user_agent` | text | User-Agent header |
| `content_type` | varchar | Content-Type header |
