"""Tests for POST /webhook/zenvia."""

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.dependencies import get_db

_SAMPLE_PAYLOAD = {
    "id": "evt-001",
    "type": "MESSAGE",
    "channel": "whatsapp",
    "timestamp": "2024-01-15T10:30:00Z",
    "message": {
        "id": "msg-abc",
        "from": "5511999990001",
        "to": "5511999990002",
        "direction": "IN",
        "contents": [{"type": "text", "text": "Olá!"}],
    },
}


def test_valid_payload_returns_200(client):
    response = client.post("/webhook/zenvia", json=_SAMPLE_PAYLOAD)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "received"
    assert "event_id" in body


def test_event_is_persisted(client, db_session):
    from app.models import WebhookEvent

    response = client.post("/webhook/zenvia", json=_SAMPLE_PAYLOAD)
    event_id = response.json()["event_id"]

    event = db_session.query(WebhookEvent).filter(WebhookEvent.id == event_id).first()
    assert event is not None
    assert event.raw_payload == _SAMPLE_PAYLOAD


def test_extracted_fields_populated(client, db_session):
    from app.models import WebhookEvent

    response = client.post("/webhook/zenvia", json=_SAMPLE_PAYLOAD)
    event_id = response.json()["event_id"]

    event = db_session.query(WebhookEvent).filter(WebhookEvent.id == event_id).first()
    assert event.zenvia_event_id == "evt-001"
    assert event.zenvia_event_type == "MESSAGE"
    assert event.zenvia_channel == "whatsapp"
    assert event.zenvia_timestamp == "2024-01-15T10:30:00Z"


def test_extracted_fields_null_when_absent(client, db_session):
    from app.models import WebhookEvent

    payload = {"message": {"text": "no top-level id or type"}}
    response = client.post("/webhook/zenvia", json=payload)
    event_id = response.json()["event_id"]

    event = db_session.query(WebhookEvent).filter(WebhookEvent.id == event_id).first()
    assert event.zenvia_event_id is None
    assert event.zenvia_event_type is None
    assert event.zenvia_channel is None
    assert event.zenvia_timestamp is None


def test_raw_payload_stored_verbatim(client, db_session):
    from app.models import WebhookEvent

    payload = {"unexpected_key": [1, 2, 3], "nested": {"deep": True}}
    response = client.post("/webhook/zenvia", json=payload)
    event_id = response.json()["event_id"]

    event = db_session.query(WebhookEvent).filter(WebhookEvent.id == event_id).first()
    assert event.raw_payload == payload


def test_non_json_body_returns_422(client):
    response = client.post(
        "/webhook/zenvia",
        content="not-json",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422


# --- Token authentication tests ---


def _make_client_with_token(db_session, token: str) -> TestClient:
    """Create a TestClient whose app has WEBHOOK_SECRET_TOKEN set."""
    import app.dependencies as deps
    import app.config as cfg

    original = cfg.settings.WEBHOOK_SECRET_TOKEN
    cfg.settings.WEBHOOK_SECRET_TOKEN = token

    application = create_app()
    application.dependency_overrides[get_db] = lambda: (yield db_session)

    client = TestClient(application, raise_server_exceptions=True)
    # Reset after test
    cfg.settings.WEBHOOK_SECRET_TOKEN = original
    return client


def test_token_auth_missing_returns_401(db_session):
    c = _make_client_with_token(db_session, "secret123")
    response = c.post("/webhook/zenvia", json=_SAMPLE_PAYLOAD)
    assert response.status_code == 401


def test_token_auth_wrong_token_returns_403(db_session):
    c = _make_client_with_token(db_session, "secret123")
    response = c.post(
        "/webhook/zenvia",
        json=_SAMPLE_PAYLOAD,
        headers={"X-Zenvia-Token": "wrongtoken"},
    )
    assert response.status_code == 403


def test_token_auth_correct_token_returns_200(db_session):
    c = _make_client_with_token(db_session, "secret123")
    response = c.post(
        "/webhook/zenvia",
        json=_SAMPLE_PAYLOAD,
        headers={"X-Zenvia-Token": "secret123"},
    )
    assert response.status_code == 200


def test_token_auth_bearer_prefix_accepted(db_session):
    c = _make_client_with_token(db_session, "secret123")
    response = c.post(
        "/webhook/zenvia",
        json=_SAMPLE_PAYLOAD,
        headers={"Authorization": "Bearer secret123"},
    )
    assert response.status_code == 200
