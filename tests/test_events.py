"""Tests for GET /events and GET /events/{id}."""

import uuid

_PAYLOAD_A = {"id": "evt-a", "type": "MESSAGE", "channel": "whatsapp"}
_PAYLOAD_B = {"id": "evt-b", "type": "STATUS", "channel": "whatsapp"}


def _post(client, payload=None):
    return client.post("/webhook/zenvia", json=payload or _PAYLOAD_A)


# --- GET /events ---


def test_list_events_empty(client):
    response = client.get("/events/")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 0
    assert body["items"] == []


def test_list_events_returns_inserted(client):
    _post(client, _PAYLOAD_A)
    _post(client, _PAYLOAD_B)

    response = client.get("/events/")
    body = response.json()
    assert body["total"] == 2
    assert len(body["items"]) == 2


def test_list_events_default_envelope(client):
    response = client.get("/events/")
    body = response.json()
    assert "total" in body
    assert "limit" in body
    assert "offset" in body
    assert "items" in body


def test_list_events_newest_first(client):
    _post(client, _PAYLOAD_A)
    _post(client, _PAYLOAD_B)

    response = client.get("/events/")
    items = response.json()["items"]
    # Most recently inserted should be first
    assert items[0]["zenvia_event_id"] == "evt-b"
    assert items[1]["zenvia_event_id"] == "evt-a"


def test_list_events_pagination(client):
    for i in range(5):
        _post(client, {"id": f"evt-{i}"})

    page1 = client.get("/events/?limit=2&offset=0").json()
    page2 = client.get("/events/?limit=2&offset=2").json()

    assert len(page1["items"]) == 2
    assert len(page2["items"]) == 2
    assert page1["total"] == 5

    ids_p1 = {item["id"] for item in page1["items"]}
    ids_p2 = {item["id"] for item in page2["items"]}
    assert ids_p1.isdisjoint(ids_p2)


def test_list_events_limit_exceeds_max_returns_422(client):
    response = client.get("/events/?limit=101")
    assert response.status_code == 422


def test_list_events_negative_offset_returns_422(client):
    response = client.get("/events/?offset=-1")
    assert response.status_code == 422


# --- GET /events/{id} ---


def test_get_event_not_found(client):
    response = client.get(f"/events/{uuid.uuid4()}")
    assert response.status_code == 404


def test_get_event_returns_correct_data(client):
    post_resp = _post(client, _PAYLOAD_A)
    event_id = post_resp.json()["event_id"]

    get_resp = client.get(f"/events/{event_id}")
    assert get_resp.status_code == 200

    body = get_resp.json()
    assert body["id"] == event_id
    assert body["raw_payload"] == _PAYLOAD_A
    assert body["zenvia_event_id"] == "evt-a"
    assert body["zenvia_event_type"] == "MESSAGE"
    assert body["zenvia_channel"] == "whatsapp"


def test_get_event_includes_headers(client):
    post_resp = _post(client)
    event_id = post_resp.json()["event_id"]

    body = client.get(f"/events/{event_id}").json()
    # raw_headers should be present (may be None in test env, but key must exist)
    assert "raw_headers" in body


def test_get_event_invalid_uuid_returns_422(client):
    response = client.get("/events/not-a-uuid")
    assert response.status_code == 422
