import pytest
from fastapi.testclient import TestClient
from src.app import app
from src.services.booking_service.db import db_manager

client = TestClient(app)

@pytest.fixture
def auth_headers():
    resp = client.post("/api/auth/login", json={"username": "student1", "password": "student123"})
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}

def test_list_resources():
    resp = client.get("/api/resources")
    assert resp.status_code == 200
    resources = resp.json()
    assert len(resources) >= 4
    # Check JSON attributes parsed
    has_attributes = any("projector" in str(r.get("attributes", {})) for r in resources)
    assert has_attributes

def test_booking_creation_and_transactional_outbox(auth_headers):
    # Book Turing Lecture Hall
    res_id = "res-hall-101"
    start_time = "2026-11-10T10:00:00Z"
    end_time = "2026-11-10T12:00:00Z"

    # Check availability first
    avail_resp = client.get(f"/api/availability?resource_id={res_id}&start_time={start_time}&end_time={end_time}")
    assert avail_resp.status_code == 200
    assert avail_resp.json()["is_available"] is True

    # Create Booking
    booking_payload = {
        "resource_id": res_id,
        "start_time": start_time,
        "end_time": end_time,
        "purpose": "Algorithms Lecture",
        "attendees_count": 80
    }
    create_resp = client.post("/api/bookings", json=booking_payload, headers=auth_headers)
    assert create_resp.status_code == 200
    data = create_resp.json()
    booking_id = data["booking_id"]
    assert data["event_type"] == "BookingConfirmed"
    assert data["resource_id"] == res_id

    # Verify Outbox table record exists for this booking (STR-03)
    conn = db_manager.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM booking_outbox WHERE aggregate_id = ?", (booking_id,))
    outbox_row = cur.fetchone()
    conn.close()
    assert outbox_row is not None
    assert outbox_row["event_type"] == "BookingConfirmed"

def test_double_booking_conflict_prevention(auth_headers):
    res_id = "res-hall-101"
    initial_start = "2026-11-10T10:00:00Z"
    initial_end = "2026-11-10T12:00:00Z"

    # Create initial confirmed booking
    client.post("/api/bookings", json={
        "resource_id": res_id,
        "start_time": initial_start,
        "end_time": initial_end,
        "purpose": "Initial Keynote",
        "attendees_count": 50
    }, headers=auth_headers)

    # Overlapping window (11:00 - 13:00)
    overlap_start = "2026-11-10T11:00:00Z"
    overlap_end = "2026-11-10T13:00:00Z"

    # Availability check should detect conflict
    avail_resp = client.get(f"/api/availability?resource_id={res_id}&start_time={overlap_start}&end_time={overlap_end}")
    assert avail_resp.status_code == 200
    assert avail_resp.json()["is_available"] is False
    assert avail_resp.json()["conflict_count"] > 0

    # Booking attempt should fail with 409 Conflict
    booking_payload = {
        "resource_id": res_id,
        "start_time": overlap_start,
        "end_time": overlap_end,
        "purpose": "Conflicting Workshop",
        "attendees_count": 40
    }
    conflict_resp = client.post("/api/bookings", json=booking_payload, headers=auth_headers)
    assert conflict_resp.status_code == 409
    assert "Double-booking conflict" in conflict_resp.json()["detail"]

def test_booking_cancellation_and_outbox(auth_headers):
    # Book another slot
    res_id = "res-conf-301"
    start_time = "2026-11-12T14:00:00Z"
    end_time = "2026-11-12T15:00:00Z"
    create_resp = client.post("/api/bookings", json={
        "resource_id": res_id,
        "start_time": start_time,
        "end_time": end_time,
        "purpose": "Team Sync"
    }, headers=auth_headers)
    bkg_id = create_resp.json()["booking_id"]

    # Cancel booking
    cancel_resp = client.post(f"/api/bookings/{bkg_id}/cancel", json={"reason": "Meeting rescheduled"}, headers=auth_headers)
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["event_type"] == "BookingCancelled"

    # Verify status in database
    conn = db_manager.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT status FROM bookings WHERE id = ?", (bkg_id,))
    status = cur.fetchone()[0]
    conn.close()
    assert status == "CANCELLED"
