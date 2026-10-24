import pytest
import asyncio
import concurrent.futures
from fastapi.testclient import TestClient
from src.app import app
from src.services.booking_service.outbox_relay import outbox_relay
from src.services.analytics_service.olap_store import olap_store
from src.services.notification_service.consumer import notification_consumer
from src.services.analytics_service.consumer import analytics_consumer
from src.services.reliability.dlq_manager import dlq_manager

client = TestClient(app)

def test_cross_service_concurrency_consistency_nfr5():
    """
    NFR 5: Reliability & Consistency under heavy concurrent load.
    Spawns 10 concurrent threads competing for the exact same slot.
    Validates that EXACTLY 1 booking succeeds and 9 are safely rejected with 409 Conflict.
    """
    # 1. Login to get token
    login_resp = client.post("/api/auth/login", json={"username": "student1", "password": "student123"})
    token = login_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    target_res = "res-lab-202"
    slot_start = "2026-12-01T10:00:00Z"
    slot_end = "2026-12-01T12:00:00Z"

    def attempt_booking(i):
        c = TestClient(app)
        return c.post("/api/bookings", json={
            "resource_id": target_res,
            "start_time": slot_start,
            "end_time": slot_end,
            "purpose": f"Concurrent Race Attempt #{i}",
            "attendees_count": 10
        }, headers=headers)

    # Launch 10 simultaneous threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(attempt_booking, i) for i in range(10)]
        results = [f.result() for f in futures]

    status_codes = [r.status_code for r in results]
    successes = status_codes.count(200)
    conflicts = status_codes.count(409)

    assert successes == 1, f"Expected exactly 1 success, got {successes}"
    assert conflicts == 9, f"Expected 9 conflicts, got {conflicts}"

@pytest.mark.asyncio
async def test_cross_service_streaming_analytics_consistency_nfr5():
    """
    Verifies end-to-end data pipeline consistency between Booking, Outbox Relay,
    Kafka Broker, and DuckDB OLAP Analytics.
    """
    notification_consumer.start()
    analytics_consumer.start()

    login_resp = client.post("/api/auth/login", json={"username": "faculty1", "password": "faculty123"})
    token = login_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # Create 3 non-overlapping bookings
    slots = [
        ("2026-12-05T09:00:00Z", "2026-12-05T10:00:00Z"),
        ("2026-12-05T10:30:00Z", "2026-12-05T11:30:00Z"),
        ("2026-12-05T12:00:00Z", "2026-12-05T13:00:00Z"),
    ]
    created_bkg_ids = []
    for s_start, s_end in slots:
        r = client.post("/api/bookings", json={
            "resource_id": "res-conf-301",
            "start_time": s_start,
            "end_time": s_end,
            "purpose": "Pipeline Consistency Test",
            "attendees_count": 12
        }, headers=headers)
        assert r.status_code == 200
        created_bkg_ids.append(r.json()["booking_id"])

    # Flush outbox to Kafka
    await outbox_relay.process_pending_outbox()

    # Verify OLAP table has received these bookings
    for b_id in created_bkg_ids:
        rows = olap_store.execute_query("SELECT 1 FROM fact_bookings WHERE booking_id = ?", [b_id])
        assert len(rows) == 1, f"Booking {b_id} missing in OLAP store"

@pytest.mark.asyncio
async def test_cross_service_fault_tolerance_nfr8():
    """
    NFR 8: Fault tolerance & recovery under failure.
    Validates DLQ capturing of exhausted failures and successful replay.
    """
    dlq_start_count = len(dlq_manager.dlq_records)
    
    # Trigger DLQ route directly
    await dlq_manager.route_to_dlq(
        record=type("DummyRecord", (), {"topic": "campus.bookings.events", "key": "res-fail", "headers": {"correlation_id": "cross-test-cid"}})(),
        consumer_group="cross-service-test-group",
        error=ConnectionResetError("Simulated broker disconnect"),
        retry_count=3,
        payload={"event_id": "evt-dlq-cross-01", "event_type": "BookingConfirmed", "resource_id": "res-cross-fail"}
    )

    assert len(dlq_manager.dlq_records) == dlq_start_count + 1
    recent_dlq_id = list(dlq_manager.dlq_records.keys())[-1]
    
    # Replay from DLQ
    replayed = await dlq_manager.replay_dlq_event(recent_dlq_id)
    assert replayed is True
