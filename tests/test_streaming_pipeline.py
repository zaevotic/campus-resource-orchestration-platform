import pytest
import asyncio
import json
import uuid
from src.common.avro_engine import schema_registry
from src.common.message_broker import broker, MessageRecord
from src.common.redis_client import kv_store
from src.common.config import settings
from src.services.booking_service.outbox_relay import outbox_relay
from src.services.booking_service.booking import booking_service
from src.services.booking_service.db import db_manager
from src.services.notification_service.consumer import notification_consumer
from src.services.reliability.dlq_manager import dlq_manager

@pytest.mark.asyncio
async def test_avro_schema_validation_and_serialization():
    # STR-01: Avro schema design and serialization
    test_event = {
        "event_id": f"evt-{uuid.uuid4().hex[:8]}",
        "event_type": "BookingConfirmed",
        "correlation_id": "corr-test-avro-01",
        "timestamp": "2026-11-10T10:00:00Z",
        "booking_id": "bkg-test-123",
        "resource_id": "res-hall-101",
        "user_id": "usr-stud-01",
        "user_role": "student",
        "department": "Computer Science",
        "start_time": "2026-11-10T10:00:00Z",
        "end_time": "2026-11-10T12:00:00Z",
        "purpose": "Avro Pipeline Test",
        "attendees_count": 50
    }

    # Validate against schema
    is_valid = schema_registry.validate("BookingConfirmed", test_event)
    assert is_valid is True

    # Binary serialize
    encoded_bytes = schema_registry.serialize("BookingConfirmed", test_event)
    assert isinstance(encoded_bytes, bytes)
    assert len(encoded_bytes) > 0

    # Deserialize back
    decoded_event = schema_registry.deserialize("BookingConfirmed", encoded_bytes)
    assert decoded_event["booking_id"] == "bkg-test-123"
    assert decoded_event["resource_id"] == "res-hall-101"

@pytest.mark.asyncio
async def test_kafka_partition_keying_by_resource_id():
    # STR-02: Partition keying by resource ID
    key1 = "res-hall-101"
    key2 = "res-hall-101"
    key3 = "res-lab-202"

    part1 = broker.get_partition(key1)
    part2 = broker.get_partition(key2)
    part3 = broker.get_partition(key3)

    # Identical keys must land on identical partition
    assert part1 == part2
    assert 0 <= part1 < broker.num_partitions
    assert 0 <= part3 < broker.num_partitions

@pytest.mark.asyncio
async def test_outbox_relay_and_notification_delivery():
    # Setup consumers
    notification_consumer.start()

    # Create a booking which writes to outbox
    booking = booking_service.create_booking(
        resource_id="res-sem-404",
        user_id="usr-fac-01",
        user_role="faculty",
        department="Electrical Engineering",
        start_time="2026-11-15T09:00:00Z",
        end_time="2026-11-15T11:00:00Z",
        purpose="Robotics Seminar",
        attendees_count=45,
        correlation_id="corr-relay-test"
    )

    # Flush outbox
    processed = await outbox_relay.process_pending_outbox()
    assert processed >= 1

    # Verify notification received and cached in Redis / KV store (STR-05)
    recent_notifs = notification_consumer.list_recent_notifications(10)
    found_notif = False
    for n in recent_notifs:
        if n.get("correlation_id") == "corr-relay-test":
            found_notif = True
            assert "Robotics Seminar" in n["body"] or "res-sem-404" in n["subject"]
            assert n["status"] == "DELIVERED"
            break
    assert found_notif is True

@pytest.mark.asyncio
async def test_dlq_and_retry_exhaustion_and_replay():
    # STR-07: Dead-letter queue & retry handling
    failing_event = {
        "event_id": "evt-fail-001",
        "event_type": "BookingConfirmed",
        "resource_id": "res-fail",
        "correlation_id": "corr-dlq-test"
    }

    dummy_record = MessageRecord(
        topic="campus.test.topic",
        key="res-fail",
        value=b"{}",
        headers={"correlation_id": "corr-dlq-test"}
    )

    # Define a handler that intentionally always fails
    def faulty_handler(payload):
        raise RuntimeError("Simulated transient network timeout!")

    # Execute with retry
    success = await dlq_manager.execute_with_retry(
        handler=faulty_handler,
        record=dummy_record,
        consumer_group="test-failing-group",
        event_payload=failing_event
    )
    assert success is False

    # Check that event was routed to DLQ
    dlq_list = dlq_manager.list_dlq_events()
    matched = [d for d in dlq_list if d["correlation_id"] == "corr-dlq-test"]
    assert len(matched) == 1
    dlq_item = matched[0]
    assert dlq_item["retry_count"] == settings.MAX_RETRY_ATTEMPTS
    assert "Simulated transient network timeout" in dlq_item["error_message"]

    # Replay DLQ event
    replayed = await dlq_manager.replay_dlq_event(dlq_item["dlq_id"])
    assert replayed is True
    # Should now be removed from active DLQ
    assert dlq_item["dlq_id"] not in dlq_manager.dlq_records
