import uuid
import datetime
import logging
from typing import List, Dict, Any, Optional
from src.common.config import settings
from src.common.avro_engine import schema_registry
from src.common.redis_client import kv_store
from src.common.message_broker import broker, MessageRecord
from src.services.reliability.dlq_manager import dlq_manager
from src.services.reliability.chaos_controller import CHAOS_FLAGS

logger = logging.getLogger(__name__)

RECENT_KEY = "notifications:recent"   # capped list (newest first) of delivered notifications

class NotificationConsumer:
    """
    STR-05: Event consumer framework for the Notification Service.
    - Consumes domain events from Kafka
    - Dispatches notifications (mock multi-channel: Email, In-App, Push)
    - Records short-lived delivery status in Redis / lightweight KV store with TTL
    - Uses DLQ & retry handler (STR-07)
    """
    CONSUMER_GROUP = "notification-service-group"

    def start(self):
        broker.subscribe(settings.TOPIC_BOOKING_EVENTS, self.on_booking_message)
        broker.subscribe(settings.TOPIC_SERVICE_REQUESTS, self.on_service_request_message)
        logger.info("NotificationConsumer subscribed to booking and service request topics")

    async def on_booking_message(self, record: MessageRecord):
        event_type = record.headers.get("event_type", "BookingConfirmed")
        try:
            payload = schema_registry.deserialize(event_type, record.value)
        except Exception:
            payload = schema_registry.deserialize("BookingConfirmed", record.value)

        # Process with retry / DLQ protection
        await dlq_manager.execute_with_retry(
            handler=self._process_booking_notification,
            record=record,
            consumer_group=self.CONSUMER_GROUP,
            event_payload=payload
        )

    async def on_service_request_message(self, record: MessageRecord):
        payload = schema_registry.deserialize("ServiceRequestCreated", record.value)
        await dlq_manager.execute_with_retry(
            handler=self._process_service_request_notification,
            record=record,
            consumer_group=self.CONSUMER_GROUP,
            event_payload=payload
        )

    def _process_booking_notification(self, payload: dict):
        # FAILURE INJECTION
        if CHAOS_FLAGS.get("fail_notifications"):
            raise Exception("Chaos Engineering: Simulated Notification Failure")

        event_type = payload.get("event_type", "BookingConfirmed")
        cid = payload.get("correlation_id", "unknown-cid")
        user_id = payload.get("user_id", "unknown-user")
        resource_id = payload.get("resource_id", "unknown-res")
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        notification_id = f"notif-{uuid.uuid4().hex[:8]}"

        if event_type == "BookingConfirmed":
            subject = f"Booking Confirmation: {resource_id}"
            body = (
                f"Your booking for {resource_id} from {payload.get('start_time')} to {payload.get('end_time')} "
                f"has been confirmed for {payload.get('purpose')}."
            )
        else:
            subject = f"Booking Cancelled: {resource_id}"
            body = f"Your reservation for {resource_id} has been cancelled. Reason: {payload.get('reason')}."

        record = {
            "notification_id": notification_id,
            "correlation_id": cid,
            "user_id": user_id,
            "event_type": event_type,
            "channel": "EMAIL_AND_PUSH",
            "subject": subject,
            "body": body,
            "status": "DELIVERED",
            "timestamp": now_iso
        }

        # Ephemeral Key-Value storage with 24-hour TTL (86400s)
        kv_store.set(f"notification:{notification_id}", record, ex=86400)
        kv_store.push_recent(RECENT_KEY, record)
        logger.info(f"Notification delivered [{notification_id}] for user {user_id} (CID: {cid})")

    def _process_service_request_notification(self, payload: dict):
        cid = payload.get("correlation_id", "unknown-cid")
        requester_id = payload.get("requester_id", "unknown")
        ticket_id = payload.get("ticket_id", "unknown")
        resource_id = payload.get("resource_id", "unknown")
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        notification_id = f"notif-{uuid.uuid4().hex[:8]}"

        record = {
            "notification_id": notification_id,
            "correlation_id": cid,
            "user_id": requester_id,
            "event_type": "ServiceRequestCreated",
            "channel": "IN_APP_ALERT",
            "subject": f"Maintenance Ticket #{ticket_id} Logged",
            "body": f"Issue '{payload.get('issue_type')}' on resource {resource_id} received. Priority: {payload.get('priority')}.",
            "status": "DELIVERED",
            "timestamp": now_iso
        }
        kv_store.set(f"notification:{notification_id}", record, ex=86400)
        kv_store.push_recent(RECENT_KEY, record)
        logger.info(f"Service request notification delivered [{notification_id}] (CID: {cid})")

    def list_recent_notifications(self, limit: int = 50) -> List[Dict[str, Any]]:
        return kv_store.recent(RECENT_KEY, limit)   # newest first

notification_consumer = NotificationConsumer()