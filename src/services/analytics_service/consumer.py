import logging
from src.common.config import settings
from src.common.avro_engine import schema_registry
from src.common.message_broker import broker, MessageRecord
from src.services.reliability.dlq_manager import dlq_manager
from src.services.analytics_service.ingestion import ingestion_pipeline

logger = logging.getLogger(__name__)

class AnalyticsConsumer:
    """
    STR-06: Event consumer framework for the Analytics Service.
    Consumes Kafka domain events and feeds them into the columnar OLAP store.
    """
    CONSUMER_GROUP = "analytics-service-group"

    def start(self):
        broker.subscribe(settings.TOPIC_BOOKING_EVENTS, self.on_booking_message)
        broker.subscribe(settings.TOPIC_SERVICE_REQUESTS, self.on_service_request_message)
        logger.info("AnalyticsConsumer subscribed to domain event topics")

    async def on_booking_message(self, record: MessageRecord):
        event_type = record.headers.get("event_type", "BookingConfirmed")
        try:
            payload = schema_registry.deserialize(event_type, record.value)
        except Exception:
            payload = schema_registry.deserialize("BookingConfirmed", record.value)

        await dlq_manager.execute_with_retry(
            handler=self._dispatch_booking_event,
            record=record,
            consumer_group=self.CONSUMER_GROUP,
            event_payload=payload
        )

    async def on_service_request_message(self, record: MessageRecord):
        payload = schema_registry.deserialize("ServiceRequestCreated", record.value)
        await dlq_manager.execute_with_retry(
            handler=ingestion_pipeline.ingest_service_request,
            record=record,
            consumer_group=self.CONSUMER_GROUP,
            event_payload=payload
        )

    def _dispatch_booking_event(self, payload: dict):
        event_type = payload.get("event_type", "BookingConfirmed")
        if event_type == "BookingConfirmed":
            ingestion_pipeline.ingest_booking_confirmed(payload)
        elif event_type == "BookingCancelled":
            ingestion_pipeline.ingest_booking_cancelled(payload)

analytics_consumer = AnalyticsConsumer()
