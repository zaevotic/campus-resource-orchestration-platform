import asyncio
import json
import logging
import uuid
import datetime
import traceback
from typing import Callable, Optional, Dict, List, Any
from src.common.config import settings
from src.common.avro_engine import schema_registry
from src.common.message_broker import broker, MessageRecord

logger = logging.getLogger(__name__)

class DeadLetterQueueManager:
    """
    STR-07: Dead-letter queue (DLQ) and retry/failure handling mechanisms.
    - Exponential backoff retry mechanism (NFR 8)
    - Dead-letter routing to DLQ topic
    - DLQ inspection and replay mechanisms
    """
    def __init__(self):
        # In-memory index of DLQ events for inspection and replay
        self.dlq_records: Dict[str, Dict[str, Any]] = {}

    async def execute_with_retry(
        self,
        handler: Callable[[dict], Any],
        record: MessageRecord,
        consumer_group: str,
        event_payload: dict
    ) -> bool:
        cid = record.headers.get("correlation_id", "unknown-cid")
        attempts = 0
        backoff = settings.INITIAL_BACKOFF_SECONDS

        while attempts < settings.MAX_RETRY_ATTEMPTS:
            try:
                attempts += 1
                result = handler(event_payload)
                if asyncio.iscoroutine(result):
                    await result
                return True
            except Exception as e:
                logger.warning(
                    f"Consumer [{consumer_group}] attempt {attempts}/{settings.MAX_RETRY_ATTEMPTS} failed for "
                    f"event on {record.topic} (CID: {cid}): {e}"
                )
                if attempts < settings.MAX_RETRY_ATTEMPTS:
                    await asyncio.sleep(backoff)
                    backoff *= settings.BACKOFF_MULTIPLIER
                else:
                    # Retries exhausted -> route to DLQ
                    await self.route_to_dlq(
                        record=record,
                        consumer_group=consumer_group,
                        error=e,
                        retry_count=attempts,
                        payload=event_payload
                    )
                    return False
        return False

    async def route_to_dlq(self, record: MessageRecord, consumer_group: str, error: Exception, retry_count: int, payload: dict):
        dlq_id = f"dlq-{uuid.uuid4().hex[:8]}"
        cid = record.headers.get("correlation_id", "unknown-cid")
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        st = traceback.format_exc()

        dlq_event = {
            "dlq_id": dlq_id,
            "correlation_id": cid,
            "original_topic": record.topic,
            "consumer_group": consumer_group,
            "error_message": str(error),
            "stack_trace": st,
            "retry_count": retry_count,
            "failed_at": now_iso,
            "payload_json": json.dumps(payload)
        }

        # Store in local registry for immediate admin query
        self.dlq_records[dlq_id] = dlq_event

        # Serialize using DLQ Avro schema
        binary_val = schema_registry.serialize("DeadLetterEvent", dlq_event)

        # Produce to DLQ topic
        await broker.produce(
            topic=settings.TOPIC_DLQ,
            key=record.key or dlq_id,
            value=binary_val,
            headers={"correlation_id": cid, "dlq_id": dlq_id}
        )
        logger.error(f"Event routed to DLQ [{settings.TOPIC_DLQ}] (DLQ ID: {dlq_id}, CID: {cid})")

    def list_dlq_events(self) -> List[Dict[str, Any]]:
        return list(self.dlq_records.values())

    async def replay_dlq_event(self, dlq_id: str) -> bool:
        """
        Replay an event from DLQ back to its original topic for reprocessing.
        """
        if dlq_id not in self.dlq_records:
            return False
        dlq_item = self.dlq_records[dlq_id]
        payload = json.loads(dlq_item["payload_json"])
        event_type = payload.get("event_type", "Unknown")
        target_topic = dlq_item["original_topic"]
        key = payload.get("resource_id", "default")
        
        # Serialize and re-produce
        val_bytes = schema_registry.serialize(event_type, payload)
        await broker.produce(
            topic=target_topic,
            key=key,
            value=val_bytes,
            headers={"correlation_id": dlq_item["correlation_id"], "replayed_from_dlq": dlq_id}
        )
        # Remove from active DLQ records upon successful replay dispatch
        del self.dlq_records[dlq_id]
        logger.info(f"Successfully replayed DLQ event {dlq_id} to {target_topic}")
        return True

dlq_manager = DeadLetterQueueManager()
