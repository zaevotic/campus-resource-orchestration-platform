import asyncio
import json
import logging
import datetime
from src.services.booking_service.db import db_manager
from src.common.config import settings
from src.common.avro_engine import schema_registry
from src.common.message_broker import broker

logger = logging.getLogger(__name__)

class OutboxRelayWorker:
    """
    STR-04: Outbox relay / Kafka producer service.
    Polls the transactional outbox table, serializes events with Avro schemas (STR-01),
    and publishes them to Kafka topics partitioned by resource_id (STR-02).
    """
    def __init__(self, poll_interval: float = 1.0):
        self.poll_interval = poll_interval
        self._running = False

    async def process_pending_outbox(self) -> int:
        processed_count = 0
        conn = db_manager.get_connection()
        cur = conn.cursor()
        
        # Fetch pending outbox records
        cur.execute("""
            SELECT id, aggregate_type, aggregate_id, event_type, payload_json, correlation_id, retry_count
            FROM booking_outbox
            WHERE status = 'PENDING'
            ORDER BY created_at ASC
            LIMIT 50
        """)
        pending_records = cur.fetchall()
        conn.close()

        for rec in pending_records:
            obx_id = rec["id"]
            event_type = rec["event_type"]
            payload = json.loads(rec["payload_json"])
            cid = rec["correlation_id"]
            resource_id = payload.get("resource_id", "default-res")
            
            # Target topic
            if event_type.startswith("Booking"):
                topic = settings.TOPIC_BOOKING_EVENTS
            else:
                topic = settings.TOPIC_SERVICE_REQUESTS

            try:
                # 1. Avro Serialization
                binary_val = schema_registry.serialize(event_type, payload)

                # 2. Produce to Kafka partitioned deterministically by resource_id
                await broker.produce(
                    topic=topic,
                    key=resource_id,
                    value=binary_val,
                    headers={"correlation_id": cid, "event_type": event_type}
                )

                # 3. Mark Outbox entry as PUBLISHED
                now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
                c_update = db_manager.get_connection()
                cur_up = c_update.cursor()
                cur_up.execute("""
                    UPDATE booking_outbox 
                    SET status = 'PUBLISHED', published_at = ?
                    WHERE id = ?
                """, (now_iso, obx_id))
                c_update.commit()
                c_update.close()
                processed_count += 1
            except Exception as e:
                logger.error(f"Failed to publish outbox record {obx_id}: {e}")
                c_err = db_manager.get_connection()
                cur_err = c_err.cursor()
                cur_err.execute("""
                    UPDATE booking_outbox 
                    SET retry_count = retry_count + 1,
                        status = CASE WHEN retry_count >= 5 THEN 'FAILED' ELSE 'PENDING' END
                    WHERE id = ?
                """, (obx_id,))
                c_err.commit()
                c_err.close()

        return processed_count

    async def run_loop(self):
        self._running = True
        while self._running:
            try:
                await self.process_pending_outbox()
            except Exception as e:
                logger.error(f"Outbox worker iteration error: {e}")
            await asyncio.sleep(self.poll_interval)

    def stop(self):
        self._running = False

outbox_relay = OutboxRelayWorker()
