import logging
from src.common.message_broker import broker
from src.common.config import settings
from src.common.avro_engine import schema_registry
from src.services.booking_service.db import db_manager
from src.services.booking_service.booking import booking_service

logger = logging.getLogger(__name__)

class WaitlistConsumer:
    def start(self):
        broker.subscribe(settings.TOPIC_BOOKING_EVENTS, self.on_booking_message)
        logger.info("WaitlistConsumer subscribed to booking events")

    async def on_booking_message(self, record):
        event_type = record.headers.get("event_type", "")
        if event_type != "BookingCancelled":
            return
            
        try:
            payload = schema_registry.deserialize(event_type, record.value)
        except Exception:
            return

        resource_id = payload.get("resource_id")
        freed_start = payload.get("start_time")
        freed_end = payload.get("end_time")

        if not (resource_id and freed_start and freed_end):
            return

        with db_manager._lock:
            conn = db_manager.get_connection()
            cur = conn.cursor()
            try:
                # 1. Query the database for the oldest waitlist entry matching this window
                cur.execute(
                    """SELECT * FROM waitlist 
                       WHERE resource_id = ? AND start_time >= ? AND end_time <= ? 
                       ORDER BY created_at ASC LIMIT 1""",
                    (resource_id, freed_start, freed_end)
                )
                waitlist_entry = cur.fetchone()
                
                if waitlist_entry:
                    entry = dict(waitlist_entry)
                    # 2. Auto-rebook the waitlisted user
                    booking_service.create_booking(
                        resource_id=entry["resource_id"],
                        user_id=entry["user_id"],
                        user_role=entry["user_role"],
                        department=entry.get("department", "General"),
                        start_time=entry["start_time"],
                        end_time=entry["end_time"],
                        purpose=entry["purpose"],
                        attendees_count=entry.get("attendees_count", 1),
                        system_override=True
                    )
                    
                    # 3. Delete from waitlist
                    cur.execute("DELETE FROM waitlist WHERE id = ?", (entry["id"],))
                    conn.commit()
                    logger.info(f"Waitlist auto-rebooking successful for {entry['user_id']}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Waitlist fulfillment failed: {e}")
            finally:
                conn.close()

waitlist_consumer = WaitlistConsumer()