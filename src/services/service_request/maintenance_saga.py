import logging
from src.common.message_broker import broker
from src.common.config import settings
from src.common.avro_engine import schema_registry
from src.services.booking_service.db import db_manager
from src.services.booking_service.booking import booking_service

logger = logging.getLogger(__name__)

class MaintenanceSaga:
    def start(self):
        broker.subscribe(settings.TOPIC_SERVICE_REQUESTS, self.on_service_request)
        logger.info("MaintenanceSaga subscribed to service request events")

    async def on_service_request(self, record):
        event_type = record.headers.get("event_type", "")
        if event_type != "ServiceRequestCreated":
            return
            
        try:
            payload = schema_registry.deserialize(event_type, record.value)
        except Exception:
            return

        if payload.get("priority") != "CRITICAL":
            return
            
        resource_id = payload["resource_id"]
        timestamp = payload["timestamp"]
        
        with db_manager._lock:
            conn = db_manager.get_connection()
            cur = conn.cursor()
            try:
                # 1. Block the resource temporarily
                cur.execute("UPDATE resources SET is_active = 0 WHERE id = ?", (resource_id,))
                
                # 2. Find impacted bookings
                cur.execute(
                    "SELECT * FROM bookings WHERE resource_id = ? AND start_time >= ? AND status = 'CONFIRMED'",
                    (resource_id, timestamp)
                )
                impacted = [dict(r) for r in cur.fetchall()]
                
                for bkg in impacted:
                    # 3. Cancel existing booking
                    booking_service.cancel_booking(
                        booking_id=bkg["id"], 
                        user_id=bkg["user_id"],
                        user_role=bkg["user_role"],
                        reason="Emergency maintenance. Booking has been relocated.",
                        system_override=True
                    )
                    
                    # 4. Attempt to find an alternative room
                    cur.execute("SELECT resource_type, capacity FROM resources WHERE id = ?", (resource_id,))
                    res_info = cur.fetchone()
                    if res_info:
                        cur.execute("""
                            SELECT id FROM resources 
                            WHERE resource_type = ? AND capacity >= ? AND is_active = 1 AND id != ?
                              AND id NOT IN (
                                  SELECT resource_id FROM bookings 
                                  WHERE status = 'CONFIRMED' AND start_time < ? AND end_time > ?
                              ) LIMIT 1
                        """, (res_info["resource_type"], res_info["capacity"], resource_id, bkg["end_time"], bkg["start_time"]))
                        
                        alt = cur.fetchone()
                        if alt:
                            # Rebook in the new room
                            booking_service.create_booking(
                                resource_id=alt["id"],
                                user_id=bkg["user_id"],
                                user_role=bkg["user_role"],
                                department=bkg["department"],
                                start_time=bkg["start_time"],
                                end_time=bkg["end_time"],
                                purpose=bkg["purpose"],
                                attendees_count=bkg["attendees_count"],
                                system_override=True
                            )
                conn.commit()
                logger.info(f"Maintenance saga completed for resource {resource_id}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Maintenance saga failed: {e}")
            finally:
                conn.close()

maintenance_saga = MaintenanceSaga()