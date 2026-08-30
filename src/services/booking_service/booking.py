import json
import uuid
import datetime
from typing import List, Dict, Optional, Any
from src.services.booking_service.db import db_manager
from src.common.correlation import get_current_correlation_id

class BookingService:
    @staticmethod
    def create_booking(
        resource_id: str,
        user_id: str,
        user_role: str,
        department: str,
        start_time: str,
        end_time: str,
        purpose: str,
        attendees_count: int = 1,
        correlation_id: Optional[str] = None,
        system_override: bool = False
    ) -> Dict[str, Any]:
        cid = correlation_id or get_current_correlation_id()
        booking_id = f"bkg-{uuid.uuid4().hex[:8]}"
        outbox_id = f"obx-{uuid.uuid4().hex[:8]}"
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Atomic transaction with concurrency exclusion
        with db_manager._lock:
            conn = db_manager.get_connection()
            cur = conn.cursor()
            try:
                # 1. Verify resource exists & is active
                cur.execute("SELECT id, name, resource_type, capacity FROM resources WHERE id = ? AND is_active = 1", (resource_id,))
                res = cur.fetchone()
                if not res:
                    raise ValueError(f"Resource {resource_id} does not exist or is inactive")

                # 1b. Validate time window and room capacity
                if not start_time or not end_time or end_time <= start_time:
                    raise ValueError("Invalid time window: end_time must be after start_time")
                if attendees_count < 1:
                    raise ValueError("Attendees count must be at least 1")
                if not system_override and attendees_count > res["capacity"]:
                    raise ValueError(
                        f"Capacity exceeded: Requested {attendees_count} attendees, but '{res['name']}' has a maximum capacity of {res['capacity']} seats."
                    )

                # 2. Strict Range Exclusion Constraint: Prevent Double Booking
                cur.execute("""
                    SELECT id, user_id, start_time, end_time 
                    FROM bookings 
                    WHERE resource_id = ? 
                      AND status = 'CONFIRMED'
                      AND start_time < ? 
                      AND end_time > ?
                """, (resource_id, end_time, start_time))
                conflicts = cur.fetchall()
                
                if len(conflicts) > 0 and not system_override:
                    # SMART ALTERNATIVES: Find free rooms of the same type and capacity
                    cur.execute("""
                        SELECT id FROM resources 
                        WHERE resource_type = ? AND capacity >= ? AND is_active = 1 AND id != ?
                          AND id NOT IN (
                              SELECT resource_id FROM bookings 
                              WHERE status = 'CONFIRMED' AND start_time < ? AND end_time > ?
                          )
                        LIMIT 3
                    """, (res["resource_type"], res["capacity"], resource_id, end_time, start_time))
                    
                    alt_rows = cur.fetchall()
                    alt_message = ""
                    if alt_rows:
                        alternatives = [row["id"] for row in alt_rows]
                        alt_message = f" Smart Alternatives available: {', '.join(alternatives)}."
                    
                    raise ValueError(f"Double-booking conflict: Resource is already booked for overlapping window by booking {conflicts[0]['id']}.{alt_message}")

                # 3. Insert Booking
                cur.execute("""
                    INSERT INTO bookings (
                        id, resource_id, user_id, user_role, department,
                        start_time, end_time, purpose, attendees_count,
                        status, correlation_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'CONFIRMED', ?, ?)
                """, (booking_id, resource_id, user_id, user_role, department, start_time, end_time, purpose, attendees_count, cid, now_iso))

                # 4. Transactional Outbox Pattern (STR-03): write domain event in SAME transaction
                event_payload = {
                    "event_id": f"evt-{uuid.uuid4().hex[:8]}",
                    "event_type": "BookingConfirmed",
                    "correlation_id": cid,
                    "timestamp": now_iso,
                    "booking_id": booking_id,
                    "resource_id": resource_id,
                    "user_id": user_id,
                    "user_role": user_role,
                    "department": department,
                    "start_time": start_time,
                    "end_time": end_time,
                    "purpose": purpose,
                    "attendees_count": attendees_count
                }

                cur.execute("""
                    INSERT INTO booking_outbox (
                        id, aggregate_type, aggregate_id, event_type,
                        payload_json, correlation_id, status, created_at
                    ) VALUES (?, 'Booking', ?, 'BookingConfirmed', ?, ?, 'PENDING', ?)
                """, (outbox_id, booking_id, json.dumps(event_payload), cid, now_iso))

                conn.commit()
                return event_payload
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    @staticmethod
    def cancel_booking(
        booking_id: str, 
        user_id: str, 
        user_role: str, 
        reason: str = "User requested cancellation", 
        correlation_id: Optional[str] = None, 
        system_override: bool = False
    ) -> Dict[str, Any]:
        cid = correlation_id or get_current_correlation_id()
        outbox_id = f"obx-{uuid.uuid4().hex[:8]}"
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with db_manager._lock:
            conn = db_manager.get_connection()
            cur = conn.cursor()
            try:
                cur.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"Booking {booking_id} not found")
                bkg = dict(row)
                
                # Check authorization
                if not system_override and user_role not in ("system_admin", "facility_admin") and bkg["user_id"] != user_id:
                    raise PermissionError("You can only cancel your own bookings")

                if bkg["status"] == "CANCELLED":
                    raise ValueError("Booking is already cancelled")

                # Update booking status
                cur.execute("UPDATE bookings SET status = 'CANCELLED' WHERE id = ?", (booking_id,))

                # Outbox event write
                event_payload = {
                    "event_id": f"evt-{uuid.uuid4().hex[:8]}",
                    "event_type": "BookingCancelled",
                    "correlation_id": cid,
                    "timestamp": now_iso,
                    "booking_id": booking_id,
                    "resource_id": bkg["resource_id"],
                    "user_id": user_id,
                    "start_time": bkg["start_time"],
                    "end_time": bkg["end_time"],
                    "reason": reason
                }

                cur.execute("""
                    INSERT INTO booking_outbox (
                        id, aggregate_type, aggregate_id, event_type,
                        payload_json, correlation_id, status, created_at
                    ) VALUES (?, 'Booking', ?, 'BookingCancelled', ?, ?, 'PENDING', ?)
                """, (outbox_id, booking_id, json.dumps(event_payload), cid, now_iso))

                conn.commit()
                return event_payload
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    @staticmethod
    def list_bookings(user_id: Optional[str] = None, resource_id: Optional[str] = None, status: Optional[str] = None) -> List[Dict[str, Any]]:
        conn = db_manager.get_connection()
        cur = conn.cursor()
        query = "SELECT * FROM bookings WHERE 1=1"
        params = []
        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)
        if resource_id:
            query += " AND resource_id = ?"
            params.append(resource_id)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC"
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

booking_service = BookingService()