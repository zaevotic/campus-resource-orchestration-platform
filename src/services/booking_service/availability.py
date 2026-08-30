from typing import List, Dict, Any
from src.services.booking_service.db import db_manager

class AvailabilityService:
    @staticmethod
    def check_availability(resource_id: str, start_time: str, end_time: str) -> Dict[str, Any]:
        """
        Check whether resource is available for the given range.
        An overlap exists if: (existing_start < new_end) AND (existing_end > new_start)
        for any booking with status = 'CONFIRMED'.
        """
        conn = db_manager.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, user_id, start_time, end_time, status 
            FROM bookings 
            WHERE resource_id = ? 
              AND status = 'CONFIRMED'
              AND start_time < ? 
              AND end_time > ?
        """, (resource_id, end_time, start_time))
        conflicts = cur.fetchall()
        conn.close()

        is_available = len(conflicts) == 0
        return {
            "resource_id": resource_id,
            "start_time": start_time,
            "end_time": end_time,
            "is_available": is_available,
            "conflict_count": len(conflicts),
            "conflicting_bookings": [dict(c) for c in conflicts]
        }

    @staticmethod
    def get_booked_intervals(resource_id: str, date_prefix: str) -> List[Dict[str, Any]]:
        conn = db_manager.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, start_time, end_time, purpose 
            FROM bookings 
            WHERE resource_id = ? 
              AND status = 'CONFIRMED'
              AND (start_time LIKE ? OR end_time LIKE ?)
            ORDER BY start_time ASC
        """, (resource_id, f"{date_prefix}%", f"{date_prefix}%"))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

availability_service = AvailabilityService()
