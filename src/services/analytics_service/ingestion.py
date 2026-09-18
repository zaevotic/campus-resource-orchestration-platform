import datetime
import logging
from typing import Dict, Any
from src.services.analytics_service.olap_store import olap_store

logger = logging.getLogger(__name__)

class AnalyticsIngestionPipeline:
    """
    ANA-02: Analytics ingestion pipeline from Kafka into the columnar OLAP store.
    Decodes events and writes directly to columnar fact tables.
    """
    @staticmethod
    def ingest_booking_confirmed(payload: dict) -> bool:
        start_str = payload.get("start_time", "")
        end_str = payload.get("end_time", "")
        duration_hours = 1.0
        try:
            # Parse ISO or %Y-%m-%d %H:%M
            dt_start = datetime.datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            dt_end = datetime.datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            duration_hours = max(0.25, (dt_end - dt_start).total_seconds() / 3600.0)
        except Exception:
            duration_hours = 1.0

        with olap_store._lock:
            conn = olap_store.get_connection()
            # Idempotent insert
            existing = conn.execute("SELECT 1 FROM fact_bookings WHERE event_id = ?", [payload["event_id"]]).fetchone()
            if existing:
                return True

            conn.execute("""
                INSERT INTO fact_bookings (
                    event_id, correlation_id, booking_id, resource_id, user_id,
                    user_role, department, start_time, end_time, duration_hours,
                    purpose, attendees_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                payload["event_id"],
                payload.get("correlation_id", "N/A"),
                payload["booking_id"],
                payload["resource_id"],
                payload["user_id"],
                payload.get("user_role", "unknown"),
                payload.get("department", "General"),
                start_str,
                end_str,
                duration_hours,
                payload.get("purpose", ""),
                payload.get("attendees_count", 1)
            ))
            logger.info(f"Ingested booking event {payload['event_id']} into fact_bookings")
            return True

    @staticmethod
    def ingest_booking_cancelled(payload: dict) -> bool:
        with olap_store._lock:
            conn = olap_store.get_connection()
            existing = conn.execute("SELECT 1 FROM fact_cancellations WHERE event_id = ?", [payload["event_id"]]).fetchone()
            if existing:
                return True

            conn.execute("""
                INSERT INTO fact_cancellations (
                    event_id, correlation_id, booking_id, resource_id, user_id, reason
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                payload["event_id"],
                payload.get("correlation_id", "N/A"),
                payload["booking_id"],
                payload["resource_id"],
                payload["user_id"],
                payload.get("reason", "Cancelled")
            ))
            logger.info(f"Ingested cancellation event {payload['event_id']} into fact_cancellations")
            return True

    @staticmethod
    def ingest_service_request(payload: dict) -> bool:
        with olap_store._lock:
            conn = olap_store.get_connection()
            existing = conn.execute("SELECT 1 FROM fact_service_requests WHERE event_id = ?", [payload["event_id"]]).fetchone()
            if existing:
                return True

            conn.execute("""
                INSERT INTO fact_service_requests (
                    event_id, correlation_id, ticket_id, resource_id, requester_id,
                    issue_type, priority, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                payload["event_id"],
                payload.get("correlation_id", "N/A"),
                payload["ticket_id"],
                payload["resource_id"],
                payload["requester_id"],
                payload.get("issue_type", "GENERAL"),
                payload.get("priority", "MEDIUM"),
                payload.get("status", "OPEN")
            ))
            logger.info(f"Ingested service request {payload['event_id']} into fact_service_requests")
            return True

ingestion_pipeline = AnalyticsIngestionPipeline()
