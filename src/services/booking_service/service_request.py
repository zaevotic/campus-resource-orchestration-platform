import json
import uuid
import datetime
from typing import List, Dict, Optional, Any
from src.services.booking_service.db import db_manager
from src.common.correlation import get_current_correlation_id

class ServiceRequestManager:
    @staticmethod
    def create_request(resource_id: str, requester_id: str, issue_type: str, description: str, priority: str = "MEDIUM", correlation_id: Optional[str] = None) -> Dict[str, Any]:
        cid = correlation_id or get_current_correlation_id()
        ticket_id = f"tkt-{uuid.uuid4().hex[:8]}"
        outbox_id = f"obx-{uuid.uuid4().hex[:8]}"
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with db_manager._lock:
            conn = db_manager.get_connection()
            cur = conn.cursor()
            try:
                cur.execute("""
                    INSERT INTO service_requests (id, resource_id, requester_id, issue_type, description, priority, status, correlation_id, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?, ?)
                """, (ticket_id, resource_id, requester_id, issue_type, description, priority, cid, now_iso))

                event_payload = {
                    "event_id": f"evt-{uuid.uuid4().hex[:8]}",
                    "event_type": "ServiceRequestCreated",
                    "correlation_id": cid,
                    "timestamp": now_iso,
                    "ticket_id": ticket_id,
                    "resource_id": resource_id,
                    "requester_id": requester_id,
                    "issue_type": issue_type,
                    "description": description,
                    "priority": priority,
                    "status": "OPEN"
                }

                cur.execute("""
                    INSERT INTO booking_outbox (id, aggregate_type, aggregate_id, event_type, payload_json, correlation_id, status, created_at)
                    VALUES (?, 'ServiceRequest', ?, 'ServiceRequestCreated', ?, ?, 'PENDING', ?)
                """, (outbox_id, ticket_id, json.dumps(event_payload), cid, now_iso))

                conn.commit()
                return event_payload
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    @staticmethod
    def list_requests(resource_id: Optional[str] = None, status: Optional[str] = None) -> List[Dict[str, Any]]:
        conn = db_manager.get_connection()
        cur = conn.cursor()
        query = "SELECT * FROM service_requests WHERE 1=1"
        params = []
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

    @staticmethod
    def update_status(ticket_id: str, new_status: str) -> Optional[Dict[str, Any]]:
        conn = db_manager.get_connection()
        cur = conn.cursor()
        cur.execute("UPDATE service_requests SET status = ? WHERE id = ?", (new_status, ticket_id))
        conn.commit()
        cur.execute("SELECT * FROM service_requests WHERE id = ?", (ticket_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

service_request_mgr = ServiceRequestManager()
