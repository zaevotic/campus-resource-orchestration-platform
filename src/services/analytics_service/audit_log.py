import uuid
import datetime
import json
import logging
from typing import List, Dict, Optional, Any
from src.services.analytics_service.olap_store import olap_store
from src.common.correlation import get_current_correlation_id

logger = logging.getLogger(__name__)

class AuditLogger:
    """
    ANA-03: Audit-log schema definition and immutable write path.
    Appends audit events to the columnar OLAP audit_logs table.
    """
    @staticmethod
    def log_action(
        action: str,
        actor_id: str,
        actor_role: str,
        resource_id: str = "N/A",
        status: str = "SUCCESS",
        details: dict = None,
        correlation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        cid = correlation_id or get_current_correlation_id()
        audit_id = f"aud-{uuid.uuid4().hex[:8]}"
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        details_str = json.dumps(details or {})

        with olap_store._lock:
            conn = olap_store.get_connection()
            conn.execute("""
                INSERT INTO audit_logs (
                    audit_id, correlation_id, timestamp, actor_id, actor_role, action, resource_id, status, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (audit_id, cid, now_iso, actor_id, actor_role, action, resource_id, status, details_str))

        record = {
            "audit_id": audit_id,
            "correlation_id": cid,
            "timestamp": now_iso,
            "actor_id": actor_id,
            "actor_role": actor_role,
            "action": action,
            "resource_id": resource_id,
            "status": status,
            "details": details or {}
        }
        logger.info(f"Audit log written [{audit_id}]: {action} by {actor_id} (CID: {cid})")
        return record

    @staticmethod
    def query_audit_logs(
        correlation_id: Optional[str] = None,
        actor_id: Optional[str] = None,
        resource_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        sql = "SELECT audit_id, correlation_id, timestamp, actor_id, actor_role, action, resource_id, status, details_json FROM audit_logs WHERE 1=1"
        params = []
        if correlation_id:
            sql += " AND correlation_id = ?"
            params.append(correlation_id)
        if actor_id:
            sql += " AND actor_id = ?"
            params.append(actor_id)
        if resource_id:
            sql += " AND resource_id = ?"
            params.append(resource_id)
        sql += f" ORDER BY timestamp DESC LIMIT {limit}"

        rows = olap_store.execute_query(sql, params)
        results = []
        for r in rows:
            details = {}
            if r[8]:
                try:
                    details = json.loads(r[8])
                except Exception:
                    pass
            results.append({
                "audit_id": r[0],
                "correlation_id": r[1],
                "timestamp": r[2],
                "actor_id": r[3],
                "actor_role": r[4],
                "action": r[5],
                "resource_id": r[6],
                "status": r[7],
                "details": details
            })
        return results

audit_logger = AuditLogger()
