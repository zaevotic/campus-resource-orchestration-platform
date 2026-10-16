import pytest
from src.services.booking_service.db import db_manager
from src.services.analytics_service.olap_store import olap_store
from src.common.redis_client import kv_store
from src.services.reliability.dlq_manager import dlq_manager

@pytest.fixture(autouse=True)
def reset_test_state():
    # Clean transactional DB tables
    conn = db_manager.get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM bookings")
    cur.execute("DELETE FROM booking_outbox")
    cur.execute("DELETE FROM service_requests")
    default_ids = tuple(r[0] for r in db_manager.DEFAULT_CATALOG)
    placeholders = ",".join("?" for _ in default_ids)
    cur.execute(f"DELETE FROM resources WHERE id NOT IN ({placeholders})", default_ids)
    conn.commit()
    conn.close()

    # Clean OLAP store
    with olap_store._lock:
        oc = olap_store.get_connection()
        oc.execute("DELETE FROM fact_bookings")
        oc.execute("DELETE FROM fact_cancellations")
        oc.execute("DELETE FROM fact_service_requests")
        oc.execute("DELETE FROM audit_logs")

    # Clean DLQ and KV cache
    dlq_manager.dlq_records.clear()
    kv_store._store.clear()
    yield
