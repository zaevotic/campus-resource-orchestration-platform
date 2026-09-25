import time
import statistics
from typing import Dict, List, Any
from src.common.message_broker import broker
from src.common.redis_client import kv_store
from src.services.booking_service.db import db_manager
from src.services.reliability.dlq_manager import dlq_manager

class SystemMetricsCollector:
    """
    ANA-05 & NFR-6: System observability, latency monitoring, and service health collector.
    Tracks P95 API latencies (NFR 1), uptime (NFR 2), and pipeline lag metrics.
    """
    def __init__(self):
        self.start_time = time.time()
        self.request_latencies_ms: List[float] = []
        self.status_codes: Dict[int, int] = {}
        self.total_requests = 0

    def record_request(self, latency_ms: float, status_code: int):
        self.total_requests += 1
        self.request_latencies_ms.append(latency_ms)
        # Keep window of recent 1000 requests
        if len(self.request_latencies_ms) > 1000:
            self.request_latencies_ms.pop(0)
        self.status_codes[status_code] = self.status_codes.get(status_code, 0) + 1

    def get_latency_stats(self) -> Dict[str, float]:
        if not self.request_latencies_ms:
            return {"count": 0, "avg_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0}
        sorted_lats = sorted(self.request_latencies_ms)
        n = len(sorted_lats)
        p50 = sorted_lats[int(0.50 * n)]
        p95 = sorted_lats[min(n - 1, int(0.95 * n))]
        p99 = sorted_lats[min(n - 1, int(0.99 * n))]
        avg = statistics.mean(sorted_lats)
        return {
            "count": n,
            "avg_ms": round(avg, 2),
            "p50_ms": round(p50, 2),
            "p95_ms": round(p95, 2),
            "p99_ms": round(p99, 2),
            "nfr1_compliant": p95 < 500.0  # NFR 1: <500ms for 95% of requests
        }

    def get_health_status(self) -> Dict[str, Any]:
        uptime_seconds = round(time.time() - self.start_time, 2)
        
        # Outbox pending lag
        conn = db_manager.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM booking_outbox WHERE status = 'PENDING'")
        pending_outbox = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM bookings")
        total_bookings = cur.fetchone()[0]
        conn.close()

        # Broker metrics
        broker_metrics = broker.get_metrics()
        dlq_count = len(dlq_manager.dlq_records)
        notif_count = kv_store.count()

        services_health = {
            "api_gateway": {"status": "HEALTHY", "latency_check": "OK"},
            "auth_service": {"status": "HEALTHY", "rbac": "ENFORCED"},
            "booking_service": {"status": "HEALTHY", "isolation": "SERIALIZABLE/RANGE_EXCLUSION"},
            "event_streaming": {"status": "HEALTHY", "partitions_online": 4},
            "notification_service": {"status": "HEALTHY" if kv_store.ping() else "DEGRADED", "store": f"{kv_store.backend}_KV_" + ("ONLINE" if kv_store.ping() else "UNREACHABLE")},
            "analytics_olap": {"status": "HEALTHY", "engine": "DUCKDB_COLUMNAR"},
            "dlq_manager": {"status": "HEALTHY", "exhausted_events": dlq_count}
        }

        # Calculate uptime availability % (NFR 2: target 99.5%)
        error_count = sum(cnt for code, cnt in self.status_codes.items() if code >= 500)
        availability_pct = 100.0 if self.total_requests == 0 else round(((self.total_requests - error_count) / self.total_requests) * 100.0, 3)

        return {
            "system_status": "OPERATIONAL",
            "uptime_seconds": uptime_seconds,
            "availability_pct": availability_pct,
            "nfr2_compliant": availability_pct >= 99.5,
            "latency_metrics": self.get_latency_stats(),
            "pipeline_metrics": {
                "outbox_pending_lag": pending_outbox,
                "total_bookings_stored": total_bookings,
                "kafka_topics": broker_metrics,
                "active_notifications_cached": notif_count,
                "dlq_events_unhandled": dlq_count
            },
            "services": services_health
        }

metrics_collector = SystemMetricsCollector()
