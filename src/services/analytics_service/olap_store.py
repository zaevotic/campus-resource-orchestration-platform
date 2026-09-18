import os
import duckdb
import threading
import logging
from src.common.config import settings

logger = logging.getLogger(__name__)

class ColumnarOlapStore:
    """
    ANA-01: Columnar OLAP store provisioning using DuckDB.
    Optimized for high-throughput scans, analytical aggregations, and immutable append-only logs.
    """
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.DUCKDB_PATH
        self._lock = threading.RLock()
        self._conn = None
        self._init_olap_schema()

    def get_connection(self):
        with self._lock:
            if self._conn is None:
                self._conn = duckdb.connect(self.db_path)
            return self._conn

    def _init_olap_schema(self):
        with self._lock:
            conn = self.get_connection()
            
            # 1. Fact Table: Bookings
            conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_bookings (
                event_id VARCHAR PRIMARY KEY,
                correlation_id VARCHAR,
                booking_id VARCHAR,
                resource_id VARCHAR,
                user_id VARCHAR,
                user_role VARCHAR,
                department VARCHAR,
                start_time VARCHAR,
                end_time VARCHAR,
                duration_hours DOUBLE,
                purpose VARCHAR,
                attendees_count INTEGER,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            # 2. Fact Table: Cancellations
            conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_cancellations (
                event_id VARCHAR PRIMARY KEY,
                correlation_id VARCHAR,
                booking_id VARCHAR,
                resource_id VARCHAR,
                user_id VARCHAR,
                reason VARCHAR,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            # 3. Fact Table: Service Requests
            conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_service_requests (
                event_id VARCHAR PRIMARY KEY,
                correlation_id VARCHAR,
                ticket_id VARCHAR,
                resource_id VARCHAR,
                requester_id VARCHAR,
                issue_type VARCHAR,
                priority VARCHAR,
                status VARCHAR,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            # 4. Audit Log Table (ANA-03): Immutable append-only audit trail
            conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                audit_id VARCHAR PRIMARY KEY,
                correlation_id VARCHAR,
                timestamp VARCHAR,
                actor_id VARCHAR,
                actor_role VARCHAR,
                action VARCHAR,
                resource_id VARCHAR,
                status VARCHAR,
                details_json VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            logger.info("Columnar OLAP store schema initialized successfully")

    def execute_query(self, sql: str, params: list = None) -> list:
        with self._lock:
            conn = self.get_connection()
            if params:
                return conn.execute(sql, params).fetchall()
            return conn.execute(sql).fetchall()

    def execute_df(self, sql: str, params: list = None):
        with self._lock:
            conn = self.get_connection()
            if params:
                return conn.execute(sql, params).df()
            return conn.execute(sql).df()

olap_store = ColumnarOlapStore()
