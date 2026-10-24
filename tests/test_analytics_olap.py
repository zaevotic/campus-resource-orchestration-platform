import pytest
from src.services.analytics_service.olap_store import olap_store
from src.services.analytics_service.ingestion import ingestion_pipeline
from src.services.analytics_service.audit_log import audit_logger
from src.services.analytics_service.aggregator import analytics_aggregator

def test_olap_columnar_tables_provisioned():
    # ANA-01: Columnar OLAP store provisioning
    tables = olap_store.execute_query("SHOW TABLES")
    table_names = [t[0] for t in tables]
    assert "fact_bookings" in table_names
    assert "fact_cancellations" in table_names
    assert "fact_service_requests" in table_names
    assert "audit_logs" in table_names

def test_analytics_ingestion_and_aggregations():
    # ANA-02: Ingest sample events
    bkg_event = {
        "event_id": "evt-olap-01",
        "correlation_id": "cid-olap-test-1",
        "booking_id": "bkg-olap-01",
        "resource_id": "res-lab-202",
        "user_id": "usr-stud-01",
        "user_role": "student",
        "department": "Computer Science",
        "start_time": "2026-11-20T14:00:00Z",
        "end_time": "2026-11-20T17:00:00Z",
        "purpose": "Machine Learning Lab",
        "attendees_count": 30
    }
    ingested = ingestion_pipeline.ingest_booking_confirmed(bkg_event)
    assert ingested is True

    # ANA-04: Test aggregations
    utilization = analytics_aggregator.get_resource_utilization()
    assert len(utilization) > 0
    target_res = [u for u in utilization if u["resource_id"] == "res-lab-202"]
    assert len(target_res) == 1
    assert target_res[0]["total_hours_booked"] >= 3.0

    peak_hours = analytics_aggregator.get_peak_hours_analysis()
    assert any(h["hour"] == "14:00" for h in peak_hours)

    dept_dist = analytics_aggregator.get_department_distribution()
    assert any(d["department"] == "Computer Science" for d in dept_dist)

def test_audit_log_write_and_correlation_id_query():
    # ANA-03 & ANA-06: Audit log schema, write path & correlation ID
    cid = "corr-security-audit-99"
    log_rec = audit_logger.log_action(
        action="UPDATE_SECURITY_POLICY",
        actor_id="usr-sys-01",
        actor_role="system_admin",
        resource_id="system-auth",
        status="SUCCESS",
        details={"mfa_enforced": True, "session_timeout_mins": 30},
        correlation_id=cid
    )
    assert log_rec["correlation_id"] == cid

    # Query audit logs by correlation ID
    results = audit_logger.query_audit_logs(correlation_id=cid)
    assert len(results) >= 1
    assert results[0]["action"] == "UPDATE_SECURITY_POLICY"
    assert results[0]["actor_role"] == "system_admin"
    assert results[0]["details"]["mfa_enforced"] is True
