import pytest
from fastapi.testclient import TestClient
from src.app import app

client = TestClient(app)

def test_uat_persona_student():
    """
    UAT 1: Student Journey
    - Login
    - Search classroom
    - Book slot
    - Receive notification
    - Cancel booking
    - Submit maintenance request
    """
    # 1. Login
    login_res = client.post("/api/auth/login", json={"username": "student1", "password": "student123"})
    assert login_res.status_code == 200
    token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # 2. Search resources
    res_list = client.get("/api/resources?resource_type=Classroom", headers=headers)
    assert res_list.status_code == 200
    assert len(res_list.json()) > 0
    res_id = res_list.json()[0]["id"]

    # 3. Create booking
    bkg_res = client.post("/api/bookings", json={
        "resource_id": res_id,
        "start_time": "2026-12-10T14:00:00Z",
        "end_time": "2026-12-10T16:00:00Z",
        "purpose": "Finals Study Group",
        "attendees_count": 5
    }, headers=headers)
    assert bkg_res.status_code == 200
    booking_id = bkg_res.json()["booking_id"]

    # 4. Check notification
    notifs = client.get("/api/notifications", headers=headers)
    assert notifs.status_code == 200

    # 5. Cancel booking
    cancel_res = client.post(f"/api/bookings/{booking_id}/cancel", json={"reason": "Study group moved online"}, headers=headers)
    assert cancel_res.status_code == 200

    # 6. Submit service ticket
    ticket_res = client.post("/api/service-requests", json={
        "resource_id": res_id,
        "issue_type": "HDMI_CABLE_FAULT",
        "description": "HDMI cable does not connect to podium laptop",
        "priority": "LOW"
    }, headers=headers)
    assert ticket_res.status_code == 200
    assert ticket_res.json()["ticket_id"].startswith("tkt-")

def test_uat_persona_faculty():
    """
    UAT 2: Faculty Journey
    - Login
    - Check lab availability
    - Reserve lab for lecture
    """
    login_res = client.post("/api/auth/login", json={"username": "faculty1", "password": "faculty123"})
    assert login_res.status_code == 200
    token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # Check lab availability
    avail = client.get("/api/availability?resource_id=res-lab-202&start_time=2026-12-11T09:00:00Z&end_time=2026-12-11T12:00:00Z", headers=headers)
    assert avail.status_code == 200
    assert avail.json()["is_available"] is True

    # Book lab
    bkg = client.post("/api/bookings", json={
        "resource_id": "res-lab-202",
        "start_time": "2026-12-11T09:00:00Z",
        "end_time": "2026-12-11T12:00:00Z",
        "purpose": "Embedded Systems Practical Exam",
        "attendees_count": 35
    }, headers=headers)
    assert bkg.status_code == 200

def test_uat_persona_facility_admin():
    """
    UAT 3: Facility Admin Journey
    - Login
    - Add new facility resource
    - Manage maintenance tickets
    """
    login_res = client.post("/api/auth/login", json={"username": "admin_facility", "password": "facility123"})
    assert login_res.status_code == 200
    token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # Create new resource
    new_res = client.post("/api/resources", json={
        "name": "VR Simulation Studio",
        "resource_type": "Laboratory",
        "building": "Digital Media Center",
        "room_number": "DMC-101",
        "capacity": 20,
        "attributes": {"headsets": 20, "tracking_sensors": 8, "gpu_rigs": 10}
    }, headers=headers)
    assert new_res.status_code == 200
    assert new_res.json()["name"] == "VR Simulation Studio"

    # View tickets
    tickets = client.get("/api/service-requests", headers=headers)
    assert tickets.status_code == 200

def test_uat_persona_system_admin():
    """
    UAT 4: System Admin Journey
    - Login
    - Review system metrics and health
    - Inspect audit logs
    - Review DLQ
    """
    login_res = client.post("/api/auth/login", json={"username": "admin_sys", "password": "sysadmin123"})
    assert login_res.status_code == 200
    token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # System Health
    health = client.get("/health", headers=headers)
    assert health.status_code == 200
    assert health.json()["system_status"] == "OPERATIONAL"

    # Audit Logs
    audit = client.get("/api/analytics/audit-logs", headers=headers)
    assert audit.status_code == 200
    assert len(audit.json()) > 0

    # DLQ view
    dlq = client.get("/api/dlq", headers=headers)
    assert dlq.status_code == 200
