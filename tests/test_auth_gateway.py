import pytest
from fastapi.testclient import TestClient
from src.app import app
from src.services.auth_gateway.auth_service import auth_service, hash_password

client = TestClient(app)

def test_login_success():
    response = client.post("/api/auth/login", json={"username": "student1", "password": "student123"})
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["user"]["role"] == "student"
    assert data["user"]["department"] == "Computer Science"

def test_login_invalid_credentials():
    response = client.post("/api/auth/login", json={"username": "student1", "password": "wrongpassword"})
    assert response.status_code == 401

def test_rbac_facility_admin_route_protection():
    # Login as student
    login_resp = client.post("/api/auth/login", json={"username": "student1", "password": "student123"})
    token = login_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # Student trying to create a resource -> should be forbidden (403)
    res_payload = {
        "name": "Unauthorized Lab",
        "resource_type": "Laboratory",
        "building": "Tower B",
        "room_number": "B-101",
        "capacity": 30
    }
    response = client.post("/api/resources", json=res_payload, headers=headers)
    assert response.status_code == 403

def test_rbac_facility_admin_allowed():
    # Login as facility admin
    login_resp = client.post("/api/auth/login", json={"username": "admin_facility", "password": "facility123"})
    token = login_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    res_payload = {
        "name": "Quantum Computing Lab",
        "resource_type": "Laboratory",
        "building": "Tech Center",
        "room_number": "TC-505",
        "capacity": 25,
        "attributes": {"superconducting_rigs": 2, "nitrogen_cooling": True}
    }
    response = client.post("/api/resources", json=res_payload, headers=headers)
    assert response.status_code == 200
    assert response.json()["name"] == "Quantum Computing Lab"

def test_correlation_id_propagation():
    custom_cid = "test-corr-id-12345"
    response = client.get("/health", headers={"X-Correlation-ID": custom_cid})
    assert response.status_code == 200
    assert response.headers.get("X-Correlation-ID") == custom_cid
