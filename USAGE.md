# Campus Resource Orchestration Platform - Usage Guide & Documentation

This documentation provides a comprehensive guide on architecture, quickstart, role-based usage, API workflows, event streaming, analytics, and operational testing for the **Campus Resource Orchestration Platform**.

---

## Table of Contents
1. [Overview & Architecture](#1-overview--architecture)
2. [Prerequisites & Environment Setup](#2-prerequisites--environment-setup)
3. [Running the Application](#3-running-the-application)
4. [Authentication & User Roles](#4-authentication--user-roles)
5. [Core Service Workflows & Examples](#5-core-service-workflows--examples)
   - [5.1 User Authentication](#51-user-authentication)
   - [5.2 Resource Discovery & Inventory](#52-resource-discovery--inventory)
   - [5.3 Availability Check & Conflict Prevention](#53-availability-check--conflict-prevention)
   - [5.4 Booking Creation & Transactional Outbox](#54-booking-creation--transactional-outbox)
   - [5.5 Booking Cancellation](#55-booking-cancellation)
   - [5.6 Service & Maintenance Tickets](#56-service--maintenance-tickets)
   - [5.7 Event-Driven Notifications](#57-event-driven-notifications)
   - [5.8 OLAP Analytics & Aggregation Queries](#58-olap-analytics--aggregation-queries)
   - [5.9 Audit Log Queries](#59-audit-log-queries)
   - [5.10 Dead-Letter Queue (DLQ) & Failure Replay](#510-dead-letter-queue-dlq--failure-replay)
6. [Observability & Health Checks](#6-observability--health-checks)
7. [Running Test Suites & Benchmarks](#7-running-test-suites--benchmarks)
8. [API Reference Summary](#8-api-reference-summary)

---

## 1. Overview & Architecture

The **Campus Resource Orchestration Platform** is an event-driven, polyglot microservice platform designed to orchestrate campus resources (lecture halls, laboratories, seminar rooms, equipment) across multiple university personas while guaranteeing ACID consistency, double-booking prevention, auditability, and analytical reporting.

### Architectural Highlights
- **Synchronous Consistency-Critical Path**:
  - API Gateway enforeces Rate Limiting and JWT Role-Based Access Control (RBAC).
  - PostgreSQL / ACID transactional storage prevents double bookings via interval overlap checks (mirrors PostgreSQL range exclusion constraints).
  - **Transactional Outbox Pattern (`STR-03`)**: Commits the business record (e.g. booking) and outbox event in the same atomic transaction to eliminate dual-write hazards.
- **Asynchronous Event-Driven Fan-Out**:
  - **Outbox Relay (`STR-04`)**: Poller daemon reads pending events, serializes them using binary **Avro schemas (`STR-01`)**, and publishes them to Kafka topics partitioned by `resource_id` (`STR-02`).
  - **Notification Consumer (`STR-05`)**: Asynchronously processes events, sends alerts, and tracks ephemeral delivery state in Redis / key-value store with TTL.
  - **Analytics Consumer (`STR-06`) & Ingestion (`ANA-02`)**: Streams domain events into a columnar **DuckDB OLAP engine (`ANA-01`)** for high-speed aggregations.
  - **Reliability & DLQ (`STR-07`)**: Handles exponential backoff retries and routes poison-pill events to a Dead-Letter Queue (DLQ) with admin replay capabilities.
  - **Observability (`ANA-06`, `ANA-05`)**: Propagates `X-Correlation-ID` headers across all distributed actions, logs immutable audit events (`ANA-03`), and tracks real-time P95 latency and availability.

---

## 2. Prerequisites & Environment Setup

### System Requirements
- Linux / macOS / Windows (WSL2)
- Python 3.11+ (Python 3.14 verified)
- `uv` (fast package manager) or standard `pip` / `venv`

### Environment Installation
From the project directory (`/home/zaevo/college/5th sem/DE/project/dir`):

```bash
# 1. Activate or create virtual environment
source .venv/bin/activate

# 2. (Optional) If setting up from scratch:
uv pip install fastapi uvicorn duckdb pydantic pyjwt pytest pytest-asyncio requests httpx fastavro redis
```

---

## 3. Running the Application

### Start the Server
Run using `uvicorn`:

```bash
source .venv/bin/activate
uvicorn src.app:app --host 0.0.0.0 --port 8000 --reload
```

- **Interactive API Documentation (Swagger UI)**: `http://localhost:8000/docs`
- **Alternative Documentation (ReDoc)**: `http://localhost:8000/redoc`
- **Root / Health Check**: `http://localhost:8000/health`

---

## 4. Authentication & User Roles

The platform enforces Role-Based Access Control (RBAC). Pre-seeded user accounts are ready for testing:

| Username | Password | Role | Department | Description |
| :--- | :--- | :--- | :--- | :--- |
| `student1` | `student123` | `student` | Computer Science | Book resources, view availability, cancel own bookings, report issues. |
| `faculty1` | `faculty123` | `faculty` | Electrical Engineering | Reserve academic facilities, seminars, and laboratories. |
| `admin_facility` | `facility123` | `facility_admin` | Facilities | Create/update resource catalog, view all tickets & audits. |
| `admin_sys` | `sysadmin123` | `system_admin` | IT Services | Full privileges: DLQ management, replay, security audits, metrics. |

---

## 5. Core Service Workflows & Examples

All requests can be executed using `curl` or any HTTP client.

### 5.1 User Authentication
Authenticate and obtain a JWT bearer token:

```bash
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "student1", "password": "student123"}'
```

**Response:**
```json
{
  "access_token": "eyJhbGciOi...",
  "token_type": "bearer",
  "user": {
    "id": "usr-stud-01",
    "username": "student1",
    "role": "student",
    "department": "Computer Science",
    "email": "student1@campus.edu"
  }
}
```

Save the token for subsequent requests:
```bash
export TOKEN="<paste_access_token_here>"
```

---

### 5.2 Resource Discovery & Inventory
Search campus facilities with optional filters (`resource_type`, `building`, `min_capacity`):

```bash
curl -X GET "http://localhost:8000/api/resources?resource_type=Classroom"
```

**Adding a Resource (Admin only):**
```bash
curl -X POST http://localhost:8000/api/resources \
  -H "Authorization: Bearer $FACILITY_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Alan Turing Lecture Hall B",
    "resource_type": "Classroom",
    "building": "Computing Complex",
    "room_number": "CC-201",
    "capacity": 150,
    "attributes": {
      "smart_board": true,
      "microphones": 4,
      "recording_rig": true
    }
  }'
```

---

### 5.3 Availability Check & Conflict Prevention
Check if a specific resource is free during a desired time window before attempting to book:

```bash
curl -X GET "http://localhost:8000/api/availability?resource_id=res-hall-101&start_time=2026-11-10T10:00:00Z&end_time=2026-11-10T12:00:00Z"
```

**Response:**
```json
{
  "resource_id": "res-hall-101",
  "start_time": "2026-11-10T10:00:00Z",
  "end_time": "2026-11-10T12:00:00Z",
  "is_available": true,
  "conflict_count": 0,
  "conflicting_bookings": []
}
```

---

### 5.4 Booking Creation & Transactional Outbox
Create a booking. The service:
1. Validates absence of conflicting bookings (`start_time < existing_end AND end_time > existing_start`).
2. Inserts booking record into the relational store.
3. Inserts a `BookingConfirmed` domain event into the `booking_outbox` table in the same transaction.
4. Relays event to Kafka stream and triggers downstream notification & analytics consumption.

```bash
curl -X POST http://localhost:8000/api/bookings \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: req-booking-101" \
  -d '{
    "resource_id": "res-hall-101",
    "start_time": "2026-11-10T10:00:00Z",
    "end_time": "2026-11-10T12:00:00Z",
    "purpose": "Operating Systems Midterm Review",
    "attendees_count": 95
  }'
```

**Double Booking Prevention:**
If another user immediately attempts to book `res-hall-101` for `2026-11-10T11:00:00Z` to `13:00:00Z`, the system rejects it with **HTTP 409 Conflict**:
```json
{
  "detail": "Double-booking conflict: Resource is already booked for overlapping window by booking bkg-..."
}
```

---

### 5.5 Booking Cancellation
Cancel an existing booking:

```bash
curl -X POST http://localhost:8000/api/bookings/<BOOKING_ID>/cancel \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason": "Event shifted to virtual seminar"}'
```

This writes a `BookingCancelled` event into the outbox and updates fact tables in OLAP downstream.

---

### 5.6 Service & Maintenance Tickets
Report broken equipment or facility maintenance issues:

```bash
curl -X POST http://localhost:8000/api/service-requests \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "resource_id": "res-hall-101",
    "issue_type": "PROJECTOR_FAILURE",
    "description": "Projector lamp blinks red and shuts down after 5 minutes",
    "priority": "HIGH"
  }'
```

---

### 5.7 Event-Driven Notifications
Retrieve recent notifications processed by the consumer:

```bash
curl -X GET "http://localhost:8000/api/notifications?limit=10"
```

**Response:**
```json
[
  {
    "notification_id": "notif-7a2e8c1b",
    "correlation_id": "req-booking-101",
    "user_id": "usr-stud-01",
    "event_type": "BookingConfirmed",
    "channel": "EMAIL_AND_PUSH",
    "subject": "Booking Confirmation: res-hall-101",
    "body": "Your booking for res-hall-101 from 2026-11-10T10:00:00Z to 2026-11-10T12:00:00Z has been confirmed for Operating Systems Midterm Review.",
    "status": "DELIVERED",
    "timestamp": "2026-10-20T21:15:00.000000+00:00"
  }
]
```

---

### 5.8 OLAP Analytics & Aggregation Queries
Query high-speed columnar aggregations powered by DuckDB:

```bash
# 1. Resource utilization rates
curl -X GET http://localhost:8000/api/analytics/utilization

# 2. Peak booking hours
curl -X GET http://localhost:8000/api/analytics/peak-hours

# 3. Departmental demand distribution
curl -X GET http://localhost:8000/api/analytics/departments

# 4. Booking vs cancellation metrics
curl -X GET http://localhost:8000/api/analytics/cancellations
```

---

### 5.9 Audit Log Queries
Facility Admins and System Admins can inspect immutable audit logs:

```bash
curl -X GET "http://localhost:8000/api/analytics/audit-logs?limit=20" \
  -H "Authorization: Bearer $SYSADMIN_TOKEN"
```

---

### 5.10 Dead-Letter Queue (DLQ) & Failure Replay
Inspect poisoned or failed messages:

```bash
# List DLQ events
curl -X GET http://localhost:8000/api/dlq \
  -H "Authorization: Bearer $SYSADMIN_TOKEN"

# Replay an event back to its primary topic
curl -X POST http://localhost:8000/api/dlq/<DLQ_ID>/replay \
  -H "Authorization: Bearer $SYSADMIN_TOKEN"
```

---

## 6. Observability & Health Checks

Verify system availability, P95 latencies, buffer lag, and service status:

```bash
curl -X GET http://localhost:8000/health
```

**Sample Output:**
```json
{
  "system_status": "OPERATIONAL",
  "uptime_seconds": 328.45,
  "availability_pct": 100.0,
  "nfr2_compliant": true,
  "latency_metrics": {
    "count": 45,
    "avg_ms": 6.82,
    "p50_ms": 4.12,
    "p95_ms": 14.50,
    "p99_ms": 28.10,
    "nfr1_compliant": true
  },
  "pipeline_metrics": {
    "outbox_pending_lag": 0,
    "total_bookings_stored": 12,
    "active_notifications_cached": 12,
    "dlq_events_unhandled": 0
  },
  "services": {
    "api_gateway": {"status": "HEALTHY"},
    "auth_service": {"status": "HEALTHY", "rbac": "ENFORCED"},
    "booking_service": {"status": "HEALTHY", "isolation": "SERIALIZABLE/RANGE_EXCLUSION"},
    "event_streaming": {"status": "HEALTHY", "partitions_online": 4},
    "notification_service": {"status": "HEALTHY", "store": "REDIS_KV_ONLINE"},
    "analytics_olap": {"status": "HEALTHY", "engine": "DUCKDB_COLUMNAR"},
    "dlq_manager": {"status": "HEALTHY", "exhausted_events": 0}
  }
}
```

---

## 7. Running Test Suites & Benchmarks

### Full Automated Pytest Suite (23 Passing Tests)
Runs auth, booking, streaming, OLAP analytics, DLQ, and role-based UAT tests:

```bash
source .venv/bin/activate
pytest -v
```

### High-Concurrency Benchmark
Validates **NFR 1** (P95 latency < 500ms) and **NFR 2** (99.5% availability target) across 200 concurrent requests:

```bash
source .venv/bin/activate
python tests/benchmark_load.py
```

---

## 8. API Reference Summary

| Method | Endpoint | Description | Auth Required |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/auth/login` | Authenticate and obtain JWT | No |
| `POST` | `/api/auth/register` | Register new user account | No |
| `GET` | `/api/auth/me` | Fetch active user identity | Yes |
| `GET` | `/api/resources` | List campus resources with filters | No |
| `GET` | `/api/resources/{id}` | Get specific resource metadata | No |
| `POST` | `/api/resources` | Create a campus resource | `facility_admin`, `system_admin` |
| `GET` | `/api/availability` | Check date/time range availability | No |
| `GET` | `/api/availability/{id}/schedule` | Get daily schedule intervals | No |
| `POST` | `/api/bookings` | Book a resource (Transactional Outbox) | Yes |
| `GET` | `/api/bookings` | List user or system bookings | Yes |
| `POST` | `/api/bookings/{id}/cancel` | Cancel booking | Yes (owner or admin) |
| `POST` | `/api/service-requests` | Create maintenance/service ticket | Yes |
| `GET` | `/api/service-requests` | List service requests | No |
| `GET` | `/api/notifications` | List consumed notifications | No |
| `GET` | `/api/dlq` | List dead-letter queue records | `system_admin` |
| `POST` | `/api/dlq/{id}/replay` | Replay dead-letter event | `system_admin` |
| `GET` | `/api/analytics/utilization` | OLAP resource utilization stats | No |
| `GET` | `/api/analytics/peak-hours` | OLAP peak hour demand distribution | No |
| `GET` | `/api/analytics/departments` | OLAP department volume aggregation | No |
| `GET` | `/api/analytics/cancellations` | OLAP cancellation conversion metrics | No |
| `GET` | `/api/analytics/audit-logs` | Immutable audit log trail | `facility_admin`, `system_admin` |
| `GET` | `/health` | System health, metrics & NFR compliance | No |
| `GET` | `/api/metrics` | Observability metrics payload | No |
| `POST` | `/api/relay/flush` | Manually trigger outbox relay flush | No |
