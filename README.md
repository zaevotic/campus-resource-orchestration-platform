# Campus Resource Orchestration Platform

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-009688.svg)](https://fastapi.tiangolo.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.8.0%20(KRaft)-231F20.svg)](https://kafka.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-OLAP%20Analytics-FFF000.svg)](https://duckdb.org/)
[![Redis](https://img.shields.io/badge/Redis-7%20In--Memory-DC382D.svg)](https://redis.io/)
[![Benchmark Status](https://img.shields.io/badge/Benchmark-76%20PASS%20%7C%200%20FAIL-success.svg)](benchmark_report.md)

An enterprise-grade, event-driven polyglot microservice platform for orchestrating university campus resources (lecture halls, smart laboratories, seminar rooms, and specialized equipment). Built with strict ACID transactional guarantees, atomic interval-exclusion conflict prevention, the **Transactional Outbox Pattern**, binary **Apache Avro** event streaming over **Apache Kafka**, ephemeral delivery tracking via **Redis**, and sub-second analytical reporting powered by an embedded **DuckDB OLAP engine**.

---

## Architecture Overview

```
                           +---------------------------------------+
                           |          Client / Web UI              |
                           |       http://localhost:8000           |
                           +---------------------------------------+
                                              |
                                              | HTTP / REST (JWT Auth + Rate Limiting)
                                              v
                           +---------------------------------------+
                           |            FastAPI Gateway            |
                           |       Correlation ID Middleware       |
                           +---------------------------------------+
                                              |
                   +--------------------------+--------------------------+
                   | (Atomic Transaction)                                | (Audit Log & Analytics)
                   v                                                     v
      +-------------------------+                           +-------------------------+
      |  PostgreSQL / SQLite    |                           |      DuckDB OLAP        |
      | - Interval Overlaps     |                           | - Columnar Aggregations |
      | - Resources & Bookings  |                           | - Hourly Peak Hours     |
      | - Transactional Outbox  |                           | - Department Util.      |
      +-------------------------+                           | - Immutable Audit Trail |
                   |                                        +-------------------------+
                   | Background Relay Poller                             ^
                   v                                                     |
      +-------------------------+                                        |
      |   Outbox Relay Worker   |                                        |
      |   (Avro Serialization)  |                                        |
      +-------------------------+                                        |
                   |                                                     |
                   | Avro Binary Messages (partitioned by resource_id)   |
                   v                                                     |
      +---------------------------------------------+                    |
      |          Apache Kafka Broker                |                    |
      |   Topics: bookings, tickets, dlq            |                    |
      +---------------------------------------------+                    |
          |                                     |                        |
          | Consumer Group                      | Consumer Group         |
          v                                     v                        |
+-------------------+                 +-------------------+              |
|   Notification    |                 |     Analytics     |--------------+
|     Consumer      |                 |     Consumer      |
+-------------------+                 +-------------------+
          |
          | Ephemeral Delivery State (TTL)
          v
+-------------------+
|     Redis 7       |
| Key-Value Storage |
+-------------------+
```

---

## Key Highlights & Design Patterns

1. **ACID Booking & Double-Booking Prevention**:
   - Zero double-booking guarantee via transactional interval overlap exclusion (`[start_time, end_time)`).
   - Validated against a 30-client millisecond-level concurrent booking stampede with zero double-allocations.
2. **Transactional Outbox Pattern (`STR-03`)**:
   - Eliminates dual-write anomalies by writing domain changes and event records within the same local database transaction.
3. **Outbox Relay & Binary Avro Schemas (`STR-01`, `STR-04`)**:
   - Background worker polls pending events, validates payloads against strict `.avsc` schemas, and publishes binary Avro payloads to Kafka partitioned by `resource_id`.
4. **Dual Runtime Modes (Zero-Docker vs. Full Docker)**:
   - **Local Fast Mode**: Runs instantaneously using SQLite + DuckDB + in-memory Kafka and Redis mocks (zero external dependencies).
   - **Production Docker Mode**: Runs against real Apache Kafka (KRaft mode), Redis 7, Kafbat Kafka UI, and RedisInsight.
5. **Fault Tolerance, Dead-Letter Queue & Chaos Engineering (`STR-07`)**:
   - Exponential backoff with jitter for transient failures. Poison pills route to a Dead-Letter Queue (DLQ) with an administrative replay API.
6. **Columnar OLAP Analytics (`ANA-01`, `ANA-02`)**:
   - Real-time streaming ingestion into DuckDB for ultra-low latency aggregation queries (utilization rates, peak-hour heatmaps, cancellation metrics).
7. **End-to-End Observability (`ANA-05`, `ANA-06`)**:
   - Distributed correlation ID (`X-Correlation-ID`) propagation across all HTTP requests, outbox records, Kafka headers, and DuckDB audit logs. Real-time P95/P99 latency tracking.

---

## Quickstart

### Prerequisites
- Python 3.11+ (Python 3.11, 3.12, 3.13, 3.14 supported)
- Optional: Docker & Docker Compose (for full Kafka/Redis container mode)

### 1. Installation

```bash
# Clone the repository
git clone https://github.com/zaevo/campus-resource-orchestration-platform.git
cd campus-resource-orchestration-platform

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

### 2. Running the Platform

You can run the platform in **two flexible modes**:

#### Mode A: Fast Zero-Docker Mode (Recommended for quick testing)
Starts the FastAPI backend, interactive web UI, SQLite ACID engine, DuckDB OLAP engine, and high-fidelity in-memory Kafka and Redis emulators:

```bash
./demo.sh start-local
```
Or run directly via Uvicorn:
```bash
python3 -m uvicorn src.app:app --host 0.0.0.0 --port 8000 --reload
```

#### Mode B: Full Production Docker Mode
Spins up Apache Kafka 3.8 (KRaft), Kafka UI, Redis 7, RedisInsight, and the application:

```bash
./demo.sh start-docker
```
Or spin up containers manually:
```bash
docker compose up -d
export USE_REAL_KAFKA=true
export USE_REAL_REDIS=true
python3 -m uvicorn src.app:app --host 0.0.0.0 --port 8000
```

---

## Web Interfaces & Dashboards

When running, the following services are available:

| Interface | URL | Description | Default Credentials / Config |
|-----------|-----|-------------|------------------------------|
| **Campus Web Dashboard** | `http://localhost:8000` | Full Web UI for bookings, inventory, live charts, audit trail, and DLQ replay | Public / Token-based |
| **API Documentation** | `http://localhost:8000/docs` | Interactive Swagger/OpenAPI documentation | - |
| **Health & Metrics** | `http://localhost:8000/health` | Live service health, broker status, and P95 latency metrics | Public |
| **Kafka UI (Kafbat)** | `http://localhost:8082` | Topic inspection, partition offsets, consumer groups, Avro records | *Requires Docker mode* |
| **RedisInsight UI** | `http://localhost:5540` | Real-time key inspection, memory usage, notification TTLs | Add database: host `redis`, port `6379` |

---

## Live Demonstration Scripts

The repository includes an interactive live demonstration toolkit designed for presentations, evaluation, and chaos testing:

### 1. Interactive Control Center (`demo.sh`)
Launch the terminal control menu:
```bash
./demo.sh
```

```
================================================================================
   CAMPUS RESOURCE ORCHESTRATION — LIVE DEMO CONTROL CENTER
================================================================================
  1) Start Platform (Local Fast Mode — SQLite + DuckDB + In-Memory Kafka/Redis)
  2) Start Platform with Docker (Real Kafka + Kafka UI + Real Redis + Redis UI)
  3) Run Interactive Step-by-Step Presenter Walkthrough (Stages 1–5)
  4) Seed Sample Bookings & Tickets (Populates Web UI Charts)
  5) Trigger 30-Way Simultaneous Double-Booking Race
  6) Trigger Chaos Engineering -> Retry -> DLQ -> Replay Demo
  7) Run Live Concurrency & Pipeline Benchmark Suite (--quick, 77 checks)
  8) Show Live /health & Pipeline Metrics JSON
  9) Stop Server / Reset Demo Databases
  0) Exit
```

### 2. Standalone Demo CLI Commands
You can also run specific automation targets directly:

```bash
./demo.sh walkthrough     # Step-by-step presenter walkthrough (pauses between stages)
./demo.sh auto            # Run all 5 demonstration stages automatically
./demo.sh seed            # Pre-seed realistic department bookings and tickets
./demo.sh race            # Fire 30 simultaneous concurrent booking requests
./demo.sh chaos           # Inject simulated consumer failure -> DLQ -> Replay
./demo.sh benchmark       # Run the 76-assertion Concurrency & Pipeline Benchmark
./demo.sh stop            # Stop running server and Docker containers
./demo.sh clean           # Reset local SQLite and DuckDB databases for a clean slate
```

### 3. Python Live Demo Driver (`live_demo.py`)
Run the Python demonstration driver with fine-grained control:
```bash
python3 live_demo.py --auto         # Automated demo
python3 live_demo.py --seed-only    # Seed data only
python3 live_demo.py --race-only    # High-concurrency race test only
python3 live_demo.py --chaos-only   # Chaos & DLQ replay sequence only
```

---

## Demo Walkthrough Stages

The automated demonstration showcases the end-to-end platform capabilities in 5 stages:

1. **Stage 1: Authentication & Role-Based Access Control (RBAC)**
   - Issues JWT tokens for `admin`, `faculty`, `student`, and `maintenance` roles.
   - Demonstrates endpoint authorization and rate-limiting.
2. **Stage 2: Transactional Outbox & Event Streaming**
   - Creates a booking within an ACID transaction.
   - Verifies the outbox record, background polling, binary Avro serialization, and event fan-out into Kafka.
   - Inspects real-time notification delivery state in Redis with TTL.
3. **Stage 3: High-Concurrency Double-Booking Stampede**
   - Fires 30 concurrent booking requests scheduled for the exact same resource and time window.
   - Proves exactly 1 booking succeeds (200 OK) while 29 fail with HTTP 409 Conflict.
4. **Stage 4: Chaos Engineering, DLQ, & Event Replay**
   - Arming chaos controller to simulate network or consumer failures.
   - Observes retry backoff, routing of failed events to the Dead-Letter Queue (DLQ), and manual administrative replay.
5. **Stage 5: Columnar DuckDB OLAP Analytics & Audit Trail**
   - Queries real-time resource utilization, peak-hour distributions, and cancellation rates.
   - Traces end-to-end user actions via the immutable DuckDB audit log filtered by `correlation_id`.

---

## Benchmark & Performance Highlights

A comprehensive benchmark suite (`tests/benchmark_full.py`) validates latency, concurrency, and pipeline invariants:

```bash
python3 tests/benchmark_full.py --quick
```

### Summary Results
- **Assertions Passed**: **76 / 76 (100% Pass Rate)**
- **Double Bookings**: **0** across all concurrency tests
- **Outbox Zero-Loss**: 100% of committed bookings produce processed outbox events
- **DuckDB Sync Rate**: 100% of outbox events ingested into analytical tables

### Latency Matrix (Excerpt)
| Operation | Concurrency | p50 Latency | p95 Latency | Throughput |
|-----------|-------------|-------------|-------------|------------|
| `GET /api/availability` | 1 | 2.01 ms | 3.73 ms | 426.8 rps |
| `GET /api/notifications` (Redis KV) | 1 | 2.58 ms | 5.31 ms | 319.0 rps |
| `POST /api/bookings` (Write + Outbox) | 1 | 6.31 ms | 12.48 ms | 137.6 rps |
| `GET /api/analytics/utilization` (DuckDB) | 1 | 6.89 ms | 10.04 ms | 141.0 rps |
| `GET /api/analytics/peak-hours` (DuckDB) | 1 | 4.45 ms | 10.44 ms | 194.9 rps |

*See [benchmark_report.md](benchmark_report.md) for full metrics, chart breakdowns, and table scaling tests.*

---

## Running Test Suites

Run the full automated test suite using `pytest`:

```bash
# Run all unit, integration, and cross-service tests
pytest -v

# Run specific test suites
pytest tests/test_auth_gateway.py        # Authentication & RBAC tests
pytest tests/test_booking_outbox.py      # Interval conflict & Outbox tests
pytest tests/test_streaming_pipeline.py  # Kafka & Avro event streaming tests
pytest tests/test_analytics_olap.py      # DuckDB OLAP ingestion & queries
pytest tests/test_cross_service.py       # End-to-end integration workflows
pytest tests/test_uat.py                 # User Acceptance Scenarios
```

---

## Project Structure

```
campus-resource-orchestration-platform/
├── demo.sh                       # Interactive live demo & service control CLI
├── live_demo.py                  # Step-by-step presenter walkthrough driver
├── docker-compose.yml            # Docker stack (Kafka 3.8 KRaft, Redis 7, UIs)
├── Dockerfile                    # Container definition for FastAPI platform
├── requirements.txt              # Core Python dependencies
├── benchmark_report.md           # Benchmark results & latency matrix
├── USAGE.md                      # Detailed API usage and reference guide
├── static/                       # Web Dashboard frontend assets
│   ├── index.html                # Single-page control dashboard & live charts
│   ├── app.js                    # Web application logic & API connectors
│   └── styles.css                # Dashboard styling
├── schemas/                      # Apache Avro binary event schemas
│   ├── booking_confirmed.avsc    # Booking confirmed event schema
│   ├── booking_cancelled.avsc    # Booking cancelled event schema
│   ├── service_request.avsc      # Maintenance ticket event schema
│   ├── dlq_event.avsc            # Dead-letter queue event schema
│   └── audit_event.avsc          # Audit event log schema
├── src/
│   ├── app.py                    # FastAPI application entrypoint & lifespan
│   ├── common/                   # Core platform utilities & adapters
│   │   ├── config.py             # Environment configuration & settings
│   │   ├── correlation.py        # Correlation ID middleware & context
│   │   ├── avro_engine.py        # FastAvro encoder/decoder engine
│   │   ├── kafka_broker.py       # AIOKafka producer/consumer manager
│   │   ├── redis_client.py       # Redis client & memory fallback
│   │   └── message_broker.py     # Unified message broker abstraction
│   └── services/
│       ├── auth_gateway/         # JWT authentication, RBAC, rate limiting
│       ├── booking_service/      # Resources, bookings, interval conflict check, Outbox relay
│       ├── service_request/      # Maintenance tickets & saga coordination
│       ├── notification_service/ # Async notification consumer & Redis TTL tracking
│       ├── analytics_service/    # DuckDB consumer, aggregator, & audit logger
│       ├── reliability/          # DLQ manager, retry policies, chaos controller
│       └── monitoring/           # Real-time metrics collector & health endpoints
└── tests/                        # Comprehensive test & benchmark suites
    ├── conftest.py               # Pytest fixtures and test database setup
    ├── test_auth_gateway.py      # Gateway unit tests
    ├── test_booking_outbox.py    # Outbox & booking unit tests
    ├── test_streaming_pipeline.py# Event pipeline integration tests
    ├── test_analytics_olap.py    # DuckDB analytical query tests
    ├── test_cross_service.py     # Cross-service end-to-end tests
    ├── test_uat.py               # User acceptance tests
    ├── benchmark_load.py         # Concurrency load testing suite
    └── benchmark_full.py         # Full 76-assertion benchmark suite
```

---

## License

This project is licensed under the MIT License.
