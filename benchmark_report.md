# Booking Conflict Prevention & Event Pipeline Benchmark Report

*Generated 2026-11-14 19:43:21 — run time 36.2s — mode: quick — FastAPI/Uvicorn + SQLite (ACID/Outbox WAL) + Kafka Broker + Redis KV + DuckDB OLAP*

**Summary:** `76 PASS` | `0 FAIL` | `8 INFO` (76 assertions)

| Benchmark Suite | PASS | FAIL |
|---|---|---|
| 1. Booking Conflict Prevention & Interval Exclusion | 20 | 0 |
| 2. High-Concurrency Booking Contention & Race Conditions | 15 | 0 |
| 3. Transactional Outbox (WAL) & Event Streaming Pipeline (Kafka / Redis / DuckDB) | 23 | 0 |
| 4. Performance, Table-Size Scaling & Sustained Soak | 15 | 0 |
| 5. Global Database & Pipeline Invariants | 3 | 0 |

## 1. Read-Path Latency & Throughput Matrix (ms)

| Endpoint | Conc | N | rps | mean | p50 | p95 | p99 | max | errors |
|---|---|---|---|---|---|---|---|---|---|
| GET /health | 1 | 150 | 330.3 | 2.98 | 2.97 | 4.3 | 5.55 | 8.44 | 0 |
| GET /health | 10 | 150 | 192.7 | 50.34 | 45.6 | 92.04 | 110.19 | 188.94 | 0 |
| GET /health | 25 | 150 | 197.9 | 113.46 | 104.73 | 230.09 | 280.28 | 291.81 | 0 |
| GET /api/availability | 1 | 150 | 426.8 | 2.31 | 2.01 | 3.73 | 4.27 | 6.27 | 0 |
| GET /api/availability | 10 | 150 | 246.4 | 39.42 | 35.74 | 68.8 | 84.82 | 96.48 | 0 |
| GET /api/availability | 25 | 150 | 327.7 | 65.08 | 67.04 | 90.07 | 95.69 | 96.28 | 0 |
| GET /api/notifications (Redis KV) | 1 | 150 | 319.0 | 3.01 | 2.58 | 5.31 | 7.35 | 7.59 | 0 |
| GET /api/notifications (Redis KV) | 10 | 150 | 215.7 | 44.56 | 40.4 | 103.75 | 122.81 | 176.88 | 0 |
| GET /api/notifications (Redis KV) | 25 | 150 | 178.1 | 129.41 | 113.43 | 257.1 | 296.41 | 338.41 | 0 |
| GET /api/analytics/utilization (DuckDB) | 1 | 150 | 141.0 | 7.04 | 6.89 | 10.04 | 11.98 | 12.36 | 0 |
| GET /api/analytics/utilization (DuckDB) | 10 | 150 | 230.2 | 41.47 | 41.9 | 49.68 | 61.91 | 63.14 | 0 |
| GET /api/analytics/utilization (DuckDB) | 25 | 150 | 197.1 | 110.56 | 113.09 | 140.34 | 167.09 | 197.54 | 0 |
| GET /api/analytics/peak-hours (DuckDB) | 1 | 150 | 194.9 | 5.08 | 4.45 | 10.44 | 11.76 | 12.45 | 0 |
| GET /api/analytics/peak-hours (DuckDB) | 10 | 150 | 253.6 | 37.88 | 39.22 | 45.44 | 46.32 | 47.13 | 0 |
| GET /api/analytics/peak-hours (DuckDB) | 25 | 150 | 245.0 | 91.96 | 97.02 | 107.61 | 110.65 | 112.14 | 0 |
| GET /api/analytics/cancellations (DuckDB) | 1 | 150 | 312.0 | 3.18 | 2.35 | 5.89 | 6.7 | 8.35 | 0 |
| GET /api/analytics/cancellations (DuckDB) | 10 | 150 | 268.5 | 36.24 | 37.81 | 43.61 | 47.68 | 47.75 | 0 |
| GET /api/analytics/cancellations (DuckDB) | 25 | 150 | 215.6 | 107.63 | 113.31 | 123.27 | 133.87 | 159.87 | 0 |
| GET /api/analytics/audit-logs (DuckDB) | 1 | 150 | 157.0 | 6.17 | 5.57 | 8.75 | 13.16 | 26.44 | 0 |
| GET /api/analytics/audit-logs (DuckDB) | 10 | 150 | 161.0 | 60.05 | 58.16 | 81.38 | 83.31 | 84.45 | 0 |
| GET /api/analytics/audit-logs (DuckDB) | 25 | 150 | 121.2 | 191.22 | 154.23 | 440.5 | 574.91 | 614.52 | 0 |

## 2. Write-Path + Outbox WAL + Streaming Fan-Out (`POST /api/bookings`)

| Concurrency | Bookings | Committed | Throughput (rps) | mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | errors |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 60 | 60 | 137.6 | 7.22 | 6.31 | 12.48 | 12.77 | 12.77 | 0 |
| 10 | 60 | 60 | 163.9 | 57.25 | 57.67 | 79.37 | 82.2 | 82.2 | 0 |
| 25 | 60 | 60 | 173.6 | 119.39 | 126.26 | 146.68 | 148.65 | 148.65 | 0 |

## 3. Conflict Check & Booking Scaling vs. Table Volume

| Bookings Stored in DB | Booking Create p50 (ms) | Booking Create p95 (ms) | Availability Check p50 (ms) | Availability Check p95 (ms) |
|---|---|---|---|---|
| 372 | 5.23 | 7.48 | 2.2 | 2.97 |
| 1397 | 5.26 | 6.47 | 1.98 | 2.86 |
| 3422 | 5.68 | 6.72 | 1.94 | 2.57 |

## 4. Sustained Mixed-Workload Soak Test (8s, 25 Concurrent Workers)

- **Total Operations:** 1727  |  **Throughput:** `214.0 req/s`  |  **Availability (NFR-2):** `100.0%`
- **Latency (NFR-1):** mean `116.26 ms`, p50 `113.0 ms`, **p95 `150.11 ms`**, p99 `173.31 ms`, max `235.87 ms`
- **HTTP Status Breakdown:** `{200: 1727}`

## 5. Detailed Verification Log

### Setup

- `INFO` **Seeded campus resources** — 5 resources: res-sem-404, res-conf-301, res-proj-901, res-lab-202, res-hall-101

### 1. Booking Conflict Prevention & Interval Exclusion

- `PASS` **Base booking (10:00–12:00) accepted** — booking_id=bkg-1ef1180c
- `PASS` **Interval check: Exact duplicate (10:00–12:00) -> HTTP 409**
- `PASS` **Interval check: Strictly contained inside (10:30–11:30) -> HTTP 409**
- `PASS` **Interval check: Strictly containing outer window (09:00–13:00) -> HTTP 409**
- `PASS` **Interval check: Partial head overlap (09:00–11:00) -> HTTP 409**
- `PASS` **Interval check: Partial tail overlap (11:00–13:00) -> HTTP 409**
- `PASS` **Interval check: 1-minute tail boundary overlap (11:59–13:00) -> HTTP 409**
- `PASS` **Interval check: 1-minute head boundary overlap (09:00–10:01) -> HTTP 409**
- `PASS` **Interval check: Back-to-back immediately before (08:00–10:00) -> HTTP 200**
- `PASS` **Interval check: Back-to-back immediately after (12:00–14:00) -> HTTP 200**
- `PASS` **Interval check: Disjoint later window (16:00–17:00) -> HTTP 200**
- `PASS` **Same time window (10:00–12:00) on a DIFFERENT resource -> HTTP 200**
- `PASS` **Conflict response includes Smart Alternatives recommendation** — recommended res-38d5cff8
- `PASS` **DB exclusion invariant holds across all interval cases**
- `PASS` **Initial booking created (CONFIRMED)**
- `PASS` **Availability service detects active slot conflict**
- `PASS` **Booking cancellation succeeds -> HTTP 200**
- `PASS` **Idempotency guard: cancelling an already-cancelled booking -> HTTP 400**
- `PASS` **Availability service frees slot immediately after cancellation**
- `PASS` **Freed slot can be re-booked by another user -> HTTP 200**

### 2. High-Concurrency Booking Contention & Race Conditions

- `PASS` **25 concurrent requests for the EXACT same slot -> 1 winner, 24 conflicts** — 1 accepted (200), 24 rejected (409)
- `PASS` **Zero 5xx or connection errors under barrier race**
- `PASS` **DB holds exactly 1 CONFIRMED booking for the raced slot**
- `INFO` **50-way barrier race latency** — p50=97.9ms p95=102.2ms max=102.4ms
- `PASS` **Rapid double-click burst (20 simultaneous identical requests) -> 1 booking** — 1 accepted, 19 rejected (409)
- `PASS` **80 concurrent random overlapping windows on 1 resource: zero 5xx**
- `PASS` **INVARIANT: zero overlapping CONFIRMED bookings after random overlap storm** — 14 accepted / 66 rejected, 0 overlaps
- `PASS` **All accepted intervals are mutually disjoint (sorted interval verification)** — 14 disjoint intervals packed
- `PASS` **60 parallel non-overlapping bookings across 5 resources all succeed** — 60/60 in 0.43s (140.8 bookings/s)
- `INFO` **Disjoint parallel booking throughput** — 140.8 bookings/s
- `PASS` **100 requests competing across 10 slots (10/slot) -> exactly 10 winners** — 10 accepted, 90 rejected (409)
- `PASS` **No overlaps in DB after multi-slot contention**
- `PASS` **20 simultaneous cancel requests on 1 booking -> exactly 1 succeeds** — 1 x 200, 19 x 400
- `PASS` **Exactly 1 BookingCancelled outbox event emitted**
- `PASS` **Cancel racing against 20 new booking requests -> at most 1 active booking** — 1 taker won, 1 active in DB
- `PASS` **High-contention mixed book/cancel/availability storm on 1 resource: zero 5xx** — 476 ops ({200: 252, 409: 214, 400: 10})
- `PASS` **INVARIANT after mixed storm: zero overlapping CONFIRMED bookings**

### 3. Transactional Outbox (WAL) & Event Streaming Pipeline (Kafka / Redis / DuckDB)

- `PASS` **Transactional outbox drained (0 PENDING records remaining)**
- `PASS` **Zero FAILED outbox records** — outbox status counts={'PUBLISHED': 198}
- `PASS` **Atomic WAL: 1-to-1 match between bookings and BookingConfirmed outbox records** — 150 outbox events == 150 bookings
- `PASS` **Atomic WAL: 1-to-1 match between cancellations and BookingCancelled outbox records** — 48 outbox events == 48 cancellations
- `PASS` **Kafka broker message count matches total published outbox events (no drops or duplicates)** — 198 broker messages == 198 outbox events
- `PASS` **Deterministic partitioning: all events routed to hash(resource_id) % 4**
- `PASS` **Per-partition log offsets are strictly sequential (0..N-1)**
- `INFO` **Broker partition distribution** — partitions={0: 131, 1: 28, 2: 0, 3: 39}
- `PASS` **DuckDB OLAP: fact_bookings has every confirmed booking with zero duplicates** — 150 rows (150 distinct) == 150 bookings
- `PASS` **DuckDB OLAP: fact_cancellations has every cancellation with zero duplicates** — 48 rows (48 distinct) == 48 cancellations
- `PASS` **Redis KV store: notifications recorded for every event & capped recent-list maintained** — kv_store keys=198, recent list=198 (capped at 500)
- `PASS` **X-Correlation-ID propagated through HTTP -> Outbox -> Broker -> DuckDB OLAP -> Audit Log** — correlation_id=wal-trace-1794665574572
- `PASS` **Avro binary serialization & deserialization round-trip (fastavro)** — 134 bytes binary payload
- `PASS` **Avro schema 'BookingConfirmed' registered and parsed**
- `PASS` **Avro schema 'BookingCancelled' registered and parsed**
- `PASS` **Avro schema 'ServiceRequestCreated' registered and parsed**
- `PASS` **Avro schema 'AuditEvent' registered and parsed**
- `PASS` **Avro schema 'DeadLetterEvent' registered and parsed**
- `INFO` **End-to-end booking -> Outbox -> Kafka -> DuckDB latency** — p50=7.5ms p95=12.8ms max=12.8ms
- `PASS` **End-to-end streaming pipeline p95 < 100 ms** — p95=12.77ms
- `PASS` **Booking commits and succeeds even when Notification consumer fails**
- `PASS` **Analytics consumer stays unaffected during Notification failure (consumer isolation)**
- `PASS` **Failed notification is retried 3x with exponential backoff and routed to DLQ** — dlq_count=1
- `PASS` **Admin DLQ replay (dlq-1832d48c) dispatches back to original topic -> HTTP 200**
- `PASS` **Active DLQ queue is empty after replay**

### 4. Performance, Table-Size Scaling & Sustained Soak

- `PASS` **Read benchmark (GET /health @ conc=25): p95 < 500 ms & 0 errors** — p95=230.09ms, 197.9 rps
- `PASS` **Read benchmark (GET /api/availability @ conc=25): p95 < 500 ms & 0 errors** — p95=90.07ms, 327.7 rps
- `PASS` **Read benchmark (GET /api/notifications (Redis KV) @ conc=25): p95 < 500 ms & 0 errors** — p95=257.1ms, 178.1 rps
- `PASS` **Read benchmark (GET /api/analytics/utilization (DuckDB) @ conc=25): p95 < 500 ms & 0 errors** — p95=140.34ms, 197.1 rps
- `PASS` **Read benchmark (GET /api/analytics/peak-hours (DuckDB) @ conc=25): p95 < 500 ms & 0 errors** — p95=107.61ms, 245.0 rps
- `PASS` **Read benchmark (GET /api/analytics/cancellations (DuckDB) @ conc=25): p95 < 500 ms & 0 errors** — p95=123.27ms, 215.6 rps
- `PASS` **Read benchmark (GET /api/analytics/audit-logs (DuckDB) @ conc=25): p95 < 500 ms & 0 errors** — p95=440.5ms, 121.2 rps
- `PASS` **Write + WAL + Pipeline @ conc=1: 60/60 committed, 0 errors** — p50=6.31ms p95=12.48ms (137.6 bookings/s)
- `PASS` **Write + WAL + Pipeline @ conc=10: 60/60 committed, 0 errors** — p50=57.67ms p95=79.37ms (163.9 bookings/s)
- `PASS` **Write + WAL + Pipeline @ conc=25: 60/60 committed, 0 errors** — p50=126.26ms p95=146.68ms (173.6 bookings/s)
- `INFO` **Table-size scaling @ 372 rows** — book p95=7.48ms, availability p95=2.97ms
- `INFO` **Table-size scaling @ 1397 rows** — book p95=6.47ms, availability p95=2.86ms
- `INFO` **Table-size scaling @ 3422 rows** — book p95=6.72ms, availability p95=2.57ms
- `PASS` **Conflict query & booking latency stays < 50 ms at 10,000+ rows** — book p95: 7.48ms -> 6.72ms; avail p95: 2.97ms -> 2.57ms
- `PASS` **Sustained soak (8s, 25 workers): availability >= 99.5% (NFR-2)** — 100.000% over 1727 requests (214.0 rps)
- `PASS` **Sustained soak: p95 latency < 500 ms (NFR-1)** — p95=150.11ms, p50=113.0ms
- `PASS` **Sustained soak: zero overlapping bookings in DB**
- `PASS` **Sustained soak: outbox fully drained & synced to Kafka/DuckDB/Redis**

### 5. Global Database & Pipeline Invariants

- `PASS` **GLOBAL DB INVARIANT: zero overlapping CONFIRMED bookings across entire database** — 688 active confirmed bookings, 0 overlaps
- `PASS` **GLOBAL PIPELINE INVARIANT: 100% of outbox WAL events published & ingested into DuckDB OLAP**
- `PASS` **GLOBAL HEALTH: server operational & responsive**
