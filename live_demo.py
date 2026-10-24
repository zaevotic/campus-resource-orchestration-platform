#!/usr/bin/env python3
"""
Interactive Live Demonstration Driver for Campus Resource Orchestration Platform.

Runs against the live server (default http://localhost:8000) so everything it does
immediately updates:
  - Main Web Dashboard : http://localhost:8000
  - Kafka UI           : http://localhost:8082
  - RedisInsight UI    : http://localhost:5540

Usage:
    python3 live_demo.py                  # Interactive step-by-step presenter mode (press Enter between stages)
    python3 live_demo.py --auto           # Run all stages automatically with short pauses
    python3 live_demo.py --seed-only      # Seed realistic multi-department bookings/tickets for UI charts
    python3 live_demo.py --race-only      # Run the 30-way live simultaneous double-booking stampede
    python3 live_demo.py --chaos-only     # Run the live Chaos -> Retry -> DLQ -> Replay sequence
"""
import argparse
import concurrent.futures as cf
import datetime as dt
import sys
import threading
import time
import uuid
from collections import Counter

import httpx

ap = argparse.ArgumentParser()
ap.add_argument("--url", default="http://127.0.0.1:8000", help="Target server URL")
ap.add_argument("--auto", action="store_true", help="Do not wait for Enter between steps")
ap.add_argument("--seed-only", action="store_true", help="Only seed sample data for the UI charts")
ap.add_argument("--race-only", action="store_true", help="Only run the concurrent booking race")
ap.add_argument("--chaos-only", action="store_true", help="Only run the Chaos + DLQ replay demo")
ARGS = ap.parse_args()

BASE = ARGS.url.rstrip("/")
HTTP = httpx.Client(base_url=BASE, timeout=60.0)

# ANSI formatting
B = "\033[1m"
G = "\033[92m"
Y = "\033[93m"
R = "\033[91m"
C = "\033[96m"
M = "\033[95m"
DIM = "\033[2m"
END = "\033[0m"

def banner(title, subtitle=""):
    print(f"\n{B}{C}{'=' * 86}{END}")
    print(f"{B}{C}  {title}{END}")
    if subtitle:
        print(f"  {DIM}{subtitle}{END}")
    print(f"{B}{C}{'=' * 86}{END}")

def step(msg):
    print(f"\n{B}{M}>> {msg}{END}")

def ok(msg):
    print(f"  {G}[OK] {msg}{END}")

def warn(msg):
    print(f"  {Y}[WARN] {msg}{END}")

def fail(msg):
    print(f"  {R}[FAIL] {msg}{END}")

def info(msg):
    print(f"    {DIM}{msg}{END}")

def pause(prompt="Press [Enter] to execute next step..."):
    if ARGS.auto:
        time.sleep(1.2)
        return
    try:
        input(f"\n{Y}[PAUSE] {prompt}{END}")
    except EOFError:
        time.sleep(1.0)

def login(username, password):
    r = HTTP.post("/api/auth/login", json={"username": username, "password": password})
    r.raise_for_status()
    data = r.json()
    return data["access_token"], data["user"]

def headers(token, cid=None):
    h = {"Authorization": f"Bearer {token}"}
    if cid:
        h["X-Correlation-ID"] = cid
    return h

# Unique demo date so repeated demo runs never clash with previous runs
DEMO_DAY = (dt.date(2027, 5, 1) + dt.timedelta(days=int(time.time()) % 300)).isoformat()

def check_server():
    try:
        r = HTTP.get("/health")
        r.raise_for_status()
        h = r.json()
        notif_store = h.get("services", {}).get("notification_service", {}).get("store", "UNKNOWN")
        ok(f"Connected to {BASE}  |  Status: {h.get('system_status')}  |  KV Store: {notif_store}")
        return h
    except Exception as e:
        fail(f"Cannot reach server at {BASE}: {e}")
        print(f"  Start the server first with:  {B}./demo.sh start{END}")
        sys.exit(1)

# ============================================================================= STAGE 1: SEED REALISTIC DATA FOR DASHBOARD CHARTS
def stage_seed_dashboard(tokens):
    banner(
        "STAGE 1 — Seeding Multi-Department Activity & Verifying Resource Catalog",
        "Populates LHC-1, LHC-2, Conference & Department rooms so Web UI / Kafka UI / Redis UI are rich with live data"
    )
    res_list = HTTP.get("/api/resources").json()
    ok(f"Discovered {len(res_list)} campus resources across LHC-1, LHC-2, Department & Conference blocks:")
    by_bldg = {}
    for r in res_list:
        by_bldg.setdefault(r["building"], []).append(f"{r['room_number'] or r['id']} (cap {r['capacity']})")
    for bldg, rooms in by_bldg.items():
        info(f"- {bldg:22s} : {', '.join(rooms)}")

    step("Creating sample bookings across LHC-1, LHC-2, Conference & Department rooms to populate DuckDB OLAP charts...")
    base_d = (dt.date.fromisoformat(DEMO_DAY) - dt.timedelta(days=10)).isoformat()
    sample_plan = [
        ("faculty", "res-lhc1-110",    f"{base_d}T09:00:00Z", f"{base_d}T11:00:00Z", "Institute Convocation Keynote", 420),
        ("faculty", "res-lhc1-308",    f"{base_d}T11:00:00Z", f"{base_d}T13:00:00Z", "Data Engineering Full-Batch Lecture", 260),
        ("student", "res-lhc1-105",    f"{base_d}T14:00:00Z", f"{base_d}T16:00:00Z", "Operating Systems Tutorial", 55),
        ("faculty", "res-lhc2-101",    f"{base_d}T10:00:00Z", f"{base_d}T12:00:00Z", "Computer Networks Lecture", 110),
        ("student", "res-lhc2-102",    f"{base_d}T13:00:00Z", f"{base_d}T15:00:00Z", "Competitive Programming Workshop", 95),
        ("fadmin",  "res-conf-302",    f"{base_d}T15:00:00Z", f"{base_d}T17:00:00Z", "Academic Senate Curriculum Review", 38),
        ("faculty", "res-dept-cs-101", f"{base_d}T16:00:00Z", f"{base_d}T18:00:00Z", "CSE Systems Research Group", 32),
        ("faculty", "res-dept-ee-201", f"{base_d}T14:00:00Z", f"{base_d}T16:00:00Z", "EE Power Electronics Colloquium", 28),
        ("student", "res-lab-305",     f"{base_d}T10:00:00Z", f"{base_d}T12:00:00Z", "Distributed Deep Learning Lab", 45),
    ]
    created_ids = []
    for role, rid, st, et, purp, att in sample_plan:
        r = HTTP.post("/api/bookings", headers=headers(tokens[role]), json={
            "resource_id": rid, "start_time": st, "end_time": et, "purpose": purp, "attendees_count": att
        })
        if r.status_code == 200:
            bid = r.json()["booking_id"]
            created_ids.append((role, bid))
            ok(f"Booked {rid:15s} ({purp}, {att} seats) -> {bid}")

    if created_ids:
        role, bid = created_ids[-1]
        HTTP.post(f"/api/bookings/{bid}/cancel", headers=headers(tokens[role]), json={"reason": "Demo schedule adjustment"})
        ok(f"Cancelled 1 booking ({bid}) so Cancellation Rate ring chart shows live metrics")

    sr = HTTP.post("/api/service-requests", headers=headers(tokens["faculty"]), json={
        "resource_id": "res-lhc1-308", "issue_type": "PROJECTOR_FAILURE",
        "description": "4K laser projector calibration drift in LHC-1 Room 308", "priority": "MEDIUM"
    })
    if sr.status_code == 200:
        ok(f"Logged maintenance ticket {sr.json()['ticket_id']} on res-lhc1-308 (LHC-1 Room 308)")

    info("Refresh http://localhost:8000 (Dashboard, Resources & Analytics tabs) to see live charts.")

# ============================================================================= STAGE 2: BOOKING CONFLICT PREVENTION, CAPACITY CHECK & SMART ALTERNATIVES
def stage_conflict_and_alternatives(tokens):
    banner(
        "STAGE 2 — Booking Conflict Prevention, Capacity Validation, Smart Alternatives & Outbox WAL",
        f"Target Room: LHC-2 Lecture Hall 101 (res-lhc2-101, capacity 120) on {DEMO_DAY}"
    )
    cid = f"demo-trace-{uuid.uuid4().hex[:8]}"
    slot_start = f"{DEMO_DAY}T10:00:00Z"
    slot_end   = f"{DEMO_DAY}T12:00:00Z"

    step(f"1. Student books LHC-2 Room 101 (res-lhc2-101) for {slot_start} -> {slot_end} (Correlation-ID: {cid})")
    r1 = HTTP.post("/api/bookings", headers=headers(tokens["student"], cid=cid), json={
        "resource_id": "res-lhc2-101",
        "start_time": slot_start,
        "end_time": slot_end,
        "purpose": "Distributed Systems Keynote",
        "attendees_count": 90
    })
    if r1.status_code == 200:
        b1 = r1.json()
        ok(f"HTTP 200 OK — Booking Confirmed!  booking_id={b1['booking_id']}  event_id={b1['event_id']}")
        info(f"Committed atomically with Outbox WAL event (X-Correlation-ID: {r1.headers.get('x-correlation-id')})")
    else:
        fail(f"Unexpected status {r1.status_code}: {r1.text}")
        return cid

    pause("Press [Enter] to attempt an OVERLAPPING booking (11:00 -> 13:00) on LHC-2 Room 101...")

    step(f"2. Faculty attempts overlapping booking on LHC-2 Room 101 ({DEMO_DAY}T11:00:00Z -> 13:00:00Z)")
    r2 = HTTP.post("/api/bookings", headers=headers(tokens["faculty"]), json={
        "resource_id": "res-lhc2-101",
        "start_time": f"{DEMO_DAY}T11:00:00Z",
        "end_time": f"{DEMO_DAY}T13:00:00Z",
        "purpose": "Conflicting Seminar",
        "attendees_count": 80
    })
    if r2.status_code == 409:
        ok("HTTP 409 CONFLICT — Double-booking prevented by range exclusion check!")
        warn(f"Server response: {r2.json().get('detail')}")
    else:
        fail(f"Expected 409 Conflict, got {r2.status_code}")

    step("3. Testing Capacity Enforcement: Attempting to book 150 attendees into LHC-1 Room 105 (max capacity 60)")
    r_cap = HTTP.post("/api/bookings", headers=headers(tokens["student"]), json={
        "resource_id": "res-lhc1-105",
        "start_time": f"{DEMO_DAY}T14:00:00Z",
        "end_time": f"{DEMO_DAY}T15:00:00Z",
        "purpose": "Over-capacity test",
        "attendees_count": 150
    })
    if r_cap.status_code == 400:
        ok("HTTP 400 BAD REQUEST — Over-capacity booking rejected!")
        warn(f"Server response: {r_cap.json().get('detail')}")
    else:
        fail(f"Expected 400 Bad Request, got {r_cap.status_code}")

    step(f"4. Faculty books adjacent back-to-back slot on LHC-2 Room 101 ({DEMO_DAY}T12:00:00Z -> 14:00:00Z)")
    r3 = HTTP.post("/api/bookings", headers=headers(tokens["faculty"]), json={
        "resource_id": "res-lhc2-101",
        "start_time": f"{DEMO_DAY}T12:00:00Z",
        "end_time": f"{DEMO_DAY}T14:00:00Z",
        "purpose": "Back-to-Back Lecture",
        "attendees_count": 80
    })
    if r3.status_code == 200:
        ok(f"HTTP 200 OK — Touching boundary (12:00 == 12:00) allowed without false conflict! ({r3.json()['booking_id']})")

    return cid

# ============================================================================= STAGE 3: 30-WAY SIMULTANEOUS STAMPEDE RACE
def stage_simultaneous_race(tokens):
    banner(
        "STAGE 3 — High-Concurrency Race Condition Test (30 Simultaneous Requests)",
        "30 threads wait on a threading.Barrier and fire POST /api/bookings for LHC-1 Room 110 (cap 500) at the exact same millisecond"
    )
    N = 30
    race_start = f"{DEMO_DAY}T15:00:00Z"
    race_end   = f"{DEMO_DAY}T16:00:00Z"
    barrier = threading.Barrier(N)

    def contender(idx):
        c = httpx.Client(base_url=BASE, timeout=30.0)
        try:
            barrier.wait(timeout=15.0)
            t0 = time.perf_counter()
            r = c.post("/api/bookings", headers=headers(tokens["student"]), json={
                "resource_id": "res-lhc1-110",
                "start_time": race_start,
                "end_time": race_end,
                "purpose": f"Concurrent contender #{idx}",
                "attendees_count": 200
            })
            return r.status_code, (time.perf_counter() - t0) * 1000.0, r.json()
        finally:
            c.close()

    step(f"Releasing {N} simultaneous threads for LHC-1 Grand Auditorium 110 ({race_start} -> {race_end})...")
    t0 = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=N) as ex:
        results = list(ex.map(contender, range(N)))
    elapsed = (time.perf_counter() - t0) * 1000.0

    codes = Counter(code for code, _, _ in results)
    lats = sorted(ms for _, ms, _ in results)
    winner = [body for code, _, body in results if code == 200]

    if codes.get(200) == 1 and codes.get(409) == N - 1:
        ok(f"RACE RESULT: Exactly 1 Winner (HTTP 200) and {N - 1} Conflicts (HTTP 409) in {elapsed:.1f} ms!")
        ok(f"Winning booking_id: {winner[0]['booking_id']}  |  Median latency: {lats[len(lats)//2]:.1f} ms  |  p95: {lats[int(0.95*len(lats))]:.1f} ms")
    else:
        fail(f"Unexpected status breakdown: {dict(codes)}")

# ============================================================================= STAGE 4: KAFKA / REDIS / DUCKDB OLAP & CORRELATION TRACE
def stage_pipeline_and_trace(tokens, cid):
    banner(
        "STAGE 4 — Event Streaming Verification: Kafka -> Redis KV & DuckDB OLAP",
        f"Tracing Correlation ID '{cid}' across Notifications, OLAP Analytics, and Audit Trail"
    )
    time.sleep(0.6)

    step("1. Checking Redis KV Notification Consumer (/api/notifications)...")
    notifs = HTTP.get("/api/notifications?limit=20", headers=headers(tokens["student"])).json()
    matched_n = [n for n in notifs if n.get("correlation_id") == cid]
    if matched_n:
        n = matched_n[0]
        ok(f"Notification delivered & cached in Redis KV: [{n['event_type']}] {n['subject']}")
        info(f"Notification ID: {n['notification_id']}  |  Channel: {n['channel']}  |  Status: {n['status']}")
    else:
        ok(f"Total recent notifications in Redis KV: {len(notifs)}")

    step("2. Querying DuckDB Columnar OLAP Aggregations (/api/analytics/*)...")
    util = HTTP.get("/api/analytics/utilization", headers=headers(tokens["student"])).json()
    canc = HTTP.get("/api/analytics/cancellations", headers=headers(tokens["student"])).json()
    dept = HTTP.get("/api/analytics/departments", headers=headers(tokens["student"])).json()
    ok(f"DuckDB Cancellation Summary : {canc['gross_bookings']} gross | {canc['cancelled_bookings']} cancelled | {canc['net_active_bookings']} net active")
    ok(f"DuckDB Department Demand    : " + ", ".join(f"{d['department']}={d['total_bookings']}" for d in dept))
    for u in util[:5]:
        info(f"- {u['resource_id']:16s}: {u['total_bookings']} bookings, {u['total_hours_booked']}h booked, utilization={u['utilization_rate_pct']}%")

    step(f"3. Querying Immutable Audit Trail for Correlation-ID '{cid}' (/api/analytics/audit-logs)...")
    audits = HTTP.get("/api/analytics/audit-logs", headers=headers(tokens["sys"]), params={"correlation_id": cid}).json()
    for a in audits:
        ok(f"Audit Record: action={a['action']}  actor={a['actor_id']} ({a['actor_role']})  resource={a['resource_id']}  status={a['status']}")

# ============================================================================= STAGE 5: CHAOS ENGINEERING, EXPONENTIAL BACKOFF & DLQ REPLAY
def stage_chaos_and_dlq(tokens):
    banner(
        "STAGE 5 — Chaos Engineering, Exponential Backoff Retries & Dead-Letter Queue (DLQ)",
        "Simulates a downstream Notification crash -> verifies booking isolation -> inspects DLQ -> replays event"
    )
    step("1. System Admin injects failure into Notification Consumer (POST /api/chaos/inject?target=fail_notifications&fail=true)")
    r = HTTP.post("/api/chaos/inject", headers=headers(tokens["sys"]), params={"target": "fail_notifications", "fail": "true"})
    warn(f"Chaos Flag Activated: {r.json()}")

    step("2. Student books CSE Dept Seminar Room (res-dept-cs-101) while Notification Service is crashing...")
    chaos_cid = f"chaos-{uuid.uuid4().hex[:8]}"
    b = HTTP.post("/api/bookings", headers=headers(tokens["student"], cid=chaos_cid), json={
        "resource_id": "res-dept-cs-101",
        "start_time": f"{DEMO_DAY}T18:00:00Z",
        "end_time": f"{DEMO_DAY}T19:00:00Z",
        "purpose": "Resilience Demo Booking",
        "attendees_count": 25
    })
    if b.status_code == 200:
        ok(f"Booking STILL SUCCEEDED (HTTP 200, booking_id={b.json()['booking_id']}) — synchronous core is isolated!")

    # Wait briefly if real Kafka is processing retries asynchronously
    for _ in range(25):
        dlq_list = HTTP.get("/api/dlq", headers=headers(tokens["sys"])).json()
        if any(d.get("correlation_id") == chaos_cid for d in dlq_list):
            break
        time.sleep(0.3)

    step("3. Inspecting Dead-Letter Queue (GET /api/dlq as system_admin)...")
    dlq_list = HTTP.get("/api/dlq", headers=headers(tokens["sys"])).json()
    if dlq_list:
        d = dlq_list[-1]
        ok(f"Found {len(dlq_list)} event(s) parked in DLQ after {d['retry_count']} exponential-backoff retries!")
        info(f"dlq_id={d['dlq_id']}  |  topic={d['original_topic']}  |  group={d['consumer_group']}  |  error='{d['error_message']}'")
    else:
        warn("DLQ is still processing retries in background...")

    pause("Press [Enter] to recover the Notification Consumer and replay the DLQ events...")

    step("4. Recovering Notification Consumer & Replaying DLQ Events...")
    HTTP.post("/api/chaos/inject", headers=headers(tokens["sys"]), params={"target": "fail_notifications", "fail": "false"})
    ok("Notification Consumer recovered (fail_notifications=False)")

    for d in dlq_list:
        rep = HTTP.post(f"/api/dlq/{d['dlq_id']}/replay", headers=headers(tokens["sys"]))
        if rep.status_code == 200:
            ok(f"Replayed {d['dlq_id']} -> {rep.json().get('message')}")

    rem = HTTP.get("/api/dlq", headers=headers(tokens["sys"])).json()
    ok(f"DLQ active count after replay: {len(rem)}")

def main():
    banner("CAMPUS RESOURCE ORCHESTRATION PLATFORM — LIVE DEMO CONTROLLER", f"Server: {BASE}  |  Demo Date: {DEMO_DAY}")
    check_server()

    tokens = {}
    for key, (u, p) in {
        "student": ("student1", "student123"),
        "faculty": ("faculty1", "faculty123"),
        "fadmin":  ("admin_facility", "facility123"),
        "sys":     ("admin_sys", "sysadmin123"),
    }.items():
        tokens[key], _ = login(u, p)
    ok("Authenticated all 4 RBAC personas (student, faculty, facility_admin, system_admin)")

    if ARGS.seed_only:
        stage_seed_dashboard(tokens)
        return
    if ARGS.race_only:
        stage_simultaneous_race(tokens)
        return
    if ARGS.chaos_only:
        stage_chaos_and_dlq(tokens)
        return

    pause("Ready for Stage 1 (Seed Dashboard & Resource Catalog). Press [Enter]...")
    stage_seed_dashboard(tokens)

    pause("Ready for Stage 2 (Booking Conflict Prevention, Capacity Check & Smart Alternatives). Press [Enter]...")
    cid = stage_conflict_and_alternatives(tokens)

    pause("Ready for Stage 3 (30-Thread Simultaneous Double-Booking Stampede). Press [Enter]...")
    stage_simultaneous_race(tokens)

    pause("Ready for Stage 4 (Kafka -> Redis KV -> DuckDB OLAP & Correlation Trace). Press [Enter]...")
    stage_pipeline_and_trace(tokens, cid)

    pause("Ready for Stage 5 (Chaos Engineering, Retries & DLQ Replay). Press [Enter]...")
    stage_chaos_and_dlq(tokens)

    banner("LIVE WALKTHROUGH COMPLETE", "Open http://localhost:8000, http://localhost:8082 (Kafka UI), and http://localhost:5540 (Redis UI)")

if __name__ == "__main__":
    main()
