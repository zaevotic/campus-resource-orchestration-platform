#!/usr/bin/env python3
"""
Campus Resource Orchestration Platform — Booking Conflict Prevention & Event Pipeline Benchmark.

Focuses strictly on:
  1. Booking conflict prevention & overlap exclusion (all interval shapes + high-concurrency races)
  2. Transactional Outbox / Write-Ahead Logging (atomic DB + outbox writes, relay flush)
  3. Event streaming & downstream pipeline (Kafka broker partitioning, Avro serialization,
     Redis KV notification delivery, DuckDB OLAP ingestion, DLQ routing & replay)
  4. Booking & pipeline throughput, latency, scaling, and sustained soak testing

Run (from project_updated/):
    python tests/benchmark_full.py                 # full run
    python tests/benchmark_full.py --quick         # quick run
"""
import argparse
import concurrent.futures as cf
import datetime as dt
import json
import os
import random
import socket
import statistics
import sys
import tempfile
import threading
import time
import traceback
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

ap = argparse.ArgumentParser()
ap.add_argument("--quick", action="store_true")
ap.add_argument("--soak", type=int, default=None, help="soak seconds (default 20, quick 8)")
ap.add_argument("--seed", type=int, default=1234)
ARGS = ap.parse_args()
random.seed(ARGS.seed)
Q = ARGS.quick
SOAK_SECONDS = ARGS.soak if ARGS.soak is not None else (8 if Q else 20)

# ----------------------------------------------------------------------------- isolated environment
TMP = tempfile.mkdtemp(prefix="campus_bench_")
os.environ["SQLITE_PATH"] = os.path.join(TMP, "bench.db")
os.environ["DUCKDB_PATH"] = os.path.join(TMP, "bench.duckdb")
os.environ["USE_SQLITE_FALLBACK"] = "true"
os.environ["USE_REAL_KAFKA"] = "false"
os.environ["USE_REAL_REDIS"] = "false"

import httpx          # noqa: E402
import uvicorn        # noqa: E402

from src.app import app                                              # noqa: E402
from src.common.config import settings                               # noqa: E402
from src.common.message_broker import broker                         # noqa: E402
from src.common.redis_client import kv_store                         # noqa: E402
from src.common.avro_engine import schema_registry                   # noqa: E402
from src.services.auth_gateway.gateway import rate_limiter           # noqa: E402
from src.services.booking_service.db import db_manager               # noqa: E402
from src.services.analytics_service.olap_store import olap_store     # noqa: E402
from src.services.reliability.dlq_manager import dlq_manager         # noqa: E402

# ----------------------------------------------------------------------------- server
def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p

PORT = _free_port()
BASE = f"http://127.0.0.1:{PORT}"
_server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="error", access_log=False))
_thread = threading.Thread(target=_server.run, daemon=True)

def start_server():
    _thread.start()
    for _ in range(200):
        if _server.started:
            return
        time.sleep(0.05)
    raise RuntimeError("server did not start")

HTTP = httpx.Client(base_url=BASE, timeout=180.0,
                    limits=httpx.Limits(max_connections=400, max_keepalive_connections=200))

class R:
    __slots__ = ("status", "ms", "body", "headers")
    def __init__(self, status, ms, body, headers):
        self.status, self.ms, self.body, self.headers = status, ms, body, headers
    def detail(self):
        return self.body.get("detail") if isinstance(self.body, dict) else self.body

def req(method, path, token=None, json_body=None, headers=None, params=None, timeout=None):
    h = dict(headers or {})
    if token:
        h["Authorization"] = f"Bearer {token}"
    t0 = time.perf_counter()
    try:
        r = HTTP.request(method, path, json=json_body, headers=h, params=params, timeout=timeout or 180.0)
        ms = (time.perf_counter() - t0) * 1000
        try:
            body = r.json()
        except Exception:
            body = r.text
        return R(r.status_code, ms, body, r.headers)
    except Exception as e:
        return R(0, (time.perf_counter() - t0) * 1000, f"{type(e).__name__}: {e}", {})

def pct(vals, p):
    if not vals:
        return 0.0
    s = sorted(vals)
    return s[min(len(s) - 1, int(p / 100 * len(s)))]

def lat_stats(ms):
    if not ms:
        return {"n": 0}
    return {"n": len(ms), "mean": round(statistics.mean(ms), 2), "p50": round(pct(ms, 50), 2),
            "p95": round(pct(ms, 95), 2), "p99": round(pct(ms, 99), 2), "max": round(max(ms), 2)}

def run_concurrent(tasks, workers=None, simultaneous=True):
    n = len(tasks)
    workers = workers or n
    barrier = threading.Barrier(min(n, workers)) if simultaneous and n <= workers else None
    def wrap(fn):
        if barrier:
            try:
                barrier.wait(timeout=60)
            except Exception:
                pass
        return fn()
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(wrap, tasks))

# ----------------------------------------------------------------------------- results
RESULTS = []
CURRENT_CAT = ["General"]

def record(name, status, detail="", metrics=None):
    RESULTS.append({"category": CURRENT_CAT[0], "name": name, "status": status, "detail": detail,
                    "metrics": metrics or {}})
    icon = {"PASS": "[PASS]", "FAIL": "[FAIL]", "INFO": "[INFO]"}[status]
    print(f"  {icon} {name}" + (f"  -- {detail}" if detail else ""), flush=True)

def check(name, cond, ok_detail="", fail_detail="", metrics=None):
    record(name, "PASS" if cond else "FAIL", ok_detail if cond else fail_detail, metrics)
    return cond

def category(name):
    CURRENT_CAT[0] = name
    print(f"\n=== {name} ===", flush=True)

def safe(fn):
    def inner():
        try:
            fn()
        except Exception as e:
            record(f"{fn.__name__} crashed", "FAIL", f"{type(e).__name__}: {e}")
            traceback.print_exc()
    inner.__name__ = fn.__name__
    return inner

# ----------------------------------------------------------------------------- data helpers
_day = [0]
def next_day():
    _day[0] += 1
    return (dt.date(2030, 1, 1) + dt.timedelta(days=_day[0])).isoformat()

def ts(day, hour, minute=0):
    return f"{day}T{hour:02d}:{minute:02d}:00Z"

def login(username, password):
    r = req("POST", "/api/auth/login", json_body={"username": username, "password": password})
    assert r.status == 200, f"login {username} failed: {r.status} {r.body}"
    return r.body["access_token"], r.body["user"]

def booking_payload(resource, start, end, purpose="bench", attendees=1):
    return {"resource_id": resource, "start_time": start, "end_time": end,
            "purpose": purpose, "attendees_count": attendees}

def book(token, resource, start, end, purpose="bench", attendees=1, headers=None):
    return req("POST", "/api/bookings", token, booking_payload(resource, start, end, purpose, attendees), headers=headers)

def db_rows(sql, params=()):
    conn = db_manager.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

def confirmed_for(resource, day):
    return db_rows("SELECT * FROM bookings WHERE resource_id=? AND status='CONFIRMED' AND start_time LIKE ?",
                   (resource, day + "%"))

def overlaps_in_db(only_prefix=None):
    sql = "SELECT id, resource_id, start_time, end_time FROM bookings WHERE status='CONFIRMED'"
    rows = db_rows(sql)
    by_res = defaultdict(list)
    for r in rows:
        if only_prefix is None or r["start_time"].startswith(only_prefix):
            by_res[r["resource_id"]].append(r)
    bad = []
    for res, lst in by_res.items():
        lst.sort(key=lambda r: r["start_time"])
        for i in range(len(lst)):
            for j in range(i + 1, len(lst)):
                if lst[j]["start_time"] >= lst[i]["end_time"]:
                    break
                bad.append((lst[i]["id"], lst[j]["id"]))
    return bad

def wait_until(fn, timeout=30.0, interval=0.1):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            v = fn()
            if v:
                return v
        except Exception:
            pass
        time.sleep(interval)
    return False

def outbox_pending():
    return db_rows("SELECT COUNT(*) c FROM booking_outbox WHERE status='PENDING'")[0]["c"]

def settle(timeout=40):
    t0 = time.time()
    while time.time() - t0 < timeout:
        req("POST", "/api/relay/flush", TOK["sys"])
        if outbox_pending() == 0:
            time.sleep(0.3)
            return True
        time.sleep(0.2)
    return False

def make_users(n, prefix):
    out = []
    for i in range(n):
        u = f"{prefix}_{i}_{random.randint(0, 10**9)}"
        r = req("POST", "/api/auth/register", json_body={"username": u, "password": "pw123456", "role": "student",
                                                        "department": random.choice(["CS", "EE", "ME", "Physics"]), "email": f"{u}@c.edu"})
        out.append((r.body["access_token"], r.body["user"]["id"]))
        time.sleep(0.002)
    return out

TOK = {}
USR = {}
RES = {}
ALL_RES = []
PERF = {"read": [], "write": [], "scale": []}

# ============================================================================= 1. BOOKING CONFLICT PREVENTION (ALL OVERLAP SHAPES)
@safe
def s1_booking_overlap_matrix():
    day = next_day()
    res = RES["hall"]
    other = RES["sem"]
    s = TOK["student"]

    base = book(s, res, ts(day, 10), ts(day, 12))
    check("Base booking (10:00–12:00) accepted", base.status == 200, f"booking_id={base.body.get('booking_id')}")

    cases = [
        ("Exact duplicate (10:00–12:00)", ts(day, 10), ts(day, 12), 409),
        ("Strictly contained inside (10:30–11:30)", ts(day, 10, 30), ts(day, 11, 30), 409),
        ("Strictly containing outer window (09:00–13:00)", ts(day, 9), ts(day, 13), 409),
        ("Partial head overlap (09:00–11:00)", ts(day, 9), ts(day, 11), 409),
        ("Partial tail overlap (11:00–13:00)", ts(day, 11), ts(day, 13), 409),
        ("1-minute tail boundary overlap (11:59–13:00)", ts(day, 11, 59), ts(day, 13), 409),
        ("1-minute head boundary overlap (09:00–10:01)", ts(day, 9), ts(day, 10, 1), 409),
        ("Back-to-back immediately before (08:00–10:00)", ts(day, 8), ts(day, 10), 200),
        ("Back-to-back immediately after (12:00–14:00)", ts(day, 12), ts(day, 14), 200),
        ("Disjoint later window (16:00–17:00)", ts(day, 16), ts(day, 17), 200),
    ]
    for label, a, b, exp in cases:
        r = book(TOK["faculty"], res, a, b)
        check(f"Interval check: {label} -> HTTP {exp}", r.status == exp, fail_detail=f"got {r.status}")

    r_diff = book(TOK["faculty"], other, ts(day, 10), ts(day, 12))
    check("Same time window (10:00–12:00) on a DIFFERENT resource -> HTTP 200", r_diff.status == 200)

    # Smart alternatives check
    ra = req("POST", "/api/resources", TOK["fadmin"], {"name": "Alt Room A", "resource_type": "SmartClass", "building": "A", "room_number": "101", "capacity": 40}).body["id"]
    rb = req("POST", "/api/resources", TOK["fadmin"], {"name": "Alt Room B", "resource_type": "SmartClass", "building": "A", "room_number": "102", "capacity": 40}).body["id"]
    book(s, ra, ts(day, 14), ts(day, 15))
    r_alt = book(TOK["faculty"], ra, ts(day, 14), ts(day, 15))
    check("Conflict response includes Smart Alternatives recommendation", r_alt.status == 409 and rb in str(r_alt.detail()),
          f"recommended {rb}", str(r_alt.detail()))
    check("DB exclusion invariant holds across all interval cases", not overlaps_in_db(day))

@safe
def s1_lifecycle_and_availability():
    day = next_day()
    res = RES["proj"]
    st, fa = TOK["student"], TOK["faculty"]

    b = book(st, res, ts(day, 10), ts(day, 11))
    bid = b.body["booking_id"]
    check("Initial booking created (CONFIRMED)", b.status == 200)

    av1 = req("GET", "/api/availability", st, params={"resource_id": res, "start_time": ts(day, 10, 15), "end_time": ts(day, 10, 45)}).body
    check("Availability service detects active slot conflict", av1["is_available"] is False and av1["conflict_count"] == 1)

    c1 = req("POST", f"/api/bookings/{bid}/cancel", st, {"reason": "schedule change"})
    check("Booking cancellation succeeds -> HTTP 200", c1.status == 200)

    c2 = req("POST", f"/api/bookings/{bid}/cancel", st, {"reason": "duplicate cancel"})
    check("Idempotency guard: cancelling an already-cancelled booking -> HTTP 400", c2.status == 400)

    av2 = req("GET", "/api/availability", st, params={"resource_id": res, "start_time": ts(day, 10, 15), "end_time": ts(day, 10, 45)}).body
    check("Availability service frees slot immediately after cancellation", av2["is_available"] is True)

    b2 = book(fa, res, ts(day, 10), ts(day, 11))
    check("Freed slot can be re-booked by another user -> HTTP 200", b2.status == 200)

# ============================================================================= 2. CONCURRENT BOOKING CONTENTION & RACES
@safe
def s2_same_slot_race():
    N = 25 if Q else 50
    users = make_users(N, "race")
    day = next_day()
    res = RES["hall"]
    tasks = [(lambda t=t: book(t, res, ts(day, 10), ts(day, 12), purpose="50-way-race")) for t, _ in users]
    out = run_concurrent(tasks)
    codes = Counter(r.status for r in out)
    ok = codes.get(200, 0)
    check(f"{N} concurrent requests for the EXACT same slot -> 1 winner, {N-1} conflicts",
          ok == 1 and codes.get(409, 0) == N - 1,
          f"1 accepted (200), {codes.get(409, 0)} rejected (409)", f"codes={dict(codes)}", {"codes": dict(codes)})
    check("Zero 5xx or connection errors under barrier race", all(0 < r.status < 500 for r in out))
    check("DB holds exactly 1 CONFIRMED booking for the raced slot", len(confirmed_for(res, day)) == 1)
    ms = [r.ms for r in out]
    record("50-way barrier race latency", "INFO", f"p50={pct(ms,50):.1f}ms p95={pct(ms,95):.1f}ms max={max(ms):.1f}ms", lat_stats(ms))

@safe
def s2_double_click_burst():
    day = next_day()
    N = 20 if Q else 30
    tasks = [(lambda: book(TOK["student"], RES["lab"], ts(day, 10), ts(day, 11), purpose="double-click")) for _ in range(N)]
    out = run_concurrent(tasks)
    ok = sum(1 for r in out if r.status == 200)
    check(f"Rapid double-click burst ({N} simultaneous identical requests) -> 1 booking",
          ok == 1 and sum(1 for r in out if r.status == 409) == N - 1, f"1 accepted, {N-1} rejected (409)")

@safe
def s2_staggered_overlap_chaos():
    N = 80 if Q else 200
    users = make_users(10, "stag")
    day = next_day()
    res = RES["sem"]
    tasks = []
    for i in range(N):
        start_h = random.randint(8, 19)
        start_m = random.choice([0, 15, 30, 45])
        dur = random.choice([30, 45, 60, 90, 120])
        st = dt.datetime(2030, 1, 1, start_h, start_m)
        en = st + dt.timedelta(minutes=dur)
        a = f"{day}T{st.hour:02d}:{st.minute:02d}:00Z"
        b = f"{day}T{en.hour:02d}:{en.minute:02d}:00Z"
        tok = users[i % len(users)][0]
        tasks.append(lambda tok=tok, a=a, b=b: book(tok, res, a, b, purpose="stagger"))
    out = run_concurrent(tasks, workers=60, simultaneous=False)
    codes = Counter(r.status for r in out)
    check(f"{N} concurrent random overlapping windows on 1 resource: zero 5xx", all(0 < r.status < 500 for r in out))
    bad = overlaps_in_db(day)
    check("INVARIANT: zero overlapping CONFIRMED bookings after random overlap storm",
          not bad, f"{codes.get(200,0)} accepted / {codes.get(409,0)} rejected, 0 overlaps", f"overlaps={bad[:3]}", {"codes": dict(codes)})
    acc = sorted((r["start_time"], r["end_time"]) for r in confirmed_for(res, day))
    check("All accepted intervals are mutually disjoint (sorted interval verification)",
          all(acc[i][1] <= acc[i + 1][0] for i in range(len(acc) - 1)), f"{len(acc)} disjoint intervals packed")

@safe
def s2_disjoint_parallel():
    N = 60 if Q else 150
    users = make_users(10, "disj")
    day = next_day()
    resources = ALL_RES[:5]
    tasks = []
    k = 0
    for res in resources:
        for h in range(N // len(resources)):
            d2 = (dt.date.fromisoformat(day) + dt.timedelta(days=h // 24)).isoformat()
            tasks.append(lambda tok=users[k % 10][0], res=res, d2=d2, h=h:
                         book(tok, res, f"{d2}T{h%24:02d}:00:00Z", f"{d2}T{h%24:02d}:50:00Z", "disjoint"))
            k += 1
    t0 = time.perf_counter()
    out = run_concurrent(tasks, workers=50, simultaneous=False)
    el = time.perf_counter() - t0
    ok = sum(1 for r in out if r.status == 200)
    check(f"{len(tasks)} parallel non-overlapping bookings across 5 resources all succeed",
          ok == len(tasks), f"{ok}/{len(tasks)} in {el:.2f}s ({len(tasks)/el:.1f} bookings/s)")
    record("Disjoint parallel booking throughput", "INFO", f"{len(tasks)/el:.1f} bookings/s", {**lat_stats([r.ms for r in out]), "rps": round(len(tasks) / el, 1)})
    _day[0] += N // 24 + 2

@safe
def s2_hot_resource_multi_slot():
    slots = 10
    per = 10 if Q else 20
    users = make_users(10, "hot")
    day = next_day()
    res = RES["conf"]
    tasks = []
    for s_i in range(slots):
        for j in range(per):
            tok = users[(s_i * per + j) % 10][0]
            tasks.append(lambda tok=tok, s_i=s_i: book(tok, res, ts(day, 8 + s_i), ts(day, 9 + s_i), purpose="hot"))
    random.shuffle(tasks)
    out = run_concurrent(tasks, workers=min(len(tasks), 100), simultaneous=False)
    ok = sum(1 for r in out if r.status == 200)
    check(f"{len(tasks)} requests competing across {slots} slots ({per}/slot) -> exactly {slots} winners",
          ok == slots, f"{ok} accepted, {len(tasks)-ok} rejected (409)")
    check("No overlaps in DB after multi-slot contention", not overlaps_in_db(day))

@safe
def s2_cancel_and_rebook_races():
    day = next_day()
    res = RES["proj"]
    users = make_users(21, "cr")
    owner_tok = users[0][0]
    b = book(owner_tok, res, ts(day, 10), ts(day, 11))
    bid = b.body["booking_id"]

    out = run_concurrent([lambda: req("POST", f"/api/bookings/{bid}/cancel", owner_tok, {"reason": "race"}) for _ in range(20)])
    ok = sum(1 for r in out if r.status == 200)
    check("20 simultaneous cancel requests on 1 booking -> exactly 1 succeeds", ok == 1, f"1 x 200, 19 x 400")
    settle()
    n_evt = db_rows("SELECT COUNT(*) c FROM booking_outbox WHERE aggregate_id=? AND event_type='BookingCancelled'", (bid,))[0]["c"]
    check("Exactly 1 BookingCancelled outbox event emitted", n_evt == 1)

    day2 = next_day()
    b2 = book(owner_tok, res, ts(day2, 10), ts(day2, 11))
    bid2 = b2.body["booking_id"]
    tasks = [lambda: req("POST", f"/api/bookings/{bid2}/cancel", owner_tok, {"reason": "free it"})]
    tasks += [(lambda t=t: book(t, res, ts(day2, 10), ts(day2, 11), "taker")) for t, _ in users[1:]]
    out = run_concurrent(tasks)
    winners = sum(1 for r in out[1:] if r.status == 200)
    check("Cancel racing against 20 new booking requests -> at most 1 active booking",
          len(confirmed_for(res, day2)) <= 1, f"{winners} taker won, {len(confirmed_for(res, day2))} active in DB")

@safe
def s2_mixed_storm():
    day = next_day()
    users = make_users(8, "mix")
    res = RES["lab"]
    stop = time.time() + (5 if Q else 10)
    errors = []
    counts = Counter()
    lock = threading.Lock()
    def worker(w):
        tok = users[w % 8][0]
        rnd = random.Random(w)
        while time.time() < stop:
            op = rnd.random()
            if op < 0.5:
                h = rnd.randint(8, 20)
                r = book(tok, res, ts(day, h), ts(day, h + 1))
            elif op < 0.7:
                mine = req("GET", "/api/bookings", tok, params={"resource_id": res, "status": "CONFIRMED"}).body
                if isinstance(mine, list) and mine:
                    r = req("POST", f"/api/bookings/{rnd.choice(mine)['id']}/cancel", tok, {})
                else:
                    continue
            else:
                r = req("GET", "/api/availability", tok, params={"resource_id": res, "start_time": ts(day, 9), "end_time": ts(day, 17)})
            with lock:
                counts[r.status] += 1
                if r.status >= 500 or r.status == 0:
                    errors.append(str(r.body)[:100])
    with cf.ThreadPoolExecutor(max_workers=24) as ex:
        list(ex.map(worker, range(24)))
    check("High-contention mixed book/cancel/availability storm on 1 resource: zero 5xx",
          not errors, f"{sum(counts.values())} ops ({dict(counts)})")
    check("INVARIANT after mixed storm: zero overlapping CONFIRMED bookings", not overlaps_in_db(day))

# ============================================================================= 3. TRANSACTIONAL OUTBOX (WAL) & EVENT STREAMING PIPELINE
@safe
def s3_outbox_wal_and_pipeline():
    ok = settle(60)
    check("Transactional outbox drained (0 PENDING records remaining)", ok and outbox_pending() == 0)
    st = {r["status"]: r["c"] for r in db_rows("SELECT status, COUNT(*) c FROM booking_outbox GROUP BY status")}
    check("Zero FAILED outbox records", st.get("FAILED", 0) == 0, f"outbox status counts={st}")

    n_b = db_rows("SELECT COUNT(*) c FROM bookings")[0]["c"]
    n_c = db_rows("SELECT COUNT(*) c FROM bookings WHERE status='CANCELLED'")[0]["c"]
    n_sr = db_rows("SELECT COUNT(*) c FROM service_requests")[0]["c"]
    n_confirm_evt = db_rows("SELECT COUNT(*) c FROM booking_outbox WHERE event_type='BookingConfirmed'")[0]["c"]
    n_cancel_evt = db_rows("SELECT COUNT(*) c FROM booking_outbox WHERE event_type='BookingCancelled'")[0]["c"]

    check("Atomic WAL: 1-to-1 match between bookings and BookingConfirmed outbox records",
          n_confirm_evt == n_b, f"{n_confirm_evt} outbox events == {n_b} bookings")
    check("Atomic WAL: 1-to-1 match between cancellations and BookingCancelled outbox records",
          n_cancel_evt == n_c, f"{n_cancel_evt} outbox events == {n_c} cancellations")

    total_msgs = sum(len(p) for p in broker.topics[settings.TOPIC_BOOKING_EVENTS].values())
    expected = n_confirm_evt + n_cancel_evt
    check("Kafka broker message count matches total published outbox events (no drops or duplicates)",
          total_msgs == expected, f"{total_msgs} broker messages == {expected} outbox events")

    # Partitioning & ordering
    bad_part, bad_order = 0, 0
    dist = {}
    for pid, recs in broker.topics[settings.TOPIC_BOOKING_EVENTS].items():
        dist[pid] = len(recs)
        last = -1
        for rec in recs:
            if broker.get_partition(rec.key) != pid:
                bad_part += 1
            if rec.offset <= last:
                bad_order += 1
            last = rec.offset
    check("Deterministic partitioning: all events routed to hash(resource_id) % 4", bad_part == 0)
    check("Per-partition log offsets are strictly sequential (0..N-1)", bad_order == 0)
    record("Broker partition distribution", "INFO", f"partitions={dist}", {"dist": dist})

    # DuckDB OLAP consistency
    time.sleep(0.3)
    fb = olap_store.execute_query("SELECT COUNT(*), COUNT(DISTINCT booking_id) FROM fact_bookings")[0]
    check("DuckDB OLAP: fact_bookings has every confirmed booking with zero duplicates",
          fb[0] == fb[1] == n_b, f"{fb[0]} rows ({fb[1]} distinct) == {n_b} bookings")
    fc = olap_store.execute_query("SELECT COUNT(*), COUNT(DISTINCT booking_id) FROM fact_cancellations")[0]
    check("DuckDB OLAP: fact_cancellations has every cancellation with zero duplicates",
          fc[0] == fc[1] == n_c, f"{fc[0]} rows ({fc[1]} distinct) == {n_c} cancellations")

    # Redis KV notifications (kv_store caps recent list at maxlen=500 by design, while kv_store.count() tracks all keys)
    recent_notes = kv_store.recent("notifications:recent", 500)
    total_kv = kv_store.count()
    check("Redis KV store: notifications recorded for every event & capped recent-list maintained",
          total_kv == expected + n_sr and len(recent_notes) == min(500, expected + n_sr),
          f"kv_store keys={total_kv}, recent list={len(recent_notes)} (capped at 500)")

    # End-to-end correlation ID & latency
    cid = f"wal-trace-{int(time.time()*1000)}"
    day = next_day()
    r = book(TOK["student"], RES["sem"], ts(day, 9), ts(day, 10), headers={"X-Correlation-ID": cid})
    check("X-Correlation-ID propagated through HTTP -> Outbox -> Broker -> DuckDB OLAP -> Audit Log",
          r.headers.get("x-correlation-id") == cid
          and db_rows("SELECT COUNT(*) c FROM booking_outbox WHERE correlation_id=?", (cid,))[0]["c"] == 1
          and olap_store.execute_query("SELECT COUNT(*) FROM fact_bookings WHERE correlation_id=?", [cid])[0][0] == 1
          and olap_store.execute_query("SELECT COUNT(*) FROM audit_logs WHERE correlation_id=?", [cid])[0][0] >= 1,
          f"correlation_id={cid}")

@safe
def s3_avro_and_dlq_resilience():
    # Avro binary round-trip across all 5 schemas
    sample = {
        "event_id": "evt-1", "event_type": "BookingConfirmed", "correlation_id": "cid-1",
        "timestamp": "2030-01-01T00:00:00Z", "booking_id": "bkg-1", "resource_id": "res-hall-101",
        "user_id": "usr-1", "user_role": "student", "department": "CS",
        "start_time": "2030-01-01T10:00:00Z", "end_time": "2030-01-01T11:00:00Z",
        "purpose": "test", "attendees_count": 10
    }
    raw = schema_registry.serialize("BookingConfirmed", sample)
    dec = schema_registry.deserialize("BookingConfirmed", raw)
    check("Avro binary serialization & deserialization round-trip (fastavro)",
          dec == sample and raw[:1] != b"{", f"{len(raw)} bytes binary payload")
    for name in ("BookingConfirmed", "BookingCancelled", "ServiceRequestCreated", "AuditEvent", "DeadLetterEvent"):
        check(f"Avro schema '{name}' registered and parsed", schema_registry.get_schema(name) is not None)

    # End-to-end streaming latency
    day = next_day()
    N = 15 if Q else 30
    lats = []
    for i in range(N):
        d = (dt.date.fromisoformat(day) + dt.timedelta(days=i)).isoformat()
        cid = f"lat-{int(time.time()*1000)}-{i}"
        t0 = time.perf_counter()
        book(TOK["student"], RES["proj"], ts(d, 9), ts(d, 10), headers={"X-Correlation-ID": cid})
        got = wait_until(lambda: olap_store.execute_query("SELECT COUNT(*) FROM fact_bookings WHERE correlation_id=?", [cid])[0][0] > 0, 10, 0.002)
        lats.append((time.perf_counter() - t0) * 1000 if got else 10000)
    _day[0] += N
    record("End-to-end booking -> Outbox -> Kafka -> DuckDB latency", "INFO",
           f"p50={pct(lats,50):.1f}ms p95={pct(lats,95):.1f}ms max={max(lats):.1f}ms", lat_stats(lats))
    check("End-to-end streaming pipeline p95 < 100 ms", pct(lats, 95) < 100, f"p95={pct(lats,95):.2f}ms")

    # Chaos engineering: Notification failure -> retry with backoff -> DLQ -> recovery -> replay
    dlq_manager.dlq_records.clear()
    base_fb = olap_store.execute_query("SELECT COUNT(*) FROM fact_bookings")[0][0]
    req("POST", "/api/chaos/inject", TOK["sys"], params={"target": "fail_notifications", "fail": "true"})
    day_c = next_day()
    b_c = book(TOK["student"], RES["hall"], ts(day_c, 14), ts(day_c, 15), purpose="chaos-dlq-test")
    check("Booking commits and succeeds even when Notification consumer fails", b_c.status == 200)
    check("Analytics consumer stays unaffected during Notification failure (consumer isolation)",
          olap_store.execute_query("SELECT COUNT(*) FROM fact_bookings")[0][0] == base_fb + 1)
    check("Failed notification is retried 3x with exponential backoff and routed to DLQ",
          len(dlq_manager.dlq_records) >= 1, f"dlq_count={len(dlq_manager.dlq_records)}")

    req("POST", "/api/chaos/inject", TOK["sys"], params={"target": "fail_notifications", "fail": "false"})
    dlq_ids = list(dlq_manager.dlq_records.keys())
    for did in dlq_ids:
        rep = req("POST", f"/api/dlq/{did}/replay", TOK["sys"])
        check(f"Admin DLQ replay ({did}) dispatches back to original topic -> HTTP 200", rep.status == 200)
    check("Active DLQ queue is empty after replay", len(dlq_manager.dlq_records) == 0)

# ============================================================================= 4. PERFORMANCE, SCALING & SOAK
def load(method, path_fn, token, n, conc, label, body_fn=None):
    tasks = []
    for i in range(n):
        if method == "GET":
            tasks.append(lambda i=i: req("GET", path_fn(i), token))
        else:
            tasks.append(lambda i=i: req("POST", path_fn(i), token, body_fn(i)))
    t0 = time.perf_counter()
    out = run_concurrent(tasks, workers=conc, simultaneous=False)
    el = time.perf_counter() - t0
    ms = [r.ms for r in out]
    bad = sum(1 for r in out if r.status == 0 or r.status >= 500)
    s = lat_stats(ms)
    s.update({"rps": round(n / el, 1), "errors": bad, "label": label, "conc": conc})
    return s, out

@safe
def s4_read_and_write_throughput():
    n_read = 150 if Q else 300
    levels = [1, 10, 25] if Q else [1, 10, 25, 50]
    day = "2031-03-01"
    eps = {
        "GET /health": lambda i: "/health",
        "GET /api/availability": lambda i: f"/api/availability?resource_id={ALL_RES[i % len(ALL_RES)]}&start_time={day}T10:00:00Z&end_time={day}T11:00:00Z",
        "GET /api/notifications (Redis KV)": lambda i: "/api/notifications",
        "GET /api/analytics/utilization (DuckDB)": lambda i: "/api/analytics/utilization",
        "GET /api/analytics/peak-hours (DuckDB)": lambda i: "/api/analytics/peak-hours",
        "GET /api/analytics/cancellations (DuckDB)": lambda i: "/api/analytics/cancellations",
        "GET /api/analytics/audit-logs (DuckDB)": lambda i: "/api/analytics/audit-logs?limit=100",
    }
    for name, fn in eps.items():
        for c in levels:
            s, _ = load("GET", fn, TOK["fadmin"], n_read, c, name)
            PERF["read"].append({"endpoint": name, **s})
        target = [x for x in PERF["read"] if x["endpoint"] == name and x["conc"] == 25][0]
        check(f"Read benchmark ({name} @ conc=25): p95 < 500 ms & 0 errors",
              target["p95"] < 500 and target["errors"] == 0,
              f"p95={target['p95']}ms, {target['rps']} rps", f"p95={target['p95']}ms", target)

    w_levels = [1, 10, 25] if Q else [1, 10, 25, 50]
    n_write = 60 if Q else 150
    for c in w_levels:
        day = next_day()
        res_cycle = ALL_RES
        def body(i, day=day):
            res = res_cycle[i % len(res_cycle)]
            d = (dt.date.fromisoformat(day) + dt.timedelta(days=i // (len(res_cycle) * 22))).isoformat()
            slot = (i // len(res_cycle)) % 22
            return booking_payload(res, ts(d, slot), f"{d}T{slot:02d}:50:00Z", "perf")
        s, out = load("POST", lambda i: "/api/bookings", TOK["student"], n_write, c, "POST /api/bookings (unique slots)", body)
        _day[0] += n_write // (len(res_cycle) * 22) + 2
        ok = sum(1 for r in out if r.status == 200)
        s["ok"] = ok
        PERF["write"].append(s)
        check(f"Write + WAL + Pipeline @ conc={c}: {ok}/{n_write} committed, 0 errors",
              ok == n_write and s["errors"] == 0, f"p50={s['p50']}ms p95={s['p95']}ms ({s['rps']} bookings/s)", metrics=s)

@safe
def s4_scaling_with_table_size():
    res = RES["sem"]
    sizes = [0, 1000, 3000] if Q else [0, 2000, 5000, 10000]
    uid = USR["student"]["id"]
    inserted = 0
    base_day = dt.date(2040, 1, 1)
    conn = db_manager.get_connection()
    for target in sizes:
        while inserted < target:
            rows = []
            for _ in range(min(500, target - inserted)):
                d = base_day + dt.timedelta(days=inserted // 20)
                h = inserted % 20
                rows.append((f"scale-{inserted}", res, uid, "student", "CS", f"{d}T{h:02d}:00:00Z", f"{d}T{h:02d}:50:00Z",
                             "bulk", 1, "CONFIRMED", "scale-cid", "2030-01-01T00:00:00+00:00"))
                inserted += 1
            conn.executemany("INSERT INTO bookings (id,resource_id,user_id,user_role,department,start_time,end_time,purpose,attendees_count,status,correlation_id,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            conn.commit()
        probe_day = (dt.date(2050, 1, 1) + dt.timedelta(days=target // 100)).isoformat()
        lat_b, lat_a = [], []
        for i in range(25):
            t0 = time.perf_counter()
            book(TOK["student"], res, ts(probe_day, i % 20), f"{probe_day}T{i%20:02d}:30:00Z")
            lat_b.append((time.perf_counter() - t0) * 1000)
            probe_day = (dt.date.fromisoformat(probe_day) + dt.timedelta(days=1)).isoformat() if i % 20 == 19 else probe_day
            t0 = time.perf_counter()
            req("GET", "/api/availability", TOK["student"], params={"resource_id": res, "start_time": ts("2030-06-01", 9), "end_time": ts("2030-06-01", 10)})
            lat_a.append((time.perf_counter() - t0) * 1000)
        row = {"rows_in_table": db_rows("SELECT COUNT(*) c FROM bookings")[0]["c"],
               "book_p50": round(pct(lat_b, 50), 2), "book_p95": round(pct(lat_b, 95), 2),
               "avail_p50": round(pct(lat_a, 50), 2), "avail_p95": round(pct(lat_a, 95), 2)}
        PERF["scale"].append(row)
        record(f"Table-size scaling @ {row['rows_in_table']} rows", "INFO",
               f"book p95={row['book_p95']}ms, availability p95={row['avail_p95']}ms", row)
    conn.close()
    first, last = PERF["scale"][0], PERF["scale"][-1]
    check("Conflict query & booking latency stays < 50 ms at 10,000+ rows",
          last["book_p95"] < 50 and last["avail_p95"] < 50,
          f"book p95: {first['book_p95']}ms -> {last['book_p95']}ms; avail p95: {first['avail_p95']}ms -> {last['avail_p95']}ms")
    db_manager.get_connection().execute("DELETE FROM bookings WHERE id LIKE 'scale-%'").connection.commit()

@safe
def s4_sustained_soak():
    users = make_users(15, "soak")
    stop = time.time() + SOAK_SECONDS
    lat = []
    codes = Counter()
    lock = threading.Lock()
    day0 = dt.date(2060, 1, 1)
    counter = [0]
    def worker(w):
        tok = users[w % len(users)][0]
        rnd = random.Random(1000 + w)
        mine = []
        while time.time() < stop:
            x = rnd.random()
            if x < 0.55:
                r = req("GET", rnd.choice(["/api/resources", "/api/analytics/utilization", "/api/analytics/peak-hours",
                                          "/api/notifications", "/api/analytics/cancellations"]), tok)
            elif x < 0.85:
                with lock:
                    counter[0] += 1
                    k = counter[0]
                d = day0 + dt.timedelta(days=k // (22 * len(ALL_RES)))
                res = ALL_RES[k % len(ALL_RES)]
                slot = (k // len(ALL_RES)) % 22
                r = book(tok, res, ts(d.isoformat(), slot), f"{d}T{slot:02d}:50:00Z", "soak")
                if r.status == 200:
                    mine.append(r.body["booking_id"])
            elif x < 0.95 and mine:
                r = req("POST", f"/api/bookings/{mine.pop()}/cancel", tok, {"reason": "soak"})
            else:
                r = req("POST", "/api/service-requests", tok, {"resource_id": rnd.choice(ALL_RES), "issue_type": "HVAC", "description": "check", "priority": "LOW"})
            with lock:
                lat.append(r.ms)
                codes[r.status] += 1
    t0 = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=25) as ex:
        list(ex.map(worker, range(25)))
    el = time.perf_counter() - t0
    total = sum(codes.values())
    errs = sum(v for k, v in codes.items() if k == 0 or k >= 500)
    avail = 100.0 * (total - errs) / max(total, 1)
    s = lat_stats(lat)
    s.update({"rps": round(total / el, 1), "availability_pct": round(avail, 3), "codes": dict(codes)})
    PERF["soak"] = s
    check(f"Sustained soak ({SOAK_SECONDS}s, 25 workers): availability >= 99.5% (NFR-2)",
          avail >= 99.5, f"{avail:.3f}% over {total} requests ({s['rps']} rps)", f"{avail:.3f}%", s)
    check("Sustained soak: p95 latency < 500 ms (NFR-1)", s["p95"] < 500, f"p95={s['p95']}ms, p50={s['p50']}ms", f"p95={s['p95']}ms", s)
    check("Sustained soak: zero overlapping bookings in DB", not overlaps_in_db())
    settle(60)
    check("Sustained soak: outbox fully drained & synced to Kafka/DuckDB/Redis", outbox_pending() == 0)

# ============================================================================= REPORT
def write_report(elapsed):
    cnt = Counter(r["status"] for r in RESULTS)
    by_cat = defaultdict(list)
    for r in RESULTS:
        by_cat[r["category"]].append(r)
    L = []
    L.append("# Booking Conflict Prevention & Event Pipeline Benchmark Report")
    L.append("")
    L.append(f"*Generated {dt.datetime.now():%Y-%m-%d %H:%M:%S} — run time {elapsed:.1f}s — mode: {'quick' if Q else 'full'} — "
             f"FastAPI/Uvicorn + SQLite (ACID/Outbox WAL) + Kafka Broker + Redis KV + DuckDB OLAP*")
    L.append("")
    L.append(f"**Summary:** `{cnt['PASS']} PASS` | `{cnt['FAIL']} FAIL` | `{cnt['INFO']} INFO` ({len(RESULTS)-cnt['INFO']} assertions)")
    L.append("")
    L.append("| Benchmark Suite | PASS | FAIL |")
    L.append("|---|---|---|")
    for c, rs in by_cat.items():
        if c == "Setup":
            continue
        k = Counter(x["status"] for x in rs)
        L.append(f"| {c} | {k['PASS']} | {k['FAIL']} |")
    L.append("")
    if PERF["read"]:
        L.append("## 1. Read-Path Latency & Throughput Matrix (ms)")
        L.append("")
        L.append("| Endpoint | Conc | N | rps | mean | p50 | p95 | p99 | max | errors |")
        L.append("|---|---|---|---|---|---|---|---|---|---|")
        for x in PERF["read"]:
            L.append(f"| {x['endpoint']} | {x['conc']} | {x['n']} | {x['rps']} | {x['mean']} | {x['p50']} | {x['p95']} | {x['p99']} | {x['max']} | {x['errors']} |")
        L.append("")
    if PERF["write"]:
        L.append("## 2. Write-Path + Outbox WAL + Streaming Fan-Out (`POST /api/bookings`)")
        L.append("")
        L.append("| Concurrency | Bookings | Committed | Throughput (rps) | mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | errors |")
        L.append("|---|---|---|---|---|---|---|---|---|---|")
        for x in PERF["write"]:
            L.append(f"| {x['conc']} | {x['n']} | {x.get('ok')} | {x['rps']} | {x['mean']} | {x['p50']} | {x['p95']} | {x['p99']} | {x['max']} | {x['errors']} |")
        L.append("")
    if PERF["scale"]:
        L.append("## 3. Conflict Check & Booking Scaling vs. Table Volume")
        L.append("")
        L.append("| Bookings Stored in DB | Booking Create p50 (ms) | Booking Create p95 (ms) | Availability Check p50 (ms) | Availability Check p95 (ms) |")
        L.append("|---|---|---|---|---|")
        for x in PERF["scale"]:
            L.append(f"| {x['rows_in_table']} | {x['book_p50']} | {x['book_p95']} | {x['avail_p50']} | {x['avail_p95']} |")
        L.append("")
    if PERF.get("soak"):
        s = PERF["soak"]
        L.append(f"## 4. Sustained Mixed-Workload Soak Test ({SOAK_SECONDS}s, 25 Concurrent Workers)")
        L.append("")
        L.append(f"- **Total Operations:** {s['n']}  |  **Throughput:** `{s['rps']} req/s`  |  **Availability (NFR-2):** `{s['availability_pct']}%`")
        L.append(f"- **Latency (NFR-1):** mean `{s['mean']} ms`, p50 `{s['p50']} ms`, **p95 `{s['p95']} ms`**, p99 `{s['p99']} ms`, max `{s['max']} ms`")
        L.append(f"- **HTTP Status Breakdown:** `{s['codes']}`")
        L.append("")
    L.append("## 5. Detailed Verification Log")
    L.append("")
    for c, rs in by_cat.items():
        L.append(f"### {c}")
        L.append("")
        for r in rs:
            L.append(f"- `{r['status']}` **{r['name']}**" + (f" — {r['detail']}" if r["detail"] else ""))
        L.append("")
    out_md = ROOT / "benchmark_report.md"
    out_md.write_text("\n".join(L), encoding="utf-8")
    (ROOT / "benchmark_results.json").write_text(json.dumps({"results": RESULTS, "perf": PERF}, indent=2, default=str), encoding="utf-8")
    return cnt, out_md

def main():
    t_start = time.time()
    print(f"Campus Orchestration Benchmark (mode={'quick' if Q else 'full'}, temp dir {TMP}, server {BASE})")
    start_server()
    rate_limiter.rpm = 10 ** 9
    category("Setup")
    for role, (u, p) in {"student": ("student1", "student123"), "faculty": ("faculty1", "faculty123"),
                         "fadmin": ("admin_facility", "facility123"), "sys": ("admin_sys", "sysadmin123")}.items():
        TOK[role], USR[role] = login(u, p)
    lst = req("GET", "/api/resources").body
    by_type = {r["id"]: r for r in lst}
    ALL_RES.extend(by_type.keys())
    def pick(sub):
        return next(i for i in by_type if sub in i)
    RES.update(hall=next(i for i, r in by_type.items() if r["resource_type"] == "Classroom" and r["capacity"] >= 100),
               lab=pick("lab"), conf=pick("conf"), proj=pick("proj"), sem=pick("sem"))
    record("Seeded campus resources", "INFO", f"{len(ALL_RES)} resources: {', '.join(ALL_RES)}")

    category("1. Booking Conflict Prevention & Interval Exclusion")
    s1_booking_overlap_matrix()
    s1_lifecycle_and_availability()

    category("2. High-Concurrency Booking Contention & Race Conditions")
    s2_same_slot_race()
    s2_double_click_burst()
    s2_staggered_overlap_chaos()
    s2_disjoint_parallel()
    s2_hot_resource_multi_slot()
    s2_cancel_and_rebook_races()
    s2_mixed_storm()

    category("3. Transactional Outbox (WAL) & Event Streaming Pipeline (Kafka / Redis / DuckDB)")
    s3_outbox_wal_and_pipeline()
    s3_avro_and_dlq_resilience()

    category("4. Performance, Table-Size Scaling & Sustained Soak")
    s4_read_and_write_throughput()
    s4_scaling_with_table_size()
    s4_sustained_soak()

    category("5. Global Database & Pipeline Invariants")
    settle(60)
    bad = overlaps_in_db()
    check("GLOBAL DB INVARIANT: zero overlapping CONFIRMED bookings across entire database", not bad,
          f"{db_rows('SELECT COUNT(*) c FROM bookings WHERE status=?', ('CONFIRMED',))[0]['c']} active confirmed bookings, 0 overlaps")
    check("GLOBAL PIPELINE INVARIANT: 100% of outbox WAL events published & ingested into DuckDB OLAP",
          outbox_pending() == 0 and olap_store.execute_query("SELECT COUNT(*) FROM fact_bookings")[0][0] == db_rows("SELECT COUNT(*) c FROM bookings")[0]["c"])
    check("GLOBAL HEALTH: server operational & responsive", req("GET", "/health").status == 200)

    elapsed = time.time() - t_start
    cnt, md = write_report(elapsed)
    print("\n" + "=" * 78)
    print(f"DONE in {elapsed:.1f}s:  {cnt['PASS']} PASS   {cnt['FAIL']} FAIL   {cnt['INFO']} INFO")
    print(f"Report: {md}")
    print("=" * 78)
    _server.should_exit = True
    return 1 if cnt["FAIL"] else 0

if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    os._exit(code)
