import time
import statistics
import concurrent.futures
from fastapi.testclient import TestClient
from src.app import app

def run_performance_benchmark(total_requests: int = 200, concurrency: int = 10):
    print("================================================================================")
    print("CAMPUS RESOURCE ORCHESTRATION PLATFORM - PERFORMANCE & AVAILABILITY BENCHMARK")
    print(f"Executing {total_requests} requests across {concurrency} worker threads...")
    print("Target NFR 1: P95 latency < 500 ms")
    print("Target NFR 2: Availability >= 99.5%")
    print("================================================================================")

    client = TestClient(app)
    # Authenticate
    login_res = client.post("/api/auth/login", json={"username": "student1", "password": "student123"})
    token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    endpoints = [
        ("GET", "/health", None),
        ("GET", "/api/resources", None),
        ("GET", "/api/availability?resource_id=res-hall-101&start_time=2026-12-25T10:00:00Z&end_time=2026-12-25T11:00:00Z", None),
        ("GET", "/api/analytics/utilization", None),
        ("GET", "/api/analytics/peak-hours", None),
        ("GET", "/api/analytics/cancellations", None),
    ]

    latencies_ms = []
    success_count = 0
    failure_count = 0

    def make_call(i):
        method, path, body = endpoints[i % len(endpoints)]
        c = TestClient(app)
        t0 = time.perf_counter()
        if method == "GET":
            res = c.get(path, headers=headers)
        else:
            res = c.post(path, json=body, headers=headers)
        dt = (time.perf_counter() - t0) * 1000.0
        return res.status_code, dt

    start_bench = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(make_call, i) for i in range(total_requests)]
        for f in concurrent.futures.as_completed(futures):
            code, lat = f.result()
            latencies_ms.append(lat)
            if code < 500:
                success_count += 1
            else:
                failure_count += 1
    total_time = time.perf_counter() - start_bench

    # Calculate statistics
    sorted_lats = sorted(latencies_ms)
    n = len(sorted_lats)
    p50 = sorted_lats[int(0.50 * n)]
    p95 = sorted_lats[min(n - 1, int(0.95 * n))]
    p99 = sorted_lats[min(n - 1, int(0.99 * n))]
    avg = statistics.mean(sorted_lats)
    rps = total_requests / total_time
    availability_pct = (success_count / total_requests) * 100.0

    print(f"Total Requests Processed : {total_requests}")
    print(f"Execution Duration        : {total_time:.3f} s")
    print(f"Throughput                : {rps:.2f} req/s")
    print(f"Mean Latency              : {avg:.2f} ms")
    print(f"P50 Latency               : {p50:.2f} ms")
    print(f"P95 Latency               : {p95:.2f} ms")
    print(f"P99 Latency               : {p99:.2f} ms")
    print(f"Availability Percentage   : {availability_pct:.2f}%")
    print("--------------------------------------------------------------------------------")
    
    nfr1_passed = p95 < 500.0
    nfr2_passed = availability_pct >= 99.5

    print(f"NFR 1 Performance Check (< 500 ms P95): {'PASSED' if nfr1_passed else 'FAILED'} ({p95:.2f} ms)")
    print(f"NFR 2 Availability Check (>= 99.5%)   : {'PASSED' if nfr2_passed else 'FAILED'} ({availability_pct:.2f}%)")
    print("================================================================================")
    
    assert nfr1_passed, f"NFR 1 failed: P95 {p95}ms >= 500ms"
    assert nfr2_passed, f"NFR 2 failed: Availability {availability_pct}% < 99.5%"

if __name__ == "__main__":
    run_performance_benchmark()
