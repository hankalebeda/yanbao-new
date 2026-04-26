#!/usr/bin/env python3
"""Benchmark API endpoints for gates 25-27 (P95 latency).

Usage:
  python scripts/benchmark_api.py [--base-url http://localhost:8010] [--iterations 20]

Thresholds (01 测试验收计划 §7.6):
  - GET /api/v1/reports: P95 ≤ 500ms
  - GET /api/v1/sim/account/summary: P95 ≤ 300ms
  - 研报详情页 TTFB: P95 ≤ 800ms (not covered; use curl/Playwright for page load)
"""
import argparse
import sys
import time
from pathlib import Path

import requests
from urllib.parse import urlparse

DEFAULT_BASE_URL = "http://localhost:8010"
DEFAULT_ITERATIONS = 20

# 01 §7.6 thresholds (ms)
THRESHOLD_REPORTS_MS = 500
THRESHOLD_SIM_SUMMARY_MS = 300

_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}


def build_session(base_url: str) -> requests.Session:
    """Create a requests.Session, disabling trust_env for loopback hosts."""
    s = requests.Session()
    host = urlparse(base_url).hostname or ""
    if host in _LOOPBACK_HOSTS:
        s.trust_env = False
    return s


def measure_get(url: str, params: dict | None = None, timeout: int = 30) -> float:
    """Return latency in ms."""
    start = time.perf_counter()
    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms


def p95(values: list[float]) -> float:
    """P95 latency in ms."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * 0.95) - 1
    idx = max(0, idx)
    return sorted_vals[idx]


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark API P95 latency (gates 25-27)")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL")
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS, help="Number of requests per endpoint")
    args = parser.parse_args()

    base = args.base_url.rstrip("/")
    n = args.iterations
    ok = True

    # Gate 26: GET /api/v1/reports
    latencies_reports = []
    for _ in range(n):
        try:
            ms = measure_get(f"{base}/api/v1/reports", params={"page": 1, "page_size": 20})
            latencies_reports.append(ms)
        except requests.RequestException as e:
            print(f"ERROR reports: {e}", file=sys.stderr)
            ok = False
            break

    if latencies_reports:
        p95_reports = p95(latencies_reports)
        status = "PASS" if p95_reports <= THRESHOLD_REPORTS_MS else "FAIL"
        print(f"GET /api/v1/reports  P95={p95_reports:.0f}ms  (threshold {THRESHOLD_REPORTS_MS}ms)  {status}")
        if p95_reports > THRESHOLD_REPORTS_MS:
            ok = False

    # Gate 27: GET /api/v1/sim/account/summary
    latencies_sim = []
    for _ in range(n):
        try:
            ms = measure_get(f"{base}/api/v1/sim/account/summary", params={"capital_tier": "10w"})
            latencies_sim.append(ms)
        except requests.RequestException as e:
            print(f"ERROR sim/account/summary: {e}", file=sys.stderr)
            ok = False
            break

    if latencies_sim:
        p95_sim = p95(latencies_sim)
        status = "PASS" if p95_sim <= THRESHOLD_SIM_SUMMARY_MS else "FAIL"
        print(f"GET /api/v1/sim/account/summary  P95={p95_sim:.0f}ms  (threshold {THRESHOLD_SIM_SUMMARY_MS}ms)  {status}")
        if p95_sim > THRESHOLD_SIM_SUMMARY_MS:
            ok = False

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
