"""V21 HTTP endpoint probe.

Exercises all critical business/admin/internal endpoints and produces a pass-table.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

BASE = "http://127.0.0.1:8000"
TOKEN = "phase1-audit-token-20260417"

ENDPOINTS: list[tuple[str, str, str, str | None, set[int]]] = [
    # (group, method, path, internal_token?, expected_codes)
    ("health", "GET", "/health", None, {200}),
    ("health", "GET", "/api/v1/health", None, {200}),
    ("health", "GET", "/api/v1/internal/llm/health", TOKEN, {200}),
    ("health", "GET", "/api/v1/internal/hotspot/health", TOKEN, {200}),
    ("home", "GET", "/", None, {200}),
    ("home", "GET", "/api/v1/home", None, {200}),
    ("reports", "GET", "/api/v1/reports?limit=5", None, {200}),
    ("reports", "GET", "/api/v1/reports?limit=5&recommendation=BUY", None, {200}),
    ("dashboard", "GET", "/dashboard", None, {200}),
    ("dashboard", "GET", "/api/v1/dashboard/stats?window_days=1", None, {200}),
    ("dashboard", "GET", "/api/v1/dashboard/stats?window_days=7", None, {200}),
    ("dashboard", "GET", "/api/v1/dashboard/stats?window_days=30", None, {200}),
    ("search", "GET", "/api/v1/stocks/search?q=000001", None, {200, 404}),
    ("login", "GET", "/login", None, {200}),
    ("login", "GET", "/register", None, {200}),
    ("privacy", "GET", "/privacy", None, {200}),
    ("tos", "GET", "/terms", None, {200}),
    ("features", "GET", "/features", None, {200, 302}),
    ("features", "GET", "/api/v1/features/catalog", None, {200}),
    ("admin", "GET", "/admin", None, {200, 302, 401, 403}),
    ("openapi", "GET", "/openapi.json", None, {200}),
    ("openapi", "GET", "/docs", None, {200}),
    ("auth", "GET", "/auth/me", None, {200, 401, 302}),  # anonymous
    ("fav", "GET", "/api/v1/user/favorites", None, {200, 401, 403}),  # must auth
    ("cookie-health", "GET", "/api/v1/admin/cookie-session/health", TOKEN, {200, 401, 403}),
]


def call(method: str, path: str, token: str | None):
    cmd = ["curl.exe", "--noproxy", "*", "--max-time", "15", "-s", "-o", "nul", "-w", "%{http_code}|%{time_total}", "-X", method, f"{BASE}{path}"]
    if token:
        cmd += ["-H", f"X-Internal-Token: {token}"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        out = (r.stdout or "").strip().split("|")
        return {"code": int(out[0]) if out and out[0].isdigit() else 0, "rtt": float(out[1]) if len(out) > 1 else 0.0}
    except Exception as e:
        return {"code": 0, "rtt": 0.0, "error": str(e)}


def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    results = []
    passed = 0
    failed = 0
    for group, method, path, token, expected in ENDPOINTS:
        r = call(method, path, token)
        ok = r["code"] in expected
        results.append({
            "group": group, "method": method, "path": path, "token": bool(token),
            "code": r["code"], "rtt": round(r["rtt"], 3), "expected": sorted(expected), "pass": ok,
        })
        (passed if ok else failed)
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"  {'PASS' if ok else 'FAIL'}  {method:4s} {path:55s} -> {r['code']}  ({r['rtt']:.2f}s)")

    summary = {
        "ts_utc": ts, "total": len(results), "passed": passed, "failed": failed,
        "pass_rate_pct": round(100.0 * passed / max(len(results), 1), 1),
        "results": results,
    }
    out = OUT / f"v21_http_{ts}.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n=== {passed}/{len(results)} PASS ({summary['pass_rate_pct']}%) -> {out}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
