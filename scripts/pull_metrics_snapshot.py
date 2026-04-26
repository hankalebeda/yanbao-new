#!/usr/bin/env python3
"""Pull metrics snapshot from /api/v1/internal/metrics/summary and /api/v1/sim/account/summary.

Appends key fields to runtime/logs/metrics_snapshot_YYYYMMDD.json (JSON Lines format).
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

DEFAULT_BASE_URL = "http://localhost:8010"


def extract_metrics(data: dict) -> dict:
    """Extract key fields from metrics/summary response.data."""
    out = {}
    llm = (data or {}).get("llm") or {}
    report = (data or {}).get("report") or {}
    prediction = (data or {}).get("prediction") or {}
    out["llm.timeout_rate"] = llm.get("timeout_rate")
    out["report.degraded_rate"] = report.get("degraded_rate")
    out["core_field_coverage"] = report.get("core_field_coverage")
    out["decision_conflict_rate"] = report.get("decision_conflict_rate")
    out["prediction.accuracy"] = prediction.get("accuracy")
    return out


def extract_sim(data: dict) -> dict:
    """Extract key fields from sim/account/summary response.data."""
    d = data or {}
    return {
        "win_rate": d.get("win_rate"),
        "pnl_ratio": d.get("pnl_ratio"),
        "total_trades": d.get("total_trades"),
        "cold_start": d.get("cold_start"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Pull metrics snapshot and append to JSON Lines file.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"API base URL (default: {DEFAULT_BASE_URL})")
    args = parser.parse_args()

    base = args.base_url.rstrip("/")
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    snapshot = {"ts": ts}

    try:
        # GET /api/v1/internal/metrics/summary
        r_metrics = requests.get(f"{base}/api/v1/internal/metrics/summary", timeout=15)
        r_metrics.raise_for_status()
        body = r_metrics.json()
        if body.get("code") != 0:
            print(f"metrics/summary error: {body.get('message', body)}", file=sys.stderr)
            return 1
        snapshot.update(extract_metrics(body.get("data")))
    except requests.RequestException as e:
        print(f"Connection error fetching metrics/summary: {e}", file=sys.stderr)
        return 1

    try:
        # GET /api/v1/sim/account/summary (default capital_tier=10w)
        r_sim = requests.get(f"{base}/api/v1/sim/account/summary", params={"capital_tier": "10w"}, timeout=15)
        r_sim.raise_for_status()
        body = r_sim.json()
        if body.get("code") != 0:
            print(f"sim/account/summary error: {body.get('message', body)}", file=sys.stderr)
            return 1
        snapshot.update(extract_sim(body.get("data")))
    except requests.RequestException as e:
        print(f"Connection error fetching sim/account/summary: {e}", file=sys.stderr)
        return 1

    # Append to runtime/logs/metrics_snapshot_YYYYMMDD.json (JSON Lines)
    root = Path(__file__).resolve().parent.parent
    log_dir = root / "runtime" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    log_file = log_dir / f"metrics_snapshot_{date_str}.json"

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot, ensure_ascii=False) + "\n")

    print(f"Appended snapshot to {log_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
