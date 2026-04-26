#!/usr/bin/env python3
"""SSOT round verification.

Checks the release-critical public endpoints and fails on weak or empty payloads.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request


def _get_json(url: str, timeout: int = 10) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run SSOT round verification.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")

    health = _get_json(f"{base_url}/health")
    _require(health.get("success") is True, "/health must return success=true")
    _require(health.get("data", {}).get("database_status") == "ok", "database_status must be ok")
    print("HEALTH_OK")

    list_url = f"{base_url}/api/v1/reports?page=1&page_size=5"
    list_body = _get_json(list_url)
    _require(list_body.get("success") is True, "reports list must return success=true")
    list_data = list_body.get("data") or {}
    items = list_data.get("items") or []
    total = list_data.get("total")
    _require(isinstance(items, list), "reports list data.items must be a list")
    _require(isinstance(total, int), "reports list data.total must be an int")
    _require(len(items) > 0, "reports list must contain at least one report for round verification")
    print(f"LIST_OK total={total} items={len(items)}")

    stats_body = _get_json(f"{base_url}/api/v1/predictions/stats")
    _require(stats_body.get("success") is True, "prediction stats must return success=true")
    stats = stats_body.get("data") or {}
    for key in ("accuracy", "total_judged", "by_window"):
        _require(key in stats, f"prediction stats missing key: {key}")
    print("STATS_OK")

    first = items[0]
    report_id = first.get("report_id")
    _require(bool(report_id), "reports list item must contain report_id")
    detail_body = _get_json(f"{base_url}/api/v1/reports/{urllib.parse.quote(str(report_id))}")
    _require(detail_body.get("success") is True, "report detail must return success=true")
    detail = detail_body.get("data") or {}
    for key in ("report_id", "stock_code", "trade_date"):
        _require(detail.get(key) is not None, f"report detail missing key: {key}")

    usage = detail.get("report_data_usage") or {}
    sources = usage.get("sources") or []
    used_data = detail.get("used_data") or []
    _require(isinstance(sources, list), "report_data_usage.sources must be a list when present")
    _require(isinstance(used_data, list), "used_data must be a list when present")
    lineage_count = len(sources) if sources else len(used_data)
    _require(lineage_count > 0, "report detail must expose non-empty lineage via report_data_usage.sources or used_data")
    print(f"DETAIL_OK report_id={report_id} lineage_items={lineage_count}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except urllib.error.HTTPError as exc:
        print(f"ERR http_error status={exc.code} url={exc.url}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:  # pragma: no cover - script entrypoint
        print(f"ERR {exc}", file=sys.stderr)
        sys.exit(1)
