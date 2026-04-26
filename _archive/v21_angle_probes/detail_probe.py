"""V21 Sample detail + advanced probe.

Randomly sample 8 visible ok published reports, hit detail page + advanced region,
verify 200 + required JSON keys + no placeholder like "—" / "暂无".
"""
from __future__ import annotations

import json
import random
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "output"
DB = ROOT / "data" / "app.db"
BASE = "http://127.0.0.1:8000"

PLACEHOLDERS = ["暂无", "暂未产出", "—", "null", "undefined", "N/A"]


def http_get(path: str) -> dict:
    cmd = ["curl.exe", "--noproxy", "*", "--max-time", "15", "-s", "-w", "\n__CODE__=%{http_code}", f"{BASE}{path}"]
    r = subprocess.run(cmd, capture_output=True, timeout=20)
    raw = r.stdout or b""
    try:
        body = raw.decode("utf-8", errors="replace")
    except Exception:
        body = ""
    code = 0
    if "__CODE__=" in body:
        body, tail = body.rsplit("__CODE__=", 1)
        try:
            code = int(tail.strip())
        except Exception:
            code = 0
    return {"code": code, "body": body}


def main() -> int:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    cur = conn.cursor()
    ids = [r[0] for r in cur.execute("""
        SELECT report_id FROM report 
        WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1 AND quality_flag='ok'
        ORDER BY random() LIMIT 8
    """).fetchall()]
    conn.close()

    results = []
    passed = 0
    for rid in ids:
        entry = {"report_id": rid, "checks": []}
        # detail API
        api = http_get(f"/api/v1/reports/{rid}")
        entry["checks"].append({"endpoint": f"/api/v1/reports/{rid}", "code": api["code"], "ok": api["code"] == 200})
        data = None
        try:
            data = json.loads(api["body"])
        except Exception:
            data = None
        # required keys
        req_keys = ["report_id", "stock_code", "recommendation", "confidence"]
        missing = [k for k in req_keys if not (isinstance(data, dict) and data.get("data", {}).get(k) is not None)]
        entry["checks"].append({"required_keys_missing": missing, "ok": not missing})
        # advanced API (requires auth; 401/403 视为符合门禁)
        adv = http_get(f"/api/v1/reports/{rid}/advanced")
        entry["checks"].append({"endpoint": f"/api/v1/reports/{rid}/advanced", "code": adv["code"], "ok": adv["code"] in (200, 401, 403)})
        # placeholder scan: 只查 detail body，且排除 JSON 字段里合法的 null（只检用户文案中的占位符）
        hay = api["body"][:200000]
        ph_hits = [p for p in PLACEHOLDERS if p in ("暂无", "暂未产出", "—", "N/A") and p in hay]
        entry["checks"].append({"placeholder_hits": ph_hits, "ok": len(ph_hits) == 0})
        entry["all_ok"] = all(c.get("ok", False) for c in entry["checks"])
        if entry["all_ok"]:
            passed += 1
        results.append(entry)

    out = OUT / f"v21_detail_advanced_{ts}.json"
    summary = {"ts_utc": ts, "sampled": len(ids), "all_ok": passed, "results": results}
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    for r in results:
        print(f"  {'PASS' if r['all_ok'] else 'FAIL'}  {r['report_id']}")
        if not r["all_ok"]:
            for c in r["checks"]:
                print("    ", c)
    print(f"\n=== {passed}/{len(ids)} PASS -> {out}")
    return 0 if passed == len(ids) else 1


if __name__ == "__main__":
    sys.exit(main())
