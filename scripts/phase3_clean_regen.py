"""Phase 3 · Clean degraded reports + regenerate via CLIProxyAPI.

Strategy:
 1. Archive all report rows where quality_flag != 'ok' OR llm_fallback_level
    IN ('failed','rule_based') (and not already deleted).
 2. Soft-delete them (is_deleted=1, deleted_at=now, delete_reason).
 3. For each (stock_code, trade_date) pair deleted, attempt regeneration
    via generate_report_ssot. Skip REPORT_DATA_INCOMPLETE (data missing).
 4. Write summary to output/phase3_clean_regen.json.

Usage:  python scripts/phase3_clean_regen.py [--limit 20]
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

os.environ.setdefault("PYTHONPATH", r"D:\yanbao-new")
sys.path.insert(0, r"D:\yanbao-new")
os.environ.setdefault("NO_PROXY", "*")

import app.models  # noqa: F401 ensure metadata populated
from sqlalchemy import text
from app.core.db import SessionLocal
from app.services.report_generation_ssot import (
    ReportGenerationServiceError,
    generate_report_ssot,
)

LIMIT = None
for i, a in enumerate(sys.argv):
    if a == "--limit" and i + 1 < len(sys.argv):
        LIMIT = int(sys.argv[i + 1])

OUT = Path(__file__).resolve().parent.parent / "output" / "phase3_clean_regen.json"
ARCHIVE = Path(__file__).resolve().parent.parent / "_archive" / f"deleted_reports_degraded_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def main() -> int:
    summary: dict = {"started_at": _now().isoformat(), "archived_count": 0,
                     "deleted_count": 0, "regen_success": 0, "regen_skip_data": 0,
                     "regen_fail": 0, "details": []}
    db = SessionLocal()
    try:
        # 1. Select targets — quality_flag != 'ok' OR llm_fallback_level failed/rule_based
        rows = db.execute(text("""
            SELECT report_id, stock_code, trade_date, quality_flag,
                   llm_fallback_level, published, status_reason
            FROM report
            WHERE (is_deleted IS NULL OR is_deleted = 0)
              AND (
                   quality_flag != 'ok'
                OR llm_fallback_level IN ('failed', 'rule_based')
              )
            ORDER BY trade_date DESC, stock_code
        """)).mappings().all()
        targets = [dict(r) for r in rows]
        print(f"degraded candidates: {len(targets)}")
        if LIMIT:
            targets = targets[:LIMIT]
            print(f"limited to: {len(targets)}")

        # 2. Archive to JSON
        ARCHIVE.parent.mkdir(parents=True, exist_ok=True)
        ARCHIVE.write_text(
            json.dumps([{k: (v.isoformat() if hasattr(v, "isoformat") else v)
                         for k, v in t.items()} for t in targets],
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        summary["archived_count"] = len(targets)
        summary["archive_file"] = str(ARCHIVE)

        # 3. Soft-delete
        now = _now()
        for t in targets:
            db.execute(text("""
                UPDATE report
                SET is_deleted = 1,
                    deleted_at = :now,
                    status_reason = COALESCE(status_reason, '') || '|v20-strict-cleanup',
                    updated_at = :now
                WHERE report_id = :rid
            """), {"now": now, "rid": t["report_id"]})
            summary["deleted_count"] += 1
        db.commit()
        print(f"soft-deleted: {summary['deleted_count']}")

        # 4. Regenerate — group by (stock_code, trade_date)
        uniq = sorted({(t["stock_code"], str(t["trade_date"])) for t in targets})
        print(f"regenerate targets (unique): {len(uniq)}")
        for idx, (code, td) in enumerate(uniq):
            if LIMIT and idx >= LIMIT:
                break
            t0 = time.time()
            detail = {"stock_code": code, "trade_date": td, "elapsed": 0, "status": "?"}
            try:
                result = generate_report_ssot(
                    db,
                    stock_code=code,
                    trade_date=td,
                    idempotency_key=f"phase3-regen-{uuid4().hex[:8]}",
                    force_same_day_rebuild=True,
                )
                detail["status"] = "ok"
                detail["report_id"] = result.get("report_id") if isinstance(result, dict) else None
                detail["quality_flag"] = result.get("quality_flag") if isinstance(result, dict) else None
                summary["regen_success"] += 1
            except ReportGenerationServiceError as e:
                code_str = str(e).upper()
                if "REPORT_DATA_INCOMPLETE" in code_str:
                    detail["status"] = "skip_data_incomplete"
                    summary["regen_skip_data"] += 1
                else:
                    detail["status"] = f"fail:{e.code}"
                    summary["regen_fail"] += 1
            except Exception as e:
                detail["status"] = f"error:{type(e).__name__}:{str(e)[:200]}"
                summary["regen_fail"] += 1
            detail["elapsed"] = round(time.time() - t0, 2)
            summary["details"].append(detail)
            print(f"  [{idx+1}/{len(uniq)}] {code} {td} -> {detail['status']} ({detail['elapsed']}s)")
    finally:
        db.close()

    summary["finished_at"] = _now().isoformat()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n=== archived={summary['archived_count']} deleted={summary['deleted_count']} "
          f"ok={summary['regen_success']} skip_data={summary['regen_skip_data']} fail={summary['regen_fail']} ===")
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
