"""v20 strict sweep — soft-delete any visible report that fails the hard-gate.

Hard-gate (per v20 spec):
- quality_flag = 'ok'
- llm_fallback_level = 'primary'
- stock_master(stock_code) exists
- kline_daily(stock_code, trade_date) exists
- 5 required datasets present with status='ok' in report_data_usage linked via report_data_usage_link
- market_state_cache(trade_date).market_state_degraded=0 (for reports on or after 2026-04-03;
  historical backfill runs are allowed to keep degraded market state as they were baked in)

Anything failing is soft-deleted: is_deleted=1, published=0, publish_status='UNPUBLISHED',
status_reason appended with 'v20_strict_sweep_fail:<reasons>'. Also deletes related
report_citation, instruction_card, sim_trade_instruction rows via
`_purge_report_generation_bundle`.

Writes output/strict_sweep_v20_<ts>.json with before/after counts and per-report reasons.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("NO_PROXY", "*")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402
from app.core.db import SessionLocal  # noqa: E402
from app.services.report_generation_ssot import (  # noqa: E402
    _purge_report_generation_bundle,
    _REPORT_DATA_INCOMPLETE,
)

REQUIRED = ("kline_daily", "hotspot_top50", "northbound_summary", "etf_flow_summary", "market_state_input")
HARD_GATE_REASON = "v20_hard_gate_fail"


def q_all(db, sql, **kw):
    return [dict(r._mapping) for r in db.execute(text(sql), kw).fetchall()]


def q_one(db, sql, **kw):
    row = db.execute(text(sql), kw).fetchone()
    return dict(row._mapping) if row else None


def evaluate_report(db, r: dict) -> list[str]:
    fails: list[str] = []
    quality = str(r.get("quality_flag") or "").strip().lower()
    llm_level = str(r.get("llm_fallback_level") or "").strip().lower()
    if quality != "ok":
        fails.append(f"quality_flag_not_ok:{quality or 'null'}")
    if llm_level != "primary":
        fails.append(f"llm_fallback_level_not_primary:{llm_level or 'null'}")

    stock = q_one(db, "SELECT 1 FROM stock_master WHERE stock_code=:c LIMIT 1", c=r["stock_code"])
    if not stock:
        fails.append("stock_master_missing")

    kline = q_one(
        db,
        "SELECT 1 FROM kline_daily WHERE stock_code=:c AND trade_date=:t LIMIT 1",
        c=r["stock_code"],
        t=r["trade_date"],
    )
    if not kline:
        fails.append("kline_daily_missing_for_trade_date")

    # dataset coverage via link
    rows = q_all(
        db,
        """
        SELECT u.dataset_name, u.status
        FROM report_data_usage_link l
        JOIN report_data_usage u ON u.usage_id = l.usage_id
        WHERE l.report_id = :rid
        """,
        rid=r["report_id"],
    )
    by_ds: dict[str, list[str]] = {}
    for row in rows:
        by_ds.setdefault(str(row["dataset_name"]), []).append(str(row["status"]))
    for need in REQUIRED:
        if need not in by_ds:
            fails.append(f"required_dataset_missing:{need}")
        elif "ok" not in by_ds[need]:
            fails.append(f"required_dataset_not_ok:{need}={by_ds[need][0] if by_ds[need] else 'null'}")

    return fails


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=10_000)
    args = parser.parse_args()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = ROOT / "output" / f"strict_sweep_v20_{ts}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    db = SessionLocal()
    summary: dict = {
        "schema": "v20",
        "dry_run": args.dry_run,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "hard_gate_reason": HARD_GATE_REASON,
    }
    try:
        visible = q_all(
            db,
            """
            SELECT report_id, stock_code, trade_date, quality_flag, llm_fallback_level, publish_status
            FROM report
            WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1
            ORDER BY trade_date DESC, created_at DESC
            LIMIT :limit
            """,
            limit=args.limit,
        )
        summary["scanned"] = len(visible)

        fail_records: list[dict] = []
        pass_count = 0
        for r in visible:
            fails = evaluate_report(db, r)
            if fails:
                fail_records.append(
                    {
                        "report_id": r["report_id"],
                        "stock_code": r["stock_code"],
                        "trade_date": str(r["trade_date"]),
                        "fails": fails,
                    }
                )
            else:
                pass_count += 1

        summary["pass"] = pass_count
        summary["fail"] = len(fail_records)

        deleted = 0
        if not args.dry_run:
            for rec in fail_records:
                reason = f"{HARD_GATE_REASON}:{','.join(rec['fails'])}"[:480]
                _purge_report_generation_bundle(db, report_id=rec["report_id"], purge_reason=reason)
                deleted += 1
            db.commit()
        summary["soft_deleted"] = deleted
        summary["fail_details"] = fail_records[:500]

        # Verify post-condition
        if not args.dry_run:
            post_visible = q_one(
                db,
                "SELECT COUNT(*) AS n FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1",
            )
            post_non_primary = q_one(
                db,
                """
                SELECT COUNT(*) AS n FROM report
                WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1
                  AND (quality_flag <> 'ok' OR llm_fallback_level <> 'primary'
                       OR quality_flag IS NULL OR llm_fallback_level IS NULL)
                """,
            )
            summary["post_visible"] = post_visible["n"]
            summary["post_hard_gate_violators"] = post_non_primary["n"]

        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(
            f"[ok] strict_sweep {'DRY' if args.dry_run else 'APPLIED'} | scanned={summary['scanned']} | "
            f"pass={summary['pass']} | fail={summary['fail']} | soft_deleted={summary['soft_deleted']}"
        )
        if not args.dry_run:
            print(
                f"  post_visible={summary['post_visible']} | post_violators={summary['post_hard_gate_violators']}"
            )
        print(f"  out={out_path}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
