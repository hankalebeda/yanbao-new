from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, text

from app.core.db import SessionLocal
from app.services.report_generation_ssot import (
    REPORT_GENERATION_ROUND_LIMIT,
    cleanup_incomplete_reports,
    cleanup_incomplete_reports_until_clean,
    generate_reports_batch,
)
from app.services.runtime_anchor_service import RuntimeAnchorService
from app.services.settlement_ssot import rebuild_fr07_snapshot
from app.services.stock_pool import get_daily_stock_pool
from app.services.trade_calendar import latest_trade_date_str

HISTORY_GUARDIAN_REPORT_ROUND_LIMIT = min(REPORT_GENERATION_ROUND_LIMIT, 3)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _ok_report_codes_for_trade_date(trade_date: str, stock_codes: list[str]) -> set[str]:
    if not stock_codes:
        return set()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT stock_code
                FROM report
                WHERE trade_date = :trade_date
                  AND published = 1
                  AND is_deleted = 0
                                    AND LOWER(COALESCE(quality_flag, 'ok')) = 'ok'
                  AND stock_code IN :stock_codes
                """
            ).bindparams(bindparam("stock_codes", expanding=True)),
            {"trade_date": trade_date, "stock_codes": stock_codes},
        ).mappings().all()
        return {str(row["stock_code"]) for row in rows if row.get("stock_code")}
    finally:
        db.close()


def _published_non_ok_count() -> int:
    """Count published=1 reports whose quality_flag is not strictly ok."""
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM report
                WHERE published = 1
                  AND is_deleted = 0
                  AND LOWER(COALESCE(quality_flag, 'ok')) != 'ok'
                """
            )
        ).mappings().first()
        return int((row or {}).get("c") or 0)
    finally:
        db.close()


def _resolve_target_trade_date() -> tuple[str, str | None, str | None]:
    db = SessionLocal()
    try:
        runtime = RuntimeAnchorService(db)
        complete_trade_date = runtime.latest_complete_public_batch_trade_date()
        if complete_trade_date:
            return complete_trade_date, complete_trade_date, "latest_complete_public_batch"

        pool_row = db.execute(
            text("SELECT MAX(trade_date) AS td FROM stock_pool_snapshot")
        ).mappings().first()
        pool_trade_date = str((pool_row or {}).get("td") or "").strip()
        if pool_trade_date:
            return pool_trade_date, None, "latest_pool_snapshot"

        return latest_trade_date_str(), None, "latest_trade_date"
    finally:
        db.close()


def _run_one_cycle(batch_size: int) -> dict[str, Any]:
    trade_date, complete_trade_date, trade_date_source = _resolve_target_trade_date()
    pool_codes = get_daily_stock_pool(trade_date=trade_date, exact_trade_date=True, allow_same_day_fallback=True)
    ok_codes = _ok_report_codes_for_trade_date(trade_date, pool_codes)
    missing_codes = sorted(set(pool_codes) - set(ok_codes))

    # Only strictly ok reports are considered publicly acceptable.
    # stale_ok/degraded/missing must be cleaned and regenerated.
    include_non_ok_cleanup = True

    db = SessionLocal()
    try:
        dry = cleanup_incomplete_reports(
            db,
            limit=5000,
            dry_run=True,
            include_non_ok=include_non_ok_cleanup,
        )
        db.rollback()
        cleanup_result = {
            "dry_candidates": int(dry.get("candidates") or 0),
            "dry_scanned": int(dry.get("scanned") or 0),
            "include_non_ok": include_non_ok_cleanup,
        }

        if cleanup_result["dry_candidates"] > 0:
            cleaned = cleanup_incomplete_reports_until_clean(
                db,
                batch_limit=500,
                max_batches=50,
                dry_run=False,
                include_non_ok=include_non_ok_cleanup,
            )
            db.commit()
            cleanup_result.update(
                {
                    "cleaned": int(cleaned.get("total_soft_deleted") or 0),
                    "remaining_candidates": int(cleaned.get("remaining_candidates") or 0),
                }
            )
        else:
            cleanup_result.update({"cleaned": 0, "remaining_candidates": 0})
    finally:
        db.close()

    generation_summary = {
        "requested": len(missing_codes),
        "scheduled_this_cycle": 0,
        "deferred_due_to_round_limit": 0,
        "succeeded": 0,
        "failed": 0,
        "batches": 0,
        "round_limit": HISTORY_GUARDIAN_REPORT_ROUND_LIMIT,
        "one_per_strategy_type": True,
        "strategy_distribution": {"A": [], "B": [], "C": []},
    }
    effective_batch_size = max(1, min(int(batch_size), HISTORY_GUARDIAN_REPORT_ROUND_LIMIT))
    if missing_codes:
        result = generate_reports_batch(
            db_factory=SessionLocal,
            stock_codes=missing_codes,
            trade_date=trade_date,
            skip_pool_check=False,
            force_same_day_rebuild=True,
            max_concurrent_override=effective_batch_size,
            one_per_strategy_type=True,
        )
        scheduled_this_cycle = int(result.get("preselected_count") or result.get("total") or 0)
        generation_summary["scheduled_this_cycle"] = scheduled_this_cycle
        generation_summary["deferred_due_to_round_limit"] = max(len(missing_codes) - scheduled_this_cycle, 0)
        generation_summary["batches"] = 1
        generation_summary["succeeded"] = int(result.get("succeeded") or 0)
        generation_summary["failed"] = int(result.get("failed") or 0)
        for strategy_type, codes in (result.get("strategy_distribution") or {}).items():
            if strategy_type in generation_summary["strategy_distribution"]:
                generation_summary["strategy_distribution"][strategy_type].extend(
                    str(code).strip().upper()
                    for code in (codes or [])
                    if str(code).strip()
                )

    snapshot_results: list[dict[str, Any]] = []
    db2 = SessionLocal()
    try:
        for window in (1, 7, 14, 30, 60):
            snapshot_results.append(
                rebuild_fr07_snapshot(
                    db2,
                    trade_day=datetime.fromisoformat(trade_date).date(),
                    window_days=window,
                    purge_invalid=True,
                )
            )
        db2.commit()
    finally:
        db2.close()

    return {
        "timestamp": _utc_now_iso(),
        "trade_date": trade_date,
        "trade_date_source": trade_date_source,
        "latest_complete_public_batch_trade_date": complete_trade_date,
        "pool_size": len(pool_codes),
        "ok_reports_for_trade_date": len(ok_codes),
        "missing_reports_for_trade_date": len(missing_codes),
        "published_non_ok_total": _published_non_ok_count(),
        "cleanup": cleanup_result,
        "generation": generation_summary,
        "fr07_snapshots": snapshot_results,
    }


def run_forever(*, interval_seconds: int, batch_size: int, output_dir: Path) -> None:
    _ensure_dir(output_dir)
    status_file = output_dir / "latest_status.json"
    log_file = output_dir / "history_guardian.jsonl"
    stop_file = output_dir / "STOP"

    while True:
        cycle: dict[str, Any]
        try:
            cycle = _run_one_cycle(batch_size=batch_size)
            cycle["status"] = "ok"
        except Exception as exc:  # pragma: no cover - runtime resilience
            cycle = {
                "timestamp": _utc_now_iso(),
                "status": "error",
                "error": str(exc),
            }

        status_file.write_text(json.dumps(cycle, ensure_ascii=False, indent=2), encoding="utf-8")
        _append_jsonl(log_file, cycle)

        if stop_file.exists():
            break

        time.sleep(max(15, interval_seconds))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuous historical report/data guardian")
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--output-dir", default="output/history_monitor")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    run_forever(
        interval_seconds=int(args.interval_seconds),
        batch_size=int(args.batch_size),
        output_dir=Path(args.output_dir),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
