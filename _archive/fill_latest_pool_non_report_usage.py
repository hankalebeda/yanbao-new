import argparse
import asyncio
import sqlite3
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from app.core.db import SessionLocal, ensure_sqlite_schema_alignment, engine
from app.services.capital_usage_collector import persist_capital_usage
from app.services.etf_flow_data import fetch_etf_flow_summary_global
from app.services.multisource_ingest import _create_batch
from app.services.northbound_data import fetch_northbound_summary
from app.services.stock_profile_collector import persist_stock_profile
import app.services.stock_snapshot_service as stock_snapshot_service


DATASETS = (
    "main_force_flow",
    "dragon_tiger_list",
    "margin_financing",
    "stock_profile",
    "northbound_summary",
    "etf_flow_summary",
)
READY_STATUSES = {"ok", "stale_ok", "proxy_ok", "realtime_only"}
CAPITAL_DATASETS = {"main_force_flow", "dragon_tiger_list", "margin_financing"}


def _now_naive_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _latest_completed_trade_date(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """
        SELECT MAX(s.trade_date)
        FROM stock_pool_snapshot s
        JOIN stock_pool_refresh_task t ON t.task_id = s.refresh_task_id
        WHERE t.status = 'COMPLETED'
        """
    ).fetchone()
    if not row or not row[0]:
        raise RuntimeError("no completed stock pool snapshot found")
    return str(row[0])


def _pool_codes(conn: sqlite3.Connection, trade_date: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT s.stock_code
        FROM stock_pool_snapshot s
        JOIN stock_pool_refresh_task t ON t.task_id = s.refresh_task_id
        WHERE s.trade_date = ?
          AND t.status = 'COMPLETED'
        ORDER BY s.rank_no ASC, s.stock_code ASC
        """,
        (trade_date,),
    ).fetchall()
    return [str(row[0]) for row in rows]


def _coverage_map(conn: sqlite3.Connection, trade_date: str) -> dict[str, set[str]]:
    placeholders = ",".join("?" for _ in DATASETS)
    rows = conn.execute(
        f"""
        SELECT stock_code, dataset_name, status
        FROM report_data_usage
        WHERE trade_date = ?
          AND dataset_name IN ({placeholders})
        ORDER BY stock_code, dataset_name, fetch_time DESC, created_at DESC, usage_id DESC
        """,
        (trade_date, *DATASETS),
    ).fetchall()
    covered_by_code: dict[str, set[str]] = {}
    seen: set[tuple[str, str]] = set()
    for stock_code, dataset_name, status in rows:
        key = (str(stock_code), str(dataset_name))
        if key in seen:
            continue
        seen.add(key)
        covered_by_code.setdefault(str(stock_code), set()).add(str(dataset_name))
    return covered_by_code


def _is_retryable(error_text: str) -> bool:
    lowered = error_text.lower()
    return "database is locked" in lowered or "unique constraint failed: data_batch" in lowered


def _finalize_batch(
    db,
    *,
    batch_id: str,
    records_total: int,
    records_success: int,
    records_failed: int,
) -> None:
    if records_success and not records_failed:
        batch_status = "SUCCESS"
        quality_flag = "ok"
        status_reason = None
    elif records_success:
        batch_status = "PARTIAL_SUCCESS"
        quality_flag = "degraded"
        status_reason = f"partial:{records_success}/{records_total}"
    else:
        batch_status = "FAILED"
        quality_flag = "missing"
        status_reason = f"failed:{records_failed}/{records_total}"
    finished_at = _now_naive_utc()
    db.execute(
        text(
            """
            UPDATE data_batch
            SET batch_status = :batch_status,
                quality_flag = :quality_flag,
                records_total = :records_total,
                records_success = :records_success,
                records_failed = :records_failed,
                status_reason = :status_reason,
                finished_at = :finished_at,
                updated_at = :updated_at
            WHERE batch_id = :batch_id
            """
        ),
        {
            "batch_id": batch_id,
            "batch_status": batch_status,
            "quality_flag": quality_flag,
            "records_total": records_total,
            "records_success": records_success,
            "records_failed": records_failed,
            "status_reason": status_reason,
            "finished_at": finished_at,
            "updated_at": finished_at,
        },
    )


def _create_shared_batches(*, trade_day: date, trade_text: str, scope_suffix: str, target_count: int) -> dict[str, str]:
    db = SessionLocal()
    try:
        batch_scope = f"stock_supplemental_{scope_suffix}"
        summary_scope = f"summary_{scope_suffix}"
        etf_summary = fetch_etf_flow_summary_global(trade_day)
        etf_source_name = stock_snapshot_service._summary_source_name("etf_flow_summary", etf_summary)[:32]

        capital_batch = _create_batch(
            db,
            source_name="supplemental_capital",
            trade_date=trade_day,
            batch_scope=batch_scope,
            batch_status="RUNNING",
            quality_flag="ok",
            records_total=target_count * 3,
            records_success=0,
            records_failed=0,
            started_at=_now_naive_utc(),
            finished_at=_now_naive_utc(),
        )
        profile_batch = _create_batch(
            db,
            source_name="stock_profile",
            trade_date=trade_day,
            batch_scope=batch_scope,
            batch_status="RUNNING",
            quality_flag="ok",
            records_total=target_count,
            records_success=0,
            records_failed=0,
            started_at=_now_naive_utc(),
            finished_at=_now_naive_utc(),
        )
        northbound_ok_batch = _create_batch(
            db,
            source_name="akshare_hsgt_hist",
            trade_date=trade_day,
            batch_scope=summary_scope,
            batch_status="RUNNING",
            quality_flag="ok",
            records_total=0,
            records_success=0,
            records_failed=0,
            started_at=_now_naive_utc(),
            finished_at=_now_naive_utc(),
        )
        northbound_missing_batch = _create_batch(
            db,
            source_name="northbound_summary",
            trade_date=trade_day,
            batch_scope=summary_scope,
            batch_status="RUNNING",
            quality_flag="missing",
            records_total=0,
            records_success=0,
            records_failed=0,
            started_at=_now_naive_utc(),
            finished_at=_now_naive_utc(),
        )
        etf_batch = _create_batch(
            db,
            source_name=etf_source_name,
            trade_date=trade_day,
            batch_scope=summary_scope,
            batch_status="RUNNING",
            quality_flag="ok",
            records_total=target_count,
            records_success=0,
            records_failed=0,
            started_at=_now_naive_utc(),
            finished_at=_now_naive_utc(),
        )
        db.commit()
        return {
            "capital": capital_batch.batch_id,
            "profile": profile_batch.batch_id,
            "northbound_ok": northbound_ok_batch.batch_id,
            "northbound_missing": northbound_missing_batch.batch_id,
            "etf": etf_batch.batch_id,
            "etf_source_name": etf_source_name,
            "etf_status": str(etf_summary.get("status") or "missing").lower(),
            "etf_status_reason": stock_snapshot_service._summary_status_reason(etf_summary),
            "trade_text": trade_text,
        }
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--codes", default=None)
    args = parser.parse_args()
    if args.shard_count <= 0:
        raise ValueError("shard_count must be >= 1")
    if args.shard_index < 0 or args.shard_index >= args.shard_count:
        raise ValueError("shard_index must satisfy 0 <= shard_index < shard_count")

    ensure_sqlite_schema_alignment(engine)

    conn = sqlite3.connect("data/app.db")
    try:
        trade_date = args.trade_date or _latest_completed_trade_date(conn)
        trade_day = date.fromisoformat(trade_date)
        scope_suffix = f"manual_fill_{trade_date.replace('-', '')}_s{args.shard_index}of{args.shard_count}"
        pool_codes = _pool_codes(conn, trade_date)
        requested_codes = None
        if args.codes:
            requested_codes = {
                code.strip().upper()
                for code in args.codes.split(",")
                if code.strip()
            }
        covered_by_code = _coverage_map(conn, trade_date)
        targets = [
            stock_code
            for stock_code in pool_codes
            if requested_codes is None or stock_code.upper() in requested_codes
            if covered_by_code.get(stock_code, set()) != set(DATASETS)
        ]
        targets = [
            stock_code
            for idx, stock_code in enumerate(targets)
            if idx % args.shard_count == args.shard_index
        ]
        if args.limit is not None:
            targets = targets[: max(0, args.limit)]
        missing_by_code = {
            stock_code: set(DATASETS) - covered_by_code.get(stock_code, set())
            for stock_code in targets
        }
        print(
            f"trade_date={trade_date} pool={len(pool_codes)} targets={len(targets)} shard={args.shard_index}/{args.shard_count}",
            flush=True,
        )
    finally:
        conn.close()

    if not targets:
        conn = sqlite3.connect("data/app.db")
        try:
            covered_by_code = _coverage_map(conn, trade_date)
            covered = sum(1 for stock_code in pool_codes if covered_by_code.get(stock_code, set()) == set(DATASETS))
            print(f"done ok=0 error=0 covered={covered}/{len(pool_codes)}", flush=True)
        finally:
            conn.close()
        return 0

    shared = _create_shared_batches(
        trade_day=trade_day,
        trade_text=trade_date,
        scope_suffix=scope_suffix,
        target_count=len(targets),
    )

    capital_success = 0
    capital_failed = 0
    profile_success = 0
    profile_failed = 0
    northbound_ok_total = 0
    northbound_missing_total = 0
    etf_success = 0
    etf_failed = 0

    ok_count = 0
    error_count = 0
    loop = asyncio.new_event_loop()
    for idx, stock_code in enumerate(targets, start=1):
        missing_datasets = missing_by_code.get(stock_code, set(DATASETS))
        last_error = None
        for attempt in range(1, 4):
            db = SessionLocal()
            try:
                capital_result = None
                if missing_datasets & CAPITAL_DATASETS:
                    capital_result = loop.run_until_complete(
                        persist_capital_usage(
                            db,
                            stock_code=stock_code,
                            trade_date=trade_date,
                            batch_id=shared["capital"],
                        )
                    )
                profile_result = None
                if "stock_profile" in missing_datasets:
                    profile_result = persist_stock_profile(
                        db,
                        stock_code=stock_code,
                        trade_date=trade_date,
                        batch_id=shared["profile"],
                    )

                nb_status = None
                if "northbound_summary" in missing_datasets:
                    northbound_summary = fetch_northbound_summary(stock_code) or {
                        "status": "missing",
                        "reason": "northbound_data_unavailable",
                    }
                    nb_status = str(northbound_summary.get("status") or "missing").lower()
                    nb_source_name = stock_snapshot_service._summary_source_name("northbound_summary", northbound_summary)
                    nb_status_reason = stock_snapshot_service._summary_status_reason(northbound_summary)
                    nb_batch_id = shared["northbound_ok"] if nb_source_name == "akshare_hsgt_hist" else shared["northbound_missing"]
                    stock_snapshot_service._upsert_report_data_usage(
                        db,
                        stock_code=stock_code,
                        trade_day=trade_day,
                        dataset_name="northbound_summary",
                        source_name=nb_source_name,
                        batch_id=nb_batch_id,
                        fetch_time=stock_snapshot_service._now_utc(),
                        status=nb_status if nb_status in {"ok", "stale_ok", "missing", "degraded"} else "missing",
                        status_reason=nb_status_reason,
                    )

                etf_status = None
                if "etf_flow_summary" in missing_datasets:
                    etf_status = str(shared["etf_status"] or "missing")
                    stock_snapshot_service._upsert_report_data_usage(
                        db,
                        stock_code=stock_code,
                        trade_day=trade_day,
                        dataset_name="etf_flow_summary",
                        source_name=shared["etf_source_name"],
                        batch_id=shared["etf"],
                        fetch_time=stock_snapshot_service._now_utc(),
                        status=etf_status if etf_status in {"ok", "stale_ok", "missing", "degraded"} else "missing",
                        status_reason=shared["etf_status_reason"],
                    )
                db.commit()

                if capital_result is not None:
                    capital_rows = (capital_result.get("per_dataset") or {}).values()
                    capital_success += sum(1 for item in capital_rows if str(item.get("persisted_status") or "").lower() in READY_STATUSES)
                    capital_failed += sum(1 for item in capital_rows if str(item.get("persisted_status") or "").lower() not in READY_STATUSES)

                if profile_result is not None:
                    profile_status = str(profile_result.get("persisted_status") or "missing").lower()
                    if profile_status in READY_STATUSES:
                        profile_success += 1
                    else:
                        profile_failed += 1

                if nb_status is not None:
                    if nb_status in READY_STATUSES:
                        northbound_ok_total += 1
                    else:
                        northbound_missing_total += 1

                if etf_status is not None:
                    if etf_status in READY_STATUSES:
                        etf_success += 1
                    else:
                        etf_failed += 1

                ok_count += 1
                if idx == 1 or idx % 5 == 0 or idx == len(targets):
                    print(f"[{idx}/{len(targets)}] OK {stock_code}", flush=True)
                break
            except Exception as exc:
                last_error = str(exc or "unknown error")
                db.rollback()
                if attempt < 3 and _is_retryable(last_error):
                    time.sleep(1.0 * attempt)
                    continue
                error_count += 1
                print(f"[{idx}/{len(targets)}] ERROR {stock_code}: {last_error}", flush=True)
                break
            finally:
                db.close()
    loop.close()

    db = SessionLocal()
    try:
        _finalize_batch(
            db,
            batch_id=shared["capital"],
            records_total=len(targets) * 3,
            records_success=capital_success,
            records_failed=capital_failed,
        )
        _finalize_batch(
            db,
            batch_id=shared["profile"],
            records_total=len(targets),
            records_success=profile_success,
            records_failed=profile_failed,
        )
        _finalize_batch(
            db,
            batch_id=shared["northbound_ok"],
            records_total=northbound_ok_total,
            records_success=northbound_ok_total,
            records_failed=0,
        )
        _finalize_batch(
            db,
            batch_id=shared["northbound_missing"],
            records_total=northbound_missing_total,
            records_success=0,
            records_failed=northbound_missing_total,
        )
        _finalize_batch(
            db,
            batch_id=shared["etf"],
            records_total=len(targets),
            records_success=etf_success,
            records_failed=etf_failed,
        )
        db.commit()
    finally:
        db.close()

    conn = sqlite3.connect("data/app.db")
    try:
        covered_by_code = _coverage_map(conn, trade_date)
        covered = sum(1 for stock_code in pool_codes if covered_by_code.get(stock_code, set()) == set(DATASETS))
        print(f"done ok={ok_count} error={error_count} covered={covered}/{len(pool_codes)}", flush=True)
    finally:
        conn.close()
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())