from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

import app.core.db as core_db
from app.models import Base
from app.services.report_generation_ssot import (
    ReportGenerationServiceError,
    _build_citations,
    _ensure_market_state_input_usage,
    _ensure_report_usage_link,
    _load_pool_version_for_refresh_task,
    _sort_used_data,
    resolve_refresh_context,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    text_value = str(value).replace(" ", "T")
    parsed = datetime.fromisoformat(text_value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _load_reports(db, *, limit: int | None = None) -> list[dict[str, Any]]:
    sql = """
        SELECT
            report_id,
            generation_task_id,
            stock_code,
            stock_name_snapshot,
            trade_date,
            pool_version,
            market_state_trade_date,
            market_state_reference_date,
            created_at
        FROM report
        WHERE is_deleted = 0
        ORDER BY created_at ASC, report_id ASC
    """
    params: dict[str, Any] = {}
    if limit:
        sql += " LIMIT :limit"
        params["limit"] = limit
    return [dict(row) for row in db.execute(text(sql), params).mappings().all()]


def _load_tasks(db, *, limit: int | None = None) -> list[dict[str, Any]]:
    sql = """
        SELECT task_id, trade_date, stock_code, market_state_trade_date, refresh_task_id
        FROM report_generation_task
        ORDER BY created_at ASC, task_id ASC
    """
    params: dict[str, Any] = {}
    if limit:
        sql += " LIMIT :limit"
        params["limit"] = limit
    return [dict(row) for row in db.execute(text(sql), params).mappings().all()]


def _load_task_by_id(db, *, task_id: str) -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT task_id, trade_date, stock_code, market_state_trade_date, refresh_task_id
            FROM report_generation_task
            WHERE task_id = :task_id
            LIMIT 1
            """
        ),
        {"task_id": task_id},
    ).mappings().first()
    return dict(row) if row else None


def _load_market_state_row(db, *, trade_day: date) -> dict[str, Any] | None:
    return db.execute(
        text(
            """
            SELECT
                trade_date,
                reference_date,
                market_state,
                state_reason,
                market_state_degraded,
                kline_batch_id,
                hotspot_batch_id,
                computed_at
            FROM market_state_cache
            WHERE trade_date <= :trade_date
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ),
        {"trade_date": trade_day},
    ).mappings().first()


def _load_used_data(db, *, report_id: str) -> list[dict[str, Any]]:
    return _sort_used_data([
        dict(row)
        for row in db.execute(
            text(
                """
                SELECT
                    u.usage_id,
                    u.trade_date,
                    u.stock_code,
                    u.dataset_name,
                    u.source_name,
                    u.batch_id,
                    u.fetch_time,
                    u.status,
                    u.status_reason
                FROM report_data_usage_link l
                JOIN report_data_usage u ON u.usage_id = l.usage_id
                WHERE l.report_id = :report_id
                ORDER BY u.fetch_time DESC, u.dataset_name ASC
                """
            ),
            {"report_id": report_id},
        ).mappings().all()
    ])


def _load_market_state_usage_links(db, *, report_id: str) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT
                l.report_data_usage_link_id,
                l.created_at AS linked_at,
                u.usage_id,
                u.trade_date,
                u.batch_id
            FROM report_data_usage_link l
            JOIN report_data_usage u ON u.usage_id = l.usage_id
            WHERE l.report_id = :report_id
              AND u.dataset_name = 'market_state_input'
              AND u.source_name = 'market_state_cache'
            ORDER BY l.created_at ASC, l.report_data_usage_link_id ASC
            """
        ),
        {"report_id": report_id},
    ).mappings().all()
    return [dict(row) for row in rows]


def _load_lineage_parent_batch_ids(db, *, child_batch_id: str) -> set[str]:
    rows = db.execute(
        text(
            """
            SELECT parent_batch_id
            FROM data_batch_lineage
            WHERE child_batch_id = :child_batch_id
              AND lineage_role = 'MERGED_FROM'
            ORDER BY parent_batch_id ASC
            """
        ),
        {"child_batch_id": child_batch_id},
    ).mappings().all()
    return {str(row["parent_batch_id"]) for row in rows if row.get("parent_batch_id")}


def _load_expected_market_state_batch_id(
    db,
    *,
    market_state_row: dict[str, Any],
) -> str | None:
    market_state_trade_date = _as_date(market_state_row.get("trade_date"))
    if market_state_trade_date is None:
        return None
    desired_parent_batch_ids = {
        str(batch_id)
        for batch_id in (
            market_state_row.get("kline_batch_id"),
            market_state_row.get("hotspot_batch_id"),
        )
        if batch_id
    }
    rows = db.execute(
        text(
            """
            SELECT batch_id
            FROM data_batch
            WHERE source_name = 'market_state_cache'
              AND trade_date = :trade_date
              AND batch_scope = 'market_state_derived'
            ORDER BY batch_seq DESC, created_at DESC, batch_id DESC
            """
        ),
        {"trade_date": market_state_trade_date},
    ).mappings().all()
    for row in rows:
        batch_id = str(row.get("batch_id") or "")
        if batch_id and _load_lineage_parent_batch_ids(db, child_batch_id=batch_id) == desired_parent_batch_ids:
            return batch_id
    return None


def _resolve_report_refresh_truth(
    db,
    *,
    stock_code: str,
    trade_day: date,
    fallback_refresh_task_id: str | None,
) -> dict[str, Any] | None:
    strict_context = resolve_refresh_context(
        db,
        trade_day=trade_day,
        stock_code=stock_code,
        allow_same_day_fallback=False,
    )
    if strict_context and strict_context.get("task_id") and strict_context.get("pool_version") is not None:
        return {
            "task_id": str(strict_context["task_id"]),
            "pool_version": int(strict_context["pool_version"]),
        }
    fallback_pool_version = _load_pool_version_for_refresh_task(
        db,
        refresh_task_id=fallback_refresh_task_id,
    )
    if fallback_refresh_task_id and fallback_pool_version is not None:
        return {
            "task_id": str(fallback_refresh_task_id),
            "pool_version": int(fallback_pool_version),
        }
    return None


def _delete_stale_market_state_usage_links(
    db,
    *,
    report_id: str,
    expected_usage_id: str,
) -> int:
    stale_link_ids = [
        str(row["report_data_usage_link_id"])
        for row in _load_market_state_usage_links(db, report_id=report_id)
        if str(row.get("usage_id")) != expected_usage_id
    ]
    if not stale_link_ids:
        return 0
    link_table = Base.metadata.tables["report_data_usage_link"]
    db.execute(
        link_table.delete().where(
            link_table.c.report_data_usage_link_id.in_(stale_link_ids)
        )
    )
    return len(stale_link_ids)


def _load_kline_row(db, *, stock_code: str, trade_day: date) -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT open, high, low, close, atr_pct
            FROM kline_daily
            WHERE stock_code = :stock_code
              AND trade_date = :trade_date
            LIMIT 1
            """
        ),
        {"stock_code": stock_code, "trade_date": trade_day},
    ).mappings().first()
    return dict(row) if row else None


def _count_placeholder_citations(db, *, include_deleted: bool | None = None) -> int:
    sql = """
        SELECT COUNT(*)
        FROM report_citation c
        JOIN report r ON r.report_id = c.report_id
        WHERE lower(c.title) LIKE '%snapshot'
    """
    params: dict[str, Any] = {}
    if include_deleted is True:
        sql += " AND r.is_deleted = 1"
    elif include_deleted is False:
        sql += " AND r.is_deleted = 0"
    return int(db.execute(text(sql), params).scalar_one())


def _count_report_trade_date_mismatches(db) -> int:
    return int(
        db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM report r
                WHERE r.is_deleted = 0
                  AND date(COALESCE(r.market_state_trade_date, '1900-01-01')) != date((
                      SELECT m.trade_date
                      FROM market_state_cache m
                      WHERE m.trade_date <= r.trade_date
                      ORDER BY m.trade_date DESC
                      LIMIT 1
                  ))
                """
            )
        ).scalar_one()
    )


def _count_report_reference_mismatches(db) -> int:
    return int(
        db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM report r
                WHERE r.is_deleted = 0
                  AND date(COALESCE(r.market_state_reference_date, '1900-01-01')) != date(COALESCE((
                      SELECT m.reference_date
                      FROM market_state_cache m
                      WHERE m.trade_date <= r.trade_date
                      ORDER BY m.trade_date DESC
                      LIMIT 1
                  ), (
                      SELECT m.trade_date
                      FROM market_state_cache m
                      WHERE m.trade_date <= r.trade_date
                      ORDER BY m.trade_date DESC
                      LIMIT 1
                  )))
                """
            )
        ).scalar_one()
    )


def _count_task_trade_date_mismatches(db) -> int:
    return int(
        db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM report_generation_task t
                WHERE date(COALESCE(t.market_state_trade_date, '1900-01-01')) != date((
                    SELECT m.trade_date
                    FROM market_state_cache m
                    WHERE m.trade_date <= t.trade_date
                    ORDER BY m.trade_date DESC
                    LIMIT 1
                ))
                """
            )
        ).scalar_one()
    )


def _count_report_pool_version_mismatches(db) -> int:
    return int(
        db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM report r
                LEFT JOIN report_generation_task t ON t.task_id = r.generation_task_id
                LEFT JOIN stock_pool_refresh_task p
                  ON p.task_id = t.refresh_task_id
                 AND p.status IN ('COMPLETED', 'FALLBACK')
                WHERE r.is_deleted = 0
                  AND (
                      t.task_id IS NULL
                      OR t.refresh_task_id IS NULL
                      OR p.task_id IS NULL
                      OR COALESCE(r.pool_version, -1) != COALESCE(p.pool_version, -1)
                  )
                """
            )
        ).scalar_one()
    )


def _count_reports_missing_market_state_usage_link(db) -> int:
    count = 0
    for report in _load_reports(db):
        trade_day = _as_date(report.get("trade_date"))
        if trade_day is None:
            count += 1
            continue
        market_state_row = _load_market_state_row(db, trade_day=trade_day)
        if not market_state_row:
            count += 1
            continue
        expected_batch_id = _load_expected_market_state_batch_id(
            db,
            market_state_row=dict(market_state_row),
        )
        if not expected_batch_id:
            count += 1
            continue
        linked_row = db.execute(
            text(
                """
                SELECT 1
                FROM report_data_usage_link l
                JOIN report_data_usage u ON u.usage_id = l.usage_id
                WHERE l.report_id = :report_id
                  AND u.dataset_name = 'market_state_input'
                  AND u.source_name = 'market_state_cache'
                  AND date(u.trade_date) = date(:trade_date)
                  AND u.batch_id = :batch_id
                LIMIT 1
                """
            ),
            {
                "report_id": report["report_id"],
                "trade_date": trade_day,
                "batch_id": expected_batch_id,
            },
        ).first()
        if linked_row is None:
            count += 1
    return count


def _iter_chunks(items: Iterable[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    chunk: list[dict[str, Any]] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def run_backfill(*, limit: int | None = None, dry_run: bool = False, commit_every: int = 200) -> dict[str, Any]:
    report_table = Base.metadata.tables["report"]
    task_table = Base.metadata.tables["report_generation_task"]
    citation_table = Base.metadata.tables["report_citation"]

    db = core_db.SessionLocal()
    started_at = _utc_now()
    try:
        before = {
            "active_placeholder_citations": _count_placeholder_citations(db, include_deleted=False),
            "deleted_placeholder_citations": _count_placeholder_citations(db, include_deleted=True),
            "report_trade_date_mismatches": _count_report_trade_date_mismatches(db),
            "report_reference_mismatches": _count_report_reference_mismatches(db),
            "task_trade_date_mismatches": _count_task_trade_date_mismatches(db),
            "report_pool_version_mismatches": _count_report_pool_version_mismatches(db),
            "reports_missing_market_state_usage_link": _count_reports_missing_market_state_usage_link(db),
        }

        reports = _load_reports(db, limit=limit)
        tasks = _load_tasks(db, limit=limit)
        task_map = {str(task["task_id"]): task for task in tasks if task.get("task_id")}

        stats = {
            "reports_scanned": len(reports),
            "reports_lineage_updated": 0,
            "reports_reference_updated": 0,
            "reports_pool_version_updated": 0,
            "reports_market_state_truth_blocked": 0,
            "tasks_refresh_task_repaired": 0,
            "report_citations_rebuilt": 0,
            "reports_market_state_usage_linked": 0,
            "reports_skipped_no_market_state": 0,
            "reports_skipped_no_usage": 0,
            "tasks_scanned": len(tasks),
            "tasks_loaded_on_demand": 0,
            "tasks_lineage_updated": 0,
        }

        for chunk in _iter_chunks(reports, commit_every):
            for report in chunk:
                trade_day = _as_date(report["trade_date"])
                if trade_day is None:
                    stats["reports_skipped_no_market_state"] += 1
                    continue

                market_state_row = _load_market_state_row(db, trade_day=trade_day)
                if not market_state_row:
                    stats["reports_skipped_no_market_state"] += 1
                    continue

                market_state_trade_date = _as_date(market_state_row.get("trade_date"))
                market_state_reference_date = (
                    _as_date(market_state_row.get("reference_date")) or market_state_trade_date
                )

                report_updates = {
                    "market_state_trade_date": market_state_trade_date,
                    "market_state_reference_date": market_state_reference_date,
                    "market_state": market_state_row.get("market_state"),
                    "market_state_reason_snapshot": market_state_row.get("state_reason"),
                    "market_state_degraded": bool(market_state_row.get("market_state_degraded")),
                    "updated_at": _utc_now(),
                }
                old_trade_date = _as_date(report.get("market_state_trade_date"))
                old_reference_date = _as_date(report.get("market_state_reference_date"))
                if old_trade_date != market_state_trade_date:
                    stats["reports_lineage_updated"] += 1
                if old_reference_date != market_state_reference_date:
                    stats["reports_reference_updated"] += 1
                task_id_text = str(report.get("generation_task_id") or "")
                task = task_map.get(task_id_text)
                if task is None and task_id_text:
                    task = _load_task_by_id(db, task_id=task_id_text)
                    if task:
                        task_map[task_id_text] = task
                        stats["tasks_loaded_on_demand"] += 1
                fallback_refresh_task_id = (
                    str(task.get("refresh_task_id"))
                    if task and task.get("refresh_task_id")
                    else None
                )
                refresh_truth = _resolve_report_refresh_truth(
                    db,
                    stock_code=str(report["stock_code"]),
                    trade_day=trade_day,
                    fallback_refresh_task_id=fallback_refresh_task_id,
                )
                if refresh_truth is not None:
                    truth_refresh_task_id = str(refresh_truth["task_id"])
                    if (
                        task_id_text
                        and str((task or {}).get("refresh_task_id") or "") != truth_refresh_task_id
                    ):
                        stats["tasks_refresh_task_repaired"] += 1
                        if not dry_run:
                            db.execute(
                                task_table.update()
                                .where(task_table.c.task_id == task_id_text)
                                .values(
                                    refresh_task_id=truth_refresh_task_id,
                                    updated_at=_utc_now(),
                                )
                            )
                        if task is not None:
                            task["refresh_task_id"] = truth_refresh_task_id
                    pool_version = int(refresh_truth["pool_version"])
                    report_updates["pool_version"] = pool_version
                    if int(report.get("pool_version") or 0) != pool_version:
                        stats["reports_pool_version_updated"] += 1
                if not dry_run:
                    db.execute(
                        report_table.update()
                        .where(report_table.c.report_id == report["report_id"])
                        .values(**report_updates)
                    )

                task_id = report.get("generation_task_id")
                if task_id and market_state_trade_date is not None and not dry_run:
                    db.execute(
                        task_table.update()
                        .where(task_table.c.task_id == task_id)
                        .values(
                            market_state_trade_date=market_state_trade_date,
                            updated_at=_utc_now(),
                        )
                    )

                expected_market_state_usage: dict[str, Any] | None = None
                try:
                    expected_market_state_usage = _ensure_market_state_input_usage(
                        db,
                        stock_code=str(report["stock_code"]),
                        report_trade_day=trade_day,
                        market_state_row=dict(market_state_row),
                    )
                except ReportGenerationServiceError as exc:
                    if exc.error_code != "DEPENDENCY_NOT_READY":
                        raise
                    stats["reports_market_state_truth_blocked"] += 1
                if expected_market_state_usage is not None:
                    market_state_usage_links = _load_market_state_usage_links(
                        db,
                        report_id=str(report["report_id"]),
                    )
                    had_market_state_link = any(
                        str(row.get("usage_id")) == str(expected_market_state_usage["usage_id"])
                        for row in market_state_usage_links
                    )
                    stale_market_state_link_count = sum(
                        1
                        for row in market_state_usage_links
                        if str(row.get("usage_id")) != str(expected_market_state_usage["usage_id"])
                    )
                    if stale_market_state_link_count and not dry_run:
                        stale_market_state_link_count = _delete_stale_market_state_usage_links(
                            db,
                            report_id=str(report["report_id"]),
                            expected_usage_id=str(expected_market_state_usage["usage_id"]),
                        )
                    _ensure_report_usage_link(
                        db,
                        report_id=str(report["report_id"]),
                        usage_id=str(expected_market_state_usage["usage_id"]),
                        created_at=_as_datetime(report.get("created_at")) or started_at,
                    )
                    if not had_market_state_link or stale_market_state_link_count:
                        stats["reports_market_state_usage_linked"] += 1

                used_data = _load_used_data(db, report_id=str(report["report_id"]))
                if not used_data:
                    stats["reports_skipped_no_usage"] += 1
                    continue

                kline_row = _load_kline_row(
                    db,
                    stock_code=str(report["stock_code"]),
                    trade_day=trade_day,
                )
                citations = _build_citations(
                    db,
                    used_data=used_data,
                    stock_name=str(report["stock_name_snapshot"]),
                    trade_day=trade_day,
                    kline_row=kline_row,
                    market_state_row=dict(market_state_row),
                )
                if not dry_run:
                    db.execute(
                        citation_table.delete().where(citation_table.c.report_id == report["report_id"])
                    )
                    citation_created_at = _as_datetime(report.get("created_at")) or started_at
                    for citation in citations:
                        db.execute(
                            citation_table.insert().values(
                                **citation,
                                report_id=report["report_id"],
                                created_at=citation_created_at,
                            )
                        )
                stats["report_citations_rebuilt"] += 1

            if dry_run:
                db.rollback()
            else:
                db.commit()

        report_task_ids = {str(report["generation_task_id"]) for report in reports if report.get("generation_task_id")}
        for chunk in _iter_chunks(tasks, commit_every):
            for task in chunk:
                if str(task["task_id"]) in report_task_ids:
                    continue
                trade_day = _as_date(task["trade_date"])
                if trade_day is None:
                    continue
                refresh_truth = _resolve_report_refresh_truth(
                    db,
                    stock_code=str(task.get("stock_code") or ""),
                    trade_day=trade_day,
                    fallback_refresh_task_id=(
                        str(task.get("refresh_task_id"))
                        if task.get("refresh_task_id")
                        else None
                    ),
                )
                if (
                    refresh_truth is not None
                    and str(task.get("refresh_task_id") or "") != str(refresh_truth["task_id"])
                ):
                    stats["tasks_refresh_task_repaired"] += 1
                    if not dry_run:
                        db.execute(
                            task_table.update()
                            .where(task_table.c.task_id == task["task_id"])
                            .values(
                                refresh_task_id=str(refresh_truth["task_id"]),
                                updated_at=_utc_now(),
                            )
                        )
                market_state_row = _load_market_state_row(db, trade_day=trade_day)
                if not market_state_row:
                    continue
                market_state_trade_date = _as_date(market_state_row.get("trade_date"))
                if _as_date(task.get("market_state_trade_date")) == market_state_trade_date:
                    continue
                stats["tasks_lineage_updated"] += 1
                if not dry_run:
                    db.execute(
                        task_table.update()
                        .where(task_table.c.task_id == task["task_id"])
                        .values(
                            market_state_trade_date=market_state_trade_date,
                            updated_at=_utc_now(),
                        )
                    )
            if dry_run:
                db.rollback()
            else:
                db.commit()

        after = {
            "active_placeholder_citations": _count_placeholder_citations(db, include_deleted=False),
            "deleted_placeholder_citations": _count_placeholder_citations(db, include_deleted=True),
            "report_trade_date_mismatches": _count_report_trade_date_mismatches(db),
            "report_reference_mismatches": _count_report_reference_mismatches(db),
            "task_trade_date_mismatches": _count_task_trade_date_mismatches(db),
            "report_pool_version_mismatches": _count_report_pool_version_mismatches(db),
            "reports_missing_market_state_usage_link": _count_reports_missing_market_state_usage_link(db),
        }

        return {
            "dry_run": dry_run,
            "started_at": started_at.isoformat(),
            "finished_at": _utc_now().isoformat(),
            "before": before,
            "after": after,
            "stats": stats,
        }
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill report pool_version, market-state usage/link, and citations.")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N reports/tasks.")
    parser.add_argument("--dry-run", action="store_true", help="Compute changes without committing.")
    parser.add_argument("--commit-every", type=int, default=200, help="Commit after processing this many rows.")
    args = parser.parse_args()

    result = run_backfill(limit=args.limit, dry_run=args.dry_run, commit_every=max(1, args.commit_every))
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
