from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.services.runtime_anchor_service import RuntimeAnchorService
from app.services import ssot_read_model as shared

_MAX_REASONABLE_SNAPSHOT_ALPHA_ANNUAL = 10.0


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _count_published_reports_outside_core_pool(
    db: Session,
    *,
    trade_date: str,
) -> int:
    return shared._to_int(
        shared._scalar(
            db,
            """
            SELECT COUNT(*)
            FROM report r
            LEFT JOIN stock_pool_snapshot s
              ON s.trade_date = r.trade_date
             AND s.stock_code = r.stock_code
             AND s.pool_role = 'core'
            WHERE r.is_deleted = 0
              AND r.published = 1
              AND r.trade_date = :trade_date
              AND s.stock_code IS NULL
            """,
            {"trade_date": trade_date},
        )
    ) or 0


def _hotspot_truth_summary(
    db: Session,
    *,
    trade_date: str | None,
) -> dict[str, Any]:
    empty = {
        "hotspot_usage_ok_count": 0,
        "hotspot_usage_missing_batch_id_count": 0,
        "hotspot_claimed_batch_count": 0,
        "hotspot_item_count": 0,
        "hotspot_link_count": 0,
        "hotspot_truth_mismatch": False,
    }
    if not trade_date:
        return empty

    usage_rows = shared._execute_mappings(
        db,
        """
                SELECT u.usage_id, u.batch_id
        FROM report_data_usage u
        JOIN report_data_usage_link l ON l.usage_id = u.usage_id
        WHERE u.trade_date = :trade_date
          AND u.dataset_name = 'hotspot_top50'
          AND u.status = 'ok'
        """,
        {"trade_date": trade_date},
    ).all()
    hotspot_batch_rows = shared._execute_mappings(
        db,
        """
        SELECT batch_id
        FROM data_batch
        WHERE trade_date = :trade_date
          AND source_name IN ('hotspot', 'hotspot_top50')
        """,
        {"trade_date": trade_date},
    ).all()
    hotspot_ref_rows = shared._execute_mappings(
        db,
        """
        SELECT hotspot_batch_id
        FROM market_state_cache
        WHERE trade_date = :trade_date
          AND hotspot_batch_id IS NOT NULL
        """,
        {"trade_date": trade_date},
    ).all()
    valid_hotspot_batch_ids = {
        str(row.get("batch_id"))
        for row in hotspot_batch_rows
        if row.get("batch_id")
    }
    valid_hotspot_batch_ids.update(
        str(row.get("hotspot_batch_id"))
        for row in hotspot_ref_rows
        if row.get("hotspot_batch_id")
    )

    filtered_usage_rows = [
        row
        for row in usage_rows
        if row.get("batch_id") and str(row.get("batch_id")) in valid_hotspot_batch_ids
    ]

    claimed_batch_ids = {
        str(row.get("batch_id"))
        for row in filtered_usage_rows
        if row.get("batch_id")
    }
    usage_missing_batch_id_count = sum(
        1
        for row in filtered_usage_rows
        if not row.get("batch_id")
    )

    batch_rows = shared._execute_mappings(
        db,
        """
        SELECT batch_id
        FROM data_batch
        WHERE trade_date = :trade_date
          AND source_name IN ('hotspot', 'hotspot_top50')
          AND batch_status = 'SUCCESS'
          AND COALESCE(quality_flag, 'ok') = 'ok'
        """,
        {"trade_date": trade_date},
    ).all()
    claimed_batch_ids.update(
        str(row.get("batch_id"))
        for row in batch_rows
        if row.get("batch_id")
    )

    item_count = 0
    link_count = 0
    if claimed_batch_ids:
        batch_params = {
            f"batch_id_{index}": batch_id
            for index, batch_id in enumerate(sorted(claimed_batch_ids))
        }
        batch_placeholders = ", ".join(f":batch_id_{index}" for index in range(len(batch_params)))
        hotspot_rows = shared._execute_mappings(
            db,
            f"""
            SELECT hotspot_item_id
            FROM market_hotspot_item
            WHERE batch_id IN ({batch_placeholders})
            """,
            batch_params,
        ).all()
        hotspot_item_ids = [str(row.get("hotspot_item_id")) for row in hotspot_rows if row.get("hotspot_item_id")]
        item_count = len(hotspot_item_ids)
        if hotspot_item_ids:
            item_params = {
                f"hotspot_item_id_{index}": hotspot_item_id
                for index, hotspot_item_id in enumerate(hotspot_item_ids)
            }
            item_placeholders = ", ".join(
                f":hotspot_item_id_{index}" for index in range(len(item_params))
            )
            link_count = shared._to_int(
                shared._scalar(
                    db,
                    f"""
                    SELECT COUNT(*)
                    FROM market_hotspot_item_stock_link
                    WHERE hotspot_item_id IN ({item_placeholders})
                    """,
                    item_params,
                )
            ) or 0

    hotspot_truth_mismatch = False
    if claimed_batch_ids and (item_count <= 0 or link_count <= 0):
        hotspot_truth_mismatch = True

    return {
        "hotspot_usage_ok_count": len(filtered_usage_rows),
        "hotspot_usage_missing_batch_id_count": usage_missing_batch_id_count,
        "hotspot_claimed_batch_count": len(claimed_batch_ids),
        "hotspot_item_count": item_count,
        "hotspot_link_count": link_count,
        "hotspot_truth_mismatch": hotspot_truth_mismatch,
    }


def current_runtime_trade_date(db: Session) -> str | None:
    return RuntimeAnchorService(db).runtime_trade_date()


def list_stray_unpublished_reports(
    db: Session,
    *,
    runtime_trade_date: str | None = None,
) -> list[dict[str, Any]]:
    effective_trade_date = runtime_trade_date or current_runtime_trade_date(db)
    if not effective_trade_date:
        return []
    return [
        dict(row)
        for row in shared._execute_mappings(
            db,
            """
            SELECT
                report_id,
                stock_code,
                trade_date,
                publish_status,
                published,
                market_state_trade_date,
                market_state_reference_date,
                created_at
            FROM report
            WHERE is_deleted = 0
              AND published = 0
              AND trade_date > :runtime_trade_date
            ORDER BY trade_date DESC, created_at DESC, report_id DESC
            """,
            {"runtime_trade_date": effective_trade_date},
        ).all()
    ]


def count_stray_unpublished_reports(
    db: Session,
    *,
    runtime_trade_date: str | None = None,
) -> int:
    return len(list_stray_unpublished_reports(db, runtime_trade_date=runtime_trade_date))


def soft_delete_stray_unpublished_reports(
    db: Session,
    *,
    runtime_trade_date: str | None = None,
    now: datetime | None = None,
) -> int:
    effective_trade_date = runtime_trade_date or current_runtime_trade_date(db)
    if not effective_trade_date:
        return 0
    current_now = now or _now_utc()
    result = db.execute(
        shared.text(
            """
            UPDATE report
            SET
                is_deleted = 1,
                deleted_at = :deleted_at,
                updated_at = :updated_at
            WHERE is_deleted = 0
              AND published = 0
              AND trade_date > :runtime_trade_date
            """
        ),
        {
            "runtime_trade_date": effective_trade_date,
            "deleted_at": current_now,
            "updated_at": current_now,
        },
    )
    return int(result.rowcount or 0)


def truth_counters(db: Session, *, runtime_trade_date: str | None = None) -> dict[str, Any]:
    runtime_trade_date = runtime_trade_date or current_runtime_trade_date(db)
    stray_reports = list_stray_unpublished_reports(db, runtime_trade_date=runtime_trade_date)
    hotspot_truth = _hotspot_truth_summary(db, trade_date=runtime_trade_date)
    published_outside_core_count = (
        _count_published_reports_outside_core_pool(db, trade_date=runtime_trade_date)
        if runtime_trade_date
        else 0
    )
    dashboard_truth_mismatch = False
    sim_truth_mismatch = False
    if runtime_trade_date:
        dashboard_60 = shared.get_dashboard_stats_payload_ssot(db, window_days=60)
        strategy_rows = shared._load_dashboard_strategy_snapshot_rows(
            db,
            snapshot_date=runtime_trade_date,
            window_days=60,
        )
        dashboard_truth_mismatch = (
            any(
                str(row.get("data_status") or "").upper() == "READY"
                for row in strategy_rows
                if (shared._to_int(row.get("sample_size")) or 0) > 0
            )
            and str(dashboard_60.get("data_status") or "").upper() == "COMPUTING"
        )

        for capital_tier in ("10k", "100k", "500k"):
            sim_payload = shared.get_sim_dashboard_payload_ssot(db, capital_tier=capital_tier)
            if (
                str(sim_payload.get("data_status") or "").upper() == "READY"
                and (
                    (shared._to_int(sim_payload.get("sample_size")) or 0) < 30
                    or not sim_payload.get("baseline_random")
                    or not sim_payload.get("baseline_ma_cross")
                )
            ):
                sim_truth_mismatch = True
                break
    billing_order_count = shared._to_int(shared._scalar(db, "SELECT COUNT(*) FROM billing_order")) or 0
    report_feedback_count = shared._to_int(shared._scalar(db, "SELECT COUNT(*) FROM report_feedback")) or 0
    return {
        "runtime_trade_date": runtime_trade_date,
        "weekend_unpublished_reports": stray_reports,
        "weekend_unpublished_report_count": len(stray_reports),
        "unpublished_outlier_reports": stray_reports,
        "unpublished_outlier_report_count": len(stray_reports),
        "published_outside_core_count": published_outside_core_count,
        "billing_order_count": billing_order_count,
        "report_feedback_count": report_feedback_count,
        "dashboard_truth_mismatch": dashboard_truth_mismatch,
        "sim_truth_mismatch": sim_truth_mismatch,
        **hotspot_truth,
    }


def normalize_snapshot_truth(db: Session) -> dict[str, int]:
    strategy_updates = db.execute(
        shared.text(
            """
            UPDATE strategy_metric_snapshot
            SET
                data_status = 'DEGRADED',
                alpha_annual = CASE
                    WHEN alpha_annual > :max_reasonable_alpha_annual
                      OR alpha_annual < -:max_reasonable_alpha_annual
                    THEN NULL
                    ELSE alpha_annual
                END,
                display_hint = COALESCE(
                    display_hint,
                    CASE
                        WHEN sample_size < 30 THEN 'sample_lt_30'
                        WHEN alpha_annual > :max_reasonable_alpha_annual
                          OR alpha_annual < -:max_reasonable_alpha_annual
                        THEN 'abnormal_alpha_annual'
                        ELSE NULL
                    END
                )
            WHERE data_status = 'READY'
              AND (
                    sample_size < 30
                 OR alpha_annual > :max_reasonable_alpha_annual
                 OR alpha_annual < -:max_reasonable_alpha_annual
              )
            """
        ),
        {"max_reasonable_alpha_annual": _MAX_REASONABLE_SNAPSHOT_ALPHA_ANNUAL},
    )
    baseline_updates = db.execute(
        shared.text(
            """
            UPDATE baseline_metric_snapshot
            SET alpha_annual = NULL
            WHERE alpha_annual > :max_reasonable_alpha_annual
               OR alpha_annual < -:max_reasonable_alpha_annual
            """
        ),
        {"max_reasonable_alpha_annual": _MAX_REASONABLE_SNAPSHOT_ALPHA_ANNUAL},
    )
    sim_updates = db.execute(
        shared.text(
            """
            UPDATE sim_dashboard_snapshot
            SET
                data_status = 'DEGRADED',
                status_reason = COALESCE(status_reason, CASE WHEN sample_size < 30 THEN 'sim_sample_lt_30' ELSE 'sim_baseline_pending' END),
                display_hint = COALESCE(
                    display_hint,
                    CASE WHEN sample_size < 30 THEN 'sample_lt_30' ELSE 'baseline_pending' END
                )
            WHERE data_status = 'READY'
              AND sample_size < 30
            """
        )
    )
    return {
        "strategy_metric_snapshot": int(strategy_updates.rowcount or 0),
        "baseline_metric_snapshot": int(baseline_updates.rowcount or 0),
        "sim_dashboard_snapshot": int(sim_updates.rowcount or 0),
    }


def report_truth_gate(
    db: Session,
    *,
    trade_date: str,
    min_published_coverage_ratio: float = 0.6,
    max_unpublished_ratio: float = 0.05,
) -> dict[str, Any]:
    runtime_trade_date = current_runtime_trade_date(db)
    pool_count = shared._to_int(
        shared._scalar(
            db,
            """
            SELECT COUNT(*)
            FROM stock_pool_snapshot
            WHERE trade_date = :trade_date
              AND pool_role = 'core'
            """,
            {"trade_date": trade_date},
        )
    ) or 0
    published_count = shared._to_int(
        shared._scalar(
            db,
            """
            SELECT COUNT(*)
            FROM report
            WHERE is_deleted = 0
              AND trade_date = :trade_date
              AND published = 1
            """,
            {"trade_date": trade_date},
        )
    ) or 0
    unpublished_count = shared._to_int(
        shared._scalar(
            db,
            """
            SELECT COUNT(*)
            FROM report
            WHERE is_deleted = 0
              AND trade_date = :trade_date
              AND published = 0
            """,
            {"trade_date": trade_date},
        )
    ) or 0
    llm_failed_count = shared._to_int(
        shared._scalar(
            db,
            """
            SELECT COUNT(*)
            FROM report
            WHERE is_deleted = 0
              AND trade_date = :trade_date
              AND llm_fallback_level = 'failed'
            """,
            {"trade_date": trade_date},
        )
    ) or 0
    stray_reports = list_stray_unpublished_reports(db, runtime_trade_date=runtime_trade_date)
    published_outside_core_count = _count_published_reports_outside_core_pool(
        db,
        trade_date=trade_date,
    )
    hotspot_truth = _hotspot_truth_summary(db, trade_date=trade_date)
    kline_coverage = shared._to_int(
        shared._scalar(
            db,
            """
            SELECT COUNT(DISTINCT stock_code)
            FROM kline_daily
            WHERE trade_date = :trade_date
            """,
            {"trade_date": trade_date},
        )
    ) or 0
    minimum_published = max(1, int(pool_count * min_published_coverage_ratio)) if pool_count > 0 else 1
    total_generated = published_count + unpublished_count
    unpublished_ratio = (unpublished_count / total_generated) if total_generated > 0 else 0.0
    failed_ratio = (llm_failed_count / total_generated) if total_generated > 0 else 0.0
    passed = (
        (kline_coverage > 0 or published_count > 0)
        and pool_count > 0
        and published_count >= minimum_published
        and not stray_reports
        and published_outside_core_count <= 0
        and unpublished_ratio <= max_unpublished_ratio
        and not hotspot_truth["hotspot_truth_mismatch"]
    )
    reasons: list[str] = []
    if kline_coverage <= 0 and published_count <= 0:
        reasons.append("trade_date_not_ready")
    if pool_count <= 0:
        reasons.append("pool_not_ready")
    if published_count < minimum_published:
        reasons.append("published_coverage_insufficient")
    if stray_reports:
        reasons.append("stray_unpublished_reports_present")
    if published_outside_core_count > 0:
        reasons.append("published_outside_core_pool")
    if unpublished_ratio > max_unpublished_ratio:
        reasons.append("unpublished_ratio_too_high")
    if hotspot_truth["hotspot_truth_mismatch"]:
        reasons.append("hotspot_truth_mismatch")
    return {
        "passed": passed,
        "reasons": reasons,
        "runtime_trade_date": runtime_trade_date,
        "trade_date": trade_date,
        "kline_coverage": kline_coverage,
        "pool_count": pool_count,
        "published_count": published_count,
        "unpublished_count": unpublished_count,
        "llm_failed_count": llm_failed_count,
        "published_outside_core_count": published_outside_core_count,
        "minimum_published": minimum_published,
        "unpublished_ratio": round(unpublished_ratio, 6),
        "llm_failed_ratio": round(failed_ratio, 6),
        "stray_unpublished_report_count": len(stray_reports),
        **hotspot_truth,
    }
