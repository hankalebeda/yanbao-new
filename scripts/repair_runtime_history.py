import argparse
from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import sys
import time
from typing import Any
from uuid import uuid4

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repair missing complete public batches inside the runtime natural-day window."
    )
    parser.add_argument("--trade-date", default=None, help="Runtime trade date to repair up to. Defaults to latest runtime trade date.")
    parser.add_argument(
        "--history-days",
        type=int,
        default=60,
        help="Natural-day window used to detect missing public batches.",
    )
    parser.add_argument(
        "--mock-llm",
        action="store_true",
        help="Force MOCK_LLM=true during missing report generation.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        default=None,
        help="Per-report LLM timeout used during gap repair. Defaults to 45s when router_primary=claude_cli, otherwise 20s.",
    )
    return parser.parse_args()


def _effective_request_timeout_seconds(
    requested_seconds: int | None,
    *,
    router_primary: str | None,
) -> int:
    base_seconds = 20 if requested_seconds is None else int(requested_seconds)
    if str(router_primary or "").strip().lower() == "claude_cli" and requested_seconds is None:
        base_seconds = max(base_seconds, 45)
    return max(base_seconds, 5)


REPAIR_REPORT_ROUND_LIMIT = 3
REPAIR_STRATEGY_TYPES = ("A", "B", "C")


_RUNTIME_SIM_DELETE_ORDER = (
    "baseline_equity_curve_point",
    "sim_dashboard_snapshot",
    "sim_equity_curve_point",
    "sim_position",
    "sim_account",
)
_RUNTIME_SIM_RESTORE_ORDER = (
    "sim_account",
    "sim_position",
    "sim_equity_curve_point",
    "sim_dashboard_snapshot",
    "baseline_equity_curve_point",
)


def _snapshot_runtime_sim_state(db) -> dict[str, list[dict[str, object]]]:
    from app.models import Base

    snapshot: dict[str, list[dict[str, object]]] = {}
    for table_name in _RUNTIME_SIM_RESTORE_ORDER:
        table = Base.metadata.tables[table_name]
        snapshot[table_name] = [dict(row) for row in db.execute(table.select()).mappings().all()]
    return snapshot


def _restore_runtime_sim_state(
    db,
    *,
    snapshot: dict[str, list[dict[str, object]]],
) -> None:
    from app.models import Base

    for table_name in _RUNTIME_SIM_DELETE_ORDER:
        db.execute(text(f"DELETE FROM {table_name}"))
    for table_name in _RUNTIME_SIM_RESTORE_ORDER:
        rows = snapshot.get(table_name) or []
        if not rows:
            continue
        table = Base.metadata.tables[table_name]
        db.execute(table.insert(), rows)
    db.commit()


def _delete_runtime_sim_state(db) -> None:
    for table_name in _RUNTIME_SIM_DELETE_ORDER:
        db.execute(text(f"DELETE FROM {table_name}"))
    db.commit()


def _fallback_batch_id(parent_trade_date_value: str) -> str:
    return f"fallback_t_minus_1:{parent_trade_date_value}"


def _repair_fallback_lineage_and_usage(
    db,
    *,
    trade_date_value: str | None = None,
    stock_codes: list[str] | None = None,
) -> dict[str, int]:
    filters = ["k.source_batch_id LIKE 'fallback_t_minus_1:%'"]
    params: dict[str, object] = {}
    if trade_date_value:
        filters.append("k.trade_date = :trade_date")
        params["trade_date"] = trade_date_value
    if stock_codes:
        code_params = {f"stock_code_{idx}": value for idx, value in enumerate(stock_codes)}
        params.update(code_params)
        placeholders = ", ".join(f":{key}" for key in code_params)
        filters.append(f"k.stock_code IN ({placeholders})")

    rows = db.execute(
        text(
            f"""
            SELECT
                k.stock_code,
                k.trade_date,
                k.source_batch_id AS child_batch_id,
                parent.source_batch_id AS parent_batch_id
            FROM kline_daily k
            LEFT JOIN kline_daily parent
              ON parent.stock_code = k.stock_code
             AND parent.trade_date = (
                 SELECT MAX(k2.trade_date)
                 FROM kline_daily k2
                 WHERE k2.stock_code = k.stock_code
                   AND k2.trade_date < k.trade_date
             )
            WHERE {' AND '.join(filters)}
            ORDER BY k.trade_date ASC, k.stock_code ASC
            """
        ),
        params,
    ).mappings().all()
    if not rows:
        return {
            "fallback_batches": 0,
            "lineage_links": 0,
            "usage_updates": 0,
        }

    now = datetime.now(timezone.utc)
    batch_meta: dict[str, dict[str, object]] = {}
    for row in rows:
        child_batch_id = str(row["child_batch_id"])
        meta = batch_meta.setdefault(
            child_batch_id,
            {
                "trade_date": str(row["trade_date"])[:10],
                "stock_codes": set(),
                "parent_batch_ids": set(),
            },
        )
        meta["stock_codes"].add(str(row["stock_code"]))
        parent_batch_id = row.get("parent_batch_id")
        if parent_batch_id:
            meta["parent_batch_ids"].add(str(parent_batch_id))

    created_or_updated_batches = 0
    created_lineages = 0
    usage_updates = 0
    for child_batch_id, meta in batch_meta.items():
        trade_day = str(meta["trade_date"])
        stock_count = len(meta["stock_codes"])
        existing = db.execute(
            text(
                """
                SELECT batch_id
                FROM data_batch
                WHERE batch_id = :batch_id
                LIMIT 1
                """
            ),
            {"batch_id": child_batch_id},
        ).first()
        values = {
            "source_name": "tdx_local",
            "trade_date": trade_day,
            "batch_scope": "repair_fallback",
            "quality_flag": "stale_ok",
            "covered_stock_count": stock_count,
            "core_pool_covered_count": stock_count,
            "records_total": stock_count,
            "records_success": stock_count,
            "records_failed": 0,
            "status_reason": "fallback_t_minus_1",
            "updated_at": now,
        }
        if existing:
            db.execute(
                text(
                    """
                    UPDATE data_batch
                    SET source_name = :source_name,
                        trade_date = :trade_date,
                        batch_scope = :batch_scope,
                        batch_status = 'SUCCESS',
                        quality_flag = :quality_flag,
                        covered_stock_count = :covered_stock_count,
                        core_pool_covered_count = :core_pool_covered_count,
                        records_total = :records_total,
                        records_success = :records_success,
                        records_failed = :records_failed,
                        status_reason = :status_reason,
                        updated_at = :updated_at
                    WHERE batch_id = :batch_id
                    """
                ),
                {"batch_id": child_batch_id, **values},
            )
        else:
            db.execute(
                text(
                    """
                    INSERT INTO data_batch (
                        batch_id, source_name, trade_date, batch_scope, batch_seq,
                        batch_status, quality_flag, covered_stock_count, core_pool_covered_count,
                        records_total, records_success, records_failed, status_reason,
                        trigger_task_run_id, started_at, finished_at, updated_at, created_at
                    ) VALUES (
                        :batch_id, :source_name, :trade_date, :batch_scope, 1,
                        'SUCCESS', :quality_flag, :covered_stock_count, :core_pool_covered_count,
                        :records_total, :records_success, :records_failed, :status_reason,
                        NULL, :created_at, :finished_at, :updated_at, :created_at
                    )
                    """
                ),
                {
                    "batch_id": child_batch_id,
                    "created_at": now,
                    "finished_at": now,
                    **values,
                },
            )
        created_or_updated_batches += 1

        for parent_batch_id in sorted(meta["parent_batch_ids"]):
            existing_lineage = db.execute(
                text(
                    """
                    SELECT 1
                    FROM data_batch_lineage
                    WHERE child_batch_id = :child_batch_id
                      AND parent_batch_id = :parent_batch_id
                      AND lineage_role = 'FALLBACK_FROM'
                    LIMIT 1
                    """
                ),
                {"child_batch_id": child_batch_id, "parent_batch_id": parent_batch_id},
            ).first()
            if existing_lineage:
                continue
            db.execute(
                text(
                    """
                    INSERT INTO data_batch_lineage (
                        batch_lineage_id, child_batch_id, parent_batch_id, lineage_role, created_at
                    ) VALUES (
                        :batch_lineage_id, :child_batch_id, :parent_batch_id, 'FALLBACK_FROM', :created_at
                    )
                    """
                ),
                {
                    "batch_lineage_id": str(uuid4()),
                    "child_batch_id": child_batch_id,
                    "parent_batch_id": parent_batch_id,
                    "created_at": now,
                },
            )
            created_lineages += 1

        for stock_code in sorted(meta["stock_codes"]):
            result = db.execute(
                text(
                    """
                    UPDATE report_data_usage
                    SET batch_id = :batch_id,
                        source_name = 'tdx_local',
                        status = 'stale_ok',
                        status_reason = 'fallback_t_minus_1'
                    WHERE trade_date = :trade_date
                      AND stock_code = :stock_code
                      AND dataset_name = 'kline_daily'
                    """
                ),
                {
                    "batch_id": child_batch_id,
                    "trade_date": trade_day,
                    "stock_code": stock_code,
                },
            )
            usage_updates += int(result.rowcount or 0)

    db.commit()
    return {
        "fallback_batches": created_or_updated_batches,
        "lineage_links": created_lineages,
        "usage_updates": usage_updates,
    }


def _select_repair_generation_targets(
    *,
    trade_date_value: str,
    missing_codes: list[str],
) -> tuple[list[str], dict[str, str]]:
    from app.core.db import SessionLocal
    from app.services.report_generation_ssot import _preselect_one_per_strategy_type

    normalized_codes = list(dict.fromkeys(str(code).strip().upper() for code in missing_codes if str(code).strip()))
    if not normalized_codes:
        return [], {}

    selected_codes, strategy_type_override_map = _preselect_one_per_strategy_type(
        SessionLocal,
        stock_codes=normalized_codes,
        trade_date=trade_date_value,
    )
    if not selected_codes:
        selected_codes = normalized_codes[:REPAIR_REPORT_ROUND_LIMIT]
        strategy_type_override_map = {}

    capped_codes = list(dict.fromkeys(selected_codes))[:REPAIR_REPORT_ROUND_LIMIT]
    capped_override_map = {
        stock_code: str(strategy_type).strip().upper()
        for stock_code, strategy_type in (strategy_type_override_map or {}).items()
        if stock_code in capped_codes and str(strategy_type).strip().upper() in REPAIR_STRATEGY_TYPES
    }
    return capped_codes, capped_override_map


def _repair_summary_is_expected_partial(summary: dict[str, Any]) -> bool:
    if bool(summary.get("complete_public_batch")):
        return False
    return (
        bool(summary.get("round_limit_hit"))
        and int(summary.get("remaining_missing_reports") or 0) > 0
        and int(summary.get("published_generated_reports") or 0) > 0
    )


def _repair_exact_pool_codes(
    db,
    *,
    trade_date_value: str,
) -> list[str]:
    from app.services.stock_pool import get_daily_stock_pool

    fallback_pool_codes = list(
        dict.fromkeys(get_daily_stock_pool(trade_date=trade_date_value, exact_trade_date=True))
    )

    latest_refresh = db.execute(
        text(
            """
            SELECT task_id
            FROM stock_pool_refresh_task
            WHERE trade_date = :trade_date
              AND status IN ('COMPLETED', 'FALLBACK')
            ORDER BY
              CASE WHEN status = 'COMPLETED' THEN 0 ELSE 1 END,
              updated_at DESC,
              finished_at DESC,
              created_at DESC
            LIMIT 1
            """
        ),
        {"trade_date": trade_date_value},
    ).mappings().first()
    if latest_refresh is None:
        return fallback_pool_codes

    snapshot_rows = db.execute(
        text(
            """
            SELECT stock_code
            FROM stock_pool_snapshot
            WHERE refresh_task_id = :refresh_task_id
              AND trade_date = :trade_date
              AND pool_role = 'core'
            ORDER BY rank_no ASC, stock_code ASC
            """
        ),
        {
            "refresh_task_id": latest_refresh["task_id"],
            "trade_date": trade_date_value,
        },
    ).fetchall()
    snapshot_codes = list(
        dict.fromkeys(
            str(row[0])
            for row in snapshot_rows
            if row[0]
        )
    )
    if snapshot_codes:
        return snapshot_codes

    return fallback_pool_codes


def _materialize_t_minus_1_klines(
    db,
    *,
    trade_date_value: str,
    stock_codes: list[str],
) -> set[str]:
    fallback_codes: set[str] = set()
    now = datetime.now(timezone.utc)
    for stock_code in stock_codes:
        exists = db.execute(
            text(
                """
                SELECT 1
                FROM kline_daily
                WHERE stock_code = :stock_code
                  AND trade_date = :trade_date
                LIMIT 1
                """
            ),
            {"stock_code": stock_code, "trade_date": trade_date_value},
        ).first()
        if exists:
            continue
        previous_row = db.execute(
            text(
                """
                SELECT
                    open, high, low, close, volume, amount, adjust_type, atr_pct, turnover_rate,
                    ma5, ma10, ma20, ma60, volatility_20d, hs300_return_20d, is_suspended,
                    source_batch_id, trade_date
                FROM kline_daily
                WHERE stock_code = :stock_code
                  AND trade_date < :trade_date
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ),
            {"stock_code": stock_code, "trade_date": trade_date_value},
        ).mappings().first()
        if previous_row is None:
            continue
        child_batch_id = _fallback_batch_id(str(previous_row["trade_date"])[:10])
        db.execute(
            text(
                """
                INSERT INTO kline_daily (
                    kline_id, stock_code, trade_date, open, high, low, close,
                    volume, amount, adjust_type, atr_pct, turnover_rate, ma5, ma10, ma20, ma60,
                    volatility_20d, hs300_return_20d, is_suspended, source_batch_id, created_at
                ) VALUES (
                    :kline_id, :stock_code, :trade_date, :open, :high, :low, :close,
                    :volume, :amount, :adjust_type, :atr_pct, :turnover_rate, :ma5, :ma10, :ma20, :ma60,
                    :volatility_20d, :hs300_return_20d, :is_suspended, :source_batch_id, :created_at
                )
                """
            ),
            {
                "kline_id": str(uuid4()),
                "stock_code": stock_code,
                "trade_date": trade_date_value,
                "open": previous_row["open"],
                "high": previous_row["high"],
                "low": previous_row["low"],
                "close": previous_row["close"],
                "volume": previous_row["volume"],
                "amount": previous_row["amount"],
                "adjust_type": previous_row["adjust_type"],
                "atr_pct": previous_row["atr_pct"],
                "turnover_rate": previous_row["turnover_rate"],
                "ma5": previous_row["ma5"],
                "ma10": previous_row["ma10"],
                "ma20": previous_row["ma20"],
                "ma60": previous_row["ma60"],
                "volatility_20d": previous_row["volatility_20d"],
                "hs300_return_20d": previous_row["hs300_return_20d"],
                "is_suspended": previous_row["is_suspended"],
                "source_batch_id": child_batch_id,
                "created_at": now,
            },
        )
        fallback_codes.add(stock_code)
    db.commit()
    if fallback_codes:
        _repair_fallback_lineage_and_usage(
            db,
            trade_date_value=trade_date_value,
            stock_codes=sorted(fallback_codes),
        )
    return fallback_codes


def _expire_repair_blocking_tasks(
    db,
    *,
    trade_date_value: str,
    stock_code: str | None = None,
) -> int:
    now = datetime.now(timezone.utc)
    sql_text = """
        UPDATE report_generation_task
        SET status = 'Expired',
            status_reason = 'repair_history_preempted_stale_task',
            finished_at = :now,
            updated_at = :now
        WHERE trade_date = :trade_date
          AND status IN ('Pending', 'Processing', 'Suspended')
    """
    params = {"trade_date": trade_date_value, "now": now}
    if stock_code:
        sql_text += " AND stock_code = :stock_code"
        params["stock_code"] = stock_code
    result = db.execute(
        text(sql_text),
        params,
    )
    db.commit()
    return int(result.rowcount or 0)


def _expire_published_report_nonterminal_tasks(
    db,
    *,
    trade_date_value: str,
) -> int:
    now = datetime.now(timezone.utc)
    result = db.execute(
        text(
            """
            UPDATE report_generation_task
            SET status = 'Expired',
                status_reason = 'repair_history_report_already_published',
                finished_at = :now,
                updated_at = :now
            WHERE trade_date = :trade_date
              AND status IN ('Pending', 'Processing', 'Suspended')
              AND EXISTS (
                    SELECT 1
                    FROM report
                    WHERE report.trade_date = report_generation_task.trade_date
                      AND report.stock_code = report_generation_task.stock_code
                      AND report.published = 1
                      AND report.is_deleted = 0
              )
            """
        ),
        {"trade_date": trade_date_value, "now": now},
    )
    db.commit()
    return int(result.rowcount or 0)


def _stabilize_complete_public_batch_trace(
    db,
    *,
    trade_date_value: str,
    max_attempts: int = 3,
    sleep_seconds: float = 0.2,
) -> bool:
    from app.services.runtime_anchor_service import RuntimeAnchorService

    for attempt in range(max_attempts):
        _expire_published_report_nonterminal_tasks(
            db,
            trade_date_value=trade_date_value,
        )
        if RuntimeAnchorService(db).has_complete_public_batch_trace(trade_date=trade_date_value):
            return True
        if attempt < max_attempts - 1:
            time.sleep(sleep_seconds)
    return False


def _runtime_history_anchor_trade_dates(
    db,
    *,
    trade_date_value: str,
) -> list[str]:
    from app.services.runtime_anchor_service import RuntimeAnchorService

    return RuntimeAnchorService(db).runtime_history_anchor_trade_dates(
        trade_date_value=trade_date_value,
    )


def _legacy_rebuild_dashboard_window_snapshots_unused(db, *, snapshot_date: str) -> None:
    from app.services.settlement_ssot import (
        _annualized_return_from_cumulative,
        _compounded_cumulative_return,
        _load_window_settled_results,
        _max_drawdown_from_returns,
        _strategy_trade_day_span,
        _window_buy_report_counts,
        baseline_ma_cross_market_metrics,
        baseline_random_market_metrics,
    )

    snapshot_day = date.fromisoformat(snapshot_date)
    from app.models import Base
    strategy_table = Base.metadata.tables["strategy_metric_snapshot"]
    baseline_table = Base.metadata.tables["baseline_metric_snapshot"]
    now = datetime.now(timezone.utc)
    zero_threshold = 0.0001

    for window_days in (1, 7, 14, 30, 60):
        settled_rows = _load_window_settled_results(
            db,
            trade_day=snapshot_day,
            window_days=window_days,
        )
        buy_counts = _window_buy_report_counts(
            db,
            trade_day=snapshot_day,
            window_days=window_days,
        )
        db.execute(
            strategy_table.delete().where(
                (strategy_table.c.snapshot_date == snapshot_day)
                & (strategy_table.c.window_days == window_days)
            )
        )
        db.execute(
            baseline_table.delete().where(
                (baseline_table.c.snapshot_date == snapshot_day)
                & (baseline_table.c.window_days == window_days)
            )
        )
        random_metrics = baseline_random_market_metrics(
            db,
            trade_day=snapshot_day,
            window_days=window_days,
        )
        ma_metrics = baseline_ma_cross_market_metrics(
            db,
            trade_day=snapshot_day,
            window_days=window_days,
        )
        strategy_cumulative = _compounded_cumulative_return(
            [float(row.get("net_return_pct") or 0.0) for row in settled_rows]
        )
        random_cumulative = random_metrics.get("cumulative_return_pct")
        signal_validity_warning = bool(
            settled_rows
            and strategy_cumulative is not None
            and random_cumulative is not None
            and strategy_cumulative < float(random_cumulative)
        )

        by_strategy: dict[str, list[dict[str, object]]] = {"A": [], "B": [], "C": []}
        for row in settled_rows:
            key = str(row.get("strategy_type") or "")
            if key in by_strategy:
                by_strategy[key].append(row)

        for strategy_type in ("A", "B", "C"):
            subset = by_strategy.get(strategy_type, [])
            sample_size = len(subset)
            denominator = buy_counts.get(strategy_type, 0)
            coverage_pct = round(sample_size / denominator, 6) if denominator > 0 else 0.0
            returns = [float(row.get("net_return_pct") or 0.0) for row in subset]
            cumulative_return_pct = _compounded_cumulative_return(returns)
            if sample_size < 30:
                payload = {
                    "win_rate": None,
                    "profit_loss_ratio": None,
                    "alpha_annual": None,
                    "max_drawdown_pct": None,
                    "cumulative_return_pct": cumulative_return_pct,
                    "display_hint": "样本积累中" if sample_size > 0 else None,
                }
            else:
                wins = [value for value in returns if value > zero_threshold]
                losses = [abs(value) for value in returns if value < -zero_threshold]
                non_zero = len(wins) + len(losses)
                avg_win = (sum(wins) / len(wins)) if wins else 0.0
                avg_loss = (sum(losses) / len(losses)) if losses else None
                trade_day_span = _strategy_trade_day_span(
                    db,
                    subset,
                    default_window_days=window_days,
                )
                payload = {
                    "win_rate": round(len(wins) / non_zero, 6) if non_zero > 0 else None,
                    "profit_loss_ratio": round(avg_win / avg_loss, 6) if avg_loss else None,
                    "alpha_annual": _annualized_return_from_cumulative(
                        cumulative_return_pct,
                        trade_day_count=trade_day_span,
                    ),
                    "max_drawdown_pct": _max_drawdown_from_returns(returns),
                    "cumulative_return_pct": cumulative_return_pct,
                    "display_hint": None,
                }
            db.execute(
                strategy_table.insert().values(
                    metric_snapshot_id=str(uuid4()),
                    snapshot_date=snapshot_day,
                    strategy_type=strategy_type,
                    window_days=window_days,
                    data_status="READY",
                    sample_size=sample_size,
                    coverage_pct=coverage_pct,
                    win_rate=payload["win_rate"],
                    profit_loss_ratio=payload["profit_loss_ratio"],
                    alpha_annual=payload["alpha_annual"],
                    max_drawdown_pct=payload["max_drawdown_pct"],
                    cumulative_return_pct=payload["cumulative_return_pct"],
                    signal_validity_warning=signal_validity_warning if sample_size > 0 else False,
                    display_hint=payload["display_hint"],
                    created_at=now,
                )
            )

        for metrics in (random_metrics, ma_metrics):
            db.execute(
                baseline_table.insert().values(
                    baseline_metric_snapshot_id=str(uuid4()),
                    snapshot_date=snapshot_day,
                    window_days=window_days,
                    baseline_type=metrics["baseline_type"],
                    simulation_runs=metrics.get("simulation_runs"),
                    sample_size=metrics["sample_size"],
                    win_rate=metrics.get("win_rate"),
                    profit_loss_ratio=metrics.get("profit_loss_ratio"),
                    alpha_annual=metrics.get("alpha_annual"),
                    max_drawdown_pct=metrics.get("max_drawdown_pct"),
                    cumulative_return_pct=metrics.get("cumulative_return_pct"),
                    display_hint=metrics.get("display_hint"),
                    created_at=now,
                )
            )
    db.commit()


def _rebuild_dashboard_window_snapshots(db, *, snapshot_date: str) -> list[dict[str, object]]:
    from app.services.settlement_ssot import VALID_WINDOWS, rebuild_fr07_snapshot_history

    summary = rebuild_fr07_snapshot_history(
        db,
        trade_days=[snapshot_date],
        window_days_list=sorted(VALID_WINDOWS),
        purge_invalid=True,
        prune_missing_dates=False,
    )
    db.commit()
    return list(summary.get("rebuilt") or [])


def _repair_trade_date(
    db,
    *,
    trade_date_value: str,
) -> dict[str, Any]:
    from app.services.market_state import compute_and_persist_market_state
    from app.services.report_generation_ssot import ReportGenerationServiceError, generate_report_ssot
    from app.services.stock_pool import refresh_stock_pool
    from scripts.rebuild_runtime_db import (
        count_kline_coverage,
        ensure_bootstrap_market_state_ready,
        ensure_report_usage_rows,
    )

    # Runtime history repair only needs enough same-day kline coverage to
    # rebuild a valid 200-name core pool. Reusing the full rebuild floor (250)
    # blocks repair on already-serviceable days like a 200-name fallback batch.
    min_repair_pool_coverage = 200
    _expire_repair_blocking_tasks(db, trade_date_value=trade_date_value)

    coverage = count_kline_coverage(db, trade_date_value=trade_date_value)
    if coverage < min_repair_pool_coverage:
        raise RuntimeError(
            f"cannot repair trade_date={trade_date_value}: kline coverage={coverage} < {min_repair_pool_coverage}"
        )

    refresh_stock_pool(db, trade_date=trade_date_value, force_rebuild=True)
    compute_and_persist_market_state(db, trade_date=date.fromisoformat(trade_date_value))
    ensure_bootstrap_market_state_ready(db, trade_date_value=trade_date_value)

    pool_codes = _repair_exact_pool_codes(
        db,
        trade_date_value=trade_date_value,
    )
    missing_kline_codes = {
        stock_code
        for stock_code in pool_codes
        if not db.execute(
            text(
                """
                SELECT 1
                FROM kline_daily
                WHERE stock_code = :stock_code
                  AND trade_date = :trade_date
                LIMIT 1
                """
            ),
            {"stock_code": stock_code, "trade_date": trade_date_value},
        ).first()
    }
    fallback_codes: set[str] = set()
    if missing_kline_codes:
        fallback_codes = _materialize_t_minus_1_klines(
            db,
            trade_date_value=trade_date_value,
            stock_codes=sorted(missing_kline_codes),
        )
        if missing_kline_codes - fallback_codes:
            unresolved = sorted(missing_kline_codes - fallback_codes)
            raise RuntimeError(
                f"cannot repair trade_date={trade_date_value}: unresolved missing kline codes={unresolved[:10]}"
        )
        refresh_stock_pool(db, trade_date=trade_date_value, force_rebuild=True)
        compute_and_persist_market_state(db, trade_date=date.fromisoformat(trade_date_value))
        ensure_bootstrap_market_state_ready(db, trade_date_value=trade_date_value)
        pool_codes = _repair_exact_pool_codes(
            db,
            trade_date_value=trade_date_value,
        )

    if len(pool_codes) < 200:
        raise RuntimeError(
            f"cannot repair trade_date={trade_date_value}: exact pool size={len(pool_codes)} < 200"
        )

    pool_code_set = set(pool_codes)
    extra_report_rows = db.execute(
        text(
            """
            SELECT report_id, stock_code
            FROM report
            WHERE trade_date = :trade_date
              AND published = 1
              AND is_deleted = 0
            """
        ),
        {"trade_date": trade_date_value},
    ).mappings().all()
    deleted_reports = 0
    if extra_report_rows:
        now = datetime.now(timezone.utc)
        for row in extra_report_rows:
            if str(row["stock_code"]) in pool_code_set:
                continue
            db.execute(
                text(
                    """
                    UPDATE report
                    SET is_deleted = 1,
                        deleted_at = :deleted_at,
                        updated_at = :updated_at
                    WHERE report_id = :report_id
                    """
                ),
                {
                    "report_id": row["report_id"],
                    "deleted_at": now,
                    "updated_at": now,
                },
            )
            db.execute(
                text(
                    """
                    DELETE FROM settlement_result
                    WHERE report_id = :report_id
                    """
                ),
                {"report_id": row["report_id"]},
            )
            deleted_reports += 1

    ensure_report_usage_rows(db, trade_date_value=trade_date_value, stock_codes=pool_codes)
    if fallback_codes:
        for stock_code in fallback_codes:
            db.execute(
                text(
                    """
                    UPDATE report_data_usage
                    SET status = 'stale_ok',
                        status_reason = 'fallback_t_minus_1'
                    WHERE trade_date = :trade_date
                      AND dataset_name = 'kline_daily'
                      AND stock_code = :stock_code
                    """
                ),
                {"trade_date": trade_date_value, "stock_code": stock_code},
            )
        _repair_fallback_lineage_and_usage(
            db,
            trade_date_value=trade_date_value,
            stock_codes=sorted(fallback_codes),
        )
    db.commit()

    existing_codes = {
        str(row[0])
        for row in db.execute(
            text(
                """
                SELECT stock_code
                FROM report
                WHERE trade_date = :trade_date
                  AND published = 1
                  AND is_deleted = 0
                  AND LOWER(COALESCE(quality_flag, 'ok')) = 'ok'
                """
            ),
            {"trade_date": trade_date_value},
        ).fetchall()
    }
    missing_codes = [stock_code for stock_code in pool_codes if stock_code not in existing_codes]
    generation_targets, strategy_type_override_map = _select_repair_generation_targets(
        trade_date_value=trade_date_value,
        missing_codes=missing_codes,
    )
    requested_missing_reports = len(missing_codes)
    scheduled_reports = len(generation_targets)
    round_limit_hit = requested_missing_reports > scheduled_reports
    deferred_reports = max(requested_missing_reports - scheduled_reports, 0)
    generated = 0
    published_generated = 0
    strategy_distribution: dict[str, list[str]] = {strategy_type: [] for strategy_type in REPAIR_STRATEGY_TYPES}

    def _record_result(stock_code: str, result: Any, *, forced_strategy_type: str | None) -> None:
        nonlocal generated, published_generated
        generated += 1
        resolved_strategy_type = ""
        if isinstance(result, dict):
            if bool(result.get("published")):
                published_generated += 1
            resolved_strategy_type = str(result.get("strategy_type") or forced_strategy_type or "").strip().upper()
        else:
            resolved_strategy_type = str(forced_strategy_type or "").strip().upper()
        if resolved_strategy_type in strategy_distribution:
            strategy_distribution[resolved_strategy_type].append(stock_code)

    for stock_code in generation_targets:
        forced_strategy_type = strategy_type_override_map.get(stock_code)
        _expire_repair_blocking_tasks(
            db,
            trade_date_value=trade_date_value,
            stock_code=stock_code,
        )
        try:
            result = generate_report_ssot(
                db,
                stock_code=stock_code,
                trade_date=trade_date_value,
                force_same_day_rebuild=True,
                forced_strategy_type=forced_strategy_type,
            )
            _record_result(stock_code, result, forced_strategy_type=forced_strategy_type)
        except ReportGenerationServiceError as exc:
            if exc.error_code != "CONCURRENT_CONFLICT":
                raise
            _expire_repair_blocking_tasks(
                db,
                trade_date_value=trade_date_value,
                stock_code=stock_code,
            )
            existing_row = db.execute(
                text(
                    """
                    SELECT 1
                    FROM report
                    WHERE trade_date = :trade_date
                      AND stock_code = :stock_code
                      AND published = 1
                      AND is_deleted = 0
                      AND LOWER(COALESCE(quality_flag, 'ok')) = 'ok'
                    LIMIT 1
                    """
                ),
                {"trade_date": trade_date_value, "stock_code": stock_code},
            ).first()
            if existing_row:
                continue
            result = generate_report_ssot(
                db,
                stock_code=stock_code,
                trade_date=trade_date_value,
                force_same_day_rebuild=True,
                forced_strategy_type=forced_strategy_type,
            )
            _record_result(stock_code, result, forced_strategy_type=forced_strategy_type)
    _expire_repair_blocking_tasks(db, trade_date_value=trade_date_value)
    _expire_published_report_nonterminal_tasks(db, trade_date_value=trade_date_value)
    db.commit()
    remaining_missing_reports = max(requested_missing_reports - published_generated, 0)
    complete_public_batch = _stabilize_complete_public_batch_trace(
        db,
        trade_date_value=trade_date_value,
    )
    return {
        "kline_coverage": coverage,
        "pool_size": len(pool_codes),
        "deleted_reports": deleted_reports,
        "generated_reports": generated,
        "published_generated_reports": published_generated,
        "requested_missing_reports": requested_missing_reports,
        "scheduled_reports": scheduled_reports,
        "deferred_reports": deferred_reports,
        "remaining_missing_reports": remaining_missing_reports,
        "round_limit": REPAIR_REPORT_ROUND_LIMIT,
        "round_limit_hit": round_limit_hit,
        "complete_public_batch": complete_public_batch,
        "strategy_distribution": strategy_distribution,
    }


def _rebuild_runtime_sim_history(
    db,
    *,
    runtime_trade_date: str,
    replay_trade_dates: list[str],
) -> dict[str, object]:
    from app.services.runtime_materialization import (
        ensure_sim_accounts,
        rebuild_runtime_sim_history as materialize_runtime_sim_history,
    )
    from app.services.settlement_ssot import (
        SettlementServiceError,
        VALID_WINDOWS,
        _load_reports,
        rebuild_fr07_snapshot_history,
        submit_settlement_task,
    )
    from app.services.sim_positioning_ssot import process_trade_date

    sim_state_snapshot = _snapshot_runtime_sim_state(db)
    try:
        _delete_runtime_sim_state(db)
        ensure_sim_accounts(db)
        for trade_date_value in replay_trade_dates:
            process_trade_date(db, trade_date_value)
        db.commit()

        db.execute(
            text(
                """
                DELETE FROM settlement_task
                WHERE trade_date = :trade_date
                  AND target_scope = 'all'
                  AND status IN ('QUEUED', 'PROCESSING')
                """
            ),
            {"trade_date": runtime_trade_date},
        )
        db.commit()

        runtime_day = date.fromisoformat(runtime_trade_date)
        for window_days in (1, 7, 14, 30, 60):
            due_reports = _load_reports(
                db,
                trade_day=runtime_day,
                window_days=window_days,
                target_scope="all",
                target_report_id=None,
                target_stock_code=None,
            )
            if not due_reports:
                for table_name in ("strategy_metric_snapshot", "baseline_metric_snapshot", "baseline_task"):
                    db.execute(
                        text(
                            f"""
                            DELETE FROM {table_name}
                            WHERE snapshot_date = :snapshot_date
                              AND window_days = :window_days
                            """
                        ),
                        {"snapshot_date": runtime_trade_date, "window_days": window_days},
                    )
                db.commit()
                print(f"skip_settlement_window={window_days} reason=no_due_reports")
                continue
            try:
                submit_settlement_task(
                    db,
                    trade_date=runtime_trade_date,
                    window_days=window_days,
                    target_scope="all",
                    force=True,
                )
            except SettlementServiceError as exc:
                if exc.error_code != "DEPENDENCY_NOT_READY":
                    raise
                db.rollback()
                for table_name in ("strategy_metric_snapshot", "baseline_metric_snapshot", "baseline_task"):
                    db.execute(
                        text(
                            f"""
                            DELETE FROM {table_name}
                            WHERE snapshot_date = :snapshot_date
                              AND window_days = :window_days
                            """
                        ),
                        {"snapshot_date": runtime_trade_date, "window_days": window_days},
                    )
                db.commit()
                print(f"skip_settlement_window={window_days} reason=dependency_not_ready")
        truth_history_summary = rebuild_fr07_snapshot_history(
            db,
            trade_days=replay_trade_dates,
            window_days_list=VALID_WINDOWS,
            purge_invalid=True,
            prune_missing_dates=True,
        )
        sim_history_summary = materialize_runtime_sim_history(
            db,
            snapshot_dates=replay_trade_dates,
            purge_existing=True,
            prune_missing_dates=True,
        )
        db.commit()
        return {
            "truth_history_summary": truth_history_summary,
            "sim_history_summary": sim_history_summary,
        }
    except BaseException:
        db.rollback()
        _restore_runtime_sim_state(db, snapshot=sim_state_snapshot)
        raise


def main() -> int:
    args = parse_args()

    from app.core.config import settings
    from app.core.db import SessionLocal, engine
    from app.services.dashboard_query import get_dashboard_stats_payload_ssot
    from app.services.home_query import get_home_payload_ssot
    from app.services.runtime_anchor_service import RuntimeAnchorService
    from app.services.sim_query import get_sim_dashboard_payload_ssot
    from app.services.trade_calendar import trade_days_in_range

    db = SessionLocal()
    try:
        effective_request_timeout_seconds = _effective_request_timeout_seconds(
            args.request_timeout_seconds,
            router_primary=getattr(settings, "router_primary", None),
        )
        settings.mock_llm = bool(args.mock_llm)
        settings.llm_audit_enabled = False
        settings.max_llm_retries = 0
        settings.request_timeout_seconds = effective_request_timeout_seconds
        settings.report_generation_llm_timeout_seconds = effective_request_timeout_seconds
        os.environ["SETTLEMENT_INLINE_EXECUTION"] = "1"

        runtime_trade_date = args.trade_date or RuntimeAnchorService(db).latest_runtime_trade_date()
        if runtime_trade_date is None:
            raise RuntimeError("runtime_trade_date is unavailable")

        window_start = (date.fromisoformat(runtime_trade_date) - timedelta(days=max(args.history_days - 1, 0))).isoformat()
        expected_trade_dates = trade_days_in_range(window_start, runtime_trade_date)
        gap_dates = [
            trade_date_value
            for trade_date_value in expected_trade_dates
            if trade_date_value < runtime_trade_date
            and not RuntimeAnchorService(db).has_complete_public_batch_trace(trade_date=trade_date_value)
        ]

        print(f"runtime_trade_date={runtime_trade_date}")
        print(f"history_days={args.history_days}")
        print(f"request_timeout_seconds={effective_request_timeout_seconds}")
        print(f"gap_dates={gap_dates}")
        if not gap_dates:
            print("no_missing_public_batches=true")

        repair_summaries = []
        for trade_date_value in gap_dates:
            summary = _repair_trade_date(
                db,
                trade_date_value=trade_date_value,
            )
            repair_summaries.append((trade_date_value, summary))
            print(
                f"repaired_trade_date={trade_date_value} "
                f"kline_coverage={summary['kline_coverage']} "
                f"pool_size={summary['pool_size']} "
                f"deleted_reports={summary['deleted_reports']} "
                f"generated_reports={summary['generated_reports']} "
                f"published_generated_reports={summary['published_generated_reports']} "
                f"scheduled_reports={summary['scheduled_reports']} "
                f"deferred_reports={summary['deferred_reports']} "
                f"complete_public_batch={summary['complete_public_batch']}"
            )
            if not summary["complete_public_batch"] and _repair_summary_is_expected_partial(summary):
                print(
                    f"repair_pending_trade_date={trade_date_value} "
                    f"remaining_missing_reports={summary['remaining_missing_reports']} "
                    f"round_limit={summary['round_limit']} "
                    f"strategy_distribution={summary['strategy_distribution']}"
                )
                continue
            if not summary["complete_public_batch"]:
                raise RuntimeError(f"trade_date={trade_date_value} still lacks a complete public batch after repair")

        fallback_repair_summary = _repair_fallback_lineage_and_usage(db)
        print(
            "fallback_lineage_repair="
            f"batches={fallback_repair_summary['fallback_batches']} "
            f"lineages={fallback_repair_summary['lineage_links']} "
            f"usage_updates={fallback_repair_summary['usage_updates']}"
        )

        replay_trade_dates = _runtime_history_anchor_trade_dates(
            db,
            trade_date_value=runtime_trade_date,
        )
        rebuild_summary = _rebuild_runtime_sim_history(
            db,
            runtime_trade_date=runtime_trade_date,
            replay_trade_dates=replay_trade_dates,
        )
        truth_history_summary = rebuild_summary["truth_history_summary"]
        sim_history_summary = rebuild_summary["sim_history_summary"]
        print(
            "fr07_truth_rebuild="
            f"snapshot_dates={truth_history_summary['snapshot_dates']} "
            f"window_days={truth_history_summary['window_days']} "
            f"rebuilt={len(truth_history_summary['rebuilt'])}"
        )
        print(
            "sim_history_rebuild="
            f"snapshot_dates={sim_history_summary['snapshot_dates']} "
            f"baseline_points={sim_history_summary['baseline_points']} "
            f"dashboard_snapshots={sim_history_summary['dashboard_snapshots']} "
            f"latest_snapshot_rows={sim_history_summary['latest_snapshot_rows']}"
        )

        home_payload = get_home_payload_ssot(db)
        dashboard_payloads = {
            window_days: get_dashboard_stats_payload_ssot(db, window_days=window_days)
            for window_days in (1, 7, 14, 30, 60)
        }
        sim_payloads = {
            tier: get_sim_dashboard_payload_ssot(db, capital_tier=tier)
            for tier in ("10k", "100k", "500k")
        }

        print(
            f"home_trade_date={home_payload.get('trade_date')} "
            f"pool_size={home_payload.get('pool_size')} "
            f"today_report_count={home_payload.get('today_report_count')} "
            f"data_status={home_payload.get('data_status')} "
            f"status_reason={home_payload.get('status_reason')}"
        )
        for window_days, payload in dashboard_payloads.items():
            print(
                f"dashboard_{window_days}d="
                f"{payload.get('date_range')} "
                f"reports={payload.get('total_reports')} "
                f"settled={payload.get('total_settled')} "
                f"status={payload.get('data_status')} "
                f"reason={payload.get('status_reason')}"
            )
        for tier, payload in sim_payloads.items():
            print(
                f"sim_{tier}="
                f"snapshot={payload.get('snapshot_date')} "
                f"status={payload.get('data_status')} "
                f"reason={payload.get('status_reason')} "
                f"sample={payload.get('sample_size')} "
                f"total_return_pct={payload.get('total_return_pct')}"
            )
        return 0
    finally:
        db.close()
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
