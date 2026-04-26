from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable
from uuid import uuid4

from sqlalchemy import bindparam, select, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.db import SessionLocal
from app.models import Base
from app.services.admin_audit import create_audit_log
from app.services.fr07_baseline_service import (
    load_random_baseline_market_returns as _load_random_baseline_market_returns_from_truth_rows,
    summarize_random_baseline_candidates,
)
from app.services.fr07_metrics import (
    FR07_MIN_SAMPLE_SIZE,
    FR07_SAMPLE_ACCUMULATING_HINT,
    annualized_return_from_cumulative,
    build_metric_payload,
    max_drawdown_pct_from_return_path,
    path_cumulative_return_pct,
)
from app.services.fr07_truth_filters import FR07_ELIGIBLE_REPORTS
from app.services.runtime_materialization import (
    materialize_baseline_equity_curve_points,
    materialize_sim_dashboard_snapshots,
)
from app.services.runtime_truth_guard import normalize_snapshot_truth
from app.services.trade_calendar import trade_date_after_n_days, trade_days_in_range

VALID_WINDOWS = {1, 7, 14, 30, 60}
VALID_TARGET_SCOPES = {"all", "report_id", "stock_code"}
ZERO_RETURN_THRESHOLD = 0.0001
SETTLEMENT_BATCH_WINDOWS = (1, 7, 14, 30, 60)
REQUIRED_STRATEGY_TYPES = ("A", "B", "C")
REQUIRED_BASELINE_TYPES = ("baseline_random", "baseline_ma_cross")
REQUIRED_SIM_CAPITAL_TIERS = ("10k", "100k", "500k")
PIPELINE_STATUS_ACCEPTED = "ACCEPTED"
PIPELINE_STATUS_RUNNING = "RUNNING"
PIPELINE_STATUS_COMPLETED = "COMPLETED"
PIPELINE_STATUS_FAILED = "FAILED"
PIPELINE_STATUS_DEGRADED = "DEGRADED"
PIPELINE_TERMINAL_STATUSES = frozenset({PIPELINE_STATUS_COMPLETED, PIPELINE_STATUS_FAILED, PIPELINE_STATUS_DEGRADED})
SETTLEMENT_PIPELINE_NAME = "settlement_pipeline"

# True mutex lock for settlement concurrency control (FR07-SETTLE-05)
_settlement_lock = threading.Lock()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _settlement_runs_inline() -> bool:
    return os.environ.get("SETTLEMENT_INLINE_EXECUTION", "").strip().lower() in {"1", "true", "yes", "on"}


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
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except ValueError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


@dataclass
class SettlementServiceError(RuntimeError):
    status_code: int
    error_code: str

    def __str__(self) -> str:
        return self.error_code


def _query_all(db: Session, sql_text: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in db.execute(text(sql_text), params).mappings().all()]


def _query_one(db: Session, sql_text: str, params: dict[str, Any]) -> dict[str, Any] | None:
    row = db.execute(text(sql_text), params).mappings().first()
    return dict(row) if row else None


def _pipeline_run_table():
    return Base.metadata.tables["pipeline_run"]


def _pipeline_suffix(*, target_scope: str, target_report_id: str | None, target_stock_code: str | None) -> str:
    suffix = target_report_id or target_stock_code or "all"
    return f"{target_scope}:{suffix}"


def _settlement_pipeline_name(
    *,
    window_days: int,
    target_scope: str,
    target_report_id: str | None,
    target_stock_code: str | None,
) -> str:
    return f"{SETTLEMENT_PIPELINE_NAME}:{window_days}:{_pipeline_suffix(target_scope=target_scope, target_report_id=target_report_id, target_stock_code=target_stock_code)}"


def _settlement_batch_pipeline_name(
    *,
    target_scope: str,
    target_report_id: str | None,
    target_stock_code: str | None,
) -> str:
    return f"{SETTLEMENT_PIPELINE_NAME}:batch:{_pipeline_suffix(target_scope=target_scope, target_report_id=target_report_id, target_stock_code=target_stock_code)}"


def _task_pipeline_name(task: dict[str, Any]) -> str:
    return _settlement_pipeline_name(
        window_days=int(task["window_days"]),
        target_scope=str(task["target_scope"]),
        target_report_id=task.get("target_report_id"),
        target_stock_code=task.get("target_stock_code"),
    )


def _task_to_pipeline_status(task_status: str | None) -> str:
    normalized = str(task_status or "").upper()
    if normalized == "PROCESSING":
        return PIPELINE_STATUS_RUNNING
    if normalized == "COMPLETED":
        return PIPELINE_STATUS_COMPLETED
    if normalized == "FAILED":
        return PIPELINE_STATUS_FAILED
    return PIPELINE_STATUS_ACCEPTED


def _generate_request_id(prefix: str) -> str:
    return f"{prefix}:{uuid4()}"


def _aggregate_pipeline_status(task_rows: list[dict[str, Any]]) -> tuple[str, bool, str | None, datetime | None, datetime | None]:
    if not task_rows:
        return PIPELINE_STATUS_ACCEPTED, False, None, None, None
    statuses = [str(row.get("status") or "").upper() for row in task_rows]
    started_candidates = [_as_datetime(row.get("started_at")) for row in task_rows if _as_datetime(row.get("started_at"))]
    finished_candidates = [_as_datetime(row.get("finished_at")) for row in task_rows if _as_datetime(row.get("finished_at"))]
    status_reason = next((str(row.get("status_reason")) for row in task_rows if row.get("status_reason")), None)
    if any(status == "PROCESSING" for status in statuses):
        return PIPELINE_STATUS_RUNNING, False, status_reason, min(started_candidates) if started_candidates else None, None
    if all(status == "QUEUED" for status in statuses):
        return PIPELINE_STATUS_ACCEPTED, False, status_reason, None, None
    if any(status == "QUEUED" for status in statuses):
        return PIPELINE_STATUS_RUNNING, False, status_reason, min(started_candidates) if started_candidates else None, None
    completed_count = sum(1 for status in statuses if status == "COMPLETED")
    failed_count = sum(1 for status in statuses if status == "FAILED")
    if completed_count == len(statuses):
        return (
            PIPELINE_STATUS_COMPLETED,
            False,
            None,
            min(started_candidates) if started_candidates else None,
            max(finished_candidates) if finished_candidates else None,
        )
    if failed_count == len(statuses):
        return (
            PIPELINE_STATUS_FAILED,
            False,
            status_reason,
            min(started_candidates) if started_candidates else None,
            max(finished_candidates) if finished_candidates else None,
        )
    if completed_count > 0 and failed_count > 0:
        return (
            PIPELINE_STATUS_DEGRADED,
            True,
            status_reason,
            min(started_candidates) if started_candidates else None,
            max(finished_candidates) if finished_candidates else None,
        )
    return PIPELINE_STATUS_RUNNING, False, status_reason, min(started_candidates) if started_candidates else None, None


def _has_strategy_metric_snapshot(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    min_created_at: datetime | None = None,
) -> bool:
    count = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM strategy_metric_snapshot
            WHERE snapshot_date = :snapshot_date
              AND window_days = :window_days
              AND (:min_created_at IS NULL OR created_at >= :min_created_at)
            """
        ),
        {
            "snapshot_date": trade_day,
            "window_days": int(window_days),
            "min_created_at": min_created_at,
        },
    ).scalar()
    return int(count or 0) >= len(REQUIRED_STRATEGY_TYPES)


def _has_baseline_metric_snapshot(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    min_created_at: datetime | None = None,
) -> bool:
    count = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM baseline_metric_snapshot
            WHERE snapshot_date = :snapshot_date
              AND window_days = :window_days
              AND (:min_created_at IS NULL OR created_at >= :min_created_at)
            """
        ),
        {
            "snapshot_date": trade_day,
            "window_days": int(window_days),
            "min_created_at": min_created_at,
        },
    ).scalar()
    return int(count or 0) >= len(REQUIRED_BASELINE_TYPES)


def _has_sim_dashboard_snapshot(
    db: Session,
    *,
    trade_day: date,
    min_created_at: datetime | None = None,
) -> bool:
    count = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM sim_dashboard_snapshot
            WHERE snapshot_date = :snapshot_date
              AND capital_tier IN :capital_tiers
              AND (:min_created_at IS NULL OR created_at >= :min_created_at)
            """
        ).bindparams(bindparam("capital_tiers", expanding=True)),
        {
            "snapshot_date": trade_day,
            "capital_tiers": REQUIRED_SIM_CAPITAL_TIERS,
            "min_created_at": min_created_at,
        },
    ).scalar()
    return int(count or 0) >= len(REQUIRED_SIM_CAPITAL_TIERS)


def _has_baseline_equity_curve_points(
    db: Session,
    *,
    trade_day: date,
    min_created_at: datetime | None = None,
) -> bool:
    count = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM baseline_equity_curve_point
            WHERE trade_date = :trade_date
              AND capital_tier IN :capital_tiers
              AND baseline_type IN :baseline_types
              AND (:min_created_at IS NULL OR created_at >= :min_created_at)
            """
        )
        .bindparams(bindparam("capital_tiers", expanding=True))
        .bindparams(bindparam("baseline_types", expanding=True)),
        {
            "trade_date": trade_day,
            "capital_tiers": REQUIRED_SIM_CAPITAL_TIERS,
            "baseline_types": REQUIRED_BASELINE_TYPES,
            "min_created_at": min_created_at,
        },
    ).scalar()
    return int(count or 0) >= (len(REQUIRED_SIM_CAPITAL_TIERS) * len(REQUIRED_BASELINE_TYPES))


def _pipeline_completion_ready(
    db: Session,
    *,
    trade_day: date,
    task_rows: list[dict[str, Any]],
) -> tuple[bool, str | None]:
    if not task_rows:
        return True, None
    task_started_at_by_window: dict[int, datetime] = {}
    for row in task_rows:
        if row.get("window_days") is None:
            continue
        window_days = int(row["window_days"])
        started_at = _as_datetime(row.get("started_at"))
        if started_at is None:
            continue
        previous_started_at = task_started_at_by_window.get(window_days)
        if previous_started_at is None or started_at > previous_started_at:
            task_started_at_by_window[window_days] = started_at
    window_days_set = sorted(
        {
            int(row["window_days"])
            for row in task_rows
            if row.get("window_days") is not None
        }
    )
    for window_days in window_days_set:
        min_created_at = task_started_at_by_window.get(window_days)
        if not _has_strategy_metric_snapshot(
            db,
            trade_day=trade_day,
            window_days=window_days,
            min_created_at=min_created_at,
        ):
            return False, "settlement_materialization_pending"
        if not _has_baseline_metric_snapshot(
            db,
            trade_day=trade_day,
            window_days=window_days,
            min_created_at=min_created_at,
        ):
            return False, "settlement_materialization_pending"
    if 30 in window_days_set:
        min_created_at = task_started_at_by_window.get(30)
        if not _has_baseline_equity_curve_points(
            db,
            trade_day=trade_day,
            min_created_at=min_created_at,
        ):
            return False, "settlement_materialization_pending"
        if not _has_sim_dashboard_snapshot(
            db,
            trade_day=trade_day,
            min_created_at=min_created_at,
        ):
            return False, "settlement_materialization_pending"
    return True, None


def _sync_pipeline_run_from_scope(
    db: Session,
    *,
    pipeline_name: str,
    trade_day: date,
    target_scope: str,
    target_report_id: str | None,
    target_stock_code: str | None,
    window_days_list: Iterable[int],
    request_id: str | None = None,
) -> None:
    normalized_windows = tuple(int(value) for value in window_days_list)
    if not normalized_windows:
        return
    query = """
        SELECT status, status_reason, started_at, finished_at, window_days
        FROM settlement_task
        WHERE trade_date = :trade_date
          AND target_scope = :target_scope
          AND window_days IN :window_days_list
    """
    params: dict[str, Any] = {
        "trade_date": trade_day,
        "target_scope": target_scope,
        "window_days_list": normalized_windows,
    }
    if target_scope == "report_id":
        query += " AND target_report_id = :target_report_id"
        params["target_report_id"] = target_report_id
    elif target_scope == "stock_code":
        query += " AND target_stock_code = :target_stock_code"
        params["target_stock_code"] = target_stock_code
    task_rows = [
        dict(row)
        for row in db.execute(
            text(query).bindparams(bindparam("window_days_list", expanding=True)),
            params,
        ).mappings().all()
    ]
    pipeline_status, degraded, status_reason, started_at, finished_at = _aggregate_pipeline_status(task_rows)
    if pipeline_status == PIPELINE_STATUS_COMPLETED:
        completion_ready, readiness_reason = _pipeline_completion_ready(
            db,
            trade_day=trade_day,
            task_rows=task_rows,
        )
        if not completion_ready:
            pipeline_status = PIPELINE_STATUS_RUNNING
            status_reason = readiness_reason
            finished_at = None
    _upsert_pipeline_run(
        db,
        pipeline_name=pipeline_name,
        trade_day=trade_day,
        pipeline_status=pipeline_status,
        request_id=request_id,
        degraded=degraded,
        status_reason=status_reason,
        started_at=started_at,
        finished_at=finished_at,
    )


def _upsert_pipeline_run(
    db: Session,
    *,
    pipeline_name: str,
    trade_day: date,
    pipeline_status: str,
    request_id: str | None = None,
    degraded: bool = False,
    status_reason: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    reset_timestamps: bool = False,
) -> None:
    table = _pipeline_run_table()
    now = utc_now()
    existing = _query_one(
        db,
        """
        SELECT pipeline_run_id, request_id, started_at, finished_at, created_at
        FROM pipeline_run
        WHERE pipeline_name = :pipeline_name
          AND trade_date = :trade_date
        LIMIT 1
        """,
        {"pipeline_name": pipeline_name, "trade_date": trade_day},
    )
    resolved_started_at = started_at
    resolved_finished_at = finished_at
    created_at = now
    if existing:
        created_at = _as_datetime(existing.get("created_at")) or now
        if reset_timestamps:
            resolved_started_at = started_at
            resolved_finished_at = finished_at
        else:
            resolved_started_at = resolved_started_at or _as_datetime(existing.get("started_at"))
            resolved_finished_at = resolved_finished_at or _as_datetime(existing.get("finished_at"))
    if pipeline_status == PIPELINE_STATUS_RUNNING and resolved_started_at is None:
        resolved_started_at = now
    if pipeline_status in PIPELINE_TERMINAL_STATUSES and resolved_finished_at is None:
        resolved_finished_at = now
    if pipeline_status == PIPELINE_STATUS_ACCEPTED and reset_timestamps:
        resolved_started_at = None
        resolved_finished_at = None
    values = dict(
        pipeline_name=pipeline_name,
        trade_date=trade_day,
        pipeline_status=pipeline_status,
        degraded=bool(degraded or pipeline_status == PIPELINE_STATUS_DEGRADED),
        status_reason=status_reason,
        request_id=request_id if request_id is not None else (existing or {}).get("request_id"),
        started_at=resolved_started_at,
        finished_at=resolved_finished_at,
        updated_at=now,
    )
    if existing:
        db.execute(
            table.update()
            .where(table.c.pipeline_run_id == existing["pipeline_run_id"])
            .values(**values)
        )
        return
    db.execute(
        table.insert().values(
            pipeline_run_id=str(uuid4()),
            created_at=created_at,
            **values,
        )
    )


def _sync_pipeline_run_from_task(db: Session, task: dict[str, Any]) -> None:
    trade_day = _as_date(task.get("trade_date"))
    request_id = str(task.get("request_id") or "").strip()
    if trade_day is None or not request_id:
        return
    pipeline_row = _query_one(
        db,
        """
        SELECT pipeline_name
        FROM pipeline_run
        WHERE trade_date = :trade_date
          AND request_id = :request_id
        LIMIT 1
        """,
        {"trade_date": trade_day, "request_id": request_id},
    )
    if pipeline_row is None:
        return
    task_rows = _query_all(
        db,
        """
        SELECT status, status_reason, started_at, finished_at, window_days
        FROM settlement_task
        WHERE trade_date = :trade_date
          AND request_id = :request_id
        ORDER BY created_at ASC, task_id ASC
        """,
        {"trade_date": trade_day, "request_id": request_id},
    )
    pipeline_status, degraded, status_reason, started_at, finished_at = _aggregate_pipeline_status(task_rows)
    if pipeline_status == PIPELINE_STATUS_COMPLETED:
        completion_ready, readiness_reason = _pipeline_completion_ready(
            db,
            trade_day=trade_day,
            task_rows=task_rows,
        )
        if not completion_ready:
            pipeline_status = PIPELINE_STATUS_RUNNING
            status_reason = readiness_reason
            finished_at = None
    _upsert_pipeline_run(
        db,
        pipeline_name=str(pipeline_row["pipeline_name"]),
        trade_day=trade_day,
        pipeline_status=pipeline_status,
        request_id=request_id,
        degraded=degraded,
        status_reason=status_reason,
        started_at=started_at,
        finished_at=finished_at,
    )


def _sync_admin_operation_from_settlement_task(db: Session, task: dict[str, Any]) -> None:
    task_id = str(task.get("task_id") or "").strip()
    task_status = str(task.get("status") or "").upper()
    if not task_id or task_status not in {"COMPLETED", "FAILED"}:
        return

    operation = _query_one(
        db,
        """
        SELECT operation_id, actor_user_id, request_id, status, after_snapshot
        FROM admin_operation
        WHERE action_type = 'RUN_SETTLEMENT'
          AND target_table = 'settlement_task'
          AND target_pk = :task_id
        ORDER BY created_at DESC, operation_id DESC
        LIMIT 1
        """,
        {"task_id": task_id},
    )
    if operation is None:
        return

    terminal_status = "FAILED" if task_status == "FAILED" else "COMPLETED"
    status_reason = str(task.get("status_reason") or "").strip() or None
    finished_at = _as_datetime(task.get("finished_at")) or utc_now()
    after_snapshot = _json_object(operation.get("after_snapshot"))
    trade_day = _as_date(task.get("trade_date"))
    after_snapshot.update(
        {
            "task_id": task_id,
            "trade_date": trade_day.isoformat() if trade_day is not None else None,
            "window_days": int(task.get("window_days") or 0),
            "target_scope": task.get("target_scope"),
            "target_report_id": task.get("target_report_id"),
            "target_stock_code": task.get("target_stock_code"),
            "force": bool(task.get("force")),
            "task_submit_status": after_snapshot.get("task_submit_status") or "QUEUED",
            "task_status_snapshot": task_status,
            "processed_count": int(task.get("processed_count") or 0),
            "skipped_count": int(task.get("skipped_count") or 0),
            "failed_count": int(task.get("failed_count") or 0),
            "status_reason": status_reason,
        }
    )

    operation_table = Base.metadata.tables["admin_operation"]
    db.execute(
        operation_table.update()
        .where(operation_table.c.operation_id == operation["operation_id"])
        .values(
            status=terminal_status,
            failure_category=status_reason if terminal_status == "FAILED" else None,
            status_reason=status_reason if terminal_status == "FAILED" else None,
            after_snapshot=after_snapshot,
            finished_at=finished_at,
        )
    )

    audit_exists = _query_one(
        db,
        """
        SELECT audit_log_id
        FROM audit_log
        WHERE operation_id = :operation_id
        LIMIT 1
        """,
        {"operation_id": operation["operation_id"]},
    )
    if audit_exists is not None:
        return

    create_audit_log(
        db,
        actor_user_id=str(operation.get("actor_user_id") or ""),
        action_type="RUN_SETTLEMENT",
        target_table="settlement_task",
        target_pk=task_id,
        request_id=str(operation.get("request_id") or ""),
        operation_id=str(operation["operation_id"]),
        failure_category=status_reason if terminal_status == "FAILED" else None,
        before_snapshot=None,
        after_snapshot=after_snapshot,
    )


def _mark_task_failed(db: Session, *, task_id: str, status_reason: str) -> None:
    task_table = Base.metadata.tables["settlement_task"]
    now = utc_now()
    db.execute(
        task_table.update()
        .where(task_table.c.task_id == task_id)
        .values(
            status="FAILED",
            status_reason=status_reason,
            finished_at=now,
            updated_at=now,
        )
    )
    task = _query_one(
        db,
        """
        SELECT *
        FROM settlement_task
        WHERE task_id = :task_id
        LIMIT 1
        """,
        {"task_id": task_id},
    )
    if task is not None:
        _sync_pipeline_run_from_task(db, task)
        _sync_admin_operation_from_settlement_task(db, task)


def _process_task_with_failure_capture(db: Session, *, task_id: str) -> None:
    try:
        _process_task(db, task_id=task_id)
        db.commit()
    except SettlementServiceError as exc:
        db.rollback()
        _mark_task_failed(db, task_id=task_id, status_reason=exc.error_code)
        db.commit()
    except Exception as exc:
        db.rollback()
        _mark_task_failed(db, task_id=task_id, status_reason=exc.__class__.__name__)
        db.commit()


def _process_task_async(task_id: str) -> None:
    db = SessionLocal()
    try:
        _process_task_with_failure_capture(db, task_id=task_id)
    finally:
        db.close()


def _scope_key(
    *,
    trade_date: str,
    window_days: int,
    target_scope: str,
    target_report_id: str | None,
    target_stock_code: str | None,
) -> str:
    suffix = target_report_id or target_stock_code or "all"
    return f"{trade_date}:{window_days}:{target_scope}:{suffix}"


def _validate_payload(
    *,
    trade_date: str,
    window_days: int,
    target_scope: str,
    target_report_id: str | None,
    target_stock_code: str | None,
) -> date:
    if window_days not in VALID_WINDOWS or target_scope not in VALID_TARGET_SCOPES:
        raise SettlementServiceError(422, "INVALID_PAYLOAD")
    if target_scope == "report_id" and not target_report_id:
        raise SettlementServiceError(422, "INVALID_PAYLOAD")
    if target_scope == "stock_code" and not target_stock_code:
        raise SettlementServiceError(422, "INVALID_PAYLOAD")
    return date.fromisoformat(trade_date)


def _load_reports(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    target_scope: str,
    target_report_id: str | None,
    target_stock_code: str | None,
) -> list[dict[str, Any]]:
    where = [
        *FR07_ELIGIBLE_REPORTS.sql_clauses(alias="r"),
        "r.recommendation = 'BUY'",
        "r.trade_date < :trade_date",
    ]
    params: dict[str, Any] = {"trade_date": trade_day}
    if target_scope == "report_id":
        where.append("r.report_id = :target_report_id")
        params["target_report_id"] = target_report_id
    elif target_scope == "stock_code":
        where.append("r.stock_code = :target_stock_code")
        params["target_stock_code"] = target_stock_code

    candidate_rows = _query_all(
        db,
        f"""
        SELECT
            r.report_id,
            r.stock_code,
            r.trade_date,
            r.strategy_type,
            r.quality_flag,
            r.confidence,
            i.signal_entry_price,
            i.stop_loss,
            i.target_price
        FROM report r
        JOIN instruction_card i ON i.report_id = r.report_id
        WHERE {' AND '.join(where)}
        ORDER BY r.trade_date ASC, r.report_id ASC
        """,
        params,
    )
    due_rows: list[dict[str, Any]] = []
    for row in candidate_rows:
        signal_day = _as_date(row.get("trade_date"))
        if signal_day is None:
            continue
        due_trade_date = _due_trade_date(signal_day, window_days)
        if due_trade_date > trade_day:
            continue
        row["due_trade_date"] = due_trade_date
        due_rows.append(row)
    return due_rows


def _due_trade_date(signal_day: date, window_days: int) -> date:
    return date.fromisoformat(trade_date_after_n_days(signal_day.isoformat(), window_days))


def _window_start(trade_day: date, window_days: int) -> date:
    lookback_start = trade_day - timedelta(days=max(window_days * 3, 90))
    trade_days = trade_days_in_range(lookback_start.isoformat(), trade_day.isoformat())
    if len(trade_days) >= window_days:
        return date.fromisoformat(trade_days[-window_days])
    if trade_days:
        return date.fromisoformat(trade_days[0])
    return trade_day - timedelta(days=window_days - 1)


def _trade_day_count_between(db: Session, start_day: date, end_day: date) -> int:
    if end_day < start_day:
        return 0
    return int(
        db.execute(
            text(
                """
                SELECT COUNT(DISTINCT trade_date)
                FROM kline_daily
                WHERE trade_date BETWEEN :start_day AND :end_day
                """
            ),
            {"start_day": start_day, "end_day": end_day},
        ).scalar()
        or 0
    )


def _compounded_cumulative_return(returns: list[float]) -> float | None:
    return path_cumulative_return_pct(returns)


def _max_drawdown_from_returns(returns: list[float]) -> float | None:
    return max_drawdown_pct_from_return_path(returns)


def _annualized_return_from_cumulative(
    cumulative_return: float | None,
    *,
    trade_day_count: int,
) -> float | None:
    return annualized_return_from_cumulative(
        cumulative_return,
        trade_day_count=trade_day_count,
    )


def _strategy_trade_day_span(
    db: Session,
    rows: list[dict[str, Any]],
    *,
    default_window_days: int,
) -> int:
    signal_days = [_as_date(row.get("signal_date")) for row in rows if row.get("signal_date")]
    exit_days = [_as_date(row.get("exit_trade_date")) for row in rows if row.get("exit_trade_date")]
    if not signal_days or not exit_days:
        return max(default_window_days, 1)
    start_day = min(signal_days)
    end_day = max(exit_days)
    trade_day_count = _trade_day_count_between(db, start_day, end_day)
    if trade_day_count > 0:
        return trade_day_count
    return max((end_day - start_day).days + 1, 1)


def _load_window_settled_results(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
) -> list[dict[str, Any]]:
    window_start = _window_start(trade_day, window_days)
    return _query_all(
        db,
        """
        SELECT DISTINCT
            s.report_id,
            s.stock_code,
            s.signal_date,
            s.exit_trade_date,
            COALESCE(r.strategy_type, s.strategy_type) AS strategy_type,
            s.net_return_pct
        FROM settlement_result s
        JOIN report r ON r.report_id = s.report_id
        WHERE s.settlement_status = 'settled'
          AND s.window_days = :window_days
          AND s.exit_trade_date BETWEEN :window_start AND :trade_day
          AND """
        + FR07_ELIGIBLE_REPORTS.sql_condition(alias="r")
        + """
        ORDER BY s.exit_trade_date ASC, s.report_id ASC
        """,
        {
            "window_days": window_days,
            "window_start": window_start,
            "trade_day": trade_day,
        },
    )


def _window_buy_report_counts(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
) -> dict[str, int]:
    window_start = _window_start(trade_day, window_days)
    rows = _query_all(
        db,
        """
        SELECT r.strategy_type, r.trade_date
        FROM report r
        WHERE """
        + FR07_ELIGIBLE_REPORTS.sql_condition(alias="r")
        + """
          AND r.recommendation = 'BUY'
          AND r.trade_date < :trade_day
        """,
        {
            "trade_day": trade_day,
        },
    )
    counts: dict[str, int] = {}
    for row in rows:
        signal_day = _as_date(row.get("trade_date"))
        if signal_day is None:
            continue
        due_trade_day = _due_trade_date(signal_day, window_days)
        if due_trade_day < window_start or due_trade_day > trade_day:
            continue
        strategy_type = str(row.get("strategy_type") or "")
        counts[strategy_type] = counts.get(strategy_type, 0) + 1
    return counts


def _purge_invalid_settlement_results(db: Session) -> int:
    result = db.execute(
        text(
            """
            DELETE FROM settlement_result
            WHERE settlement_result_id IN (
                SELECT s.settlement_result_id
                FROM settlement_result s
                LEFT JOIN report r ON r.report_id = s.report_id
                WHERE r.report_id IS NULL
                   OR """
        + FR07_ELIGIBLE_REPORTS.invalid_sql_condition(alias="r")
        + """
            )
            """
        )
    )
    return int(result.rowcount or 0)


def purge_invalid_settlement_results(db: Session) -> int:
    return _purge_invalid_settlement_results(db)


def _fee_breakdown(buy_price: float, sell_price: float, shares: int = 100) -> dict[str, float]:
    buy_amount = buy_price * shares
    sell_amount = sell_price * shares
    buy_commission = max(buy_amount * 0.00025, 5.0)
    sell_commission = max(sell_amount * 0.00025, 5.0)
    stamp_duty = sell_amount * 0.0005
    buy_slippage_cost = buy_amount * 0.0005
    sell_slippage_cost = sell_amount * 0.0005
    gross_return_pct = (sell_amount - buy_amount) / buy_amount if buy_amount else 0.0
    buy_paid = buy_amount + buy_commission + buy_slippage_cost
    sell_get = sell_amount - sell_commission - stamp_duty - sell_slippage_cost
    net_return_pct = (sell_get - buy_paid) / buy_paid if buy_paid else 0.0
    return {
        "shares": shares,
        "buy_commission": round(buy_commission, 4),
        "sell_commission": round(sell_commission, 4),
        "stamp_duty": round(stamp_duty, 4),
        "buy_slippage_cost": round(buy_slippage_cost, 4),
        "sell_slippage_cost": round(sell_slippage_cost, 4),
        "gross_return_pct": round(gross_return_pct, 6),
        "net_return_pct": round(net_return_pct, 6),
    }


def _summarize_return_series(
    returns: list[float],
    *,
    baseline_type: str,
    window_days: int,
) -> dict[str, Any]:
    sample_size = len(returns)
    if sample_size <= 0:
        return {
            "baseline_type": baseline_type,
            "simulation_runs": None,
            "sample_size": 0,
            "win_rate": None,
            "profit_loss_ratio": None,
            "alpha_annual": None,
            "max_drawdown_pct": None,
            "cumulative_return_pct": None,
            "display_hint": FR07_SAMPLE_ACCUMULATING_HINT,
            "window_days": window_days,
        }
    metric_payload = build_metric_payload(
        returns,
        trade_day_count=max(window_days, 1),
    )
    return {
        "baseline_type": baseline_type,
        "simulation_runs": None,
        "sample_size": sample_size,
        "win_rate": metric_payload["win_rate"],
        "profit_loss_ratio": metric_payload["profit_loss_ratio"],
        "alpha_annual": metric_payload["alpha_annual"],
        "max_drawdown_pct": metric_payload["max_drawdown_pct"],
        "cumulative_return_pct": metric_payload["cumulative_return_pct"],
        "display_hint": metric_payload["display_hint"],
        "window_days": window_days,
    }


def _window_trade_date_strings(
    *,
    start_day: date,
    end_day: date,
) -> set[str]:
    if end_day < start_day:
        return set()
    return set(trade_days_in_range(start_day.isoformat(), end_day.isoformat()))


def _load_core_pool_market_signal_rows(
    db: Session,
    *,
    start_day: date,
    end_day: date,
) -> list[dict[str, Any]]:
    valid_trade_dates = _window_trade_date_strings(
        start_day=start_day,
        end_day=end_day,
    )
    rows = _query_all(
        db,
        """
        SELECT DISTINCT
            k.stock_code,
            k.trade_date,
            k.close,
            k.ma5,
            k.ma20
        FROM kline_daily k
        JOIN (
            SELECT DISTINCT trade_date, stock_code
            FROM stock_pool_snapshot
            WHERE pool_role = 'core'
        ) core
          ON core.stock_code = k.stock_code
         AND core.trade_date = k.trade_date
        WHERE k.trade_date BETWEEN :start_day AND :end_day
        ORDER BY k.trade_date ASC, k.stock_code ASC
        """,
        {"start_day": start_day, "end_day": end_day},
    )
    return [
        row
        for row in rows
        if _as_date(row.get("trade_date")) is not None
        and _as_date(row.get("trade_date")).isoformat() in valid_trade_dates
    ]


def _load_market_kline_history(
    db: Session,
    *,
    stock_codes: set[str],
    start_day: date,
    end_day: date,
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[tuple[str, str], dict[str, Any]],
    dict[str, dict[str, int]],
]:
    if not stock_codes or end_day < start_day:
        return {}, {}, {}
    valid_trade_dates = _window_trade_date_strings(
        start_day=start_day,
        end_day=end_day,
    )

    kline_table = Base.metadata.tables["kline_daily"]
    rows = [
        dict(row)
        for row in db.execute(
            select(
                kline_table.c.stock_code,
                kline_table.c.trade_date,
                kline_table.c.close,
                kline_table.c.ma5,
                kline_table.c.ma20,
            )
            .where(
                kline_table.c.stock_code.in_(sorted(stock_codes)),
                kline_table.c.trade_date >= start_day,
                kline_table.c.trade_date <= end_day,
            )
            .order_by(kline_table.c.stock_code.asc(), kline_table.c.trade_date.asc())
        ).mappings()
    ]

    by_code: dict[str, list[dict[str, Any]]] = {}
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    by_index: dict[str, dict[str, int]] = {}
    for row in rows:
        stock_code = str(row.get("stock_code") or "")
        trade_day_value = _as_date(row.get("trade_date"))
        if (
            not stock_code
            or trade_day_value is None
            or trade_day_value.isoformat() not in valid_trade_dates
        ):
            continue
        trade_day_text = trade_day_value.isoformat()
        normalized = {
            "stock_code": stock_code,
            "trade_date": trade_day_text,
            "close": row.get("close"),
            "ma5": row.get("ma5"),
            "ma20": row.get("ma20"),
        }
        history = by_code.setdefault(stock_code, [])
        history.append(normalized)
        by_key[(stock_code, trade_day_text)] = normalized
        by_index.setdefault(stock_code, {})[trade_day_text] = len(history) - 1
    return by_code, by_key, by_index


def _load_ma_cross_baseline_returns(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
) -> list[dict[str, Any]]:
    exit_window_start = _window_start(trade_day, window_days)
    signal_scan_start = _window_start(trade_day, max(window_days * 2, window_days + 1))
    signal_rows = _load_core_pool_market_signal_rows(
        db,
        start_day=signal_scan_start,
        end_day=trade_day,
    )
    history_start = _window_start(trade_day, max(window_days * 2 + 1, window_days + 2))
    history_by_code, history_by_key, history_index = _load_market_kline_history(
        db,
        stock_codes={str(row.get("stock_code") or "") for row in signal_rows},
        start_day=history_start,
        end_day=trade_day,
    )

    baseline_rows: list[dict[str, Any]] = []
    for row in signal_rows:
        signal_day = _as_date(row.get("trade_date"))
        stock_code = str(row.get("stock_code") or "")
        entry_price = float(row.get("close") or 0.0)
        current_ma5 = row.get("ma5")
        current_ma20 = row.get("ma20")
        signal_index = history_index.get(stock_code, {}).get(signal_day.isoformat() if signal_day else "")
        if signal_index is None or signal_index <= 0:
            continue
        history = history_by_code.get(stock_code, [])
        previous_row = history[signal_index - 1]
        previous_ma5 = previous_row.get("ma5")
        previous_ma20 = previous_row.get("ma20")
        if (
            signal_day is None
            or not stock_code
            or entry_price <= 0
            or current_ma5 is None
            or current_ma20 is None
            or previous_ma5 is None
            or previous_ma20 is None
        ):
            continue
        if not (float(current_ma5) > float(current_ma20) and float(previous_ma5) <= float(previous_ma20)):
            continue

        max_exit_day = _due_trade_date(signal_day, window_days)
        exit_row = None
        for future_row in history[signal_index + 1 :]:
            future_day = _as_date(future_row.get("trade_date"))
            if future_day is None or future_day > min(max_exit_day, trade_day):
                break
            future_ma5 = future_row.get("ma5")
            future_ma20 = future_row.get("ma20")
            if future_ma5 is None or future_ma20 is None:
                continue
            if float(future_ma5) < float(future_ma20):
                exit_row = future_row
                break
        if exit_row is None:
            exit_row = history_by_key.get((stock_code, max_exit_day.isoformat()))
        if exit_row is None or float(exit_row.get("close") or 0.0) <= 0:
            continue
        exit_trade_day = _as_date(exit_row.get("trade_date"))
        if exit_trade_day is None or exit_trade_day < exit_window_start or exit_trade_day > trade_day:
            continue

        fees = _fee_breakdown(entry_price, float(exit_row["close"]))
        baseline_rows.append(
            {
                "stock_code": stock_code,
                "signal_date": signal_day,
                "exit_trade_date": exit_trade_day,
                "net_return_pct": fees["net_return_pct"],
            }
        )
    return baseline_rows


def baseline_ma_cross_market_metrics(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
) -> dict[str, Any]:
    returns = [
        float(row.get("net_return_pct") or 0.0)
        for row in _load_ma_cross_baseline_returns(
            db,
            trade_day=trade_day,
            window_days=window_days,
        )
    ]
    return _summarize_return_series(
        returns,
        baseline_type="baseline_ma_cross",
        window_days=window_days,
    )


def baseline_random_market_metrics(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    truth_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return summarize_random_baseline_candidates(
        load_random_baseline_market_returns(
            db,
            trade_day=trade_day,
            window_days=window_days,
            truth_rows=truth_rows,
        ),
        window_days=window_days,
        trade_day=trade_day,
    )


def baseline_random_metrics(results: list[dict[str, Any]], *, window_days: int) -> dict[str, Any]:
    """Legacy compatibility helper.

    Runtime truth uses ``baseline_random_market_metrics()`` together with
    ``rebuild_fr07_snapshot()``. Keep this helper stable for older direct-call
    tests, but do not route runtime settlement or rebuild flows through it.
    """

    if not results:
        return {
            "baseline_type": "baseline_random",
            "simulation_runs": 500,
            "sample_size": 0,
            "win_rate": None,
            "profit_loss_ratio": None,
            "alpha_annual": None,
            "max_drawdown_pct": None,
            "cumulative_return_pct": None,
            "display_hint": FR07_SAMPLE_ACCUMULATING_HINT,
            "window_days": window_days,
        }
    import random
    sample_size = len(results)
    returns = [float(item.get("net_return_pct") or 0.0) for item in results]
    if sample_size < 30:
        return {
            "baseline_type": "baseline_random",
            "simulation_runs": 500,
            "sample_size": sample_size,
            "win_rate": None,
            "profit_loss_ratio": None,
            "alpha_annual": None,
            "max_drawdown_pct": None,
            "cumulative_return_pct": None,
            "display_hint": FR07_SAMPLE_ACCUMULATING_HINT,
            "window_days": window_days,
        }
    # 蒙特卡洛：500轮自助法采样(有放回)收益序列，模拟随机买卖的期望绩效
    # 注意：简单shuffle不改变win_rate/pnl_ratio（集合不变），需有放回采样才能产生差异
    RUNS = 500
    win_rates = []
    pnl_ratios = []
    max_dds = []
    cum_rets = []
    rng = random.Random(42)  # 固定种子确保可复现
    n = len(returns)
    for _ in range(RUNS):
        # 有放回采样，每轮抽取与原序列等长的随机样本
        sampled = [rng.choice(returns) for _ in range(n)]
        wins = [r for r in sampled if r > ZERO_RETURN_THRESHOLD]
        losses = [abs(r) for r in sampled if r < -ZERO_RETURN_THRESHOLD]
        decisive_positions = len(wins) + len(losses)
        wr = len(wins) / decisive_positions if decisive_positions > 0 else 0.5
        win_rates.append(wr)
        avg_w = sum(wins) / len(wins) if wins else 0.0
        avg_l = sum(losses) / len(losses) if losses else 1.0
        pnl_ratios.append(avg_w / avg_l if avg_l > 0 else 0.0)
        max_dds.append(_max_drawdown_from_returns(sampled) or 0.0)
        # settlement_result.net_return_pct is already a full-trade outcome.
        # Use the run's average sampled trade return instead of compounding
        # independent trades into one path, which over-amplifies outliers.
        cum_rets.append((sum(sampled) / len(sampled)) if sampled else 0.0)
    avg_wr = float(median(win_rates))
    avg_pnl = float(median(pnl_ratios))
    avg_dd = float(median(max_dds))
    avg_cum = float(median(cum_rets))
    return {
        "baseline_type": "baseline_random",
        "simulation_runs": RUNS,
        "sample_size": sample_size,
        "win_rate": round(avg_wr, 6),
        "profit_loss_ratio": round(avg_pnl, 6),
        "alpha_annual": _annualized_return_from_cumulative(avg_cum, trade_day_count=max(window_days, 1)),
        "max_drawdown_pct": round(avg_dd, 6),
        "cumulative_return_pct": round(avg_cum, 6),
        "display_hint": None,
        "window_days": window_days,
    }


def load_random_baseline_market_returns(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    truth_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return random-market candidates.

    Runtime callers should pass explicit ``truth_rows`` so the baseline remains
    anchored to the same eligible FR-07 truth sample. Direct callers without an
    explicit truth skeleton now only fall back to settled FR-07 truth rows and
    never rebuild a baseline from raw report candidates.
    """

    effective_truth_rows = truth_rows
    if effective_truth_rows is None:
        effective_truth_rows = _load_window_settled_results(
            db,
            trade_day=trade_day,
            window_days=window_days,
        )
    if not effective_truth_rows:
        return []
    return _load_random_baseline_market_returns_from_truth_rows(
        db,
        trade_day=trade_day,
        window_days=window_days,
        truth_rows=effective_truth_rows,
    )


def _load_random_baseline_market_returns_from_truth_rows(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    truth_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for truth in truth_rows:
        signal_day = _as_date(truth.get("signal_date"))
        exit_day = _as_date(truth.get("exit_trade_date"))
        if signal_day is None or exit_day is None:
            continue

        core_stocks = _query_all(
            db,
            """
            SELECT DISTINCT stock_code
            FROM stock_pool_snapshot
            WHERE trade_date = :signal_date
              AND pool_role = 'core'
            """,
            {"signal_date": signal_day},
        )
        if not core_stocks:
            continue

        for stock_row in core_stocks:
            stock_code = str(stock_row["stock_code"])
            entry_kline = _query_one(
                db,
                "SELECT close FROM kline_daily WHERE stock_code = :sc AND trade_date = :td LIMIT 1",
                {"sc": stock_code, "td": signal_day},
            )
            exit_kline = _query_one(
                db,
                "SELECT close FROM kline_daily WHERE stock_code = :sc AND trade_date = :td LIMIT 1",
                {"sc": stock_code, "td": exit_day},
            )
            if not entry_kline or not exit_kline:
                continue
            entry_price = float(entry_kline["close"] or 0)
            exit_price = float(exit_kline["close"] or 0)
            if entry_price <= 0 or exit_price <= 0:
                continue
            fees = _fee_breakdown(entry_price, exit_price)
            results.append({
                "stock_code": stock_code,
                "signal_date": signal_day,
                "exit_trade_date": exit_day,
                "net_return_pct": fees["net_return_pct"],
            })
    return results


def baseline_ma_cross_metrics(results: list[dict[str, Any]], *, window_days: int) -> dict[str, Any]:
    """Legacy compatibility helper.

    Runtime truth uses ``baseline_ma_cross_market_metrics()`` together with
    ``rebuild_fr07_snapshot()``. Keep this helper for older direct-call tests.
    """

    if not results:
        return {
            "baseline_type": "baseline_ma_cross",
            "simulation_runs": None,
            "sample_size": 0,
            "win_rate": None,
            "profit_loss_ratio": None,
            "alpha_annual": None,
            "max_drawdown_pct": None,
            "cumulative_return_pct": None,
            "display_hint": None,
            "window_days": window_days,
        }
    # MA金叉基线: 仅保留strategy_type=B的结果作为MA金叉信号子集
    ma_subset = [r for r in results if str(r.get("strategy_type", "")) == "B"]
    if len(ma_subset) < 5:
        ma_subset = results  # 数据不足时退化为全量
    returns = [float(r.get("net_return_pct") or 0.0) for r in ma_subset]
    metric_payload = build_metric_payload(
        returns,
        trade_day_count=max(window_days, 1),
        sample_size=len(ma_subset),
    )
    return {
        "baseline_type": "baseline_ma_cross",
        "simulation_runs": None,
        "sample_size": len(ma_subset),
        "win_rate": metric_payload["win_rate"],
        "profit_loss_ratio": metric_payload["profit_loss_ratio"],
        "alpha_annual": metric_payload["alpha_annual"],
        "max_drawdown_pct": metric_payload["max_drawdown_pct"],
        "cumulative_return_pct": metric_payload["cumulative_return_pct"],
        "display_hint": metric_payload["display_hint"],
        "window_days": window_days,
    }


def _write_baseline_rows(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    random_metrics: dict[str, Any],
    ma_metrics: dict[str, Any],
) -> None:
    baseline_task_table = Base.metadata.tables["baseline_task"]
    baseline_metric_table = Base.metadata.tables["baseline_metric_snapshot"]
    now = utc_now()

    # Delete existing rows for this (snapshot_date, window_days) before re-inserting
    db.execute(
        baseline_task_table.delete().where(
            (baseline_task_table.c.snapshot_date == trade_day)
            & (baseline_task_table.c.window_days == window_days)
        )
    )
    db.execute(
        baseline_metric_table.delete().where(
            (baseline_metric_table.c.snapshot_date == trade_day)
            & (baseline_metric_table.c.window_days == window_days)
        )
    )

    db.execute(
        baseline_task_table.insert().values(
            baseline_task_id=str(uuid4()),
            snapshot_date=trade_day,
            window_days=window_days,
            baseline_type="baseline_random",
            simulation_runs=random_metrics.get("simulation_runs"),
            status="BASELINE_COMPLETED",
            status_reason=None,
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    db.execute(
        baseline_task_table.insert().values(
            baseline_task_id=str(uuid4()),
            snapshot_date=trade_day,
            window_days=window_days,
            baseline_type="baseline_ma_cross",
            simulation_runs=ma_metrics.get("simulation_runs"),
            status="BASELINE_COMPLETED",
            status_reason=None,
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    for item in (random_metrics, ma_metrics):
        db.execute(
            baseline_metric_table.insert().values(
                baseline_metric_snapshot_id=str(uuid4()),
                snapshot_date=trade_day,
                window_days=window_days,
                baseline_type=item["baseline_type"],
                simulation_runs=item.get("simulation_runs"),
                sample_size=item["sample_size"],
                win_rate=item.get("win_rate"),
                profit_loss_ratio=item.get("profit_loss_ratio"),
                alpha_annual=item.get("alpha_annual"),
                max_drawdown_pct=item.get("max_drawdown_pct"),
                cumulative_return_pct=item.get("cumulative_return_pct"),
                display_hint=item.get("display_hint"),
                created_at=now,
            )
        )


def compute_baseline_metrics(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    truth_rows: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    return (
        baseline_random_market_metrics(
            db,
            trade_day=trade_day,
            window_days=window_days,
            truth_rows=truth_rows,
        ),
        baseline_ma_cross_market_metrics(
            db,
            trade_day=trade_day,
            window_days=window_days,
        ),
    )


def rebuild_baseline_snapshot(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    truth_rows: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    random_metrics, ma_metrics = compute_baseline_metrics(
        db,
        trade_day=trade_day,
        window_days=window_days,
        truth_rows=truth_rows,
    )
    _write_baseline_rows(
        db,
        trade_day=trade_day,
        window_days=window_days,
        random_metrics=random_metrics,
        ma_metrics=ma_metrics,
    )
    return random_metrics, ma_metrics


def backfill_baseline_snapshot_history(
    db: Session,
    *,
    trade_dates: Iterable[str | date],
    window_days: int = 30,
    prune_missing_dates: bool = False,
) -> dict[str, Any]:
    snapshot_days = sorted({_as_date(value) for value in trade_dates if value is not None})
    if not snapshot_days:
        return {"window_days": window_days, "snapshot_dates": 0}

    if prune_missing_dates:
        baseline_task_table = Base.metadata.tables["baseline_task"]
        baseline_metric_table = Base.metadata.tables["baseline_metric_snapshot"]
        min_day = snapshot_days[0]
        max_day = snapshot_days[-1]
        db.execute(
            baseline_task_table.delete().where(
                (baseline_task_table.c.window_days == window_days)
                & (baseline_task_table.c.snapshot_date >= min_day)
                & (baseline_task_table.c.snapshot_date <= max_day)
                & baseline_task_table.c.snapshot_date.notin_(snapshot_days)
            )
        )
        db.execute(
            baseline_metric_table.delete().where(
                (baseline_metric_table.c.window_days == window_days)
                & (baseline_metric_table.c.snapshot_date >= min_day)
                & (baseline_metric_table.c.snapshot_date <= max_day)
                & baseline_metric_table.c.snapshot_date.notin_(snapshot_days)
            )
        )

    for snapshot_day in snapshot_days:
        rebuild_baseline_snapshot(
            db,
            trade_day=snapshot_day,
            window_days=window_days,
        )
    db.flush()
    return {"window_days": window_days, "snapshot_dates": len(snapshot_days)}


def rebuild_fr07_snapshot_history(
    db: Session,
    *,
    trade_days: Iterable[str | date],
    window_days_list: Iterable[int] = VALID_WINDOWS,
    purge_invalid: bool = True,
    prune_missing_dates: bool = False,
) -> dict[str, Any]:
    snapshot_days = sorted({_as_date(value) for value in trade_days if value is not None})
    normalized_windows = tuple(sorted({int(value) for value in window_days_list}))
    invalid_windows = sorted({value for value in normalized_windows if value not in VALID_WINDOWS})
    if invalid_windows:
        raise ValueError(f"Unsupported window_days: {invalid_windows}")
    if not snapshot_days or not normalized_windows:
        return {
            "snapshot_dates": [],
            "window_days": list(normalized_windows),
            "rebuilt": [],
        }

    if prune_missing_dates:
        min_day = snapshot_days[0]
        max_day = snapshot_days[-1]
        strategy_table = Base.metadata.tables["strategy_metric_snapshot"]
        baseline_metric_table = Base.metadata.tables["baseline_metric_snapshot"]
        baseline_task_table = Base.metadata.tables["baseline_task"]
        db.execute(
            strategy_table.delete().where(
                strategy_table.c.window_days.in_(normalized_windows),
                strategy_table.c.snapshot_date >= min_day,
                strategy_table.c.snapshot_date <= max_day,
                strategy_table.c.snapshot_date.notin_(snapshot_days),
            )
        )
        db.execute(
            baseline_metric_table.delete().where(
                baseline_metric_table.c.window_days.in_(normalized_windows),
                baseline_metric_table.c.snapshot_date >= min_day,
                baseline_metric_table.c.snapshot_date <= max_day,
                baseline_metric_table.c.snapshot_date.notin_(snapshot_days),
            )
        )
        db.execute(
            baseline_task_table.delete().where(
                baseline_task_table.c.window_days.in_(normalized_windows),
                baseline_task_table.c.snapshot_date >= min_day,
                baseline_task_table.c.snapshot_date <= max_day,
                baseline_task_table.c.snapshot_date.notin_(snapshot_days),
            )
        )

    summaries: list[dict[str, Any]] = []
    purge_once = bool(purge_invalid)
    for snapshot_day in snapshot_days:
        for current_window_days in normalized_windows:
            summaries.append(
                rebuild_fr07_snapshot(
                    db,
                    trade_day=snapshot_day,
                    window_days=current_window_days,
                    purge_invalid=purge_once,
                )
            )
            purge_once = False
    db.flush()
    return {
        "snapshot_dates": [value.isoformat() for value in snapshot_days],
        "window_days": list(normalized_windows),
        "rebuilt": summaries,
    }


def _write_strategy_metrics(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    results: list[dict[str, Any]],
    buy_report_count_by_strategy: dict[str, int],
    signal_validity_warning_by_strategy: dict[str, bool],
) -> None:
    table = Base.metadata.tables["strategy_metric_snapshot"]
    now = utc_now()
    # Delete existing rows for (snapshot_date, window_days) before re-inserting
    db.execute(
        table.delete().where(
            (table.c.snapshot_date == trade_day)
            & (table.c.window_days == window_days)
        )
    )
    by_strategy = {key: [] for key in REQUIRED_STRATEGY_TYPES}
    for row in results:
        by_strategy.setdefault(str(row["strategy_type"]), []).append(row)

    for strategy_type in REQUIRED_STRATEGY_TYPES:
        subset = by_strategy.get(strategy_type, [])
        sample_size = len(subset)
        denominator = buy_report_count_by_strategy.get(strategy_type, 0)
        coverage_pct = round(sample_size / max(denominator, 1), 6) if denominator > 0 else 0.0
        returns = [float(row.get("net_return_pct") or 0.0) for row in subset]
        trade_day_span = _strategy_trade_day_span(
            db,
            subset,
            default_window_days=window_days,
        )
        metric_payload = build_metric_payload(
            returns,
            trade_day_count=trade_day_span,
            sample_size=sample_size,
        )
        data_status = "READY" if sample_size >= FR07_MIN_SAMPLE_SIZE else "DEGRADED"
        db.execute(
            table.insert().values(
                metric_snapshot_id=str(uuid4()),
                snapshot_date=trade_day,
                strategy_type=strategy_type,
                window_days=window_days,
                data_status=data_status,
                sample_size=sample_size,
                coverage_pct=coverage_pct,
                win_rate=metric_payload["win_rate"],
                profit_loss_ratio=metric_payload["profit_loss_ratio"],
                alpha_annual=metric_payload["alpha_annual"],
                max_drawdown_pct=metric_payload["max_drawdown_pct"],
                cumulative_return_pct=metric_payload["cumulative_return_pct"],
                signal_validity_warning=(
                    bool(signal_validity_warning_by_strategy.get(strategy_type))
                    if sample_size > 0
                    else False
                ),
                display_hint=metric_payload["display_hint"],
                created_at=now,
            )
        )


def _compute_strategy_signal_validity_warnings(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    results: list[dict[str, Any]],
) -> dict[str, bool]:
    by_strategy = {key: [] for key in REQUIRED_STRATEGY_TYPES}
    for row in results:
        by_strategy.setdefault(str(row.get("strategy_type") or ""), []).append(row)

    warning_by_strategy = {key: False for key in REQUIRED_STRATEGY_TYPES}
    for strategy_type in REQUIRED_STRATEGY_TYPES:
        subset = by_strategy.get(strategy_type, [])
        if not subset:
            continue
        strategy_cumulative = _compounded_cumulative_return(
            [float(row.get("net_return_pct") or 0.0) for row in subset]
        )
        if strategy_cumulative is None:
            continue
        baseline_metrics = baseline_random_market_metrics(
            db,
            trade_day=trade_day,
            window_days=window_days,
            truth_rows=subset,
        )
        baseline_sample_size = int(baseline_metrics.get("sample_size") or 0)
        baseline_cumulative = baseline_metrics.get("cumulative_return_pct")
        # Fail close only when the baseline sample is smaller than the truth
        # subset. A larger independent market sample is still valid.
        if baseline_sample_size < len(subset) or baseline_cumulative is None:
            continue
        warning_by_strategy[strategy_type] = strategy_cumulative < float(baseline_cumulative)
    return warning_by_strategy


def rebuild_fr07_snapshot(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    purge_invalid: bool = True,
) -> dict[str, Any]:
    purged_invalid_results = _purge_invalid_settlement_results(db) if purge_invalid else 0
    window_settled_rows = _load_window_settled_results(
        db,
        trade_day=trade_day,
        window_days=window_days,
    )
    buy_report_count_by_strategy = _window_buy_report_counts(
        db,
        trade_day=trade_day,
        window_days=window_days,
    )
    random_metrics, ma_metrics = compute_baseline_metrics(
        db,
        trade_day=trade_day,
        window_days=window_days,
        truth_rows=window_settled_rows,
    )
    signal_validity_warning_by_strategy = _compute_strategy_signal_validity_warnings(
        db,
        trade_day=trade_day,
        window_days=window_days,
        results=window_settled_rows,
    )
    strategy_cumulative = _compounded_cumulative_return(
        [float(row.get("net_return_pct") or 0.0) for row in window_settled_rows]
    )
    random_cumulative = random_metrics.get("cumulative_return_pct")
    signal_validity_warning = any(signal_validity_warning_by_strategy.values())
    _write_baseline_rows(
        db,
        trade_day=trade_day,
        window_days=window_days,
        random_metrics=random_metrics,
        ma_metrics=ma_metrics,
    )
    _write_strategy_metrics(
        db,
        trade_day=trade_day,
        window_days=window_days,
        results=window_settled_rows,
        buy_report_count_by_strategy=buy_report_count_by_strategy,
        signal_validity_warning_by_strategy=signal_validity_warning_by_strategy,
    )
    _materialize_settlement_runtime_artifacts(
        db,
        trade_day=trade_day,
        window_days=window_days,
    )
    db.flush()
    return {
        "trade_day": trade_day.isoformat(),
        "window_days": int(window_days),
        "purged_invalid_results": int(purged_invalid_results),
        "settled_sample_size": len(window_settled_rows),
        "strategy_cumulative_return_pct": strategy_cumulative,
        "baseline_random_cumulative_return_pct": random_cumulative,
        "signal_validity_warning": signal_validity_warning,
    }


def _materialize_settlement_runtime_artifacts(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
) -> None:
    if int(window_days) != 30:
        return
    materialize_baseline_equity_curve_points(db, snapshot_date=trade_day)
    materialize_sim_dashboard_snapshots(db, snapshot_date=trade_day)
    normalize_snapshot_truth(db)


def _process_task(db: Session, *, task_id: str) -> None:
    task_table = Base.metadata.tables["settlement_task"]
    result_table = Base.metadata.tables["settlement_result"]
    now = utc_now()
    task = _query_one(
        db,
        """
        SELECT *
        FROM settlement_task
        WHERE task_id = :task_id
        LIMIT 1
        """,
        {"task_id": task_id},
    )
    if not task:
        raise SettlementServiceError(500, "VALIDATION_FAILED")

    trade_day = _as_date(task["trade_date"])
    reports = _load_reports(
        db,
        trade_day=trade_day,
        window_days=task["window_days"],
        target_scope=task["target_scope"],
        target_report_id=task.get("target_report_id"),
        target_stock_code=task.get("target_stock_code"),
    )
    if not reports:
        raise SettlementServiceError(500, "DEPENDENCY_NOT_READY")

    db.execute(
        task_table.update()
        .where(task_table.c.task_id == task_id)
        .values(status="PROCESSING", started_at=now, updated_at=now)
    )
    task["status"] = "PROCESSING"
    task["started_at"] = now
    task["updated_at"] = now
    _sync_pipeline_run_from_task(db, task)

    processed = skipped = failed = 0
    for report in reports:
        row_scope = (
            result_table.c.report_id == report["report_id"],
            result_table.c.window_days == task["window_days"],
        )
        existing = _query_one(
            db,
            """
            SELECT settlement_result_id, settlement_status, exit_trade_date
            FROM settlement_result
            WHERE report_id = :report_id AND window_days = :window_days
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            {"report_id": report["report_id"], "window_days": task["window_days"]},
        )
        due_trade_day = _as_date(report.get("due_trade_date")) or trade_day
        exit_trade_day = trade_day
        if (
            existing
            and existing["settlement_status"] == "settled"
            and _as_date(existing.get("exit_trade_date")) == exit_trade_day
            and not task["force"]
        ):
            skipped += 1
            continue

        exit_row = _query_one(
            db,
            """
            SELECT close
            FROM kline_daily
            WHERE stock_code = :stock_code AND trade_date = :trade_date
            LIMIT 1
            """,
            {"stock_code": report["stock_code"], "trade_date": exit_trade_day},
        )
        if not exit_row:
            # Maturity is decided by due_trade_day, but settlement realizes on the
            # current run's trade_day. If an older run persisted a stale exit day,
            # force reruns must clear that drifted row.
            if existing and _as_date(existing.get("exit_trade_date")) != exit_trade_day:
                db.execute(result_table.delete().where(*row_scope))
            failed += 1
            continue

        if existing:
            db.execute(result_table.delete().where(*row_scope))

        fees = _fee_breakdown(float(report["signal_entry_price"]), float(exit_row["close"]))
        signal_day = _as_date(report["trade_date"])
        result_row = {
            "settlement_id": str(uuid4()),
            "settlement_result_id": str(uuid4()),
            "report_id": report["report_id"],
            "stock_code": report["stock_code"],
            "trade_date": str(signal_day) if signal_day else None,
            "signal_date": signal_day,
            "window_days": task["window_days"],
            "strategy_type": report["strategy_type"],
            "settlement_status": "settled",
            "is_misclassified": 1 if fees["net_return_pct"] < 0 else 0,
            "quality_flag": report["quality_flag"],
            "status_reason": None,
            "entry_trade_date": signal_day,
            "exit_trade_date": exit_trade_day,
            "shares": fees["shares"],
            "buy_price": round(float(report["signal_entry_price"]), 4),
            "sell_price": round(float(exit_row["close"]), 4),
            "buy_commission": fees["buy_commission"],
            "sell_commission": fees["sell_commission"],
            "stamp_duty": fees["stamp_duty"],
            "buy_slippage_cost": fees["buy_slippage_cost"],
            "sell_slippage_cost": fees["sell_slippage_cost"],
            "gross_return_pct": fees["gross_return_pct"],
            "net_return_pct": fees["net_return_pct"],
            "exit_reason": "window_expired",
            "settled_at": now.isoformat() if hasattr(now, "isoformat") else str(now),
            "display_hint": None,
            "created_at": now,
            "updated_at": now,
        }
        db.execute(result_table.insert().values(**result_row))
        processed += 1

    rebuild_fr07_snapshot(
        db,
        trade_day=trade_day,
        window_days=int(task["window_days"]),
        purge_invalid=True,
    )
    finished_at = utc_now()

    # Determine task status based on coverage threshold
    total_attempted = processed + failed
    coverage_ok = total_attempted == 0 or (processed / total_attempted) >= 0.50
    task_status = "COMPLETED"
    task_reason = None if coverage_ok else "KLINE_COVERAGE_INSUFFICIENT"

    db.execute(
        task_table.update()
        .where(task_table.c.task_id == task_id)
        .values(
            status=task_status,
            processed_count=processed,
            skipped_count=skipped,
            failed_count=failed,
            status_reason=task_reason,
            finished_at=finished_at,
            updated_at=finished_at,
        )
    )
    task["status"] = task_status
    task["processed_count"] = processed
    task["skipped_count"] = skipped
    task["failed_count"] = failed
    task["status_reason"] = task_reason
    task["finished_at"] = finished_at
    task["updated_at"] = finished_at
    _sync_pipeline_run_from_task(db, task)
    _sync_admin_operation_from_settlement_task(db, task)
    db.flush()


def submit_settlement_task(
    db: Session,
    *,
    trade_date: str,
    window_days: int,
    target_scope: str,
    target_report_id: str | None = None,
    target_stock_code: str | None = None,
    force: bool = False,
    request_id: str | None = None,
    requested_by_user_id: str | None = None,
    pipeline_name_override: str | None = None,
    run_inline: bool | None = None,
) -> dict[str, Any]:
    trade_day = _validate_payload(
        trade_date=trade_date,
        window_days=window_days,
        target_scope=target_scope,
        target_report_id=target_report_id,
        target_stock_code=target_stock_code,
    )
    task_table = Base.metadata.tables["settlement_task"]
    now = utc_now()
    effective_request_id = str(request_id or _generate_request_id("settlement"))
    pipeline_name = str(
        pipeline_name_override
        or _settlement_pipeline_name(
            window_days=window_days,
            target_scope=target_scope,
            target_report_id=target_report_id,
            target_stock_code=target_stock_code,
        )
    )
    task_scope_key = _scope_key(
        trade_date=trade_date,
        window_days=window_days,
        target_scope=target_scope,
        target_report_id=target_report_id,
        target_stock_code=target_stock_code,
    )
    existing = _query_one(
        db,
        """
        SELECT *
        FROM settlement_task
        WHERE task_scope_key = :task_scope_key
        ORDER BY created_at DESC
        LIMIT 1
        """,
        {"task_scope_key": task_scope_key},
    )
    if existing and not force:
        if existing["status"] in {"QUEUED", "PROCESSING"}:
            raise SettlementServiceError(409, "CONCURRENT_CONFLICT")
        _upsert_pipeline_run(
            db,
            pipeline_name=pipeline_name,
            trade_day=trade_day,
            pipeline_status=PIPELINE_STATUS_ACCEPTED,
            request_id=effective_request_id,
            reset_timestamps=True,
        )
        _sync_pipeline_run_from_scope(
            db,
            pipeline_name=pipeline_name,
            trade_day=trade_day,
            target_scope=target_scope,
            target_report_id=target_report_id,
            target_stock_code=target_stock_code,
            window_days_list=(window_days,),
            request_id=effective_request_id,
        )
        return {
            "task_id": existing["task_id"],
            "status": "QUEUED",
            "force": bool(existing["force"]),
        }

    # Acquire true mutex lock to eliminate check-then-act race window
    acquired = _settlement_lock.acquire(blocking=False)
    if not acquired:
        raise SettlementServiceError(409, "CONCURRENT_CONFLICT")
    try:
        # Re-check inside lock
        existing2 = _query_one(
            db,
            """
            SELECT task_id, status
            FROM settlement_task
            WHERE task_scope_key = :task_scope_key AND status IN ('QUEUED','PROCESSING')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            {"task_scope_key": task_scope_key},
        )
        if existing2:
            raise SettlementServiceError(409, "CONCURRENT_CONFLICT")
        # force=True with existing record: delete old record to avoid UNIQUE constraint
        if existing and force:
            db.execute(
                task_table.delete().where(task_table.c.task_scope_key == task_scope_key)
            )
            _upsert_pipeline_run(
                db,
                pipeline_name=pipeline_name,
                trade_day=trade_day,
                pipeline_status=PIPELINE_STATUS_ACCEPTED,
                request_id=effective_request_id,
                reset_timestamps=True,
            )

        task_id = str(uuid4())
        db.execute(
            task_table.insert().values(
                task_id=task_id,
                task_scope_key=task_scope_key,
                trade_date=trade_day,
                window_days=window_days,
                target_scope=target_scope,
                target_report_id=target_report_id,
                target_stock_code=target_stock_code,
                force=force,
                status="QUEUED",
                processed_count=0,
                skipped_count=0,
                failed_count=0,
                status_reason=None,
                lock_key=f"settlement:{task_scope_key}",
                request_id=effective_request_id,
                requested_by_user_id=requested_by_user_id,
                started_at=None,
                finished_at=None,
                updated_at=now,
                created_at=now,
            )
        )
        _upsert_pipeline_run(
            db,
            pipeline_name=pipeline_name,
            trade_day=trade_day,
            pipeline_status=PIPELINE_STATUS_ACCEPTED,
            request_id=effective_request_id,
            reset_timestamps=force,
        )
        should_run_inline = _settlement_runs_inline() if run_inline is None else bool(run_inline)
        if should_run_inline:
            _process_task_with_failure_capture(db, task_id=task_id)
        else:
            db.commit()
            threading.Thread(
                target=_process_task_async,
                args=(task_id,),
                daemon=True,
                name=f"settlement-task-{task_id[:8]}",
            ).start()
        return {
            "task_id": task_id,
            "status": "QUEUED",
            "force": force,
        }
    finally:
        _settlement_lock.release()


def submit_settlement_batch(
    db: Session,
    *,
    trade_date: str,
    force: bool = False,
    request_id: str | None = None,
    requested_by_user_id: str | None = None,
    window_days_list: Iterable[int] = SETTLEMENT_BATCH_WINDOWS,
    target_scope: str = "all",
    target_report_id: str | None = None,
    target_stock_code: str | None = None,
    run_inline: bool | None = False,
) -> list[dict[str, Any]]:
    trade_day = date.fromisoformat(trade_date)
    normalized_windows = tuple(int(value) for value in window_days_list)
    batch_request_id = str(request_id or _generate_request_id("settlement-batch"))
    pipeline_name = _settlement_batch_pipeline_name(
        target_scope=target_scope,
        target_report_id=target_report_id,
        target_stock_code=target_stock_code,
    )
    _upsert_pipeline_run(
        db,
        pipeline_name=pipeline_name,
        trade_day=trade_day,
        pipeline_status=PIPELINE_STATUS_ACCEPTED,
        request_id=batch_request_id,
        reset_timestamps=force,
    )
    accepted_rows: list[dict[str, Any]] = []
    try:
        for window_days in normalized_windows:
            accepted_rows.append(
                submit_settlement_task(
                    db,
                    trade_date=trade_date,
                    window_days=int(window_days),
                    target_scope=target_scope,
                    target_report_id=target_report_id,
                    target_stock_code=target_stock_code,
                force=force,
                request_id=batch_request_id,
                requested_by_user_id=requested_by_user_id,
                pipeline_name_override=pipeline_name,
                run_inline=run_inline,
            )
        )
        _sync_pipeline_run_from_scope(
            db,
            pipeline_name=pipeline_name,
            trade_day=trade_day,
            target_scope=target_scope,
            target_report_id=target_report_id,
            target_stock_code=target_stock_code,
            window_days_list=normalized_windows,
            request_id=batch_request_id,
        )
    except SettlementServiceError as exc:
        _upsert_pipeline_run(
            db,
            pipeline_name=pipeline_name,
            trade_day=trade_day,
            pipeline_status=PIPELINE_STATUS_FAILED,
            request_id=batch_request_id,
            status_reason=exc.error_code,
        )
        raise
    return accepted_rows


def get_settlement_pipeline_status(
    db: Session,
    *,
    trade_date: str,
    target_scope: str = "all",
    target_report_id: str | None = None,
    target_stock_code: str | None = None,
    window_days: int | None = None,
    window_days_list: Iterable[int] | None = None,
) -> dict[str, Any]:
    normalized_window_list = tuple(int(value) for value in window_days_list) if window_days_list is not None else None
    if window_days is not None:
        pipeline_name = _settlement_pipeline_name(
            window_days=int(window_days),
            target_scope=target_scope,
            target_report_id=target_report_id,
            target_stock_code=target_stock_code,
        )
    elif normalized_window_list is not None and len(normalized_window_list) == 1:
        single_window = normalized_window_list[0]
        pipeline_name = _settlement_pipeline_name(
            window_days=single_window,
            target_scope=target_scope,
            target_report_id=target_report_id,
            target_stock_code=target_stock_code,
        )
    else:
        pipeline_name = _settlement_batch_pipeline_name(
            target_scope=target_scope,
            target_report_id=target_report_id,
            target_stock_code=target_stock_code,
        )
    table = _pipeline_run_table()
    pipeline_run_total = int(
        db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM pipeline_run
                """
            )
        ).scalar()
        or 0
    )
    matching_pipeline_run_total = int(
        db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM pipeline_run
                WHERE trade_date = :trade_date
                  AND pipeline_name = :pipeline_name
                """
            ),
            {
                "trade_date": date.fromisoformat(trade_date),
                "pipeline_name": pipeline_name,
            },
        ).scalar()
        or 0
    )
    row = db.execute(
        select(table)
        .where(
            table.c.trade_date == date.fromisoformat(trade_date),
            table.c.pipeline_name == pipeline_name,
        )
        .limit(1)
    ).mappings().first()
    if row is None:
        return {
            "pipeline_name": pipeline_name,
            "trade_date": trade_date,
            "pipeline_status": "NOT_RUN",
            "degraded": False,
            "status_reason": None,
            "started_at": None,
            "finished_at": None,
            "updated_at": None,
            "pipeline_run_total": pipeline_run_total,
            "matching_pipeline_run_total": matching_pipeline_run_total,
        }
    return {
        "pipeline_name": str(row.get("pipeline_name") or pipeline_name),
        "trade_date": trade_date,
        "pipeline_status": str(row.get("pipeline_status") or PIPELINE_STATUS_ACCEPTED),
        "degraded": bool(row.get("degraded")),
        "status_reason": row.get("status_reason"),
        "started_at": _as_datetime(row.get("started_at")),
        "finished_at": _as_datetime(row.get("finished_at")),
        "updated_at": _as_datetime(row.get("updated_at")),
        "pipeline_run_total": pipeline_run_total,
        "matching_pipeline_run_total": matching_pipeline_run_total,
    }


def wait_for_settlement_pipeline(
    *,
    trade_date: str,
    timeout_seconds: float | None = None,
    poll_interval_seconds: float = 0.05,
    target_scope: str = "all",
    target_report_id: str | None = None,
    target_stock_code: str | None = None,
    window_days: int | None = None,
    window_days_list: Iterable[int] | None = None,
) -> dict[str, Any]:
    effective_timeout_seconds = float(timeout_seconds or getattr(settings, "sim_settle_timeout_seconds", 1800) or 1800)
    deadline = time.monotonic() + effective_timeout_seconds
    while True:
        db = SessionLocal()
        try:
            status = get_settlement_pipeline_status(
                db,
                trade_date=trade_date,
                target_scope=target_scope,
                target_report_id=target_report_id,
                target_stock_code=target_stock_code,
                window_days=window_days,
                window_days_list=window_days_list,
            )
        finally:
            db.close()
        if status["pipeline_status"] in PIPELINE_TERMINAL_STATUSES:
            return status
        if time.monotonic() >= deadline:
            raise SettlementServiceError(503, "UPSTREAM_TIMEOUT")
        time.sleep(poll_interval_seconds)


def get_settlement_task_status(
    db: Session,
    *,
    task_id: str,
) -> dict[str, Any]:
    row = _query_one(
        db,
        """
        SELECT *
        FROM settlement_task
        WHERE task_id = :task_id
        LIMIT 1
        """,
        {"task_id": task_id},
    )
    if row is None:
        raise SettlementServiceError(404, "NOT_FOUND")

    return {
        "task_id": str(row.get("task_id")),
        "trade_date": _as_date(row.get("trade_date")).isoformat() if _as_date(row.get("trade_date")) else None,
        "window_days": int(row.get("window_days") or 0),
        "target_scope": str(row.get("target_scope") or ""),
        "target_report_id": row.get("target_report_id"),
        "target_stock_code": row.get("target_stock_code"),
        "status": str(row.get("status") or "UNKNOWN"),
        "processed_count": int(row.get("processed_count") or 0),
        "skipped_count": int(row.get("skipped_count") or 0),
        "failed_count": int(row.get("failed_count") or 0),
        "force": bool(row.get("force")),
        "status_reason": row.get("status_reason"),
    }
