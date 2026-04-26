from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Base


STALE_RUNNING_TIMEOUT = timedelta(hours=6)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _as_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _iso_date(value: date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _iso_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _ensure_utc(value).isoformat()


def _scheduler_run_reference_time(row) -> datetime | None:
    return (
        _ensure_utc(getattr(row, "started_at", None))
        or _ensure_utc(getattr(row, "updated_at", None))
        or _ensure_utc(getattr(row, "triggered_at", None))
    )


def _is_stale_running_row(row, *, now: datetime) -> bool:
    if getattr(row, "status", None) != "RUNNING":
        return False
    reference_time = _scheduler_run_reference_time(row)
    if reference_time is None:
        return False
    return reference_time <= now - STALE_RUNNING_TIMEOUT


def _auto_fail_stale_running_rows(db: Session, *, now: datetime) -> int:
    run_table = Base.metadata.tables["scheduler_task_run"]
    running_rows = db.execute(
        select(run_table).where(run_table.c.status == "RUNNING")
    ).fetchall()
    stale_run_ids = [row.task_run_id for row in running_rows if _is_stale_running_row(row, now=now)]
    if not stale_run_ids:
        return 0
    db.execute(
        run_table.update()
        .where(run_table.c.task_run_id.in_(stale_run_ids))
        .values(
            status="FAILED",
            status_reason="stale_running_auto_failed",
            error_message="stale running task auto-failed after 6h timeout",
            finished_at=now,
            updated_at=now,
        )
    )
    db.flush()
    return len(stale_run_ids)


def register_scheduler_run(
    db: Session,
    *,
    task_name: str,
    trade_date: str | date | None,
    schedule_slot: str,
    trigger_source: str,
    dependency_event_name: str | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    now = _ensure_utc(now) or _now_utc()
    trade_day = _as_date(trade_date)
    run_table = Base.metadata.tables["scheduler_task_run"]
    event_table = Base.metadata.tables["dag_event"]

    _auto_fail_stale_running_rows(db, now=now)

    existing = db.execute(
        select(run_table).where(
            run_table.c.task_name == task_name,
            run_table.c.trade_date == trade_day,
            run_table.c.schedule_slot == schedule_slot,
        )
    ).first()
    if existing:
        if existing.status == "SUCCESS":
            return {
                "action": "skipped_existing_success",
                "task_run_id": existing.task_run_id,
                "status": existing.status,
            }
        if existing.status == "FAILED" and existing.status_reason == "stale_running_auto_failed":
            waiting_for_upstream = False
            if dependency_event_name:
                waiting_for_upstream = (
                    db.execute(
                        select(event_table.c.dag_event_id).where(
                            event_table.c.event_name == dependency_event_name,
                            event_table.c.trade_date == trade_day,
                        )
                    ).first()
                    is None
                )
            status = "WAITING_UPSTREAM" if waiting_for_upstream else "RUNNING"
            next_lock_version = int(existing.lock_version or 0) + 1
            next_retry_count = int(existing.retry_count or 0) + 1
            db.execute(
                run_table.update()
                .where(run_table.c.task_run_id == existing.task_run_id)
                .values(
                    status=status,
                    retry_count=next_retry_count,
                    lock_key=f"{trigger_source}:{trade_day.isoformat() if trade_day else 'none'}:{task_name}",
                    lock_version=max(1, next_lock_version),
                    trigger_source=trigger_source,
                    status_reason=None,
                    error_message=None,
                    triggered_at=now,
                    started_at=None if waiting_for_upstream else now,
                    finished_at=None,
                    updated_at=now,
                )
            )
            db.flush()
            return {
                "action": "reclaimed_stale_running",
                "task_run_id": existing.task_run_id,
                "status": status,
            }
        return {
            "action": "existing",
            "task_run_id": existing.task_run_id,
            "status": existing.status,
        }

    waiting_for_upstream = False
    if dependency_event_name:
        waiting_for_upstream = (
            db.execute(
                select(event_table.c.dag_event_id).where(
                    event_table.c.event_name == dependency_event_name,
                    event_table.c.trade_date == trade_day,
                )
            ).first()
            is None
        )

    task_run_id = str(uuid4())
    status = "WAITING_UPSTREAM" if waiting_for_upstream else "RUNNING"
    db.execute(
        run_table.insert().values(
            task_run_id=task_run_id,
            task_name=task_name,
            trade_date=trade_day,
            schedule_slot=schedule_slot,
            status=status,
            retry_count=0,
            lock_key=f"{trigger_source}:{trade_day.isoformat() if trade_day else 'none'}:{task_name}",
            lock_version=1,
            trigger_source=trigger_source,
            status_reason=None,
            error_message=None,
            triggered_at=now,
            started_at=None if waiting_for_upstream else now,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    return {
        "action": "created",
        "task_run_id": task_run_id,
        "status": status,
    }


def mark_scheduler_run_success(
    db: Session,
    *,
    task_run_id: str,
    finished_at: datetime | None = None,
) -> None:
    run_table = Base.metadata.tables["scheduler_task_run"]
    finished_at = _ensure_utc(finished_at) or _now_utc()
    db.execute(
        run_table.update()
        .where(run_table.c.task_run_id == task_run_id)
        .values(
            status="SUCCESS",
            finished_at=finished_at,
            updated_at=finished_at,
            status_reason=None,
            error_message=None,
        )
    )
    db.flush()


def list_scheduler_runs(
    db: Session,
    *,
    page: int,
    page_size: int,
    now: datetime | None = None,
) -> dict[str, object]:
    now = _ensure_utc(now) or _now_utc()
    _auto_fail_stale_running_rows(db, now=now)
    cutoff = now - timedelta(days=7)
    table = Base.metadata.tables["scheduler_task_run"]

    base_query = select(table).where(table.c.triggered_at >= cutoff)
    ordered_query = base_query.order_by(table.c.triggered_at.desc(), table.c.task_run_id.asc())
    rows = db.execute(
        ordered_query.offset((page - 1) * page_size).limit(page_size)
    ).fetchall()
    total = int(db.execute(select(func.count()).select_from(base_query.subquery())).scalar_one())
    latest_row = db.execute(ordered_query.limit(1)).first()

    return {
        "items": [
            {
                "task_run_id": row.task_run_id,
                "task_name": row.task_name,
                "trade_date": _iso_date(row.trade_date),
                "schedule_slot": row.schedule_slot,
                "trigger_source": row.trigger_source,
                "status": row.status,
                "triggered_at": _iso_datetime(row.triggered_at),
                "started_at": _iso_datetime(row.started_at),
                "finished_at": _iso_datetime(row.finished_at),
                "retry_count": row.retry_count,
                "status_reason": row.status_reason,
                "error_message": row.error_message,
            }
            for row in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "last_run_at": _iso_datetime(latest_row.triggered_at) if latest_row else None,
    }
