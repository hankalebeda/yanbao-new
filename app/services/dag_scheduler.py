"""
DAG Scheduler — SSOT 02 §2.4.2 / 03 §FR-02 compliant implementation.

Replaces fixed-cron downstream triggering with event-driven DAG propagation.
Cron is only used for initial L0 triggers; all downstream tasks wait for
upstream completion events (dag_event table).

Key invariants (HC-03):
  - FR-06 waits for FR04_CORE_POOL_COLLECTION_COMPLETED
  - FR-07/FR-08 wait for FR06_BATCH_COMPLETED
  - Cascade timeout before next open terminates stale chains
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from threading import Lock, Thread
from typing import Callable
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.db import SessionLocal
from app.models import Base
from app.services.trade_calendar import is_trade_day, latest_trade_date_str

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG Node definitions (SSOT 02 §4.3)
# ---------------------------------------------------------------------------

DAG_DEPENDENCIES: dict[str, list[str]] = {
    "fr01_stock_pool":  [],
    "fr05_market_state": [],
    "fr04_data_collect": ["fr01_stock_pool"],
    "fr05_non_report_truth_materialize": ["fr04_data_collect", "fr05_market_state"],
    "fr06_report_gen":  ["fr05_non_report_truth_materialize"],
    "fr07_settlement":  ["fr06_report_gen"],
    "fr08_sim_trade":   ["fr06_report_gen"],
    "fr13_event_notify": ["fr08_sim_trade"],
}

EVENT_NAMES: dict[str, str] = {
    "fr01_stock_pool":  "FR01_STOCK_POOL_REFRESHED",
    "fr04_data_collect": "FR04_CORE_POOL_COLLECTION_COMPLETED",
    "fr05_market_state": "FR05_MARKET_STATE_COMPUTED",
    "fr05_non_report_truth_materialize": "FR05_NON_REPORT_TRUTH_MATERIALIZED",
    "fr06_report_gen":  "FR06_BATCH_COMPLETED",
    "fr07_settlement":  "FR07_SETTLEMENT_COMPLETED",
    "fr08_sim_trade":   "FR08_SIM_TRADE_COMPLETED",
    "fr13_event_notify": "FR13_NOTIFICATION_DISPATCHED",
}

# Reverse: event_name → task_name that produces it
_EVENT_PRODUCER: dict[str, str] = {v: k for k, v in EVENT_NAMES.items()}

# Map: task_name → event_name it waits for (from upstream)
UPSTREAM_EVENTS: dict[str, list[str]] = {}
for _task, _deps in DAG_DEPENDENCIES.items():
    UPSTREAM_EVENTS[_task] = [EVENT_NAMES[dep] for dep in _deps]

# Task policies (SSOT 03 §FR-02 step 1)
TRADING_DAY_REQUIRED = {
    "fr01_stock_pool", "fr04_data_collect", "fr05_market_state",
    "fr05_non_report_truth_materialize",
    "fr06_report_gen", "fr07_settlement", "fr08_sim_trade",
    "fr13_event_notify",
}

# Status constants (SSOT 04 §4.3)
STATUS_PENDING = "PENDING"
STATUS_WAITING = "WAITING_UPSTREAM"
STATUS_RUNNING = "RUNNING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"

TERMINAL_STATES = frozenset({STATUS_SUCCESS, STATUS_FAILED, STATUS_SKIPPED})
FR07_SETTLEMENT_WINDOWS = (1, 7, 14, 30, 60)

# Lock TTL defaults (SSOT 03 §FR-02 step 3)
DEFAULT_LOCK_TTL_SECONDS = 300
HEARTBEAT_INTERVAL_SECONDS = 30

# ---------------------------------------------------------------------------
# Handler registry (SSOT 02 §2.4.3 callback model)
# ---------------------------------------------------------------------------

_handlers: dict[str, Callable] = {}
_handler_lock = Lock()


def register_handler(task_name: str, handler: Callable) -> None:
    with _handler_lock:
        _handlers[task_name] = handler


def get_handler(task_name: str) -> Callable | None:
    with _handler_lock:
        return _handlers.get(task_name)


# ---------------------------------------------------------------------------
# DAG event operations
# ---------------------------------------------------------------------------

def emit_dag_event(
    db: Session,
    *,
    event_name: str,
    trade_date: date,
    producer_task_run_id: str | None = None,
    payload: dict | None = None,
) -> str:
    """Insert a DAG event and trigger downstream tasks."""
    import json
    event_table = Base.metadata.tables["dag_event"]
    event_key = f"{event_name}:{trade_date.isoformat()}"
    existing = db.execute(
        select(event_table.c.dag_event_id).where(event_table.c.event_key == event_key)
    ).first()
    if existing:
        return existing.dag_event_id

    event_id = str(uuid4())
    now = datetime.now(timezone.utc)
    db.execute(
        event_table.insert().values(
            dag_event_id=event_id,
            event_key=event_key,
            event_name=event_name,
            trade_date=trade_date,
            producer_task_run_id=producer_task_run_id,
            payload_json=json.dumps(payload or {}),
            created_at=now,
        )
    )
    db.flush()
    logger.info("dag_event_emitted event=%s trade_date=%s id=%s", event_name, trade_date, event_id)
    return event_id


def check_upstream_ready(db: Session, task_name: str, trade_date: date) -> bool:
    """Check if all upstream DAG events exist for this task+trade_date."""
    required_events = UPSTREAM_EVENTS.get(task_name, [])
    if not required_events:
        return True
    event_table = Base.metadata.tables["dag_event"]
    for event_name in required_events:
        exists = db.execute(
            select(event_table.c.dag_event_id).where(
                event_table.c.event_name == event_name,
                event_table.c.trade_date == trade_date,
            )
        ).first()
        if not exists:
            return False
    return True


# ---------------------------------------------------------------------------
# Distributed lock (SSOT 03 §FR-02 step 3)
# ---------------------------------------------------------------------------

def try_acquire_lock(
    db: Session,
    *,
    task_name: str,
    trade_date: date,
    lock_ttl_seconds: int = DEFAULT_LOCK_TTL_SECONDS,
) -> tuple[str | None, int]:
    """
    Try to acquire task-level mutex.
    Returns (task_run_id, lock_version) or (None, 0) if lock held.
    Uses trade_date+task_name as unique key (SSOT 03 §FR-02).
    """
    run_table = Base.metadata.tables["scheduler_task_run"]
    now = datetime.now(timezone.utc)
    lock_key = f"dag:{trade_date.isoformat()}:{task_name}"

    # Check existing row
    existing = db.execute(
        select(run_table).where(
            run_table.c.task_name == task_name,
            run_table.c.trade_date == trade_date,
        ).order_by(run_table.c.created_at.desc())
    ).first()

    if existing:
        if existing.status in TERMINAL_STATES:
            # Already completed — idempotent skip
            return None, 0

        # PENDING tasks (promoted from WAITING by _trigger_downstream) can be
        # directly acquired without TTL checks.
        if existing.status == STATUS_PENDING:
            new_version = (existing.lock_version or 0) + 1
            db.execute(
                run_table.update()
                .where(
                    run_table.c.task_run_id == existing.task_run_id,
                    run_table.c.lock_version == existing.lock_version,
                )
                .values(
                    status=STATUS_RUNNING,
                    lock_key=lock_key,
                    lock_version=new_version,
                    started_at=now,
                    updated_at=now,
                    trigger_source="event",
                    status_reason=None,
                )
            )
            db.flush()
            logger.info("lock_acquire_pending task=%s version=%d", task_name, new_version)
            return existing.task_run_id, new_version

        # Check TTL expiry for stale locks
        lock_age = (now - (existing.updated_at.replace(tzinfo=timezone.utc)
                          if existing.updated_at and existing.updated_at.tzinfo is None
                          else (existing.updated_at or now))).total_seconds()
        if lock_age < lock_ttl_seconds:
            # Lock still held by another instance
            logger.info("lock_held task=%s trade_date=%s holder=%s age=%.0fs",
                        task_name, trade_date, existing.task_run_id, lock_age)
            return None, 0

        # TTL expired — take over with fencing token increment
        new_version = (existing.lock_version or 0) + 1
        db.execute(
            run_table.update()
            .where(
                run_table.c.task_run_id == existing.task_run_id,
                run_table.c.lock_version == existing.lock_version,
            )
            .values(
                status=STATUS_RUNNING,
                lock_key=lock_key,
                lock_version=new_version,
                started_at=now,
                updated_at=now,
                trigger_source="event",
            )
        )
        db.flush()
        logger.info("lock_takeover task=%s version=%d (was %d)",
                     task_name, new_version, existing.lock_version or 0)
        return existing.task_run_id, new_version

    # No existing row — create
    task_run_id = str(uuid4())
    has_deps = bool(UPSTREAM_EVENTS.get(task_name))
    upstream_ready = check_upstream_ready(db, task_name, trade_date) if has_deps else True
    initial_status = STATUS_RUNNING if upstream_ready else STATUS_WAITING

    schedule_slot = "dag_event"
    db.execute(
        run_table.insert().values(
            task_run_id=task_run_id,
            task_name=task_name,
            trade_date=trade_date,
            schedule_slot=schedule_slot,
            status=initial_status,
            retry_count=0,
            lock_key=lock_key,
            lock_version=1,
            trigger_source="event",
            status_reason="waiting_upstream" if not upstream_ready else None,
            error_message=None,
            triggered_at=now,
            started_at=now if upstream_ready else None,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    db.flush()
    return task_run_id, 1


def heartbeat_lock(db: Session, task_run_id: str, lock_version: int) -> bool:
    """Renew lock TTL by updating updated_at. Returns False if fenced."""
    run_table = Base.metadata.tables["scheduler_task_run"]
    now = datetime.now(timezone.utc)
    result = db.execute(
        run_table.update()
        .where(
            run_table.c.task_run_id == task_run_id,
            run_table.c.lock_version == lock_version,
        )
        .values(updated_at=now)
    )
    db.flush()
    return result.rowcount > 0


def mark_success(db: Session, task_run_id: str) -> None:
    run_table = Base.metadata.tables["scheduler_task_run"]
    now = datetime.now(timezone.utc)
    db.execute(
        run_table.update()
        .where(run_table.c.task_run_id == task_run_id)
        .values(status=STATUS_SUCCESS, finished_at=now, updated_at=now,
                error_message=None, status_reason=None)
    )
    db.flush()


def mark_failed(db: Session, task_run_id: str, error: str, reason: str | None = None) -> None:
    run_table = Base.metadata.tables["scheduler_task_run"]
    now = datetime.now(timezone.utc)
    db.execute(
        run_table.update()
        .where(run_table.c.task_run_id == task_run_id)
        .values(status=STATUS_FAILED, finished_at=now, updated_at=now,
                error_message=str(error)[:1000], status_reason=reason)
    )
    db.flush()


def mark_skipped(db: Session, task_run_id: str, reason: str = "non_trade_day") -> None:
    run_table = Base.metadata.tables["scheduler_task_run"]
    now = datetime.now(timezone.utc)
    db.execute(
        run_table.update()
        .where(run_table.c.task_run_id == task_run_id)
        .values(status=STATUS_SKIPPED, finished_at=now, updated_at=now,
                status_reason=reason)
    )
    db.flush()


# ---------------------------------------------------------------------------
# Cascade timeout (SSOT 03 §FR-02 step 6, HC-03)
# ---------------------------------------------------------------------------

def _parse_timeout_time(trade_date: date) -> datetime:
    """Parse dag_cascade_timeout_before_open config as next-day HH:MM."""
    parts = settings.dag_cascade_timeout_before_open.split(":")
    hour, minute = int(parts[0]), int(parts[1])
    next_day = trade_date + timedelta(days=1)
    return datetime(next_day.year, next_day.month, next_day.day,
                    hour, minute, tzinfo=timezone(timedelta(hours=8)))


def enforce_cascade_timeout(db: Session, trade_date: date) -> list[str]:
    """
    Check for WAITING_UPSTREAM/RUNNING tasks past the cascade deadline.
    Mark them and all downstream as FAILED with upstream_timeout_next_open.
    Returns list of affected task_run_ids.
    """
    deadline = _parse_timeout_time(trade_date)
    now = datetime.now(timezone.utc)
    if now < deadline.astimezone(timezone.utc):
        return []

    run_table = Base.metadata.tables["scheduler_task_run"]
    stale = db.execute(
        select(run_table).where(
            run_table.c.trade_date == trade_date,
            run_table.c.status.in_([STATUS_WAITING, STATUS_RUNNING]),
        )
    ).fetchall()

    affected = []
    for row in stale:
        db.execute(
            run_table.update()
            .where(run_table.c.task_run_id == row.task_run_id)
            .values(
                status=STATUS_FAILED,
                status_reason="upstream_timeout_next_open",
                error_message=f"cascade_timeout at {deadline.isoformat()}",
                finished_at=now,
                updated_at=now,
            )
        )
        affected.append(row.task_run_id)
        logger.warning("cascade_timeout task=%s run_id=%s trade_date=%s",
                        row.task_name, row.task_run_id, trade_date)
    if affected:
        db.flush()
    return affected


# ---------------------------------------------------------------------------
# Core DAG execution engine
# ---------------------------------------------------------------------------

def execute_dag_node(task_name: str, trade_date: date | None = None, *, force: bool = False) -> dict:
    """
    Execute a single DAG node with full SSOT compliance:
    1. Trade day check
    2. Idempotent restart skip
    3. Lock acquisition
    4. Upstream dependency check
    5. Handler execution with retry
    6. Event emission on success

    When force=True (admin retrigger), the force flag is forwarded
    to handlers that accept it (e.g. fr07_settlement → submit_settlement_task).
    """
    trade_date = trade_date or date.fromisoformat(latest_trade_date_str())
    db = SessionLocal()
    try:
        # Step 1: Trade day check
        if task_name in TRADING_DAY_REQUIRED and not is_trade_day(trade_date):
            task_run_id, _ = try_acquire_lock(db, task_name=task_name, trade_date=trade_date)
            if task_run_id:
                mark_skipped(db, task_run_id, "non_trade_day")
                db.commit()
            return {"task_name": task_name, "status": STATUS_SKIPPED, "reason": "non_trade_day"}

        # Step 2+3: Lock (includes idempotent check)
        task_run_id, lock_version = try_acquire_lock(
            db, task_name=task_name, trade_date=trade_date
        )
        if task_run_id is None:
            # Either already succeeded or lock held
            run_table = Base.metadata.tables["scheduler_task_run"]
            existing = db.execute(
                select(run_table.c.status).where(
                    run_table.c.task_name == task_name,
                    run_table.c.trade_date == trade_date,
                ).order_by(run_table.c.created_at.desc())
            ).first()
            if existing and existing.status in TERMINAL_STATES:
                db.commit()
                reason = "idempotent_skip" if existing.status == STATUS_SUCCESS else "terminal_state_exists"
                return {
                    "task_name": task_name,
                    "status": existing.status,
                    "reason": reason,
                }
            db.commit()
            return {"task_name": task_name, "status": "LOCK_HELD",
                    "reason": "another_instance_running"}

        # Step 4: Upstream dependency check
        if not check_upstream_ready(db, task_name, trade_date):
            run_table = Base.metadata.tables["scheduler_task_run"]
            db.execute(
                run_table.update()
                .where(run_table.c.task_run_id == task_run_id)
                .values(status=STATUS_WAITING, status_reason="waiting_upstream",
                        started_at=None, updated_at=datetime.now(timezone.utc))
            )
            db.commit()
            return {"task_name": task_name, "status": STATUS_WAITING,
                    "task_run_id": task_run_id}

        # Ensure status is RUNNING
        run_table = Base.metadata.tables["scheduler_task_run"]
        db.execute(
            run_table.update()
            .where(run_table.c.task_run_id == task_run_id)
            .values(status=STATUS_RUNNING, started_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc))
        )
        db.commit()

        # Step 5: Execute handler with retry
        handler = get_handler(task_name)
        if handler is None:
            mark_failed(db, task_run_id, f"no_handler_registered:{task_name}")
            db.commit()
            return {"task_name": task_name, "status": STATUS_FAILED,
                    "reason": "no_handler"}

        max_retries = settings.scheduler_retry_count
        last_error = None
        for attempt in range(max_retries + 1):
            try:
                # Heartbeat before execution
                heartbeat_lock(db, task_run_id, lock_version)
                db.commit()

                import inspect as _inspect
                _sig = _inspect.signature(handler)
                if "force" in _sig.parameters:
                    result = handler(trade_date, force=force)
                else:
                    result = handler(trade_date)

                # Success
                db = SessionLocal()
                if task_name == "fr07_settlement":
                    from app.services.settlement_ssot import get_settlement_pipeline_status

                    pipeline_status = get_settlement_pipeline_status(
                        db,
                        trade_date=trade_date.isoformat(),
                        target_scope="all",
                        window_days_list=FR07_SETTLEMENT_WINDOWS,
                    )
                    if pipeline_status.get("pipeline_status") != "COMPLETED":
                        raise RuntimeError(
                            "settlement_pipeline_not_completed:"
                            f"{pipeline_status.get('pipeline_status') or 'UNKNOWN'}:"
                            f"{pipeline_status.get('status_reason') or 'unknown'}"
                        )
                mark_success(db, task_run_id)

                # Step 6: Emit completion event
                event_name = EVENT_NAMES.get(task_name)
                if event_name:
                    emit_dag_event(
                        db,
                        event_name=event_name,
                        trade_date=trade_date,
                        producer_task_run_id=task_run_id,
                        payload={"result_summary": str(result)[:500] if result else None},
                    )
                db.commit()

                logger.info("dag_node_success task=%s trade_date=%s run_id=%s",
                            task_name, trade_date, task_run_id)
                # Trigger downstream
                _trigger_downstream(task_name, trade_date)

                return {"task_name": task_name, "status": STATUS_SUCCESS,
                        "task_run_id": task_run_id, "result": result}

            except Exception as exc:
                last_error = exc
                logger.warning("dag_node_attempt_failed task=%s attempt=%d/%d err=%s",
                               task_name, attempt + 1, max_retries + 1, exc)
                if attempt < max_retries:
                    backoff = settings.scheduler_backoff_base_seconds ** (attempt + 1)
                    time.sleep(backoff)
                    # Update retry count
                    db = SessionLocal()
                    run_table = Base.metadata.tables["scheduler_task_run"]
                    db.execute(
                        run_table.update()
                        .where(run_table.c.task_run_id == task_run_id)
                        .values(retry_count=attempt + 1,
                                updated_at=datetime.now(timezone.utc))
                    )
                    db.commit()

        # All retries exhausted
        db = SessionLocal()
        mark_failed(db, task_run_id, str(last_error), "retries_exhausted")
        db.commit()
        return {"task_name": task_name, "status": STATUS_FAILED,
                "task_run_id": task_run_id, "error": str(last_error)}

    except Exception as exc:
        logger.exception("dag_node_unexpected_error task=%s err=%s", task_name, exc)
        try:
            db = SessionLocal()
            if task_run_id:
                mark_failed(db, task_run_id, str(exc), "unexpected_error")
            db.commit()
        except Exception:
            pass
        return {"task_name": task_name, "status": STATUS_FAILED, "error": str(exc)}
    finally:
        try:
            db.close()
        except Exception:
            pass


def _trigger_downstream(completed_task: str, trade_date: date) -> None:
    """Find and trigger tasks that depend on the completed task's event."""
    event_name = EVENT_NAMES.get(completed_task)
    if not event_name:
        return

    for task_name, deps in DAG_DEPENDENCIES.items():
        if completed_task in deps:
            # Check if ALL upstream events are ready
            db = SessionLocal()
            try:
                if check_upstream_ready(db, task_name, trade_date):
                    # Check if already running/completed
                    run_table = Base.metadata.tables["scheduler_task_run"]
                    existing = db.execute(
                        select(run_table.c.status).where(
                            run_table.c.task_name == task_name,
                            run_table.c.trade_date == trade_date,
                        ).order_by(run_table.c.created_at.desc())
                    ).first()

                    if existing and existing.status in TERMINAL_STATES:
                        continue
                    if existing and existing.status == STATUS_RUNNING:
                        continue

                    # Promote WAITING_UPSTREAM to ready, or create new
                    if existing and existing.status == STATUS_WAITING:
                        _promote_waiting = db.execute(
                            select(run_table.c.task_run_id).where(
                                run_table.c.task_name == task_name,
                                run_table.c.trade_date == trade_date,
                                run_table.c.status == STATUS_WAITING,
                            )
                        ).first()
                        if _promote_waiting:
                            # Set to PENDING so execute_dag_node can properly acquire
                            # the lock via try_acquire_lock (avoids self-lock).
                            db.execute(
                                run_table.update()
                                .where(run_table.c.task_run_id == _promote_waiting.task_run_id)
                                .values(status=STATUS_PENDING,
                                        updated_at=datetime.now(timezone.utc),
                                        status_reason="upstream_ready")
                            )
                            db.commit()

                    logger.info("dag_trigger_downstream task=%s trade_date=%s (upstream=%s)",
                                task_name, trade_date, completed_task)
                    # Execute in thread to avoid blocking
                    Thread(
                        target=execute_dag_node,
                        args=(task_name, trade_date),
                        daemon=True,
                        name=f"dag-{task_name}-{trade_date}",
                    ).start()
            finally:
                db.close()


# ---------------------------------------------------------------------------
# DAG chain runner (replaces cron-only start_scheduler)
# ---------------------------------------------------------------------------

def run_daily_dag_chain(trade_date: date | None = None) -> dict:
    """
    Kick off the full daily DAG chain for a trade date.
    This is the main entry point called by the scheduler on trade days.
    
    Execution order is driven by DAG dependencies, not fixed times:
    1. fr01_stock_pool (no deps) — runs immediately
    2. fr05_market_state (no deps) — runs immediately  
    3. fr04_data_collect (waits for fr01) — triggered by fr01 completion
    4. fr06_report_gen (waits for fr04) — triggered by fr04 completion
    5. fr07_settlement (waits for fr06) — triggered by fr06 completion
    6. fr08_sim_trade (waits for fr06) — triggered by fr06 completion
    7. fr13_event_notify (waits for fr08) — triggered by fr08 completion
    """
    trade_date = trade_date or date.fromisoformat(latest_trade_date_str())

    if not is_trade_day(trade_date):
        logger.info("dag_chain_skipped reason=non_trade_day trade_date=%s", trade_date)
        return {"status": "skipped", "reason": "non_trade_day"}

    logger.info("dag_chain_started trade_date=%s", trade_date)

    # Launch root nodes (no upstream dependencies) in parallel
    root_tasks = [t for t, deps in DAG_DEPENDENCIES.items() if not deps]
    results = {}
    threads = []
    for task_name in root_tasks:
        t = Thread(
            target=lambda tn=task_name: results.__setitem__(tn, execute_dag_node(tn, trade_date)),
            daemon=True,
            name=f"dag-root-{task_name}-{trade_date}",
        )
        threads.append(t)
        t.start()

    # Wait for root tasks (they'll cascade trigger downstream via _trigger_downstream)
    for t in threads:
        t.join(timeout=settings.scheduler_job_timeout_seconds)

    logger.info("dag_chain_root_tasks_done trade_date=%s results=%s",
                trade_date, {k: v.get("status") for k, v in results.items()})
    return {"status": "started", "trade_date": trade_date.isoformat(),
            "root_results": {k: v.get("status") for k, v in results.items()}}


# ---------------------------------------------------------------------------
# Cascade timeout watcher
# ---------------------------------------------------------------------------

_timeout_watcher_running = False
_timeout_watcher_thread: Thread | None = None


def start_timeout_watcher() -> None:
    """Start background thread that enforces cascade timeouts."""
    global _timeout_watcher_running, _timeout_watcher_thread
    if _timeout_watcher_running:
        return
    _timeout_watcher_running = True
    _timeout_watcher_thread = Thread(
        target=_timeout_watcher_loop,
        daemon=True,
        name="dag-timeout-watcher",
    )
    _timeout_watcher_thread.start()


def stop_timeout_watcher() -> None:
    global _timeout_watcher_running
    _timeout_watcher_running = False


def _timeout_watcher_loop() -> None:
    while _timeout_watcher_running:
        try:
            db = SessionLocal()
            try:
                td_str = latest_trade_date_str()
                if td_str:
                    trade_date = date.fromisoformat(td_str)
                    affected = enforce_cascade_timeout(db, trade_date)
                    if affected:
                        db.commit()
                        logger.warning("cascade_timeout_enforced trade_date=%s affected=%d",
                                       trade_date, len(affected))
            finally:
                db.close()
        except OperationalError:
            pass  # table may not exist in test/ephemeral databases
        except Exception as exc:
            logger.warning("timeout_watcher_error err=%s", exc)
        time.sleep(60)  # Check every minute
