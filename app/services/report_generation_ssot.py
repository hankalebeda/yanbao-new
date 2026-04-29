from __future__ import annotations

import asyncio
import json
import logging
import re
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from threading import Lock, Thread
from typing import Any
from uuid import UUID, uuid4, uuid5

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.request_context import ensure_request_id
from app.models import Base
from app.services.admin_audit import create_audit_log
from app.services.stock_pool import get_daily_stock_pool
from app.services.trade_calendar import latest_trade_date_str

logger = logging.getLogger(__name__)

SSOT_TIERS = ("10k", "100k", "500k")
CAPITAL_BY_TIER = {"10k": 10_000.0, "100k": 100_000.0, "500k": 500_000.0}
BASE_POSITION_RATIO = {"10k": 0.10, "100k": 0.20, "500k": 0.30}
LLM_LEVELS = {"primary", "backup", "cli", "local", "failed"}
QUALITY_FLAGS = {"ok", "stale_ok", "degraded"}
_REPORT_DATA_INCOMPLETE = "REPORT_DATA_INCOMPLETE"
REPORT_GENERATION_ROUND_LIMIT = 5
# Strict completeness policy: any non-ok input status is treated as incomplete.
_REPORT_ALLOWED_INPUT_STATUS = {"ok"}
_REPORT_REQUIRED_INPUT_DATASETS = frozenset(
    {
        "kline_daily",
        "hotspot_top50",
        # "northbound_summary",  # R-02: 外部数据源 eastmoney 不可用，降为可选
        # "etf_flow_summary",    # R-02: 外部数据源 eastmoney 不可用，降为可选
        "market_state_input",
        # "main_force_flow",   # R-03: 外部数据源 eastmoney 不可用，降为可选（同 R-02）
        # "dragon_tiger_list", # R-03: 外部数据源 eastmoney 不可用，降为可选
        # "margin_financing",  # R-03: 外部数据源 eastmoney 不可用，降为可选
        # "stock_profile",     # R-03: 外部数据源 eastmoney 不可用，降为可选
    }
)
_REQUIRED_TEST_INPUT_USAGE_ROWS = (
    ("kline_daily", "tdx_local"),
    ("hotspot_top50", "eastmoney"),
    ("northbound_summary", "eastmoney"),
    ("etf_flow_summary", "eastmoney"),
)
_NON_REPORT_USAGE_DATASETS = frozenset(
    {
        "main_force_flow",
        "dragon_tiger_list",
        "margin_financing",
        "stock_profile",
        "northbound_summary",
        "etf_flow_summary",
    }
)
_NON_REPORT_USAGE_READY_STATUSES = {"ok", "stale_ok", "proxy_ok", "realtime_only"}


def _required_input_datasets_for_strategy(strategy_type: str | None) -> set[str]:
    strategy = str(strategy_type or "").strip().upper()
    required = {"kline_daily", "market_state_input"}
    if strategy in {"", "A"}:
        required.add("hotspot_top50")
    return required

# Persistent event loop for sync→async LLM calls.
# Never closed — avoids httpx "Event loop is closed" errors when the
# singleton CodexAPIClient / DeepSeekAPIClient are reused across calls.
_llm_loop: asyncio.AbstractEventLoop | None = None
_llm_loop_thread: Thread | None = None
_report_generation_locks_guard = Lock()
_report_generation_locks: dict[str, Lock] = {}
_ACTIVE_TASK_STATUSES = {"Pending", "Processing", "Suspended"}
_RETRYABLE_TASK_STATUSES = {"Failed", "Expired"}
_RESUMABLE_TASK_STATUSES = {"Pending", "Suspended"}
_TEST_STOCK_NAMES = {
    "600519.SH": "MOUTAI",
    "000001.SZ": "PINGAN BANK",
    "300750.SZ": "CATL",
}


def _llm_loop_worker(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _get_llm_loop() -> asyncio.AbstractEventLoop:
    """Return a persistent background event loop for sync→async LLM bridge calls."""
    global _llm_loop, _llm_loop_thread
    if _llm_loop is None or _llm_loop.is_closed():
        _llm_loop = asyncio.new_event_loop()
        _llm_loop_thread = Thread(target=_llm_loop_worker, args=(_llm_loop,), daemon=True)
        _llm_loop_thread.start()
    return _llm_loop


def _run_llm_coro(awaitable_factory, *, timeout_sec: float | None = None):
    result = awaitable_factory()
    if not asyncio.iscoroutine(result):
        return result
    loop = _get_llm_loop()
    future = asyncio.run_coroutine_threadsafe(result, loop)
    try:
        return future.result(timeout=timeout_sec)
    except FutureTimeoutError as exc:
        future.cancel()
        timeout_text = int(timeout_sec) if timeout_sec and float(timeout_sec).is_integer() else timeout_sec
        raise RuntimeError(f"llm_timeout_after_{timeout_text}s") from exc


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dataset_batch_id(base_batch_id: str, dataset_name: str) -> str:
    """Derive a stable per-dataset batch_id from a base batch_id.

    Prevents UNIQUE constraint failures on (trade_date, stock_code, source_name, batch_id)
    when multiple datasets share the same source_name (e.g., multiple eastmoney datasets).
    Each (base_batch_id, dataset_name) pair maps to a deterministic, distinct UUID.
    """
    try:
        ns = UUID(base_batch_id)
    except (ValueError, AttributeError):
        ns = uuid5(UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), str(base_batch_id))
    return str(uuid5(ns, dataset_name))


def _lock_for_generation_key(idempotency_key: str) -> Lock:
    with _report_generation_locks_guard:
        lock = _report_generation_locks.get(idempotency_key)
        if lock is None:
            lock = Lock()
            _report_generation_locks[idempotency_key] = lock
        return lock


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
    return datetime.fromisoformat(text_value)


def _normalize_atr_ratio(atr_pct: float | None) -> float | None:
    if atr_pct is None:
        return None
    value = float(atr_pct)
    if value <= 0:
        return None
    return value / 100.0 if value > 1 else value


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric_text_variants(value: Any, *, percent: bool = False) -> set[str]:
    numeric_value = _coerce_float(value)
    if numeric_value is None:
        return set()

    candidates = [numeric_value]
    if percent:
        candidates.append(numeric_value * 100.0 if abs(numeric_value) <= 1 else numeric_value)

    variants: set[str] = set()
    for candidate in candidates:
        for decimals in (0, 1, 2):
            rendered = f"{candidate:.{decimals}f}".rstrip("0").rstrip(".")
            if not rendered:
                continue
            variants.add(rendered.lower())
            if percent:
                variants.add(f"{rendered}%".lower())
    return variants


def _text_contains_any(text: str, tokens: set[str]) -> bool:
    lowered = str(text or "").lower()
    return any(str(token).lower() in lowered for token in tokens if token)


def _count_group_hits(text: str, token_groups: list[set[str]]) -> int:
    return sum(1 for group in token_groups if _text_contains_any(text, group))


def _evaluate_llm_grounding(
    *,
    text: str,
    strategy_type: str,
    signal_entry_price: float | None,
    kline_row: dict[str, Any] | None,
    used_data: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    metric_groups: list[set[str]] = []
    for value in (
        signal_entry_price,
        (kline_row or {}).get("ma5"),
        (kline_row or {}).get("ma20"),
    ):
        variants = _metric_text_variants(value)
        if variants:
            metric_groups.append(variants)

    atr_ratio = _normalize_atr_ratio(_coerce_float((kline_row or {}).get("atr_pct")))
    atr_variants = _metric_text_variants(atr_ratio, percent=True)
    if atr_variants:
        metric_groups.append(atr_variants)

    ready_datasets = {
        str(item.get("dataset_name") or "").strip()
        for item in (used_data or [])
        if str(item.get("status") or "").strip().lower() in _NON_REPORT_USAGE_READY_STATUSES
    }
    strategy_keywords = {
        "A": {"事件", "热点", "公告", "催化"},
        "B": {"ma20", "均线", "趋势", "多头", "空头"},
        "C": {"atr", "低波", "波动率", "震荡"},
    }

    return {
        "available_metric_groups": len(metric_groups),
        "required_metric_hits": min(2, len(metric_groups)),
        "metric_hits": _count_group_hits(text, metric_groups),
        "strategy_keywords_ok": _text_contains_any(text, strategy_keywords.get(str(strategy_type or "").upper(), set())),
        "capital_keywords_expected": bool(ready_datasets & {"main_force_flow", "dragon_tiger_list", "margin_financing"}),
        "capital_keywords_ok": _text_contains_any(text, {"主力", "净流", "龙虎榜", "融资", "北向"}),
        "valuation_keywords_expected": "stock_profile" in ready_datasets,
        "valuation_keywords_ok": _text_contains_any(text, {"pe", "pb", "roe", "市盈率", "市净率", "估值", "行业"}),
        "hotspot_keywords_expected": str(strategy_type or "").upper() == "A" and "hotspot_top50" in ready_datasets,
        "hotspot_keywords_ok": _text_contains_any(text, {"事件", "热点", "公告", "催化"}),
    }


def _ensure_required_generation_input_usage_rows(
    db: Session,
    *,
    stock_code: str,
    trade_day: date,
    batch_id: str,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    usage_table = Base.metadata.tables["report_data_usage"]
    inserted_rows: list[dict[str, Any]] = []
    base_fetch_time = now or utc_now()

    for offset, (dataset_name, source_name) in enumerate(_REQUIRED_TEST_INPUT_USAGE_ROWS):
        fetch_time = base_fetch_time + timedelta(microseconds=offset)
        # Use a per-dataset stable batch_id to avoid UNIQUE constraint failures on
        # (trade_date, stock_code, source_name, batch_id) when multiple datasets
        # share the same source_name (e.g. hotspot_top50, northbound_summary and
        # etf_flow_summary all use source_name='eastmoney').
        row_batch_id = _dataset_batch_id(batch_id, dataset_name)
        existing = _query_one(
            db,
            """
            SELECT usage_id, trade_date, stock_code, dataset_name, source_name, batch_id, fetch_time, status, status_reason
            FROM report_data_usage
            WHERE stock_code = :stock_code
              AND trade_date = :trade_date
              AND dataset_name = :dataset_name
              AND source_name = :source_name
            ORDER BY fetch_time DESC, created_at DESC
            LIMIT 1
            """,
            {
                "stock_code": stock_code,
                "trade_date": trade_day,
                "dataset_name": dataset_name,
                "source_name": source_name,
            },
        )
        if existing:
            db.execute(
                usage_table.update()
                .where(usage_table.c.usage_id == existing["usage_id"])
                .values(
                    batch_id=row_batch_id,
                    fetch_time=fetch_time,
                    status="ok",
                    status_reason=None,
                )
            )
            existing.update(
                batch_id=row_batch_id,
                fetch_time=fetch_time,
                status="ok",
                status_reason=None,
            )
            inserted_rows.append(existing)
            continue

        values = {
            "usage_id": str(uuid4()),
            "trade_date": trade_day,
            "stock_code": stock_code,
            "dataset_name": dataset_name,
            "source_name": source_name,
            "batch_id": row_batch_id,
            "fetch_time": fetch_time,
            "status": "ok",
            "status_reason": None,
            "created_at": base_fetch_time,
        }
        db.execute(usage_table.insert().values(**values))
        inserted_rows.append(values)

    return inserted_rows


def ensure_test_generation_context(db: Session, *, stock_code: str, trade_date: str) -> None:
    trade_day = date.fromisoformat(trade_date)
    now = utc_now()
    batch_row = _query_one(
        db,
        """
        SELECT batch_id
        FROM data_batch
        WHERE trade_date = :trade_date
        ORDER BY created_at DESC
        LIMIT 1
        """,
        {"trade_date": trade_day},
    )
    batch_id = str((batch_row or {}).get("batch_id") or str(uuid4()))
    if not batch_row:
        db.execute(
            Base.metadata.tables["data_batch"].insert().values(
                batch_id=batch_id,
                source_name="tdx_local",
                trade_date=trade_day,
                batch_scope="core_pool",
                batch_seq=1,
                batch_status="SUCCESS",
                quality_flag="ok",
                covered_stock_count=1,
                core_pool_covered_count=1,
                records_total=1,
                records_success=1,
                records_failed=0,
                status_reason=None,
                trigger_task_run_id=None,
                started_at=now,
                finished_at=now,
                updated_at=now,
                created_at=now,
            )
        )

    stock_row = _query_one(
        db,
        "SELECT stock_code FROM stock_master WHERE stock_code = :stock_code LIMIT 1",
        {"stock_code": stock_code},
    )
    if not stock_row:
        db.execute(
            Base.metadata.tables["stock_master"].insert().values(
                stock_code=stock_code,
                stock_name=_TEST_STOCK_NAMES.get(stock_code, stock_code),
                exchange="SH" if stock_code.endswith(".SH") else "SZ",
                is_suspended=False,
                is_delisted=False,
                is_st=False,
                created_at=now,
                updated_at=now,
            )
        )

    kline_row = _query_one(
        db,
        """
        SELECT stock_code
        FROM kline_daily
        WHERE stock_code = :stock_code AND trade_date = :trade_date
        LIMIT 1
        """,
        {"stock_code": stock_code, "trade_date": trade_day},
    )
    if not kline_row:
        db.execute(
            Base.metadata.tables["kline_daily"].insert().values(
                kline_id=str(uuid4()),
                stock_code=stock_code,
                trade_date=trade_day,
                open=118.8,
                high=123.6,
                low=116.4,
                close=120.0,
                volume=1_000_000.0,
                amount=120_000_000.0,
                adjust_type="qfq",
                atr_pct=0.03,
                ma5=119.0,
                ma20=116.0,
                volatility_20d=0.02,
                is_suspended=False,
                source_batch_id=batch_id,
            )
        )

    _ensure_required_generation_input_usage_rows(
        db,
        stock_code=stock_code,
        trade_day=trade_day,
        batch_id=batch_id,
        now=now,
    )

    refresh_row = _query_one(
        db,
        """
        SELECT task_id, pool_version
        FROM stock_pool_refresh_task
        WHERE trade_date = :trade_date
          AND status IN ('COMPLETED', 'FALLBACK')
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
        """,
        {"trade_date": trade_day},
    )
    refresh_task_id = str((refresh_row or {}).get("task_id") or str(uuid4()))
    pool_version = int((refresh_row or {}).get("pool_version") or 1)
    if not refresh_row:
        db.execute(
            Base.metadata.tables["stock_pool_refresh_task"].insert().values(
                task_id=refresh_task_id,
                trade_date=trade_day,
                status="COMPLETED",
                pool_version=pool_version,
                fallback_from=None,
                filter_params_json={"target_pool_size": 1},
                core_pool_size=1,
                standby_pool_size=0,
                evicted_stocks_json=[],
                status_reason=None,
                request_id=str(uuid4()),
                started_at=now,
                finished_at=now,
                updated_at=now,
                created_at=now,
            )
        )
    snapshot_row = _query_one(
        db,
        """
        SELECT pool_snapshot_id
        FROM stock_pool_snapshot
        WHERE stock_code = :stock_code AND trade_date = :trade_date
        LIMIT 1
        """,
        {"stock_code": stock_code, "trade_date": trade_day},
    )
    if not snapshot_row:
        db.execute(
            Base.metadata.tables["stock_pool_snapshot"].insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=refresh_task_id,
                trade_date=trade_day,
                pool_version=pool_version,
                stock_code=stock_code,
                pool_role="core",
                rank_no=1,
                score=88.5,
                is_suspended=False,
                created_at=now,
            )
        )

    market_state_row = _query_one(
        db,
        """
        SELECT trade_date, market_state_degraded
        FROM market_state_cache
        WHERE trade_date = :trade_date
        LIMIT 1
        """,
        {"trade_date": trade_day},
    )
    if not market_state_row:
        db.execute(
            Base.metadata.tables["market_state_cache"].insert().values(
                trade_date=trade_day,
                market_state="BULL",
                cache_status="FRESH",
                state_reason="market ok",
                reference_date=trade_day,
                market_state_degraded=False,
                a_type_pct=0.4,
                b_type_pct=0.4,
                c_type_pct=0.2,
                kline_batch_id=batch_id,
                hotspot_batch_id=batch_id,
                computed_at=now,
                created_at=now,
            )
        )
    elif bool(market_state_row.get("market_state_degraded")):
        db.execute(
            Base.metadata.tables["market_state_cache"].update()
            .where(Base.metadata.tables["market_state_cache"].c.trade_date == trade_day)
            .values(
                market_state="BULL",
                cache_status="FRESH",
                state_reason="market ok",
                reference_date=trade_day,
                market_state_degraded=False,
                a_type_pct=0.4,
                b_type_pct=0.4,
                c_type_pct=0.2,
                kline_batch_id=batch_id,
                hotspot_batch_id=batch_id,
                computed_at=now,
            )
        )
    db.commit()


def resolve_refresh_task_id(
    db: Session,
    *,
    trade_day: date,
    stock_code: str | None = None,
) -> str | None:
    if stock_code:
        # v26 strict: stock-specific generation must bind to same-day snapshot row.
        exact_row = _query_one(
            db,
            """
            SELECT s.refresh_task_id
            FROM stock_pool_snapshot s
            JOIN stock_pool_refresh_task r ON r.task_id = s.refresh_task_id
            WHERE s.trade_date = :trade_date
              AND s.stock_code = :stock_code
              AND r.status IN ('COMPLETED', 'FALLBACK', 'SUCCESS')
            ORDER BY
              CASE s.pool_role WHEN 'core' THEN 0 WHEN 'standby' THEN 1 ELSE 2 END,
              s.created_at DESC,
              s.pool_snapshot_id DESC
            LIMIT 1
            """,
            {"trade_date": trade_day, "stock_code": stock_code},
        )
        if exact_row and exact_row.get("refresh_task_id"):
            return str(exact_row["refresh_task_id"])
        return None

    same_day_row = _query_one(
        db,
        """
        SELECT task_id
        FROM stock_pool_refresh_task
        WHERE trade_date = :trade_date
          AND status IN ('COMPLETED', 'FALLBACK', 'SUCCESS')
        ORDER BY updated_at DESC, created_at DESC, task_id DESC
        LIMIT 1
        """,
        {"trade_date": trade_day},
    )
    if same_day_row and same_day_row.get("task_id"):
        return str(same_day_row["task_id"])

    return None


def _load_pool_version_for_refresh_task(db: Session, *, refresh_task_id: str) -> int | None:
    row = _query_one(
        db,
        "SELECT pool_version FROM stock_pool_refresh_task WHERE task_id = :task_id LIMIT 1",
        {"task_id": refresh_task_id},
    )
    return int(row["pool_version"]) if row and row.get("pool_version") is not None else None


def resolve_refresh_context(
    db: Session,
    *,
    trade_day: date,
    stock_code: str | None = None,
    allow_same_day_fallback: bool = True,
) -> dict[str, Any] | None:
    task_id = resolve_refresh_task_id(db, trade_day=trade_day, stock_code=stock_code)
    if task_id:
        pv = _load_pool_version_for_refresh_task(db, refresh_task_id=task_id)
        row_td = _query_one(
            db,
            "SELECT trade_date FROM stock_pool_refresh_task WHERE task_id = :task_id LIMIT 1",
            {"task_id": task_id},
        )
        task_trade_date = _as_date(row_td["trade_date"]) if row_td else None
        if task_trade_date == trade_day:
            return {"task_id": task_id, "pool_version": pv}
        if allow_same_day_fallback and task_trade_date is not None and task_trade_date < trade_day:
            return {"task_id": task_id, "pool_version": pv}

    # When stock-bound snapshot points to a non-terminal/invalid refresh task,
    # resolve_refresh_context may still recover via same-day terminal task.
    # (Generation path keeps strict snapshot-bound checks elsewhere.)
    if allow_same_day_fallback:
        same_day_row = _query_one(
            db,
            """
            SELECT task_id FROM stock_pool_refresh_task
            WHERE trade_date = :trade_date
              AND status IN ('COMPLETED', 'FALLBACK', 'SUCCESS')
            ORDER BY updated_at DESC, created_at DESC, task_id DESC
            LIMIT 1
            """,
            {"trade_date": trade_day},
        )
        if same_day_row and same_day_row.get("task_id"):
            same_day_tid = str(same_day_row["task_id"])
            pv = _load_pool_version_for_refresh_task(db, refresh_task_id=same_day_tid)
            return {"task_id": same_day_tid, "pool_version": pv}

    # Previous-day fallback: find most recent terminal task before trade_day
    if allow_same_day_fallback:
        prev_row = _query_one(
            db,
            """
            SELECT task_id FROM stock_pool_refresh_task
            WHERE trade_date < :trade_date
              AND status IN ('COMPLETED', 'FALLBACK', 'SUCCESS')
            ORDER BY trade_date DESC, updated_at DESC, task_id DESC
            LIMIT 1
            """,
            {"trade_date": trade_day},
        )
        if prev_row and prev_row.get("task_id"):
            prev_tid = str(prev_row["task_id"])
            pv = _load_pool_version_for_refresh_task(db, refresh_task_id=prev_tid)
            return {"task_id": prev_tid, "pool_version": pv}

    return None


def _ensure_market_state_input_usage(
    db: Session,
    *,
    stock_code: str,
    report_trade_day: date,
    market_state_row: dict[str, Any],
) -> dict[str, Any]:
    from app.services.stock_snapshot_service import (
        _ensure_market_state_input_usage as _ensure_truth_market_state_input_usage,
    )

    return _ensure_truth_market_state_input_usage(
        db,
        stock_code=stock_code,
        usage_trade_day=report_trade_day,
        market_state_row=market_state_row,
    )


def _ensure_report_usage_link(
    db: Session,
    *,
    report_id: str,
    usage_id: str,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    link_table = Base.metadata.tables["report_data_usage_link"]
    existing = _query_one(
        db,
        """
        SELECT report_data_usage_link_id, report_id, usage_id
        FROM report_data_usage_link
        WHERE report_id = :report_id AND usage_id = :usage_id
        LIMIT 1
        """,
        {"report_id": report_id, "usage_id": usage_id},
    )
    if existing:
        return existing
    link_id = str(uuid4())
    now = created_at or utc_now()
    db.execute(
        link_table.insert().values(
            report_data_usage_link_id=link_id,
            report_id=report_id,
            usage_id=usage_id,
            created_at=now,
        )
    )
    return {"report_data_usage_link_id": link_id, "report_id": report_id, "usage_id": usage_id}


_DATASET_SORT_ORDER = {
    "kline_daily": 0,
    "hotspot_top50": 1,
    "etf_flow_summary": 2,
    "northbound_summary": 3,
    "market_state_input": 4,
}


def _sort_used_data(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _sort_key(item: dict[str, Any]):
        ds = str(item.get("dataset_name") or "")
        prio = _DATASET_SORT_ORDER.get(ds, 99)
        ft = item.get("fetch_time")
        if isinstance(ft, datetime):
            ts = ft.timestamp()
        elif ft is not None:
            ts = _as_datetime(ft).timestamp() if _as_datetime(ft) else 0
        else:
            ts = 0
        return (prio, -ts)
    return sorted(items, key=_sort_key)


@dataclass
class ReportGenerationServiceError(RuntimeError):
    status_code: int
    error_code: str

    def __str__(self) -> str:
        return self.error_code


def _load_active_report_row(db: Session, *, idempotency_key: str) -> dict[str, Any] | None:
    return _query_one(
        db,
        """
        SELECT report_id, stock_code, trade_date
        FROM report
        WHERE idempotency_key = :idempotency_key
          AND is_deleted = 0
          AND superseded_by_report_id IS NULL
        ORDER BY generation_seq DESC, created_at DESC
        LIMIT 1
        """,
        {"idempotency_key": idempotency_key},
    )


def _load_active_task_row(db: Session, *, idempotency_key: str) -> dict[str, Any] | None:
    return _query_one(
        db,
        """
        SELECT task_id, stock_code, trade_date, generation_seq, status, retry_count, status_reason
        FROM report_generation_task
        WHERE idempotency_key = :idempotency_key
          AND superseded_at IS NULL
        ORDER BY generation_seq DESC, created_at DESC
        LIMIT 1
        """,
        {"idempotency_key": idempotency_key},
    )


def _resolve_generation_conflict(
    db: Session,
    *,
    idempotency_key: str,
    stock_code: str,
    trade_date: str,
) -> dict[str, Any]:
    active_report = _load_active_report_row(db, idempotency_key=idempotency_key)
    if active_report:
        if active_report["stock_code"] != stock_code or str(active_report["trade_date"]) != trade_date:
            raise ReportGenerationServiceError(409, "IDEMPOTENCY_CONFLICT")
        return _load_report_result(db, str(active_report["report_id"]))

    active_task = _load_active_task_row(db, idempotency_key=idempotency_key)
    if active_task:
        if active_task["stock_code"] != stock_code or str(active_task["trade_date"]) != trade_date:
            raise ReportGenerationServiceError(409, "IDEMPOTENCY_CONFLICT")
        raise ReportGenerationServiceError(409, "CONCURRENT_CONFLICT")

    raise ReportGenerationServiceError(409, "CONCURRENT_CONFLICT")


def _prepare_generation_version(
    db: Session,
    *,
    idempotency_key: str,
    stock_code: str,
    trade_day: date,
    allow_resume_active: bool = False,
    allow_retry_completed: bool = False,
) -> tuple[int, str | None, dict[str, Any] | None, int]:
    active_task = _load_active_task_row(db, idempotency_key=idempotency_key)
    if not active_task:
        max_seq_row = _query_one(
            db,
            "SELECT MAX(generation_seq) AS max_seq FROM report_generation_task WHERE idempotency_key = :key",
            {"key": idempotency_key},
        )
        max_seq = int((max_seq_row or {}).get("max_seq") or 0)
        return max_seq + 1, None, None, 0

    if active_task["stock_code"] != stock_code or str(active_task["trade_date"]) != trade_day.isoformat():
        raise ReportGenerationServiceError(409, "IDEMPOTENCY_CONFLICT")

    status = str(active_task["status"])
    if status in _ACTIVE_TASK_STATUSES:
        if allow_resume_active and status in _RESUMABLE_TASK_STATUSES:
            return (
                int(active_task["generation_seq"] or 1),
                None,
                active_task,
                int(active_task.get("retry_count") or 0),
            )
        raise ReportGenerationServiceError(409, "CONCURRENT_CONFLICT")

    if status in _RETRYABLE_TASK_STATUSES:
        return (
            int(active_task["generation_seq"] or 1) + 1,
            str(active_task["task_id"]),
            None,
            int(active_task.get("retry_count") or 0) + 1,
        )

    if allow_retry_completed and status == "Completed":
        return (
            int(active_task["generation_seq"] or 1) + 1,
            str(active_task["task_id"]),
            None,
            int(active_task.get("retry_count") or 0) + 1,
        )

    # Completed without an active report is still an occupied current-effective task.
    raise ReportGenerationServiceError(409, "CONCURRENT_CONFLICT")


def _build_llm_prompt(
    *,
    stock_code: str,
    stock_name: str,
    strategy_type: str,
    market_state: str,
    quality_flag: str,
    prior_stats: dict[str, Any] | None,
    signal_entry_price: float,
    used_data: list[dict[str, Any]] | None = None,
    kline_row: dict[str, Any] | None = None,
) -> str:
    """构建发送给 LLM 的研报生成 prompt，含多源数据证据。"""
    prior_text = "暂无历史绩效数据（冷启动期）"
    if prior_stats:
        prior_text = (
            f"以下为本系统已结算历史研报统计（截至{prior_stats.get('data_cutoff', 'N/A')}），**仅供参考，不代表未来**：\n"
            f"- 策略类型: {prior_stats.get('strategy_type', strategy_type)}\n"
            f"- 有效样本数: {prior_stats.get('sample_count', 'N/A')}\n"
            f"- 历史胜率: {prior_stats.get('win_rate_historical', 'N/A')}\n"
            f"- 历史盈亏比: {prior_stats.get('avg_profit_loss_ratio', 'N/A')}\n"
            f"（注：禁止直接以历史胜率作为当前置信度依据，仅作量级参照）"
        )

    # 技术指标
    tech_text = ""
    if kline_row:
        close_v = kline_row.get("close")
        ma5_v = kline_row.get("ma5")
        ma20_v = kline_row.get("ma20")
        atr_v = kline_row.get("atr_pct")
        vol_v = kline_row.get("volatility_20d")
        tech_text = (
            f"\n## 技术指标\n"
            f"- 开盘: {kline_row.get('open', 'N/A')}, 最高: {kline_row.get('high', 'N/A')}, "
            f"最低: {kline_row.get('low', 'N/A')}, 收盘: {close_v}\n"
            f"- MA5: {ma5_v}, MA20: {ma20_v}\n"
            f"- ATR%: {atr_v}, 20日波动率: {vol_v}\n"
            f"- 是否停牌: {'是' if kline_row.get('is_suspended') else '否'}"
        )
        # OPT-03: 衍生技术信号层
        try:
            _c = float(close_v) if close_v is not None else None
            _m5 = float(ma5_v) if ma5_v is not None else None
            _m20 = float(ma20_v) if ma20_v is not None else None
            _atr = float(atr_v) / 100.0 if atr_v is not None else None
            _vol = float(vol_v) if vol_v is not None else None

            ma_pos = "N/A"
            if _c and _m5 and _m20:
                if _c > _m5 > _m20:
                    ma_pos = "多头排列（close > MA5 > MA20，强势）"
                elif _c < _m5 < _m20:
                    ma_pos = "空头排列（close < MA5 < MA20，弱势）"
                elif _c > _m20 and _m5 > _m20:
                    ma_pos = "价格与MA5均站上MA20，中期偏多"
                elif _c < _m20:
                    ma_pos = "价格跌破MA20，中期偏弱"
                else:
                    ma_pos = "均线纠缠，方向待确认"

            atr_level = "N/A"
            if _atr is not None:
                if _atr < 0.01:
                    atr_level = "极低波动（ATR<1%）"
                elif _atr < 0.02:
                    atr_level = "低波动（ATR 1%~2%）"
                elif _atr < 0.04:
                    atr_level = "中等波动（ATR 2%~4%）"
                else:
                    atr_level = "高波动（ATR>4%，风险较大）"

            tech_text += (
                f"\n\n## 衍生技术信号\n"
                f"- MA结构: {ma_pos}\n"
                f"- ATR波动档位: {atr_level}\n"
            )
        except (TypeError, ValueError):
            pass

    # 多源数据证据（含资金和基本面快照解析）
    evidence_text = ""
    if used_data:
        from app.services.capital_usage_collector import parse_capital_snapshot
        from app.services.stock_profile_collector import parse_profile_snapshot
        evidence_lines = []
        for item in used_data:
            ds = item.get("dataset_name", "unknown")
            src = item.get("source_name", "")
            status = item.get("status", "")
            reason = item.get("status_reason") or ""
            line = f"- {ds} (来源: {src}, 状态: {status})"
            # Append fact values when available from snapshots or direct fields
            if ds == "northbound_summary" and status == "ok":
                net_1d = item.get("net_inflow_1d")
                if net_1d is not None:
                    line += f"  北向资金净流入(当日): {net_1d:.2f}亿"
            elif ds == "etf_flow_summary" and status == "ok":
                net_cr = item.get("net_creation_redemption")
                if net_cr is not None:
                    line += f"  ETF净申赎: {net_cr:.2f}亿"
            elif ds == "hotspot_top50":
                topic = item.get("topic_title")
                if topic:
                    line += f"  热点话题: {topic}"
            elif ds == "main_force_flow" and status == "ok":
                cap = parse_capital_snapshot(str(reason)) or {}
                if cap:
                    net_1d = cap.get("net_inflow_1d")
                    net_5d = cap.get("net_inflow_5d")
                    if net_1d is not None:
                        line += f"  主力净流入(1d): {net_1d/1e8:.2f}亿"
                    if net_5d is not None:
                        line += f"  主力净流入(5d): {net_5d/1e8:.2f}亿"
            elif ds == "dragon_tiger_list" and status == "ok":
                cap = parse_capital_snapshot(str(reason)) or {}
                if cap:
                    lhb_30d = cap.get("lhb_count_30d")
                    net_buy = cap.get("net_buy_total")
                    if lhb_30d is not None:
                        line += f"  龙虎榜30天入榜: {lhb_30d}次"
                    if net_buy is not None:
                        line += f"  龙虎榜净买入: {net_buy/1e8:.2f}亿"
            elif ds == "margin_financing" and status == "ok":
                cap = parse_capital_snapshot(str(reason)) or {}
                if cap:
                    rzye = cap.get("latest_rzye")
                    rzye_delta = cap.get("rzye_delta_5d")
                    if rzye is not None:
                        line += f"  融资余额: {rzye/1e8:.2f}亿"
                    if rzye_delta is not None:
                        line += f"  融资余额5d变化: {rzye_delta/1e8:.2f}亿"
            elif ds == "stock_profile" and status == "ok":
                profile = parse_profile_snapshot(str(reason)) or {}
                if profile:
                    pe = profile.get("pe_ttm") or profile.get("pe")
                    pb = profile.get("pb")
                    roe = profile.get("roe_pct") or profile.get("roe")
                    industry = profile.get("industry")
                    if pe is not None:
                        line += f"  PE(TTM): {pe:.2f}"
                    if pb is not None:
                        line += f"  PB: {pb:.2f}"
                    if roe is not None:
                        line += f"  ROE: {roe:.2f}%"
                    if industry:
                        line += f"  行业: {industry}"
            evidence_lines.append(line)
        evidence_text = "\n## 所用数据源\n" + "\n".join(evidence_lines)

    # OPT-10: 检查 hotspot_top50 可用性，给策略A注入数据降级警告
    _hs_item = next((item for item in (used_data or []) if item.get("dataset_name") == "hotspot_top50"), None)
    _hs_note = (
        "\n**⚠️ 数据降级提示：hotspot_top50 当前不可用（状态=missing/stale）。"
        "请基于个股行业地位、同期龙头涨跌、近期政策面推断事件催化信号。"
        "若无法确认事件来源，须在 validation_check 中标注「事件信号未经数据源验证」且 confidence ≤ 0.62。**"
    ) if not _hs_item or _hs_item.get("status") != "ok" else ""
    # OPT-05: 策略差异化执行清单（必要证据 + 否决条件三段式）
    strategy_checklist = {
        "A": (
            "## 策略A 事件驱动 — 专属执行清单\n"
            "**必须提供**：\n"
            "1. 事件类型（政策利好/公告/行业热点/突发事件），引用来源名称\n"
            "2. 事件窗口剩余天数（预计还有几个交易日能受到催化）\n"
            "3. 催化兑现概率评估（高/中/低），并说明依据\n"
            "**否决条件（满足任一即强制 confidence ≤ 0.62，结论倾向 HOLD）**：\n"
            "- 所用数据源中无 hotspot_top50 / 热点公告 citation → 标注：「事件信号无法验证」\n"
            "- 事件已发酵超过 3 个交易日且未有进一步确认→ 标注：「事件窗口衰减」\n"
            "- 主力资金 5 日净流出为负（事件炒作但资金不跟） → 标注：「事件资金背离」\n"
            + _hs_note
        ),
        "B": (
            "## 策略B 趋势跟踪 — 专属执行清单\n"
            "**必须提供**：\n"
            "1. MA5/MA20 排列状态（多头/空头/纠缠）\n"
            "2. 近 5 日累计涨幅（具体百分比）\n"
            "3. 量能同比（今日成交额 vs 5 日均量，放量/缩量/持平）\n"
            "**否决条件（满足任一即强制 confidence ≤ 0.60，结论倾向 HOLD）**：\n"
            "- MA20 实际向下（close < MA20 或 MA20 斜率为负） → 标注：「均线趋势尚未确立」\n"
            "- 近 5 日涨幅已超 15%（追涨风险大） → 标注：「短线涨幅过快」\n"
            "- 成交量持续萎缩（上涨无量）→ 标注：「量能不支撑趋势」\n"
        ),
        "C": (
            "## 策略C 低波套利 — 专属执行清单\n"
            "**必须提供**：\n"
            "1. ATR% 具体数值（需 < 2%，否则说明不符合策略条件）\n"
            "2. 近 20 日波动率在市场的分位（需为后 30% 低波段）\n"
            "3. 近 20 日最大回撤区间（高点到低点百分比）\n"
            "**否决条件（满足任一即强制 confidence ≤ 0.62，结论倾向 HOLD）**：\n"
            "- ATR% ≥ 2%（波动已放大，不符合低波条件）→ 标注：「波动脱离低波区间」\n"
            "- 近 5 日出现单日涨跌 ≥ 5%（突发波动）→ 标注：「低波特征被破坏」\n"
            "- 本策略 confidence 上限为 0.70，禁止追涨，不得给出 confidence ≥ 0.71\n"
        ),
    }.get(strategy_type, "通用策略：基于多源证据给出稳健结论，无专属清单。")

    return f"""你是一位专业的A股分析师，请基于以下数据对个股进行分析并输出研报结论。

## 个股信息
- 股票代码: {stock_code}
- 股票名称: {stock_name}
- 当前价格: {signal_entry_price:.2f}元（前复权调整后）

## 市场与策略
- 市场状态: {market_state} (BULL=牛市/NEUTRAL=震荡/BEAR=熊市)
- 策略类型: {strategy_type} (A=事件驱动/B=趋势跟踪/C=低波套利)
- 数据质量: {quality_flag}

{strategy_checklist}
{tech_text}
{evidence_text}

## 历史绩效参考
{prior_text}

## OPT-02 分析框架（必须按顺序执行）
1. **数据解读**：列出本次最关键的技术/资金信号（不超过5条，每条须含具体数值）
2. **多空论据**：
   - 看多依据（最强1条）：...
   - 看空依据（最强1条）：...
3. **矛盾识别**：技术面与资金面信号是否一致？
   - 若一致 → 可给出正常置信度
   - 若矛盾（如MA多头但主力5日净流出为负）→ **置信度不超过0.65**，结论倾向HOLD
4. **结论及置信度**：给出建议，置信度须与矛盾程度负相关
5. **失效条件**：明确说明在何种情况下建议立即失效

## 负面示例（禁止重蹈覆辙）
❌ 错误示例：MA5金叉MA20 → 直接给出BUY confidence=0.82，忽略主力5日净流出2.3亿
✅ 正确做法：技术面看多但资金面矛盾 → confidence≤0.65，明确说明矛盾：「技术面偏多但主力净流出2.3亿构成对冲，暂时观望」

## 输出要求
请严格按以下JSON格式输出，不要输出其他内容：
```json
{{
  "recommendation": "BUY或HOLD或SELL",
  "confidence": 0.50到0.85之间的浮点数,
  "conclusion_text": "120字以上的中文分析结论，必须：1)引用具体数值（收盘价/MA5/MA20/ATR%等，至少2处数字引用）；2)说明策略类型{strategy_type}的触发依据；3)若存在多空矛盾信号须明确说明处理方式",
  "reasoning_chain_md": "分步推理过程（200字以上），使用markdown格式，必须包含：## 技术面分析（引用MA结构数值）→ ## 资金面分析（如有数据）→ ## 多空矛盾判断 → ## 风险因素 → ## 综合结论",
  "strategy_specific_evidence": {{
    "strategy_type": "{strategy_type}",
    "key_signal": "触发本策略的核心信号（A=事件名称/B=MA排列状态+5日涨幅/C=ATR%+波动分位）",
    "validation_check": "专属执行清单的否决条件逐项检查结果（通过/未通过+原因）"
  }}
}}
```

## 约束
1. confidence 取值 [0.50, 0.85]，基于数据确定性和市场环境给出合理值
2. BEAR 市场下应倾向 HOLD 或降低 confidence（最高 0.65）
3. 策略类型 C(低波动) 的 confidence 天然偏低（通常≤0.70）
4. 数据质量 degraded 时 confidence 应适当下调（≤0.62）
5. 主力资金持续净流出（5日净流出为负）通常是偏空信号，应在结论中体现
6. PE偏高（>40）叠加资金净流出时，应适当下调推荐等级
7. conclusion_text 必须包含至少2个具体数字引用，不能空泛描述
8. reasoning_chain_md 必须至少包含5个分析步骤段落（含多空矛盾判断段）
9. 在结论中明确说明被判定为该策略类型的具体依据（哪个信号触发）
10. 技术面与资金面信号矛盾时，confidence 不得超过 0.65，且必须在结论中标注矛盾点
11. strategy_specific_evidence 必须填写，key_signal 须含具体数值，validation_check 须逐条对照策略清单否决条件
12. 若已提供主力/龙虎榜/融资数据，结论或推理链必须至少引用其中一项资金证据
13. 若已提供 PE/PB/ROE/行业等基本面信息，结论或推理链必须至少引用其中一项估值或行业证据
"""


def _fix_triple_quotes(text: str) -> str:
    """将 LLM 输出中的 Python 三引号 \"\"\"...\"\"\" 转为合法 JSON 字符串。"""
    result = text
    while '"""' in result:
        start = result.index('"""')
        rest = result[start + 3:]
        end = rest.find('"""')
        if end == -1:
            break
        inner = rest[:end]
        escaped = inner.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
        result = result[:start] + '"' + escaped + '"' + rest[end + 3:]
    return result


def _parse_llm_response(raw_text: str) -> dict[str, Any] | None:
    """从 LLM 返回文本中提取 JSON 结构。"""
    # 预处理：修复三引号
    preprocessed = _fix_triple_quotes(raw_text) if '"""' in raw_text else raw_text
    for text_source in (preprocessed, raw_text):
        candidate = str(text_source or "").strip()
        if candidate.startswith("{") and candidate.endswith("}"):
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
    # 尝试从 ```json ... ``` 块提取
    for text_source in (preprocessed, raw_text):
        match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text_source, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
    # 尝试直接解析整段文本中的 JSON
    for text_source in (preprocessed, raw_text):
        match = re.search(r"\{[^{}]*\"recommendation\"[^{}]*\}", text_source, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
    # Regex fallback: 从残缺 JSON 中按字段提取
    rec_m = re.search(r'"recommendation"\s*:\s*"([^"]+)"', raw_text)
    conf_m = re.search(r'"confidence"\s*:\s*([\d.]+)', raw_text)
    conc_m = re.search(r'"conclusion_text"\s*:\s*"([^"]+)"', raw_text)
    reas_m = re.search(r'"reasoning_chain_md"\s*:\s*"([^"]*)"', raw_text)
    if rec_m and conf_m:
        return {
            "recommendation": rec_m.group(1),
            "confidence": float(conf_m.group(1)),
            "conclusion_text": conc_m.group(1) if conc_m else "",
            "reasoning_chain_md": reas_m.group(1) if reas_m else "",
        }
    return None


def _validate_llm_result(
    parsed: dict[str, Any],
    *,
    strategy_type: str = "",
    market_state: str = "",
    signal_entry_price: float | None = None,
    kline_row: dict[str, Any] | None = None,
    used_data: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """校验并修正 LLM 输出。

    OPT-03：收紧校验阈值与数字引用要求，与 Prompt 约束对齐：
    - conclusion_text 最小长度从 80 升为 100 字
    - 要求至少 2 处数字引用（\\d.*\\d 模式）
    - reasoning_chain_md 最小长度从 150 升为 200 字

    OPT-13：策略类型置信度分层硬约束（验证层强制执行，与 prompt 约束一致）：
    - C-type: confidence 上限 0.70（低波套利，天然低确定性）
    - A-type + BEAR市场: confidence 上限 0.60（熊市中事件驱动风险高）
    """
    rec = str(parsed.get("recommendation", "HOLD")).upper().strip()
    if rec not in ("BUY", "HOLD", "SELL"):
        rec = "HOLD"
    conf = parsed.get("confidence", 0.60)
    try:
        conf = float(conf)
    except (TypeError, ValueError):
        conf = 0.60
    conf = max(0.50, min(0.85, conf))
    # OPT-13: 策略类型置信度分层上限（验证层强制，补齐 prompt 约束的执行缺口）
    _st = str(strategy_type or "").upper().strip()
    _ms = str(market_state or "").upper().strip()
    if _st == "C":
        conf = min(conf, 0.70)
    elif _st == "A" and _ms == "BEAR":
        conf = min(conf, 0.60)
    conclusion = str(parsed.get("conclusion_text", "")).strip()
    # OPT-03: 要求 ≥100 字且含至少 2 处数字引用（间接验证 "引用具体数值" 约束）
    # OPT-08: 对齐提示词 "120字以上" 要求
    num_matches = re.findall(r"\d+\.?\d*", conclusion)
    if len(conclusion) < 120 or len(num_matches) < 2:
        conclusion = "LLM 分析结论待补充"
    reasoning = str(parsed.get("reasoning_chain_md", "")).strip()
    # OPT-09: 推理链结构校验（要求5段 markdown 标题，允许最多3个缺失）
    _REQUIRED_SECTIONS = {"## 技术面分析", "## 资金面分析", "## 多空矛盾判断", "## 风险因素", "## 综合结论"}
    _missing_sections = [s for s in _REQUIRED_SECTIONS if s not in reasoning]
    if len(reasoning) < 200 or len(_missing_sections) >= 4:
        reasoning = "推理链待补充"
    # OPT-06: 软校验 strategy_specific_evidence
    # 若 LLM 未输出该字段或 key_signal 为空，则 confidence clip 到 0.55 并标记
    sse = parsed.get("strategy_specific_evidence")
    sse_valid = (
        isinstance(sse, dict)
        and bool(str(sse.get("key_signal") or "").strip())
        and bool(str(sse.get("validation_check") or "").strip())
        and any(c.isdigit() for c in str(sse.get("key_signal") or ""))
    )
    if not sse_valid:
        conf = min(conf, 0.55)
        if isinstance(sse, dict):
            sse["_missing_key_signal"] = True
        else:
            sse = {"strategy_type": "", "key_signal": "", "validation_check": "", "_missing_key_signal": True}

    grounding_text = "\n".join(
        part
        for part in (
            conclusion,
            reasoning,
            str((sse or {}).get("key_signal") or "") if isinstance(sse, dict) else "",
        )
        if part
    )
    grounding = _evaluate_llm_grounding(
        text=grounding_text,
        strategy_type=strategy_type,
        signal_entry_price=signal_entry_price,
        kline_row=kline_row,
        used_data=used_data,
    )
    hard_grounding_reasons: list[str] = []
    required_metric_hits = int(grounding.get("required_metric_hits") or 0)
    if grounding["metric_hits"] < required_metric_hits:
        hard_grounding_reasons.append("missing_metric_binding")
    if not grounding["strategy_keywords_ok"]:
        hard_grounding_reasons.append("missing_strategy_binding")
    if grounding["hotspot_keywords_expected"] and not grounding["hotspot_keywords_ok"]:
        hard_grounding_reasons.append("missing_hotspot_binding")

    soft_grounding_reasons: list[str] = []
    if grounding["capital_keywords_expected"] and not grounding["capital_keywords_ok"]:
        soft_grounding_reasons.append("capital_data_not_used")
    if grounding["valuation_keywords_expected"] and not grounding["valuation_keywords_ok"]:
        soft_grounding_reasons.append("valuation_data_not_used")

    grounding_state = "ok"
    if hard_grounding_reasons:
        grounding_state = "hard_fail"
        conf = min(conf, 0.55)
        if isinstance(sse, dict):
            sse["_grounding_hard_fail"] = hard_grounding_reasons
        else:
            sse = {
                "strategy_type": strategy_type,
                "key_signal": "",
                "validation_check": "",
                "_grounding_hard_fail": hard_grounding_reasons,
            }
    elif soft_grounding_reasons:
        grounding_state = "soft_gap"
        conf = min(conf, 0.62)
        if isinstance(sse, dict):
            sse["_grounding_soft_gap"] = soft_grounding_reasons
        else:
            sse = {
                "strategy_type": strategy_type,
                "key_signal": "",
                "validation_check": "",
                "_grounding_soft_gap": soft_grounding_reasons,
            }
    return {
        "recommendation": rec,
        "confidence": round(conf, 4),
        "conclusion_text": conclusion,
        "reasoning_chain_md": reasoning,
        "strategy_specific_evidence": sse,
        "_grounding_state": grounding_state,
    }


def run_generation_model(
    *,
    stock_code: str,
    stock_name: str,
    strategy_type: str,
    market_state: str,
    quality_flag: str,
    prior_stats: dict[str, Any] | None,
    signal_entry_price: float,
    used_data: list[dict[str, Any]] | None = None,
    kline_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """调用 LLM 生成研报核心结论。

    调用链: ai-api/codex 池（多中转站冗余）→ ollama。
    mock_llm=True 时走规则占位（仅测试）。
    """
    # 构建多源数据证据摘要（用于 mock/fallback reasoning）
    def _build_evidence_summary() -> str:
        parts = [f"market_state={market_state}", f"strategy_type={strategy_type}", f"quality_flag={quality_flag}"]
        if kline_row:
            parts.append(f"close={kline_row.get('close', 'N/A')}, ma5={kline_row.get('ma5', 'N/A')}, ma20={kline_row.get('ma20', 'N/A')}")
            parts.append(f"atr_pct={kline_row.get('atr_pct', 'N/A')}, volatility_20d={kline_row.get('volatility_20d', 'N/A')}")
        if used_data:
            ds_names = [item.get("dataset_name", "unknown") for item in used_data]
            parts.append(f"data_sources={','.join(ds_names)}")
            ds_statuses = {item.get("dataset_name", "?"): item.get("status", "?") for item in used_data}
            parts.append(f"source_status={ds_statuses}")
            # Include fact values when available
            for item in used_data:
                ds = item.get("dataset_name")
                if ds == "northbound_summary" and item.get("status") == "ok":
                    net_1d = item.get("net_inflow_1d")
                    if net_1d is not None:
                        parts.append(f"northbound_net_1d={net_1d}")
                elif ds == "etf_flow_summary" and item.get("status") == "ok":
                    net_cr = item.get("net_creation_redemption")
                    if net_cr is not None:
                        parts.append(f"etf_net_cr={net_cr}")
        return "\n".join(parts)

    # 测试模式：规则占位（OPT-07：A/B/C 差异化 mock 输出）
    if settings.mock_llm:
        recommendation = "BUY" if market_state != "BEAR" and strategy_type in {"A", "B", "C"} else "HOLD"
        confidence = {"A": 0.74, "B": 0.68, "C": 0.66}.get(strategy_type, 0.66)
        if market_state == "NEUTRAL" and recommendation == "BUY":
            confidence = min(confidence, 0.67)
        if quality_flag == "degraded":
            confidence = min(confidence, 0.62)
        # OPT-15: Mock模式置信度基于MA结构动态微调（使测试更贴近真实场景）
        if kline_row:
            try:
                _c_val = float(kline_row.get("close") or 0)
                _m5_val = float(kline_row.get("ma5") or 0)
                _m20_val = float(kline_row.get("ma20") or 0)
                if _c_val > 0 and _m5_val > 0 and _m20_val > 0:
                    if _c_val > _m5_val > _m20_val:  # 多头排列
                        confidence = min(confidence + 0.03, 0.85)
                    elif _c_val < _m5_val < _m20_val:  # 空头排列
                        confidence = max(confidence - 0.05, 0.50)
            except (TypeError, ValueError):
                pass
        if market_state == "BEAR":
            confidence = max(confidence - 0.04, 0.50)
        # OPT-13: 策略类型置信度上限（mock分支同步执行）
        if strategy_type == "C":
            confidence = min(confidence, 0.70)
        elif strategy_type == "A" and market_state == "BEAR":
            confidence = min(confidence, 0.60)
        confidence = round(confidence, 4)
        # OPT-07: 按策略类型给出差异化的 mock 结论与推理链
        _close = (kline_row or {}).get("close", "N/A")
        _ma20 = (kline_row or {}).get("ma20", "N/A")
        _atr = (kline_row or {}).get("atr_pct", "N/A")
        _mock_conclusion = {
            "A": (
                f"{stock_name}({stock_code}) 事件驱动策略（mock）：当前收盘价{_close}元，"
                f"热点事件信号已触发，基于事件催化窗口期判断，短线建议{'买入' if recommendation=='BUY' else '观望'}。"
                f"策略A触发依据：所用数据源包含热点/公告触发；事件催化有效性待观察。"
                f"MA20={_ma20}，置信度{confidence:.2f}。（注：mock模式生成）"
            ),
            "B": (
                f"{stock_name}({stock_code}) 趋势跟踪策略（mock）：当前收盘价{_close}元，"
                f"MA20={_ma20}，策略B触发依据：价格站上MA20且近5日具备上升趋势。"
                f"ATR%={_atr}，量能跟随度中性。整体市场{market_state}，建议{'买入' if recommendation=='BUY' else '观望'}，"
                f"置信度{confidence:.2f}。（注：mock模式生成）"
            ),
            "C": (
                f"{stock_name}({stock_code}) 低波套利策略（mock）：当前收盘价{_close}元，"
                f"ATR%={_atr}（处于低波区间），近20日波动率处于市场后30%分位。"
                f"策略C触发依据：低波特征确认，回撤可控。建议{'买入' if recommendation=='BUY' else '观望'}，"
                f"置信度{confidence:.2f}（C类上限0.70）。（注：mock模式生成）"
            ),
        }.get(strategy_type, f"{stock_name} {stock_code} generated by mock LLM")
        _mock_reasoning = {
            "A": (
                f"## 技术面分析\n收盘价{_close}，MA20={_ma20}，技术面中性。\n"
                f"## 资金面分析\n{_build_evidence_summary()}\n"
                f"## 事件面分析（策略A专属）\n热点/公告事件信号触发，催化持续性待验证，窗口期约2-3交易日。\n"
                f"## 多空矛盾判断\n事件催化偏多，但需防范事件兑现后的资金回撤。\n"
                f"## 风险因素\n事件信号减弱风险；市场状态{market_state}下的系统性风险。\n"
                f"## 综合结论\n策略A事件驱动，confidence={confidence:.2f}，建议{recommendation}。mock=true"
            ),
            "B": (
                f"## 技术面分析\n收盘价{_close}，MA20={_ma20}，"
                f"{'价格站上MA20，多头结构。' if _ma20 != 'N/A' and _close != 'N/A' else '均线数据待确认。'}\n"
                f"## 资金面分析\n{_build_evidence_summary()}\n"
                f"## 趋势面分析（策略B专属）\n近5日趋势中性，量能跟随度待核实；ATR%={_atr}。\n"
                f"## 多空矛盾判断\n技术面偏多，资金面跟随度中性，整体无明显矛盾。\n"
                f"## 风险因素\nMA20趋势翻转风险；市场状态{market_state}下的流动性风险。\n"
                f"## 综合结论\n策略B趋势跟踪，confidence={confidence:.2f}，建议{recommendation}。mock=true"
            ),
            "C": (
                f"## 技术面分析\n收盘价{_close}，ATR%={_atr}（低波确认），MA20={_ma20}。\n"
                f"## 资金面分析\n{_build_evidence_summary()}\n"
                f"## 波动面分析（策略C专属）\n近20日波动率处于市场后30%低波分位，回撤区间可控。\n"
                f"## 多空矛盾判断\n低波特征稳定，无显著多空矛盾；需防范突发事件破坏低波条件。\n"
                f"## 风险因素\n低波突破（ATR%放大）风险；C类策略confidence上限0.70。\n"
                f"## 综合结论\n策略C低波套利，confidence={confidence:.2f}，建议{recommendation}。mock=true"
            ),
        }.get(strategy_type, f"## 分析过程（mock模式）\n{_build_evidence_summary()}\nmock=true")
        _mock_strategy_evidence = {
            "strategy_type": strategy_type,
            "key_signal": {
                "A": f"热点事件触发，收盘价{_close}",
                "B": f"MA20={_ma20}，价格趋势向上，ATR%={_atr}",
                "C": f"ATR%={_atr}（低波），波动率分位后30%",
            }.get(strategy_type, "N/A"),
            "validation_check": "mock模式：否决条件跳过实质性检验",
        }
        return {
            "recommendation": recommendation,
            "confidence": round(confidence, 4),
            "llm_fallback_level": "local",
            "risk_audit_status": "not_triggered",
            "risk_audit_skip_reason": "mock_llm",
            "conclusion_text": _mock_conclusion,
            "reasoning_chain_md": _mock_reasoning,
            "strategy_specific_evidence": _mock_strategy_evidence,
            "signal_entry_price": signal_entry_price,
        }

    # 生产模式：调用真实 LLM
    prompt = _build_llm_prompt(
        stock_code=stock_code,
        stock_name=stock_name,
        strategy_type=strategy_type,
        market_state=market_state,
        quality_flag=quality_flag,
        prior_stats=prior_stats,
        signal_entry_price=signal_entry_price,
        used_data=used_data,
        kline_row=kline_row,
    )

    llm_fallback_level = "primary"
    generation_timeout_seconds = max(
        int(
            getattr(
                settings,
                "report_generation_llm_timeout_seconds",
                getattr(settings, "request_timeout_seconds", 60),
            )
            or getattr(settings, "request_timeout_seconds", 60)
        ),
        5,
    )
    try:
        from app.services.llm_router import route_and_call, LLMScene
        result = _run_llm_coro(
            lambda: route_and_call(prompt, scene=LLMScene.GENERAL, temperature=0.3),
            timeout_sec=generation_timeout_seconds,
        )
        raw_text = result.response
        # Map model_used to SSOT llm_fallback_level: primary|backup|cli|local|failed
        _MODEL_TO_FALLBACK = {
            "codex_api": "primary",
            "deepseek_api": "backup",
            "gemini_api": "cli",
            "claude_cli": "cli",
            "ollama": "local",
        }
        if result.model_used == "codex_api":
            llm_fallback_level = str(result.extra.get("pool_level") or ("backup" if result.degraded else "primary"))
        else:
            llm_fallback_level = _MODEL_TO_FALLBACK.get(result.model_used, "backup" if result.degraded else "primary")
        # v26 P0: 拽出实际命中的模型名 / 网关名 / 端点（供审计 "是否真用 gpt-5.4" 使用）
        llm_actual_model = str(result.extra.get("model") or "").strip() or None
        llm_provider_name = str(result.extra.get("provider_name") or getattr(result, "source", "") or "").strip() or None
        llm_endpoint = str(result.extra.get("endpoint") or "").strip() or None
        logger.info(
            "llm.actual provider=%s model=%s endpoint=%s fallback_level=%s elapsed=%.1fs",
            llm_provider_name, llm_actual_model, llm_endpoint, llm_fallback_level, result.elapsed_s,
        )
    except Exception as exc:
        logger.error("llm_call_failed err=%s, falling back to rules", exc)
        llm_fallback_level = "failed"
        llm_actual_model = None
        llm_provider_name = None
        llm_endpoint = None
        raw_text = None

    # 解析 LLM 输出
    if raw_text:
        parsed = _parse_llm_response(raw_text)
        if parsed:
            validated = _validate_llm_result(
                parsed,
                strategy_type=strategy_type,
                market_state=market_state,
                signal_entry_price=signal_entry_price,
                kline_row=kline_row,
                used_data=used_data,
            )
            grounding_state = str(validated.pop("_grounding_state", "ok") or "ok")
            if grounding_state == "hard_fail":
                logger.warning(
                    "llm_grounding_validation_failed stock=%s strategy=%s market_state=%s",
                    stock_code,
                    strategy_type,
                    market_state,
                )
            else:
                # OPT-04: 贝叶斯置信度校准（冷启动保护）
                # 样本 < 30 时 _prior_shrink 会把 0.75 压到 0.52，
                # 冷启动期应保留 LLM 原始值，不做收缩。
                try:
                    from app.services.confidence_calibration import ConfidenceCalibrator
                    _calibrator = ConfidenceCalibrator.load()
                    raw_conf = validated["confidence"]
                    if _calibrator.is_ready():
                        # 有足够样本：使用等频分箱校准
                        calibrated_conf = _calibrator.calibrate(raw_conf)
                        validated["raw_confidence"] = raw_conf
                        validated["confidence"] = round(calibrated_conf, 4)
                        logger.debug("confidence_calibrated raw=%.4f calibrated=%.4f samples=%d",
                                     raw_conf, calibrated_conf, _calibrator.n_total_samples)
                    else:
                        # 冷启动期（< 30 样本）：保留原始值，仅记录日志
                        validated["raw_confidence"] = raw_conf
                        logger.debug("confidence_calibration_cold_start samples=%d raw_conf_retained=%.4f",
                                     _calibrator.n_total_samples, raw_conf)
                except Exception as _cal_err:
                    logger.debug("confidence_calibration_skipped reason=%s", _cal_err)
                # 质量降级后修正
                if quality_flag == "degraded":
                    validated["confidence"] = min(validated["confidence"], 0.62)
                return {
                    **validated,
                    "llm_fallback_level": llm_fallback_level,
                    "llm_actual_model": llm_actual_model,
                    "llm_provider_name": llm_provider_name,
                    "llm_endpoint": llm_endpoint,
                    "risk_audit_status": "not_triggered",
                    "risk_audit_skip_reason": None,
                    "signal_entry_price": signal_entry_price,
                }

    # LLM 全部失败或解析失败：规则兜底
    logger.warning("llm_parse_failed, using rule fallback")
    recommendation = "BUY" if market_state != "BEAR" and strategy_type in {"A", "B", "C"} else "HOLD"
    confidence = {"A": 0.74, "B": 0.68, "C": 0.66}.get(strategy_type, 0.66)
    if market_state == "NEUTRAL" and recommendation == "BUY":
        confidence = min(confidence, 0.67)
    if quality_flag == "degraded":
        confidence = min(confidence, 0.62)
    rec_cn = {"BUY": "买入", "SELL": "卖出", "HOLD": "观望等待"}.get(recommendation, "观望等待")
    mkt_cn = {"BULL": "偏多", "NEUTRAL": "震荡", "BEAR": "偏空"}.get(market_state, "震荡")
    # 使用 kline 数据构建白话版结论
    if kline_row and kline_row.get("close"):
        close = kline_row.get("close")
        ma20 = kline_row.get("ma20")
        trend_desc = ""
        if ma20 and close and float(close) > float(ma20):
            trend_desc = f"，当前价格高于20日均线"
        elif ma20 and close and float(close) < float(ma20):
            trend_desc = f"，当前价格低于20日均线"
        fallback_conclusion = (
            f"{stock_name}当日收盘价{close}元{trend_desc}，"
            f"整体市场{mkt_cn}。基于量化规则分析，短线建议{rec_cn}。"
            f"（注：本结论基于规则模型生成，仅供参考。）"
        )
    else:
        fallback_conclusion = (
            f"{stock_name}当前市场环境{mkt_cn}，"
            f"基于量化规则分析，短线建议{rec_cn}。"
            f"（注：本结论基于规则模型生成，仅供参考。）"
        )
    evidence = _build_evidence_summary()
    fallback_reasoning = (
        f"## {stock_name}分析摘要\n"
        f"- 市场状态：{mkt_cn}\n"
        f"- 策略分类：{strategy_type}\n"
        f"- 数据质量：{quality_flag}\n"
    )
    if kline_row and kline_row.get("close"):
        fallback_reasoning += (
            f"- 收盘价：{kline_row.get('close')}\n"
            f"- MA5：{kline_row.get('ma5', 'N/A')}\n"
            f"- MA20：{kline_row.get('ma20', 'N/A')}\n"
        )
    fallback_reasoning += f"- 信号方向：{rec_cn}\n"
    return {
        "recommendation": recommendation,
        "confidence": round(confidence, 4),
        "llm_fallback_level": "failed",
        "risk_audit_status": "not_triggered",
        "risk_audit_skip_reason": "llm_all_failed_rule_fallback",
        "conclusion_text": fallback_conclusion,
        "reasoning_chain_md": fallback_reasoning,
        "signal_entry_price": signal_entry_price,
    }


def _resolve_trade_date(trade_date: str | None) -> tuple[str, date]:
    target = trade_date or latest_trade_date_str()
    return target, date.fromisoformat(target)


def _expire_stale_tasks(db: Session, *, current_trade_date: date) -> None:
    task_table = Base.metadata.tables["report_generation_task"]
    now = utc_now()
    active_threshold = now - timedelta(seconds=max(300, settings.report_generation_active_task_stale_seconds))
    terminal_retryable_threshold = now - timedelta(hours=72)
    stale_trade_cutoff = current_trade_date - timedelta(days=1)
    db.execute(
        task_table.update()
        .where(task_table.c.status.in_(("Pending", "Processing", "Suspended")))
        .where(
            (task_table.c.trade_date < stale_trade_cutoff)
            | (task_table.c.updated_at < active_threshold)
        )
        .values(
            status="Expired",
            status_reason="stale_task_expired",
            finished_at=now,
            updated_at=now,
        )
    )
    db.execute(
        task_table.update()
        .where(task_table.c.status == "Failed")
        .where(
            (task_table.c.trade_date < stale_trade_cutoff)
            | (task_table.c.updated_at < terminal_retryable_threshold)
        )
        .values(
            status="Expired",
            status_reason="stale_task_expired",
            finished_at=now,
            updated_at=now,
        )
    )
    db.flush()


def _query_one(db: Session, sql_text: str, params: dict[str, Any]) -> dict[str, Any] | None:
    row = db.execute(text(sql_text), params).mappings().first()
    return dict(row) if row else None


def _query_all(db: Session, sql_text: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in db.execute(text(sql_text), params).mappings().all()]


def _collect_generation_input_issues(
    *,
    used_data: list[dict[str, Any]],
    market_state_row: dict[str, Any] | None,
    strategy_type: str | None = None,
) -> list[str]:
    issues: list[str] = []
    if not used_data:
        issues.append("used_data_missing")
    required_datasets = _required_input_datasets_for_strategy(strategy_type)
    present_datasets = {
        str(item.get("dataset_name") or "").strip()
        for item in used_data
        if item.get("dataset_name")
    }
    for dataset_name in sorted(required_datasets - present_datasets):
        issues.append(f"{dataset_name}:missing_required_usage")
    for item in used_data:
        dataset_name = str(item.get("dataset_name") or "unknown_dataset")
        if dataset_name not in required_datasets:
            continue
        status = str(item.get("status") or "").strip().lower()
        if status in _REPORT_ALLOWED_INPUT_STATUS:
            continue
        reason = str(item.get("status_reason") or status or "unknown_reason")
        issues.append(f"{dataset_name}:{reason}")
    if market_state_row and bool(market_state_row.get("market_state_degraded")):
        issues.append(str(market_state_row.get("state_reason") or "market_state_degraded"))
    return issues


def _needs_non_report_usage_refresh(
    db: Session,
    *,
    stock_code: str,
    trade_day: date,
) -> bool:
    rows = _query_all(
        db,
        """
        SELECT dataset_name, status, fetch_time, created_at, usage_id
        FROM report_data_usage
        WHERE stock_code = :stock_code
          AND trade_date = :trade_date
        ORDER BY dataset_name ASC, fetch_time DESC, created_at DESC, usage_id DESC
        """,
        {
            "stock_code": stock_code,
            "trade_date": trade_day,
        },
    )
    latest_status_by_dataset: dict[str, str] = {}
    for row in rows:
        dataset_name = str(row.get("dataset_name") or "").strip()
        if dataset_name not in _NON_REPORT_USAGE_DATASETS or dataset_name in latest_status_by_dataset:
            continue
        latest_status_by_dataset[dataset_name] = str(row.get("status") or "").strip().lower()
    for dataset_name in _NON_REPORT_USAGE_DATASETS:
        if latest_status_by_dataset.get(dataset_name) not in _NON_REPORT_USAGE_READY_STATUSES:
            return True
    return False


def _maybe_collect_non_report_usage(
    db: Session,
    *,
    stock_code: str,
    trade_date: str,
) -> None:
    if not _needs_non_report_usage_refresh(db, stock_code=stock_code, trade_day=date.fromisoformat(trade_date)):
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        logger.debug(
            "skip_sync_non_report_collect_inside_running_loop stock=%s trade_date=%s",
            stock_code,
            trade_date,
        )
        return
    try:
        from app.services.stock_snapshot_service import collect_non_report_usage_sync

        collect_non_report_usage_sync(db, stock_code=stock_code, trade_date=trade_date)
    except Exception as exc:
        logger.warning("single_non_report_collect_failed stock=%s trade_date=%s err=%s", stock_code, trade_date, exc)


async def ensure_non_report_usage_collected_if_needed(
    db: Session,
    *,
    stock_code: str,
    trade_date: str | None,
) -> None:
    target_trade_date, trade_day = _resolve_trade_date(trade_date)
    if not _needs_non_report_usage_refresh(db, stock_code=stock_code, trade_day=trade_day):
        return
    try:
        from app.services.stock_snapshot_service import collect_non_report_usage

        await collect_non_report_usage(db, stock_code=stock_code, trade_date=target_trade_date)
    except Exception as exc:
        logger.warning("async_non_report_collect_failed stock=%s trade_date=%s err=%s", stock_code, target_trade_date, exc)


def _usage_by_dataset(used_data: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for item in used_data:
        ds = str(item.get("dataset_name") or "").strip()
        if ds and ds not in mapping:
            mapping[ds] = item
    return mapping


def _truncate_summary_text(value: Any, *, limit: int = 96) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _build_used_data_summary(used_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from app.services.capital_usage_collector import parse_capital_snapshot
    from app.services.stock_profile_collector import parse_profile_snapshot

    summary: list[dict[str, Any]] = []
    for item in _sort_used_data(list(used_data or [])):
        dataset_name = str(item.get("dataset_name") or "").strip()
        source_name = str(item.get("source_name") or "").strip()
        status = str(item.get("status") or "").strip().lower() or "missing"
        status_reason = str(item.get("status_reason") or "").strip()
        highlights: list[str] = []

        if dataset_name in {"main_force_flow", "dragon_tiger_list", "margin_financing"} and status in {"ok", "stale_ok"}:
            snapshot = parse_capital_snapshot(status_reason) or {}
            if snapshot.get("net_inflow_5d") is not None:
                highlights.append(f"5日净流入={snapshot['net_inflow_5d']}")
            if snapshot.get("latest_rzye") is not None:
                highlights.append(f"融资余额={snapshot['latest_rzye']}")
            if snapshot.get("board_count") is not None:
                highlights.append(f"龙虎榜次数={snapshot['board_count']}")
        elif dataset_name == "stock_profile" and status in {"ok", "stale_ok"}:
            snapshot = parse_profile_snapshot(status_reason) or {}
            if snapshot.get("pe_ttm") is not None:
                highlights.append(f"PE={snapshot['pe_ttm']}")
            if snapshot.get("pb") is not None:
                highlights.append(f"PB={snapshot['pb']}")
            if snapshot.get("roe_pct") is not None:
                highlights.append(f"ROE={snapshot['roe_pct']}%")
            if snapshot.get("industry"):
                highlights.append(f"行业={snapshot['industry']}")

        if not highlights:
            preview = _truncate_summary_text(status_reason, limit=72)
            if preview:
                highlights.append(preview)

        summary.append(
            {
                "dataset_name": dataset_name,
                "source_name": source_name,
                "status": status,
                "trade_date": _as_date(item.get("trade_date")).isoformat() if _as_date(item.get("trade_date")) else None,
                "fetch_time": _as_datetime(item.get("fetch_time")).isoformat() if _as_datetime(item.get("fetch_time")) else None,
                "highlights": highlights,
            }
        )
    return summary


def _build_generation_process_summary(
    *,
    recommendation: str,
    confidence: float,
    strategy_type: str,
    quality_flag: str,
    llm_fallback_level: str | None,
    market_state_row: dict[str, Any],
    kline_row: dict[str, Any],
    used_data_summary: list[dict[str, Any]],
    data_completeness: dict[str, Any],
    strategy_specific_evidence: dict[str, Any] | None,
) -> dict[str, Any]:
    sse = strategy_specific_evidence if isinstance(strategy_specific_evidence, dict) else {}
    hard_gaps = [str(item) for item in (sse.get("_grounding_hard_fail") or []) if str(item).strip()]
    soft_gaps = [str(item) for item in (sse.get("_grounding_soft_gap") or []) if str(item).strip()]
    grounding_state = "hard_fail" if hard_gaps else "soft_gap" if soft_gaps else "ok"
    total_required = int(data_completeness.get("total_required") or 0)
    total_ok = int(data_completeness.get("total_ok") or 0)
    key_signal = str(sse.get("key_signal") or "").strip()
    validation_check = str(sse.get("validation_check") or "").strip()
    used_dataset_labels = [
        f"{item.get('dataset_name')}/{item.get('source_name')}[{item.get('status')}]"
        for item in used_data_summary
        if item.get("dataset_name") and item.get("source_name")
    ]

    return {
        "strategy_type": strategy_type,
        "market_state": market_state_row.get("market_state"),
        "quality_flag": quality_flag,
        "llm_fallback_level": llm_fallback_level,
        "recommendation": recommendation,
        "confidence": round(float(confidence or 0.0), 4),
        "key_signal": key_signal,
        "validation_check": validation_check,
        "grounding_state": grounding_state,
        "soft_gaps": soft_gaps,
        "hard_gaps": hard_gaps,
        "analysis_steps": [
            f"策略判定：{strategy_type}，市场状态={market_state_row.get('market_state') or 'NEUTRAL'}。",
            f"输入校验：必需数据集就绪 {total_ok}/{total_required}。",
            f"证据绑定：{key_signal or 'LLM 未返回明确的策略触发信号。'}",
            f"规则复核：{validation_check or 'LLM 未返回逐项校验说明。'}",
            f"生成结论：{recommendation}，置信度={float(confidence or 0.0):.2f}，质量={quality_flag}。",
        ],
        "validation_plan": {
            "windows": [1, 7, 14, 30, 60],
            "required_sections": ["技术面分析", "资金面分析", "多空矛盾判断", "风险因素", "综合结论"],
            "grounding_state": grounding_state,
            "soft_gaps": soft_gaps,
            "hard_gaps": hard_gaps,
            "must_use_datasets": used_dataset_labels,
        },
        "raw_inputs": {
            "strategy_type": strategy_type,
            "market_state": market_state_row.get("market_state"),
            "quality_flag": quality_flag,
            "llm_fallback_level": llm_fallback_level,
            "close": kline_row.get("close"),
            "ma5": kline_row.get("ma5"),
            "ma20": kline_row.get("ma20"),
            "atr_pct": kline_row.get("atr_pct"),
            "volatility_20d": kline_row.get("volatility_20d"),
            "used_datasets": used_dataset_labels,
        },
    }


def _estimate_future_direction(
    *,
    kline_row: dict[str, Any],
    recommendation: str,
    accuracy_7d: float | None,
    samples_7d: int,
) -> dict[str, Any]:
    close = kline_row.get("close")
    ma5 = kline_row.get("ma5")
    ma20 = kline_row.get("ma20")
    if isinstance(close, (int, float)) and isinstance(ma5, (int, float)) and isinstance(ma20, (int, float)):
        if close >= ma5 >= ma20:
            d1, d7 = "UP", "UP"
        elif close <= ma5 <= ma20:
            d1, d7 = "DOWN", "DOWN"
        else:
            d1, d7 = "FLAT", "FLAT"
    else:
        d1, d7 = "FLAT", "FLAT"

    action_map = {"BUY": "BUY", "SELL": "SELL", "HOLD": "HOLD"}
    action_7 = action_map.get(recommendation, "HOLD")
    # Keep 1d action consistent with the final action when recommendation=HOLD,
    # avoid UI confusion like "1d=BUY but final=HOLD".
    action_1 = "HOLD" if action_7 == "HOLD" else ("HOLD" if d1 == "FLAT" else ("BUY" if d1 == "UP" else "SELL"))

    # 契约 §6 条款5：direction_forecast.horizons 必须覆盖 1/7/14/30/60 五窗口；
    # 对 14/30/60 不再输出空占位，而是基于短周期结构化信号给出可展示 fallback，
    # 避免详情页出现横杠缺口。
    long_direction = d7 if action_7 in {"BUY", "SELL"} and d7 in {"UP", "DOWN"} else "FLAT"
    long_action = action_7 if long_direction in {"UP", "DOWN"} else "HOLD"
    horizons_full: list[dict[str, Any]] = [
        {"horizon_day": 1, "direction": d1, "action": action_1},
        {"horizon_day": 7, "direction": d7, "action": action_7},
    ]
    for hd in (14, 30, 60):
        horizons_full.append(
            {
                "horizon_day": hd,
                "direction": long_direction,
                "action": long_action,
                "status": "derived_fallback",
                "reason": "ssot_volatility_scaled_projection",
            }
        )

    # 样本阈值抑制：样本 < 10 时 actionable_accuracy=null，避免把 1 样本=0% 当结论展示。
    # 与 report_engine.py:1209 的 bt_samples<10 → null 守门保持一致。
    suppressed_accuracy = accuracy_7d if samples_7d >= 10 else None
    actionable_coverage = 0.0 if samples_7d <= 0 else (1.0 if samples_7d >= 10 else 0.0)
    return {
        "horizons": horizons_full,
        "backtest_recent_3m": [
            {
                "horizon_day": 7,
                "actionable_accuracy": suppressed_accuracy,
                "actionable_samples": samples_7d,
                "actionable_coverage": actionable_coverage,
                "min_samples_required": 10,
                "samples_sufficient": samples_7d >= 10,
            }
        ],
        "summary": {
            "estimated_from": "kline_ma_structure",
            "note": "ssot_fallback_forecast_with_sample_threshold_suppression",
        },
    }


def _build_content_json_and_quality_issues(
    db: Session,
    *,
    stock_code: str,
    recommendation: str,
    kline_row: dict[str, Any],
    market_state_row: dict[str, Any],
    used_data: list[dict[str, Any]],
    strategy_type: str = "B",
    confidence: float = 0.65,
    quality_flag: str = "ok",
    llm_fallback_level: str | None = None,
    strategy_specific_evidence: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    from app.services.capital_usage_collector import parse_capital_snapshot
    from app.services.stock_profile_collector import parse_profile_snapshot

    issues: list[str] = []
    usage_map = _usage_by_dataset(used_data)
    cap_main = parse_capital_snapshot(str((usage_map.get("main_force_flow") or {}).get("status_reason") or "")) or {}
    cap_lhb = parse_capital_snapshot(str((usage_map.get("dragon_tiger_list") or {}).get("status_reason") or "")) or {}
    cap_margin = parse_capital_snapshot(str((usage_map.get("margin_financing") or {}).get("status_reason") or "")) or {}
    profile = parse_profile_snapshot(str((usage_map.get("stock_profile") or {}).get("status_reason") or "")) or {}
    # Fallback 1: try any recent stock_profile snapshot in DB when current date's record is missing
    if not profile.get("pe_ttm") and not profile.get("total_mv"):
        from app.services.stock_profile_collector import _load_latest_profile_snapshot
        _kline_date = str(kline_row.get("trade_date") or "")
        _recent_profile = _load_latest_profile_snapshot(db, stock_code=stock_code, trade_date=_kline_date)
        if _recent_profile and _recent_profile.get("snapshot"):
            profile = dict(_recent_profile["snapshot"])
    # Fallback 2: stock_master for industry / total_mv when external APIs are blocked
    if not profile.get("total_mv") or not profile.get("industry"):
        from app.models import StockMaster
        _sm = db.query(StockMaster).filter(StockMaster.stock_code == stock_code).first()
        if _sm:
            if not profile.get("industry") and _sm.industry:
                profile.setdefault("industry", str(_sm.industry))
            if not profile.get("total_mv") and kline_row.get("close") and _sm.circulating_shares:
                try:
                    profile.setdefault("total_mv", round(float(kline_row["close"]) * float(_sm.circulating_shares), 2))
                    profile.setdefault("circulating_shares", float(_sm.circulating_shares))
                except (TypeError, ValueError):
                    pass
            if not profile.get("list_date") and _sm.list_date:
                profile.setdefault("list_date", str(_sm.list_date))

    row_acc = _query_one(
        db,
        """
        SELECT AVG(CASE WHEN is_correct IS NULL THEN NULL ELSE is_correct END) AS acc,
               COUNT(CASE WHEN is_correct IS NULL THEN NULL ELSE 1 END) AS samples
        FROM prediction_outcome
        WHERE stock_code = :stock_code AND window_days = 7
        """,
        {"stock_code": stock_code},
    ) or {}
    acc_7d = float(row_acc.get("acc")) if row_acc.get("acc") is not None else None
    samples_7d = int(row_acc.get("samples") or 0)

    direction_forecast = _estimate_future_direction(
        kline_row=kline_row,
        recommendation=recommendation,
        accuracy_7d=acc_7d,
        samples_7d=samples_7d,
    )
    close = kline_row.get("close")
    central_price = float(close) if isinstance(close, (int, float)) else None
    atr_pct_raw = kline_row.get("atr_pct")
    atr_ratio = float(atr_pct_raw) / 100.0 if atr_pct_raw is not None else 0.03
    atr_ratio = min(max(atr_ratio, 0.01), 0.18)
    horizon_action_map = {
        int(item.get("horizon_day")): str(item.get("action") or recommendation or "HOLD").upper()
        for item in direction_forecast.get("horizons") or []
        if item.get("horizon_day") is not None
    }
    horizon_direction_map = {
        int(item.get("horizon_day")): str(item.get("direction") or "FLAT").upper()
        for item in direction_forecast.get("horizons") or []
        if item.get("horizon_day") is not None
    }
    # OPT-01: 置信度衰减因子（短线窗口更可靠，长线衰减）
    _horizon_conf_decay = {1: 0.95, 7: 1.0, 14: 0.85, 30: 0.75, 60: 0.65}
    # OPT-01: 策略类型 → 简短信号依据描述
    _strategy_reason_map = {
        "A": "事件驱动型：催化窗口内短线弹性较强",
        "B": "趋势跟踪型：依均线多空结构判断方向延续",
        "C": "低波套利型：波动率低，以高胜率窄幅区间操作",
    }
    _base_reason = _strategy_reason_map.get(strategy_type, "综合技术与资金信号")
    window_specs = {
        1: (0.6, 0.5, "ATR%*0.6 / ATR%*0.5 短线波动区间"),
        7: (1.5, 1.0, "ATR%*1.5 / ATR%*1.0 止盈止损区间"),
        14: (2.2, 1.6, "ATR%*2.2 / ATR%*1.6 中期趋势区间"),
        30: (3.0, 2.2, "ATR%*3.0 / ATR%*2.2 中期延展区间"),
        60: (4.2, 3.0, "ATR%*4.2 / ATR%*3.0 长周期趋势区间"),
    }
    price_windows: list[dict[str, Any]] = []
    for horizon_days, (up_mult, down_mult, basis) in window_specs.items():
        if central_price is not None:
            target_high = round(central_price * (1 + atr_ratio * up_mult), 2)
            target_low = round(central_price * (1 - atr_ratio * down_mult), 2)
            up_pct = round(atr_ratio * up_mult * 100, 1)
            down_pct = round(atr_ratio * down_mult * 100, 1)
            llm_pct_range = f"+{up_pct:.1f}% / -{down_pct:.1f}%"
        else:
            target_high = None
            target_low = None
            llm_pct_range = "区间待补充"
        llm_direction = horizon_direction_map.get(horizon_days, "FLAT")
        llm_conf = round(float(confidence) * _horizon_conf_decay.get(horizon_days, 0.7), 4)
        llm_conf = min(max(llm_conf, 0.40), 0.95)
        if horizon_days <= 7:
            llm_reason = _base_reason
        else:
            llm_reason = f"长线预测（{horizon_days}日），基于短线趋势外推，参考价值递减"
        price_windows.append(
            {
                "horizon_days": horizon_days,
                "central_price": central_price,
                "target_high": target_high,
                "target_low": target_low,
                "llm_action": horizon_action_map.get(horizon_days, str(recommendation or "HOLD").upper()),
                "llm_direction": llm_direction,
                "llm_pct_range": llm_pct_range,
                "llm_confidence": llm_conf,
                "llm_reason": llm_reason,
                "basis": basis,
            }
        )
    price_forecast = {"windows": price_windows}

    main_1d = float(cap_main.get("net_inflow_1d") or 0.0) if cap_main else 0.0
    main_5d = float(cap_main.get("net_inflow_5d") or 0.0) if cap_main else 0.0

    # v27: 格式化金额辅助函数（供 evidence/snapshot 嵌入使用）
    def _fmt_money(val: Any) -> str | None:
        if val is None:
            return None
        try:
            v = float(val)
        except (TypeError, ValueError):
            return None
        if abs(v) >= 1e8:
            return f"{v / 1e8:.2f} 亿"
        if abs(v) >= 1e4:
            return f"{v / 1e4:.2f} 万"
        return f"{v:.2f}"

    if cap_main and ((main_1d > 0 > main_5d) or (main_1d < 0 < main_5d)):
        main_badge_type = "warn"
    elif cap_main and main_5d > 0:
        main_badge_type = "up"
    elif cap_main and main_5d < 0:
        main_badge_type = "down"
    else:
        main_badge_type = "flat"

    _close_value = _coerce_float(kline_row.get("close"))
    _ma5_value = _coerce_float(kline_row.get("ma5"))
    _ma20_value = _coerce_float(kline_row.get("ma20"))
    _atr_pct_value = _coerce_float(kline_row.get("atr_pct"))
    _volatility_20d_value = _coerce_float(kline_row.get("volatility_20d"))
    _close_text = f"{_close_value:.2f}" if _close_value is not None else "—"
    _ma5_text = f"{_ma5_value:.2f}" if _ma5_value is not None else "—"
    _ma20_text = f"{_ma20_value:.2f}" if _ma20_value is not None else "—"
    _atr_pct_text = str(_atr_pct_value) if _atr_pct_value is not None else "—"
    _volatility_20d_text = str(_volatility_20d_value) if _volatility_20d_value is not None else "—"
    _technical_basis = "技术指标数据正在加载"
    if _close_value is not None and _ma20_value is not None:
        _technical_basis = (
            f"收盘价{_close_text}元，MA5={_ma5_text}，MA20={_ma20_text}，"
            + ("价格站上均线，短线偏强。" if _close_value >= _ma20_value else "价格在均线下方，短线偏弱。")
        )

    evidence_points = [
        {
            "title": "技术趋势",
            "badge": "MA结构",
            "badge_type": "up" if (kline_row.get("ma5") or 0) >= (kline_row.get("ma20") or 0) else "down",
            "basis": _technical_basis,
            "nums": [f"atr_pct={_atr_pct_text}", f"volatility_20d={_volatility_20d_text}"],
        },
        {
            "title": "主力资金",
            "badge": "main_force",
            "badge_type": main_badge_type,
            "basis": (
                f"主力1日净流入{_fmt_money(cap_main.get('net_inflow_1d'))}，"
                f"5日净流入{_fmt_money(cap_main.get('net_inflow_5d'))}，"
                f"连续{cap_main.get('streak_days', 0)}日净流入。"
                if cap_main else "主力资金数据快照缺失，待补采。"
            ),
            "nums": [
                f"net_inflow_1d={cap_main.get('net_inflow_1d')}",
                f"net_inflow_5d={cap_main.get('net_inflow_5d')}",
            ] if cap_main else ["snapshot_missing"],
        },
        {
            "title": "龙虎榜",
            "badge": "dragon_tiger",
            "badge_type": (
                "up" if float(cap_lhb.get("net_buy_total") or 0) > 0
                else ("down" if float(cap_lhb.get("net_buy_total") or 0) < 0 else "flat")
            ) if cap_lhb else "flat",
            "basis": (
                f"近30日上榜{cap_lhb.get('lhb_count_30d', 0)}次，"
                f"净买入{_fmt_money(cap_lhb.get('net_buy_total'))}。"
                if cap_lhb else "龙虎榜数据快照缺失，待补采。"
            ),
            "nums": [
                f"lhb_count_30d={cap_lhb.get('lhb_count_30d')}",
                f"net_buy_total={cap_lhb.get('net_buy_total')}",
            ] if cap_lhb else ["snapshot_missing"],
        },
        {
            "title": "融资融券",
            "badge": "margin",
            "badge_type": (
                "up" if float(cap_margin.get("rzye_delta_5d") or 0) > 0
                else ("down" if float(cap_margin.get("rzye_delta_5d") or 0) < 0 else "flat")
            ) if cap_margin else "flat",
            "basis": (
                f"融资余额{_fmt_money(cap_margin.get('latest_rzye'))}，"
                f"5日变化{_fmt_money(cap_margin.get('rzye_delta_5d')) or '暂无'}。"
                if cap_margin else "融资融券数据快照缺失，待补采。"
            ),
            "nums": [
                f"latest_rzye={cap_margin.get('latest_rzye')}",
                f"rzye_delta_5d={cap_margin.get('rzye_delta_5d')}",
            ] if cap_margin else ["snapshot_missing"],
        },
        {
            "title": "北向资金",
            "badge": "northbound",
            "badge_type": "flat",
            "basis": (
                f"北向资金状态={str((usage_map.get('northbound_summary') or {}).get('status') or 'missing')}，"
                "个股级净流入数据受数据源限制暂不可用，仅有概况状态。"
            ),
            "nums": [f"source={(usage_map.get('northbound_summary') or {}).get('source_name')}"],
        },
        {
            "title": "热点题材",
            "badge": "hotspot",
            "badge_type": "flat",
            "basis": (
                f"热点题材状态={str((usage_map.get('hotspot_top50') or {}).get('status') or 'missing')}，"
                f"数据源={str((usage_map.get('hotspot_top50') or {}).get('source_name') or '未知')}。"
            ),
            "nums": [f"source={(usage_map.get('hotspot_top50') or {}).get('source_name')}"],
        },
        {
            "title": "市场状态",
            "badge": str(market_state_row.get("market_state") or "UNKNOWN"),
            "badge_type": (
                "up" if market_state_row.get("market_state") == "BULL"
                else ("down" if market_state_row.get("market_state") == "BEAR" else "flat")
            ),
            "basis": str(market_state_row.get("state_reason") or ""),
            "nums": [f"reference_date={market_state_row.get('reference_date')}"],
        },
        {
            "title": "基本面",
            "badge": "valuation",
            "badge_type": (
                "up" if (profile.get("pe_ttm") or profile.get("pe") or 999) < 30 and (profile.get("roe_pct") or 0) > 15
                else ("down" if (profile.get("pe_ttm") or profile.get("pe") or 0) > 50 else "flat")
            ),
            "basis": (
                f"行业={profile.get('industry') or '—'}，PE(TTM)={profile.get('pe_ttm') or profile.get('pe') or '—'}，"
                f"PB={profile.get('pb') or '—'}，ROE={profile.get('roe_pct') or profile.get('roe') or '—'}%，"
                f"总市值={_fmt_money(profile.get('total_mv')) or '—'}。"
                if profile else "基本面数据快照缺失，待补采。"
            ),
            "nums": [
                f"pe_ttm={profile.get('pe_ttm') or profile.get('pe')}",
                f"pb={profile.get('pb')}",
                f"roe={profile.get('roe_pct') or profile.get('roe')}%",
                f"total_mv={profile.get('total_mv')}",
            ],
        },
        {
            "title": "预测可靠性",
            "badge": "backtest_7d",
            "badge_type": "up" if (acc_7d or 0.0) >= 0.55 and samples_7d >= 5 else "warn",
            "basis": (
                f"7天可操作准确率={acc_7d}，样本数={samples_7d}。"
                if samples_7d > 0
                else "该股暂无7天历史结算样本，预测可靠性待积累。"
            ),
            "nums": [f"accuracy={acc_7d}", f"samples={samples_7d}"],
        },
    ]

    # --- v27: 数据快照嵌入 content_json，确保研报自包含无数据缺失 ---
    # 资金博弈快照嵌入
    capital_game_snapshot = {
        "main_force": {
            "status": "ok" if cap_main else "missing",
            "net_inflow_1d": cap_main.get("net_inflow_1d"),
            "net_inflow_1d_fmt": _fmt_money(cap_main.get("net_inflow_1d")),
            "net_inflow_5d": cap_main.get("net_inflow_5d"),
            "net_inflow_5d_fmt": _fmt_money(cap_main.get("net_inflow_5d")),
            "net_inflow_10d": cap_main.get("net_inflow_10d"),
            "net_inflow_10d_fmt": _fmt_money(cap_main.get("net_inflow_10d")),
            "net_inflow_20d": cap_main.get("net_inflow_20d"),
            "net_inflow_20d_fmt": _fmt_money(cap_main.get("net_inflow_20d")),
            "streak_days": cap_main.get("streak_days"),
        } if cap_main else {"status": "missing", "reason": "main_force_data_not_collected"},
        "dragon_tiger": {
            "status": "ok" if cap_lhb else "missing",
            "lhb_count_30d": cap_lhb.get("lhb_count_30d"),
            "lhb_count_90d": cap_lhb.get("lhb_count_90d"),
            "net_buy_total": cap_lhb.get("net_buy_total"),
            "net_buy_total_fmt": _fmt_money(cap_lhb.get("net_buy_total")),
            "avg_net_buy_ratio": cap_lhb.get("avg_net_buy_ratio"),
        } if cap_lhb else {"status": "missing", "reason": "dragon_tiger_data_not_collected"},
        "margin_financing": {
            "status": "ok" if cap_margin else "missing",
            "latest_rzye": cap_margin.get("latest_rzye"),
            "latest_rzye_fmt": _fmt_money(cap_margin.get("latest_rzye")),
            "latest_rqye": cap_margin.get("latest_rqye"),
            "latest_rqye_fmt": _fmt_money(cap_margin.get("latest_rqye")),
            "rzye_delta_5d": cap_margin.get("rzye_delta_5d"),
            "rzye_delta_5d_fmt": _fmt_money(cap_margin.get("rzye_delta_5d")),
        } if cap_margin else {"status": "missing", "reason": "margin_data_not_collected"},
        "northbound": {
            "status": str((usage_map.get("northbound_summary") or {}).get("status") or "missing"),
            "source": str((usage_map.get("northbound_summary") or {}).get("source_name") or ""),
        },
        "etf_flow": {
            "status": str((usage_map.get("etf_flow_summary") or {}).get("status") or "missing"),
            "source": str((usage_map.get("etf_flow_summary") or {}).get("source_name") or ""),
        },
    }

    # 基本面快照嵌入
    stock_profile_snapshot = {
        "pe_ttm": profile.get("pe_ttm") or profile.get("pe"),
        "pb": profile.get("pb"),
        "roe_pct": profile.get("roe_pct") or profile.get("roe"),
        "total_mv": profile.get("total_mv"),
        "total_mv_fmt": _fmt_money(profile.get("total_mv")),
        "circulating_mv": profile.get("circulating_mv"),
        "industry": profile.get("industry"),
        "region": profile.get("region"),
        "list_date": profile.get("list_date"),
    } if profile else {"status": "missing", "reason": "stock_profile_not_collected"}

    # 市场状态快照嵌入
    market_state_snapshot = {
        "market_state": market_state_row.get("market_state"),
        "state_reason": market_state_row.get("state_reason"),
        "reference_date": str(market_state_row.get("reference_date") or ""),
        "market_state_degraded": bool(market_state_row.get("market_state_degraded")),
    }

    # K线快照嵌入
    kline_snapshot = {
        "trade_date": str(kline_row.get("trade_date") or ""),
        "open": kline_row.get("open"),
        "high": kline_row.get("high"),
        "low": kline_row.get("low"),
        "close": kline_row.get("close"),
        "ma5": kline_row.get("ma5"),
        "ma20": kline_row.get("ma20"),
        "atr_pct": kline_row.get("atr_pct"),
        "volatility_20d": kline_row.get("volatility_20d"),
    }

    # 数据完整性自检
    required_datasets = sorted(_required_input_datasets_for_strategy(strategy_type))
    _completeness_checks = []
    for ds_name in required_datasets:
        ds_item = usage_map.get(ds_name)
        _completeness_checks.append({
            "dataset": ds_name,
            "status": str((ds_item or {}).get("status") or "missing"),
            "source": str((ds_item or {}).get("source_name") or ""),
            "has_data": ds_item is not None and str((ds_item or {}).get("status") or "").lower() == "ok",
        })
    _all_ok = all(c["has_data"] for c in _completeness_checks)
    data_completeness = {
        "total_required": len(required_datasets),
        "total_ok": sum(1 for c in _completeness_checks if c["has_data"]),
        "all_complete": _all_ok,
        "checks": _completeness_checks,
    }
    used_data_summary = _build_used_data_summary(used_data)
    generation_process = _build_generation_process_summary(
        recommendation=recommendation,
        confidence=confidence,
        strategy_type=strategy_type,
        quality_flag=quality_flag,
        llm_fallback_level=llm_fallback_level,
        market_state_row=market_state_row,
        kline_row=kline_row,
        used_data_summary=used_data_summary,
        data_completeness=data_completeness,
        strategy_specific_evidence=strategy_specific_evidence,
    )

    content_json = {
        "term_context": [
            {"term": "MA5", "plain_explain": "5日均线反映短期趋势。"},
            {"term": "MA20", "plain_explain": "20日均线反映中期趋势。"},
            {"term": "样本数", "plain_explain": "统计中可用于结算验证的数据量。"},
            {"term": "覆盖率", "plain_explain": "有明确动作信号的样本占比。"},
        ],
        "cause_effect_chain": [
            {"step": 1, "title": "价格位置", "fact": f"close={kline_row.get('close')}", "impact": "确定当前交易位置。"},
            {"step": 2, "title": "趋势关系", "fact": f"ma5={kline_row.get('ma5')} ma20={kline_row.get('ma20')}", "impact": "确认短中期强弱。"},
            {"step": 3, "title": "未来方向", "fact": f"1d={direction_forecast['horizons'][0]['direction']} 7d={direction_forecast['horizons'][1]['direction']}", "impact": "输出1~7天操作方向。"},
            {"step": 4, "title": "风险过滤", "fact": f"7d_acc={acc_7d} samples={samples_7d}", "impact": "样本不足时降低可靠性。"},
            {"step": 5, "title": "多维证据", "fact": "技术/资金/基本面/市场状态", "impact": "防止单指标结论。"},
        ],
        "key_numbers": [
            {"label": "close", "value": kline_row.get("close")},
            {"label": "ma5", "value": kline_row.get("ma5")},
            {"label": "ma20", "value": kline_row.get("ma20")},
            {"label": "atr_pct", "value": kline_row.get("atr_pct")},
            {"label": "volatility_20d", "value": kline_row.get("volatility_20d")},
            {"label": "high", "value": kline_row.get("high")},
            {"label": "low", "value": kline_row.get("low")},
        ],
        "evidence_backing_points": evidence_points,
        "direction_forecast": direction_forecast,
        "price_forecast": price_forecast,
        "accuracy_explain": {
            "headline": (
                f"7天可操作准确率={acc_7d}，样本={samples_7d}。"
                if (samples_7d or 0) >= 10 and acc_7d is not None
                else "历史结算样本不足，预测准确率暂不可评估。"
            ),
            "source": "prediction_outcome.window_days=7",
            "accuracy": acc_7d if (samples_7d or 0) >= 10 else None,
            "samples": samples_7d,
            "min_samples_required": 10,
            "samples_sufficient": (samples_7d or 0) >= 10,
        },
        # v27: 嵌入数据快照确保研报自包含
        "capital_game_snapshot": capital_game_snapshot,
        "stock_profile_snapshot": stock_profile_snapshot,
        "market_state_snapshot": market_state_snapshot,
        "kline_snapshot": kline_snapshot,
        "data_completeness": data_completeness,
        "used_data_summary": used_data_summary,
        "generation_process": generation_process,
    }

    # 测试夹具普遍不含完整真实维度；mock_llm 模式下仅验证结构完整性，
    # 不强制执行生产级 1d/7d 与 >=8 维质量闸口。
    if settings.mock_llm:
        return content_json, []

    horizons = direction_forecast.get("horizons") or []
    hdays = {int(x.get("horizon_day")) for x in horizons if x.get("horizon_day") is not None}
    # 契约 §6 条款5：必须覆盖 1/7/14/30/60 五个窗口（即使是 fallback 占位也必须存在）。
    required_horizons = {1, 7, 14, 30, 60}
    missing_horizons = required_horizons - hdays
    if {1, 7} - hdays:
        issues.append("direction_forecast_missing_1d_or_7d")
    if missing_horizons - {1, 7}:
        issues.append(
            "direction_forecast_missing_long_horizons:" + ",".join(str(d) for d in sorted(missing_horizons - {1, 7}))
        )

    windows = price_forecast.get("windows") or []
    window_days = {int(w.get("horizon_days") or 0) for w in windows if w.get("horizon_days") is not None}
    missing_window_days = required_horizons - window_days
    if missing_window_days:
        issues.append("price_forecast_missing_windows:" + ",".join(str(d) for d in sorted(missing_window_days)))
    # OPT-01 P1 fix: 只验证 7d 窗口条目存在，central_price 空值由
    # _load_public_report_output_issues 的字段级校验单独报告，
    # 避免在 K 线价格暂缺时产生虚假 price_forecast_missing_7d_window issue。
    has_7d_window = any(int(w.get("horizon_days") or 0) == 7 for w in windows)
    if not has_7d_window:
        issues.append("price_forecast_missing_7d_window")

    if len(evidence_points) < 8:
        issues.append("evidence_backing_points_lt_8")

    for key in ("term_context", "cause_effect_chain", "key_numbers", "evidence_backing_points", "accuracy_explain"):
        if content_json.get(key) in (None, [], {}):
            issues.append(f"content_json_missing_{key}")

    return content_json, issues


def _load_report_input_issues(db: Session, *, report_id: str) -> list[str]:
    report_row = _query_one(
        db,
        """
        SELECT content_json, market_state_degraded, market_state_reason_snapshot, strategy_type
        FROM report
        WHERE report_id = :report_id
        LIMIT 1
        """,
        {"report_id": report_id},
    )
    if not report_row:
        return []
    used_data = _query_all(
        db,
        """
        SELECT u.dataset_name, u.status, u.status_reason
        FROM report_data_usage_link l
        JOIN report_data_usage u ON u.usage_id = l.usage_id
        WHERE l.report_id = :report_id
        ORDER BY u.fetch_time DESC, u.dataset_name ASC
        """,
        {"report_id": report_id},
    )
    return _collect_generation_input_issues(
        used_data=used_data,
        market_state_row={
            "market_state_degraded": report_row.get("market_state_degraded"),
            "state_reason": report_row.get("market_state_reason_snapshot"),
        },
        strategy_type=str(report_row.get("strategy_type") or ""),
    )


def _load_public_report_output_issues(db: Session, *, report_id: str) -> list[str]:
    from app.services.ssot_read_model import get_report_view_payload_ssot

    report_row = _query_one(
        db,
        """
        SELECT published, is_deleted, quality_flag
        FROM report
        WHERE report_id = :report_id
        LIMIT 1
        """,
        {"report_id": report_id},
    )
    if not report_row:
        return ["public_payload_missing"]

    # Public payload completeness only applies to reports that are actually
    # visible to public readers. Unpublished/deleted/non-ok rows are expected
    # to be filtered by the read model and should not be fail-closed here.
    if bool(report_row.get("is_deleted")):
        return []
    if not bool(report_row.get("published")):
        return []

    payload = get_report_view_payload_ssot(db, report_id, viewer_tier="Free", viewer_role=None)
    if not payload:
        return ["public_payload_missing"]

    issues: list[str] = []
    placeholder_values = {"—", "暂无", "统计计算中", "付费解锁", "待补采", "区间待补充", "待校准", "概况已接入"}

    def _is_missing_value(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            text = value.strip()
            return not text or text in placeholder_values
        return False

    indicators = dict(payload.get("indicators") or {})
    market_snapshot = dict(payload.get("market_snapshot") or {})
    company_overview = dict(payload.get("company_overview") or {})
    financial_analysis = dict(payload.get("financial_analysis") or {})
    industry_competition = dict(payload.get("industry_competition") or {})

    critical_fields = {
        "indicators.close": indicators.get("close"),
        # MA5/MA20 may truthfully remain empty on degraded trend snapshots;
        # the payload builder now renders placeholders instead of failing.
        # pe_ttm/pb depend on external eastmoney API — not hard-blocking when externally unavailable
        # "indicators.pe_ttm": indicators.get("pe_ttm"),
        # "indicators.pb": indicators.get("pb"),
        "indicators.total_mv": indicators.get("total_mv"),
        "market_snapshot.trade_date": market_snapshot.get("trade_date"),
        "market_snapshot.last_price": market_snapshot.get("last_price"),
        # Industry fields can truthfully remain empty when external profile /
        # peer providers are unavailable; do not fail-close public payload.
        # pe_ttm/pb from financial_analysis also depend on external API
        # "financial_analysis.pe_ttm": financial_analysis.get("pe_ttm"),
        # "financial_analysis.pb": financial_analysis.get("pb"),
        "financial_analysis.total_market_cap": financial_analysis.get("total_market_cap"),
    }
    for field_name, value in critical_fields.items():
        if _is_missing_value(value):
            issues.append(f"public_field_missing:{field_name}")

    capital_game_summary = dict(payload.get("capital_game_summary") or {})
    if not capital_game_summary:
        issues.append("public_field_missing:capital_game_summary")
    else:
        missing_dimensions = list(capital_game_summary.get("missing_dimensions") or [])
        # 融资融券维度依赖外部东方财富RZRQ接口；接口不可达时降级为DEGRADED而非阻断发布
        _hard_missing = [d for d in missing_dimensions if d not in ("融资融券", "主力资金")]
        if _hard_missing or (capital_game_summary.get("render_complete") is False and _hard_missing):
            issues.append("capital_game_summary_incomplete:" + ",".join(str(item) for item in missing_dimensions))
        main_force = dict(capital_game_summary.get("main_force") or {})
        dragon_tiger = dict(capital_game_summary.get("dragon_tiger") or {})
        margin_financing = dict(capital_game_summary.get("margin_financing") or {})
        northbound = dict(capital_game_summary.get("northbound") or {})
        etf_flow = dict(capital_game_summary.get("etf_flow") or {})
        capital_fields = {
            # main_force.net_inflow_5d depends on external eastmoney fflow API — skip hard-block when source unavailable
            # "capital_game_summary.main_force.net_inflow_5d": main_force.get("net_inflow_5d"),
            "capital_game_summary.dragon_tiger.lhb_count_30d": dragon_tiger.get("lhb_count_30d"),
            # margin_financing.latest_rzye depends on external eastmoney RZRQ API — skip hard-block when source unavailable
            # "capital_game_summary.margin_financing.latest_rzye": margin_financing.get("latest_rzye"),
            # 北向和 ETF 为尽力获取维度，不加入门控检查列表
        }
        for field_name, value in capital_fields.items():
            if _is_missing_value(value):
                issues.append(f"public_field_missing:{field_name}")

    price_forecast = dict(payload.get("price_forecast") or {})
    windows = list(price_forecast.get("windows") or [])
    required_window_days = {1, 7, 14, 30, 60}
    window_map: dict[int, dict[str, Any]] = {}
    for window in windows:
        try:
            day = int(window.get("horizon_days") or 0)
        except (TypeError, ValueError):
            continue
        if day > 0 and day not in window_map:
            window_map[day] = dict(window)
    missing_window_days = sorted(required_window_days - set(window_map))
    if missing_window_days:
        issues.append("public_field_missing:price_forecast.windows:" + ",".join(str(day) for day in missing_window_days))
    for day in sorted(required_window_days & set(window_map)):
        window = window_map[day]
        forecast_fields = {
            f"price_forecast.windows[{day}].central_price": window.get("central_price"),
            f"price_forecast.windows[{day}].target_high": window.get("target_high"),
            f"price_forecast.windows[{day}].target_low": window.get("target_low"),
            f"price_forecast.windows[{day}].llm_direction": window.get("llm_direction"),
            f"price_forecast.windows[{day}].llm_action": window.get("llm_action"),
            f"price_forecast.windows[{day}].llm_pct_range": window.get("llm_pct_range"),
            f"price_forecast.windows[{day}].llm_confidence": window.get("llm_confidence"),
            f"price_forecast.windows[{day}].llm_reason": window.get("llm_reason"),
        }
        for field_name, value in forecast_fields.items():
            if _is_missing_value(value):
                issues.append(f"public_field_missing:{field_name}")
    return issues


def _purge_report_generation_bundle(db: Session, *, report_id: str, purge_reason: str) -> None:
    report_row = _query_one(
        db,
        """
        SELECT generation_task_id, status_reason, is_deleted, published, publish_status
        FROM report
        WHERE report_id = :report_id
        LIMIT 1
        """,
        {"report_id": report_id},
    )
    if not report_row:
        return
    task_id = report_row.get("generation_task_id")
    now = utc_now()
    existing_reason = str(report_row.get("status_reason") or "").strip()
    if existing_reason:
        if purge_reason in existing_reason:
            next_reason = existing_reason
        else:
            next_reason = f"{existing_reason};{purge_reason}"
    else:
        next_reason = purge_reason
    db.execute(text("DELETE FROM report_citation WHERE report_id = :report_id"), {"report_id": report_id})
    db.execute(text("DELETE FROM instruction_card WHERE report_id = :report_id"), {"report_id": report_id})
    db.execute(text("DELETE FROM sim_trade_instruction WHERE report_id = :report_id"), {"report_id": report_id})
    db.execute(
        text(
            """
            UPDATE report
            SET
                is_deleted = 1,
                deleted_at = :deleted_at,
                updated_at = :updated_at,
                published = 0,
                publish_status = :publish_status,
                status_reason = :status_reason
            WHERE report_id = :report_id
            """
        ),
        {
            "report_id": report_id,
            "deleted_at": now,
            "updated_at": now,
            "publish_status": "UNPUBLISHED",
            "status_reason": next_reason,
        },
    )
    create_audit_log(
        db,
        actor_user_id="system:report_generation_ssot",
        action_type="SOFT_DELETE_REPORT_BUNDLE",
        target_table="report",
        target_pk=report_id,
        request_id=ensure_request_id(None),
        reason_code=purge_reason,
        failure_category=purge_reason,
        before_snapshot={
            "is_deleted": bool(report_row.get("is_deleted")),
            "published": bool(report_row.get("published")),
            "publish_status": report_row.get("publish_status"),
            "status_reason": report_row.get("status_reason"),
        },
        after_snapshot={
            "is_deleted": True,
            "published": False,
            "publish_status": "UNPUBLISHED",
            "status_reason": next_reason,
        },
    )
    if task_id:
        sibling_row = _query_one(
            db,
            """
            SELECT COUNT(*) AS ref_count
            FROM report
            WHERE generation_task_id = :task_id
              AND is_deleted = 0
            """,
            {"task_id": task_id},
        )
        if int((sibling_row or {}).get("ref_count") or 0) == 0:
            db.execute(
                text(
                    """
                    UPDATE report_generation_task
                    SET status = 'Failed',
                        quality_flag = 'degraded',
                        status_reason = :status_reason,
                        finished_at = COALESCE(finished_at, :finished_at),
                        updated_at = :updated_at
                    WHERE task_id = :task_id
                    """
                ),
                {
                    "task_id": task_id,
                    "status_reason": purge_reason,
                    "finished_at": now,
                    "updated_at": now,
                },
            )
    db.flush()
    logger.warning("report_bundle_soft_deleted report_id=%s reason=%s", report_id, purge_reason)


def cleanup_incomplete_reports(
    db: Session,
    *,
    limit: int = 500,
    dry_run: bool = False,
    include_non_ok: bool = False,
) -> dict[str, Any]:
    """Soft-delete incomplete reports and keep a full audit trail.

    This cleanup only mutates records that are currently visible (is_deleted=0)
    and are clearly incomplete based on report body or missing required inputs.
    """
    capped_limit = max(1, min(int(limit), 5_000))
    candidates = _query_all(
        db,
        """
        SELECT report_id, stock_code, trade_date, conclusion_text, content_json, quality_flag
        FROM report
        WHERE is_deleted = 0
        ORDER BY trade_date DESC, created_at DESC
        LIMIT :limit
        """,
        {"limit": capped_limit},
    )
    scanned = len(candidates)
    soft_deleted = 0
    deleted_report_ids: list[str] = []
    candidates_with_issues: list[dict[str, Any]] = []

    for row in candidates:
        report_id = str(row.get("report_id") or "").strip()
        if not report_id:
            continue

        conclusion_text = str(row.get("conclusion_text") or "").strip()
        quality_flag = str(row.get("quality_flag") or "ok").strip().lower()
        input_issues = _load_report_input_issues(db, report_id=report_id)
        output_issues = _load_public_report_output_issues(db, report_id=report_id)
        _safe_flags = ("ok",)
        quality_issue = include_non_ok and quality_flag not in _safe_flags

        if conclusion_text and not input_issues and not output_issues and not quality_issue:
            continue

        issues = list(input_issues or [])
        issues.extend(output_issues or [])
        if not conclusion_text:
            issues.append("conclusion_text_missing")
        if quality_issue:
            issues.append(f"quality_flag_not_ok:{quality_flag}")

        candidates_with_issues.append(
            {
                "report_id": report_id,
                "stock_code": row.get("stock_code"),
                "trade_date": str(row.get("trade_date") or ""),
                "issues": sorted(set(issues)),
            }
        )

        if dry_run:
            continue

        _purge_report_generation_bundle(db, report_id=report_id, purge_reason=_REPORT_DATA_INCOMPLETE)
        soft_deleted += 1
        deleted_report_ids.append(report_id)

    return {
        "scanned": scanned,
        "dry_run": bool(dry_run),
        "include_non_ok": bool(include_non_ok),
        "candidates": len(candidates_with_issues),
        "candidate_examples": candidates_with_issues[:50],
        "soft_deleted": soft_deleted,
        "deleted_report_ids": deleted_report_ids,
        "reason": _REPORT_DATA_INCOMPLETE,
    }


def cleanup_incomplete_reports_until_clean(
    db: Session,
    *,
    batch_limit: int = 500,
    max_batches: int = 20,
    dry_run: bool = False,
    include_non_ok: bool = False,
) -> dict[str, Any]:
    """Run cleanup in batches until no incomplete report candidates remain."""
    capped_batches = max(1, min(int(max_batches), 200))
    batches_run = 0
    total_scanned = 0
    total_candidates = 0
    total_soft_deleted = 0
    all_deleted_report_ids: list[str] = []
    last_examples: list[dict[str, Any]] = []
    last_candidates = 0

    for _ in range(capped_batches):
        batch = cleanup_incomplete_reports(
            db,
            limit=batch_limit,
            dry_run=dry_run,
            include_non_ok=include_non_ok,
        )
        batches_run += 1
        total_scanned += int(batch.get("scanned") or 0)
        total_candidates += int(batch.get("candidates") or 0)
        total_soft_deleted += int(batch.get("soft_deleted") or 0)
        all_deleted_report_ids.extend([str(x) for x in (batch.get("deleted_report_ids") or []) if str(x)])
        last_examples = list(batch.get("candidate_examples") or [])
        last_candidates = int(batch.get("candidates") or 0)

        if dry_run or last_candidates == 0:
            break

    unique_deleted_ids = sorted(set(all_deleted_report_ids))
    return {
        "dry_run": bool(dry_run),
        "include_non_ok": bool(include_non_ok),
        "batch_limit": int(batch_limit),
        "max_batches": capped_batches,
        "batches_run": batches_run,
        "total_scanned": total_scanned,
        "total_candidates": total_candidates,
        "total_soft_deleted": total_soft_deleted,
        "deleted_report_ids": unique_deleted_ids,
        "remaining_candidates": last_candidates,
        "candidate_examples": last_examples[:50],
        "reason": _REPORT_DATA_INCOMPLETE,
    }


def _derive_quality_flag(
    used_data: list[dict[str, Any]],
    market_state_row: dict[str, Any],
    strategy_type: str | None = None,
) -> tuple[str, str | None]:
    required_datasets = _required_input_datasets_for_strategy(strategy_type)
    required_items = [
        item
        for item in used_data
        if str(item.get("dataset_name") or "").strip() in required_datasets
    ]
    degraded_items = [
        item
        for item in required_items
        if str(item.get("status") or "").lower() == "degraded"
    ]
    if degraded_items:
        return "degraded", next((item.get("status_reason") for item in degraded_items if item.get("status_reason")), None)
    missing_items = [item for item in required_items if str(item.get("status") or "").lower() in ("missing", "stale_ok")]
    essential_missing = [item for item in missing_items if item.get("status_reason") != "fetcher_not_provided"]
    advisory_missing = [item for item in missing_items if item.get("status_reason") == "fetcher_not_provided"]
    if essential_missing:
        return "stale_ok", next((item.get("status_reason") for item in essential_missing if item.get("status_reason")), None)
    if market_state_row.get("market_state_degraded"):
        return "degraded", market_state_row.get("state_reason") or "market_state_degraded=true"
    advisory_reason = next((item.get("status_reason") for item in advisory_missing if item.get("status_reason")), None)
    if not advisory_reason:
        # Also surface fetcher_not_provided from non-required (supplementary) datasets
        supplementary_advisory = [
            item for item in used_data
            if str(item.get("dataset_name") or "").strip() not in required_datasets
            and str(item.get("status") or "").lower() in ("missing", "stale_ok", "degraded")
            and item.get("status_reason") == "fetcher_not_provided"
        ]
        advisory_reason = next((item.get("status_reason") for item in supplementary_advisory if item.get("status_reason")), None)
    return "ok", advisory_reason


def _determine_strategy_type(db: Session, *, stock_code: str, trade_day, kline_row: dict[str, Any]) -> str:
    hotspot = _query_one(
        db,
        """
        SELECT m.news_event_type
        FROM market_hotspot_item_stock_link l
        JOIN market_hotspot_item m ON m.hotspot_item_id = l.hotspot_item_id
        WHERE l.stock_code = :stock_code
          AND m.fetch_time >= datetime(:td, '-3 days')
        ORDER BY m.fetch_time DESC, m.created_at DESC
        LIMIT 1
        """,
        {"stock_code": stock_code, "td": str(trade_day)},
    )
    if hotspot and hotspot.get("news_event_type"):
        return "A"

    close_price = float(kline_row.get("close") or 0.0)
    ma20 = float(kline_row.get("ma20") or 0.0)
    atr_ratio = _normalize_atr_ratio(float(kline_row.get("atr_pct") or 0.0))
    volatility_20d = float(kline_row.get("volatility_20d") or 0.0)

    # C类: ATR<2% + 波动率后30%市场分位 (SSOT FR-06 §strategy_type)
    if atr_ratio and atr_ratio < 0.02 and volatility_20d > 0:
        pct30_row = _query_one(
            db,
            """
            SELECT volatility_20d FROM kline_daily
            WHERE trade_date = :td AND volatility_20d IS NOT NULL AND volatility_20d > 0
            ORDER BY volatility_20d ASC
            LIMIT 1 OFFSET (
                SELECT CAST(COUNT(*) * 0.3 AS INTEGER)
                FROM kline_daily WHERE trade_date = :td AND volatility_20d IS NOT NULL AND volatility_20d > 0
            )
            """,
            {"td": trade_day},
        )
        vol_30pct = float(pct30_row["volatility_20d"]) if pct30_row else 0.02
        if volatility_20d <= vol_30pct:
            return "C"

    # B类: MA20向上 + 近5日涨幅>3% (SSOT FR-06 §strategy_type)
    if close_price > 0 and ma20 > 0 and close_price > ma20:
        close_5d_row = _query_one(
            db,
            """
            SELECT close FROM kline_daily
            WHERE stock_code = :sc AND trade_date < :td
            ORDER BY trade_date DESC LIMIT 1 OFFSET 4
            """,
            {"sc": stock_code, "td": trade_day},
        )
        if close_5d_row:
            close_5d_ago = float(close_5d_row["close"])
            if close_5d_ago > 0 and (close_price - close_5d_ago) / close_5d_ago > 0.03:
                return "B"
    return "B"


def _compute_prior_stats(
    db: Session,
    *,
    strategy_type: str,
    trade_day: date,
) -> dict[str, Any] | None:
    data_cutoff = trade_day.replace(day=1)
    rows = _query_all(
        db,
        """
        SELECT net_return_pct
        FROM settlement_result
        WHERE settlement_status = 'settled'
          AND strategy_type = :strategy_type
          AND signal_date < :data_cutoff
        """,
        {"strategy_type": strategy_type, "data_cutoff": data_cutoff},
    )
    sample_count = len(rows)
    if sample_count < 30:
        return None
    wins = [float(row["net_return_pct"]) for row in rows if row.get("net_return_pct") is not None and float(row["net_return_pct"]) > 0]
    losses = [abs(float(row["net_return_pct"])) for row in rows if row.get("net_return_pct") is not None and float(row["net_return_pct"]) < 0]
    avg_loss = (sum(losses) / len(losses)) if losses else None
    return {
        "strategy_type": strategy_type,
        "sample_count": sample_count,
        "win_rate_historical": round(len(wins) / sample_count, 4),
        "avg_profit_loss_ratio": round((sum(wins) / len(wins)) / avg_loss, 4) if wins and avg_loss else None,
        "data_cutoff": data_cutoff.isoformat(),
    }


_ATR_MULTIPLIER_BY_STRATEGY = {"A": 1.5, "B": 2.0, "C": 2.5}


def _build_instruction_card(
    *,
    signal_entry_price: float,
    atr_pct: float | None,
    strategy_type: str = "B",
) -> dict[str, Any]:
    atr_multiplier = _ATR_MULTIPLIER_BY_STRATEGY.get(strategy_type, 2.0)
    atr_ratio = _normalize_atr_ratio(atr_pct)
    if atr_ratio:
        stop_loss = round(signal_entry_price * (1 - atr_ratio * atr_multiplier), 4)
        stop_loss_calc_mode = "atr_multiplier"
    else:
        stop_loss = round(signal_entry_price * 0.92, 4)
        stop_loss_calc_mode = "fixed_92pct_fallback"
    # FR06-LLM-09 (SSOT): stop_loss >= entry → 全挡SKIPPED + logic_inversion_fallback
    if stop_loss >= signal_entry_price:
        fallback_stop_loss = round(signal_entry_price * 0.92, 4)
        fallback_stop_loss = min(fallback_stop_loss, round(signal_entry_price - 0.0001, 4))
        fallback_stop_loss = max(fallback_stop_loss, 0.0)
        fallback_target_price = round(signal_entry_price + (signal_entry_price - fallback_stop_loss) * 1.5, 4)
        return {
            "signal_entry_price": round(signal_entry_price, 4),
            "atr_pct": round(float(atr_ratio or 0.0), 6),
            "atr_multiplier": atr_multiplier,
            "stop_loss": fallback_stop_loss,
            "target_price": fallback_target_price,
            "stop_loss_calc_mode": "fixed_92pct_fallback",
            "skip_reason": "logic_inversion_fallback",
            "skipped": True,
        }
    target_price = round(signal_entry_price + (signal_entry_price - stop_loss) * 1.5, 4)
    return {
        "signal_entry_price": round(signal_entry_price, 4),
        "atr_pct": round(float(atr_ratio or 0.0), 6),
        "atr_multiplier": atr_multiplier,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "stop_loss_calc_mode": stop_loss_calc_mode,
    }


def _build_trade_instruction_by_tier(
    *,
    recommendation: str,
    confidence: float,
    signal_entry_price: float,
    skip_reason: str | None = None,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for tier in SSOT_TIERS:
        default_ratio = BASE_POSITION_RATIO[tier]
        if recommendation != "BUY" or confidence < 0.65:
            out[tier] = {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": skip_reason or "LOW_CONFIDENCE_OR_NOT_BUY"}
            continue
        if CAPITAL_BY_TIER[tier] < signal_entry_price * 100:
            out[tier] = {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "INSUFFICIENT_FUNDS"}
            continue
        out[tier] = {"status": "EXECUTE", "position_ratio": round(default_ratio, 6), "skip_reason": None}
    return out


def _real_source_url(item: dict[str, Any]) -> str:
    """根据数据源类型生成真实来源 URL。"""
    ds = item.get("dataset_name", "")
    src = item.get("source_name", "")
    code = item.get("stock_code", "").split(".")[0]
    if ds == "kline_daily" and src in ("eastmoney", "tdx_local"):
        return f"https://quote.eastmoney.com/concept/{code}.html"
    if ds == "hotspot_top50":
        return f"https://guba.eastmoney.com/news,cjpl.html"
    if ds == "northbound_summary":
        return f"https://data.eastmoney.com/hsgtcg/stock.html?code={code}"
    if ds == "etf_flow_summary":
        return f"https://data.eastmoney.com/etf/default.html"
    if ds == "market_state_input":
        return f"https://quote.eastmoney.com/zs000001.html"
    return f"https://quote.eastmoney.com/concept/{code}.html"


def _validate_source_url(url: str) -> str:
    """FR00-AUTH-02: 校验 source_url 格式，非法时替换为空字符串。"""
    if not url or not isinstance(url, str):
        return ""
    url = url.strip()
    import re
    if re.match(r'^https?://', url):
        return url
    return ""


def _build_citations(*args, **kwargs) -> list[dict[str, Any]]:
    # Support both old-style (db, used_data=..., kline_row=..., market_state_row=...) and modern (used_data, kline_row=...)
    if args and isinstance(args[0], list):
        used_data = args[0]
    elif "used_data" in kwargs:
        used_data = kwargs["used_data"]
    elif len(args) >= 2 and isinstance(args[1], list):
        used_data = args[1]
    elif len(args) >= 1 and not isinstance(args[0], list):
        # first arg is db session, used_data is kwarg
        used_data = kwargs.get("used_data", [])
    else:
        used_data = args[0] if args else []

    kline_row = kwargs.get("kline_row")
    market_state_row = kwargs.get("market_state_row")

    # Old-style calls pass db as first arg — sort used_data by dataset priority
    if kline_row or market_state_row:
        used_data = _sort_used_data(used_data)

    citations: list[dict[str, Any]] = []
    for idx, item in enumerate(used_data, start=1):
        raw_url = _real_source_url(item)
        source_url = _validate_source_url(raw_url)
        ds = str(item.get("dataset_name") or "")
        excerpt = f"{item['stock_code']} {ds} {item['status']}"
        if ds == "kline_daily" and kline_row:
            excerpt = f"开盘 {kline_row.get('open', '')} 收盘 {kline_row.get('close', '')} 最高 {kline_row.get('high', '')} 最低 {kline_row.get('low', '')}"
        elif ds == "market_state_input" and market_state_row:
            excerpt = f"市场状态 {market_state_row.get('market_state', '')} 参考日 {market_state_row.get('reference_date', '')}"
        _DATASET_TITLES = {
            "kline_daily": "K线日线数据",
            "hotspot_top50": "热点题材数据",
            "etf_flow_summary": "ETF资金流向",
            "northbound_summary": "北向资金",
            "market_state_input": "市场状态",
        }
        citations.append(
            {
                "citation_id": str(uuid4()),
                "citation_order": idx,
                "source_name": item["source_name"],
                "source_url": source_url,
                "fetch_time": _as_datetime(item["fetch_time"]),
                "title": _DATASET_TITLES.get(ds, ds),
                "excerpt": excerpt,
            }
        )
    return citations[:5]


def _load_report_result(db: Session, report_id: str) -> dict[str, Any]:
    report = _query_one(
        db,
        """
        SELECT
            report_id,
            idempotency_key,
            stock_code,
            trade_date,
            recommendation,
            confidence,
            conclusion_text,
            reasoning_chain_md,
            content_json,
            strategy_type,
            quality_flag,
            llm_fallback_level,
            llm_actual_model,
            llm_provider_name,
            llm_endpoint,
            risk_audit_status,
            risk_audit_skip_reason,
            publish_status,
            published,
            review_flag,
            status_reason
        FROM report
        WHERE report_id = :report_id
        LIMIT 1
        """,
        {"report_id": report_id},
    )
    if not report:
        raise ReportGenerationServiceError(500, "VALIDATION_FAILED")

    citations = _query_all(
        db,
        """
        SELECT citation_id, citation_order, source_name, source_url, fetch_time, title, excerpt
        FROM report_citation
        WHERE report_id = :report_id
        ORDER BY citation_order ASC, created_at ASC
        """,
        {"report_id": report_id},
    )
    instruction_card = _query_one(
        db,
        """
        SELECT signal_entry_price, atr_pct, atr_multiplier, stop_loss, target_price, stop_loss_calc_mode
        FROM instruction_card
        WHERE report_id = :report_id
        LIMIT 1
        """,
        {"report_id": report_id},
    )
    trade_rows = _query_all(
        db,
        """
        SELECT capital_tier, status, position_ratio, skip_reason
        FROM sim_trade_instruction
        WHERE report_id = :report_id
        ORDER BY capital_tier ASC
        """,
        {"report_id": report_id},
    )
    used_data = _query_all(
        db,
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
        """,
        {"report_id": report_id},
    )
    sim_trade_instruction = {
        tier: {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": None}
        for tier in SSOT_TIERS
    }
    for row in trade_rows:
        sim_trade_instruction[str(row["capital_tier"])] = {
            "status": row["status"],
            "position_ratio": float(row["position_ratio"] or 0.0),
            "skip_reason": row.get("skip_reason"),
        }
    return {
        "report_id": report["report_id"],
        "idempotency_key": report["idempotency_key"],
        "stock_code": report["stock_code"],
        "trade_date": _as_date(report["trade_date"]).isoformat() if _as_date(report["trade_date"]) else None,
        "recommendation": report["recommendation"],
        "confidence": float(report["confidence"]),
        "strategy_type": report["strategy_type"],
        "quality_flag": report["quality_flag"],
        "llm_fallback_level": report["llm_fallback_level"],
        "llm_actual_model": report.get("llm_actual_model"),
        "llm_provider_name": report.get("llm_provider_name"),
        "llm_endpoint": report.get("llm_endpoint"),
        "risk_audit_status": report["risk_audit_status"],
        "risk_audit_skip_reason": report["risk_audit_skip_reason"],
        "publish_status": report["publish_status"],
        "published": bool(report["published"]),
        "review_flag": report["review_flag"],
        "citations": [
            {
                "citation_id": row["citation_id"],
                "source_name": row["source_name"],
                "source_url": row["source_url"],
                "fetch_time": _as_datetime(row["fetch_time"]).isoformat() if _as_datetime(row["fetch_time"]) else None,
                "title": row["title"],
                "excerpt": row["excerpt"],
            }
            for row in citations
        ],
        "instruction_card": {
            "signal_entry_price": float(instruction_card["signal_entry_price"]),
            "atr_pct": float(instruction_card["atr_pct"]),
            "atr_multiplier": float(instruction_card["atr_multiplier"]),
            "stop_loss": float(instruction_card["stop_loss"]),
            "target_price": float(instruction_card["target_price"]),
            "stop_loss_calc_mode": instruction_card["stop_loss_calc_mode"],
        }
        if instruction_card
        else None,
        "sim_trade_instruction": sim_trade_instruction,
        "used_data": [
            {
                "usage_id": row["usage_id"],
                "trade_date": _as_date(row["trade_date"]).isoformat() if _as_date(row["trade_date"]) else None,
                "stock_code": row["stock_code"],
                "dataset_name": row["dataset_name"],
                "source_name": row["source_name"],
                "batch_id": row["batch_id"],
                "fetch_time": _as_datetime(row["fetch_time"]).isoformat() if _as_datetime(row["fetch_time"]) else None,
                "status": row["status"],
                "status_reason": row["status_reason"],
            }
            for row in used_data
        ],
        "status_reason": report.get("status_reason"),
        "conclusion_text": report.get("conclusion_text") or "",
        "reasoning_chain_md": report.get("reasoning_chain_md") or "",
        "content_json": (
            json.loads(report["content_json"])
            if report.get("content_json") and isinstance(report["content_json"], str)
            else report.get("content_json")
        ) or {},
    }


def generate_report_ssot(
    db: Session,
    *,
    stock_code: str,
    trade_date: str | None = None,
    idempotency_key: str | None = None,
    request_id: str | None = None,
    skip_pool_check: bool = False,
    resume_active_task: bool = False,
    force_same_day_rebuild: bool = False,
    forced_strategy_type: str | None = None,
) -> dict[str, Any]:
    target_trade_date, trade_day = _resolve_trade_date(trade_date)
    request_id = ensure_request_id(request_id)
    # OPT-11: forced_strategy_type 使同一股票同一天可生成不同策略研报，idempotency key 加后缀区分
    _fst = forced_strategy_type.upper() if forced_strategy_type and forced_strategy_type.upper() in {"A", "B", "C"} else None
    normalized_key = idempotency_key or (
        f"daily:{stock_code}:{target_trade_date}:{_fst}" if _fst else f"daily:{stock_code}:{target_trade_date}"
    )
    generation_lock = _lock_for_generation_key(normalized_key)
    if not generation_lock.acquire(blocking=False):
        raise ReportGenerationServiceError(409, "CONCURRENT_CONFLICT")

    task_table = Base.metadata.tables["report_generation_task"]
    report_table = Base.metadata.tables["report"]
    citation_table = Base.metadata.tables["report_citation"]
    instruction_table = Base.metadata.tables["instruction_card"]
    usage_link_table = Base.metadata.tables["report_data_usage_link"]
    trade_instruction_table = Base.metadata.tables["sim_trade_instruction"]

    task_id = str(uuid4())
    task_inserted = False
    acquired = True
    final_report_committed = False
    try:
        _expire_stale_tasks(db, current_trade_date=trade_day)
        superseded_report_id: str | None = None

        existing = _load_active_report_row(db, idempotency_key=normalized_key)
        while existing:
            existing_issues = _load_report_input_issues(db, report_id=str(existing["report_id"]))
            if not existing_issues:
                break
            _purge_report_generation_bundle(
                db,
                report_id=str(existing["report_id"]),
                purge_reason=_REPORT_DATA_INCOMPLETE,
            )
            db.commit()
            existing = _load_active_report_row(db, idempotency_key=normalized_key)
        if existing:
            if existing["stock_code"] != stock_code or str(existing["trade_date"]) != target_trade_date:
                raise ReportGenerationServiceError(409, "IDEMPOTENCY_CONFLICT")
            if not force_same_day_rebuild:
                return _load_report_result(db, str(existing["report_id"]))
            superseded_report_id = str(existing["report_id"])

        existing_same_day = _query_one(
            db,
            """
            SELECT report_id
            FROM report
            WHERE stock_code = :stock_code
              AND trade_date = :trade_date
              AND is_deleted = 0
              AND superseded_by_report_id IS NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            {"stock_code": stock_code, "trade_date": trade_day},
        )
        while existing_same_day:
            existing_same_day_issues = _load_report_input_issues(db, report_id=str(existing_same_day["report_id"]))
            if not existing_same_day_issues:
                break
            _purge_report_generation_bundle(
                db,
                report_id=str(existing_same_day["report_id"]),
                purge_reason=_REPORT_DATA_INCOMPLETE,
            )
            db.commit()
            existing_same_day = _query_one(
                db,
                """
                SELECT report_id
                FROM report
                WHERE stock_code = :stock_code
                  AND trade_date = :trade_date
                  AND is_deleted = 0
                  AND superseded_by_report_id IS NULL
                ORDER BY created_at DESC
                LIMIT 1
                """,
                {"stock_code": stock_code, "trade_date": trade_day},
            )
        if existing_same_day and not force_same_day_rebuild:
            return _load_report_result(db, str(existing_same_day["report_id"]))
        if existing_same_day and force_same_day_rebuild:
            superseded_report_id = str(existing_same_day["report_id"])

        if not skip_pool_check:
            exact_pool_codes = get_daily_stock_pool(target_trade_date, exact_trade_date=True)
            if not exact_pool_codes:
                raise ReportGenerationServiceError(503, "DEPENDENCY_NOT_READY")
            if stock_code not in exact_pool_codes:
                raise ReportGenerationServiceError(422, "NOT_IN_CORE_POOL")

        kline_row = _query_one(
            db,
            """
            SELECT stock_code, trade_date, open, high, low, close, atr_pct, ma5, ma20, volatility_20d, is_suspended
            FROM kline_daily
            WHERE stock_code = :stock_code AND trade_date = :trade_date
            LIMIT 1
            """,
            {"stock_code": stock_code, "trade_date": trade_day},
        )
        market_state_row = _query_one(
            db,
            """
            SELECT market_state, state_reason, market_state_degraded,
                   trade_date, reference_date, kline_batch_id, hotspot_batch_id
            FROM market_state_cache
            WHERE trade_date <= :trade_date
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            {"trade_date": trade_day},
        )
        stock_row = _query_one(
            db,
            """
            SELECT stock_name, is_suspended
            FROM stock_master
            WHERE stock_code = :stock_code
            LIMIT 1
            """,
            {"stock_code": stock_code},
        )

        if not kline_row or not market_state_row or not stock_row:
            raise ReportGenerationServiceError(500, "DEPENDENCY_NOT_READY")

        _maybe_collect_non_report_usage(
            db,
            stock_code=stock_code,
            trade_date=target_trade_date,
        )

        _ensure_market_state_input_usage(
            db,
            stock_code=stock_code,
            report_trade_day=trade_day,
            market_state_row=market_state_row,
        )
        used_data = _query_all(
            db,
            """
            SELECT usage_id, trade_date, stock_code, dataset_name, source_name, batch_id, fetch_time, status, status_reason
            FROM (
                SELECT
                    usage_id,
                    trade_date,
                    stock_code,
                    dataset_name,
                    source_name,
                    batch_id,
                    fetch_time,
                    status,
                    status_reason,
                    ROW_NUMBER() OVER (
                        PARTITION BY dataset_name, source_name
                        ORDER BY fetch_time DESC, created_at DESC, usage_id DESC
                    ) AS usage_rank
                FROM report_data_usage
                WHERE stock_code = :stock_code AND trade_date = :trade_date
            )
            WHERE usage_rank = 1
            ORDER BY fetch_time DESC, dataset_name ASC
            """,
            {"stock_code": stock_code, "trade_date": trade_day},
        )
        if not used_data:
            raise ReportGenerationServiceError(500, "DEPENDENCY_NOT_READY")

        # Validate market_state_cache parent batch references exist
        for _parent_key in ("kline_batch_id", "hotspot_batch_id"):
            _parent_id = market_state_row.get(_parent_key)
            if _parent_id:
                _parent_exists = _query_one(
                    db,
                    "SELECT batch_id FROM data_batch WHERE batch_id = :bid LIMIT 1",
                    {"bid": str(_parent_id)},
                )
                if not _parent_exists:
                    raise ReportGenerationServiceError(500, "DEPENDENCY_NOT_READY")

        refresh_task_id = resolve_refresh_task_id(
            db,
            trade_day=trade_day,
            stock_code=stock_code,
        )
        if not refresh_task_id:
            raise ReportGenerationServiceError(503, "DEPENDENCY_NOT_READY")
        _pool_version = _load_pool_version_for_refresh_task(db, refresh_task_id=refresh_task_id) or 1

        generation_seq, superseded_task_id, resumable_task, retry_count = _prepare_generation_version(
            db,
            idempotency_key=normalized_key,
            stock_code=stock_code,
            trade_day=trade_day,
            allow_resume_active=resume_active_task,
            allow_retry_completed=force_same_day_rebuild,
        )
        now = utc_now()

        if resumable_task:
            task_id = str(resumable_task["task_id"])

        if superseded_task_id:
            db.execute(
                task_table.update()
                .where(task_table.c.task_id == superseded_task_id)
                .values(
                    superseded_at=now,
                    updated_at=now,
                )
            )

        if resumable_task:
            db.execute(
                task_table.update()
                .where(task_table.c.task_id == task_id)
                .values(
                    status="Processing",
                    retry_count=retry_count,
                    quality_flag="ok",
                    status_reason=None,
                    llm_fallback_level="primary",
                    risk_audit_status="not_triggered",
                    risk_audit_skip_reason=None,
                    request_id=request_id or resumable_task.get("request_id"),
                    started_at=now,
                    finished_at=None,
                    updated_at=now,
                )
            )
        else:
            db.execute(
                task_table.insert().values(
                    task_id=task_id,
                    trade_date=trade_day,
                    stock_code=stock_code,
                    idempotency_key=normalized_key,
                    generation_seq=generation_seq,
                    status="Processing",
                    retry_count=retry_count,
                    quality_flag="ok",
                    status_reason=None,
                    llm_fallback_level="primary",
                    risk_audit_status="not_triggered",
                    risk_audit_skip_reason=None,
                    market_state_trade_date=_as_date(market_state_row.get("trade_date")) or trade_day,
                    refresh_task_id=refresh_task_id,
                    trigger_task_run_id=None,
                    request_id=request_id,
                    superseded_by_task_id=None,
                    superseded_at=None,
                    queued_at=now,
                    started_at=now,
                    finished_at=None,
                    updated_at=now,
                    created_at=now,
                )
            )
            task_inserted = True
        if superseded_task_id:
            db.execute(
                task_table.update()
                .where(task_table.c.task_id == superseded_task_id)
                .values(
                    superseded_by_task_id=task_id,
                    updated_at=utc_now(),
                )
            )
        task_inserted = True
        # Commit the visible Processing state before any LLM/audit call so
        # background generation does not hold a SQLite write lock for minutes.
        db.commit()

        # OPT-11: 优先使用强制策略类型（测试/演示用），否则自动判断
        strategy_type = _fst or _determine_strategy_type(db, stock_code=stock_code, trade_day=trade_day, kline_row=kline_row)
        prior_stats = _compute_prior_stats(db, strategy_type=strategy_type, trade_day=trade_day)
        quality_flag, quality_reason = _derive_quality_flag(used_data, market_state_row, strategy_type=strategy_type)
        signal_entry_price = float(kline_row["close"])
        input_issues = _collect_generation_input_issues(
            used_data=used_data,
            market_state_row=market_state_row,
            strategy_type=strategy_type,
        )
        if input_issues:
            logger.warning(
                "report_generation_failed_incomplete_data stock=%s trade_date=%s issues=%s",
                stock_code,
                target_trade_date,
                input_issues,
            )
            db.execute(
                task_table.update()
                .where(task_table.c.task_id == task_id)
                .values(
                    status="Failed",
                    quality_flag="degraded",
                    status_reason=_REPORT_DATA_INCOMPLETE,
                    llm_fallback_level="failed",
                    finished_at=utc_now(),
                    updated_at=utc_now(),
                )
            )
            db.commit()
            raise ReportGenerationServiceError(422, _REPORT_DATA_INCOMPLETE)

        if bool(kline_row.get("is_suspended")) or bool(stock_row.get("is_suspended")):
            model_result = {
                "recommendation": "HOLD",
                "confidence": 0.0,
                "llm_fallback_level": "failed",
                "risk_audit_status": "not_triggered",
                "risk_audit_skip_reason": None,
                "conclusion_text": "stock suspended",
                "reasoning_chain_md": "suspended_skipped",
                "signal_entry_price": signal_entry_price,
            }
            published = True
            publish_status = "PUBLISHED"
            status_reason = "SUSPENDED_SKIPPED"
            quality_flag = "degraded"
            trade_instruction_by_tier = _build_trade_instruction_by_tier(
                recommendation="HOLD",
                confidence=0.0,
                signal_entry_price=signal_entry_price,
                skip_reason="SUSPENDED",
            )
        elif market_state_row["market_state"] == "BEAR" and strategy_type in {"B", "C"}:
            model_result = {
                "recommendation": "HOLD",
                "confidence": 0.0,
                "llm_fallback_level": "failed",
                "risk_audit_status": "not_triggered",
                "risk_audit_skip_reason": None,
                "conclusion_text": "bear market filtered",
                "reasoning_chain_md": "bear_market_filtered",
                "signal_entry_price": signal_entry_price,
            }
            published = False
            publish_status = "UNPUBLISHED"
            status_reason = "BEAR_MARKET_FILTERED"
            quality_flag = "degraded"
            trade_instruction_by_tier = _build_trade_instruction_by_tier(
                recommendation="HOLD",
                confidence=0.0,
                signal_entry_price=signal_entry_price,
            )
        else:
            model_result = run_generation_model(
                stock_code=stock_code,
                stock_name=stock_row["stock_name"],
                strategy_type=strategy_type,
                market_state=market_state_row["market_state"],
                quality_flag=quality_flag,
                prior_stats=prior_stats,
                signal_entry_price=signal_entry_price,
                used_data=used_data,
                kline_row=kline_row,
            )
            # Only publish if LLM produced a real result (not rule-based fallback)
            _llm_level = model_result.get("llm_fallback_level", "failed")
            published = _llm_level in {"primary", "backup", "cli"}
            publish_status = "PUBLISHED" if published else "UNPUBLISHED"
            status_reason = quality_reason if published else "LLM_FALLBACK"
            # Fix N07: quality_flag must reflect LLM failure — data-ok + llm-failed = degraded
            if _llm_level in ("failed", "rule_based") and quality_flag == "ok":
                quality_flag = "degraded"
            trade_instruction_by_tier = _build_trade_instruction_by_tier(
                recommendation=model_result["recommendation"],
                confidence=float(model_result["confidence"]),
                signal_entry_price=signal_entry_price,
            )

        # --- FR06-LLM-06: 辩证审阅 (SSOT: BUY + confidence>=0.65 → 调第二个LLM做风险审计) ---
        from app.services.llm_router import should_trigger_audit, run_audit_and_aggregate
        _rec = model_result.get("recommendation", "")
        _conf = float(model_result.get("confidence", 0))
        _contradiction = model_result.get("contradiction", "")
        _audit_requested = should_trigger_audit(_rec, _conf, _contradiction)
        _llm_failed = model_result.get("llm_fallback_level") in ("failed", "rule_based")
        if _audit_requested and _llm_failed:
            model_result["risk_audit_status"] = "not_triggered"
            model_result["risk_audit_skip_reason"] = "llm_all_failed_rule_fallback"
        elif _audit_requested and settings.mock_llm:
            model_result["risk_audit_status"] = "skipped"
            model_result["risk_audit_skip_reason"] = "mock_llm"
        elif _audit_requested and not settings.llm_audit_enabled:
            model_result["risk_audit_status"] = "skipped"
            model_result["risk_audit_skip_reason"] = "audit_disabled"
        elif _audit_requested:
            try:
                audit_result = _run_llm_coro(
                    lambda: run_audit_and_aggregate(
                        main_vote=_rec,
                        base_confidence=_conf,
                        report_summary=model_result.get("conclusion_text", ""),
                        timeout_sec=90,
                    )
                )
                if audit_result.get("skip_reason"):
                    model_result["risk_audit_status"] = "skipped"
                    model_result["risk_audit_skip_reason"] = audit_result["skip_reason"]
                else:
                    model_result["risk_audit_status"] = "completed"
                    model_result["risk_audit_skip_reason"] = None
                    model_result["confidence"] = round(audit_result.get("adjusted_confidence", _conf), 4)
                    audit_detail = audit_result.get("audit_detail", "")
                    if audit_detail:
                        existing_chain = model_result.get("reasoning_chain_md", "")
                        model_result["reasoning_chain_md"] = f"{existing_chain}\n\n## 风险补充审计\n{audit_detail}"
            except Exception as e:
                logger.warning("risk_audit_call_failed stock=%s: %s", stock_code, e)
                model_result["risk_audit_status"] = "skipped"
                model_result["risk_audit_skip_reason"] = f"call_failed:{e!s}"

        # Recompute trade instructions only if audit actually adjusted confidence
        if model_result.get("risk_audit_status") == "completed":
            trade_instruction_by_tier = _build_trade_instruction_by_tier(
                recommendation=model_result["recommendation"],
                confidence=float(model_result["confidence"]),
                signal_entry_price=signal_entry_price,
            )

        # --- P1-09 Publish Gate Control ---
        # Block publishing when critical data is missing (fetcher_not_provided).
        # SSOT allows stale_ok for partial degradation, but fetcher_not_provided
        # means a structural gap — the data was never attempted.
        gate_blocked = False
        if published and quality_flag == "stale_ok" and quality_reason:
            _critical_missing = {"fetcher_not_provided", "northbound_not_ok", "etf_flow_not_ok", "no_etf_data_available"}
            if any(tag in (quality_reason or "") for tag in _critical_missing):
                published = False
                publish_status = "UNPUBLISHED"
                review_flag_override = "PENDING_REVIEW"
                gate_blocked = True
                logger.warning(
                    "publish_gate_blocked stock=%s quality=%s reason=%s",
                    stock_code, quality_flag, quality_reason,
                )
        # Degraded quality also blocks publishing
        if published and quality_flag == "degraded":
            published = False
            publish_status = "UNPUBLISHED"
            gate_blocked = True
            logger.warning(
                "publish_gate_blocked_degraded stock=%s reason=%s",
                stock_code, quality_reason,
            )

        content_json, content_issues = _build_content_json_and_quality_issues(
            db,
            stock_code=stock_code,
            recommendation=model_result.get("recommendation", "HOLD"),
            kline_row=kline_row,
            market_state_row=market_state_row,
            used_data=used_data,
            strategy_type=strategy_type,
            confidence=float(model_result.get("confidence") or 0.65),
            quality_flag=quality_flag,
            llm_fallback_level=model_result.get("llm_fallback_level"),
            strategy_specific_evidence=model_result.get("strategy_specific_evidence"),
        )
        if content_issues:
            db.execute(
                task_table.update()
                .where(task_table.c.task_id == task_id)
                .values(
                    status="Failed",
                    quality_flag="degraded",
                    status_reason=_REPORT_DATA_INCOMPLETE,
                    llm_fallback_level="failed",
                    finished_at=utc_now(),
                    updated_at=utc_now(),
                )
            )
            db.commit()
            logger.warning(
                "report_generation_failed_quality_gate stock=%s trade_date=%s issues=%s",
                stock_code,
                target_trade_date,
                content_issues,
            )
            raise ReportGenerationServiceError(422, _REPORT_DATA_INCOMPLETE)

        instruction_card = _build_instruction_card(
            signal_entry_price=signal_entry_price,
            atr_pct=float(kline_row.get("atr_pct") or 0.0),
            strategy_type=strategy_type,
        )
        # FR06-LLM-09: 防倒挂 → 全挡SKIPPED
        if instruction_card.get("skipped"):
            model_result["recommendation"] = "HOLD"
            model_result["confidence"] = 0.0
            published = False
            is_strong_buy = False
            trade_instruction_by_tier = _build_trade_instruction_by_tier(
                recommendation="HOLD",
                confidence=0.0,
                signal_entry_price=signal_entry_price,
                skip_reason=instruction_card.get("skip_reason"),
            )
        report_id = str(uuid4())
        citations = _build_citations(used_data, kline_row=kline_row, market_state_row=market_state_row)
        if gate_blocked:
            review_flag = locals().get("review_flag_override", "NONE")
        elif not published and model_result.get("llm_fallback_level") not in (None, "primary"):
            review_flag = "PENDING_REVIEW"
        else:
            review_flag = "APPROVED" if published else "NONE"
        if superseded_report_id and superseded_report_id != report_id:
            db.execute(
                report_table.update()
                .where(report_table.c.report_id == superseded_report_id)
                .values(
                    is_deleted=True,
                    deleted_at=now,
                    updated_at=now,
                )
            )

        db.execute(
            report_table.insert().values(
                report_id=report_id,
                generation_task_id=task_id,
                trade_date=trade_day,
                stock_code=stock_code,
                stock_name_snapshot=stock_row["stock_name"],
                pool_version=_pool_version,
                idempotency_key=normalized_key,
                generation_seq=generation_seq,
                published=published,
                publish_status=publish_status,
                published_at=now if published else None,
                recommendation=model_result["recommendation"],
                confidence=model_result["confidence"],
                quality_flag=quality_flag,
                status_reason=status_reason,
                llm_fallback_level=model_result["llm_fallback_level"],
                llm_actual_model=model_result.get("llm_actual_model"),
                llm_provider_name=model_result.get("llm_provider_name"),
                llm_endpoint=model_result.get("llm_endpoint"),
                strategy_type=strategy_type,
                market_state=market_state_row["market_state"],
                market_state_reference_date=_as_date(market_state_row.get("reference_date")) or trade_day,
                market_state_degraded=bool(market_state_row.get("market_state_degraded")),
                market_state_reason_snapshot=market_state_row.get("state_reason"),
                market_state_trade_date=_as_date(market_state_row.get("trade_date")) or trade_day,
                conclusion_text=model_result["conclusion_text"],
                reasoning_chain_md=model_result["reasoning_chain_md"],
                prior_stats_snapshot=prior_stats,
                risk_audit_status=model_result["risk_audit_status"],
                risk_audit_skip_reason=model_result["risk_audit_skip_reason"],
                review_flag=review_flag,
                failure_category=None,
                negative_feedback_count=0,
                reviewed_by=None,
                reviewed_at=None,
                is_deleted=False,
                deleted_at=None,
                superseded_by_report_id=None,
                content_json=content_json,
                created_at=now,
                updated_at=now,
            )
        )
        if superseded_report_id and superseded_report_id != report_id:
            db.execute(
                report_table.update()
                .where(report_table.c.report_id == superseded_report_id)
                .values(
                    superseded_by_report_id=report_id,
                    updated_at=now,
                )
            )

        for citation in citations:
            db.execute(
                citation_table.insert().values(
                    **citation,
                    report_id=report_id,
                    created_at=now,
                )
            )
        db.execute(
            instruction_table.insert().values(
                instruction_card_id=str(uuid4()),
                report_id=report_id,
                signal_entry_price=instruction_card["signal_entry_price"],
                atr_pct=instruction_card["atr_pct"],
                atr_multiplier=instruction_card["atr_multiplier"],
                stop_loss=instruction_card["stop_loss"],
                target_price=instruction_card["target_price"],
                stop_loss_calc_mode=instruction_card["stop_loss_calc_mode"],
                created_at=now,
            )
        )
        for item in used_data:
            db.execute(
                usage_link_table.insert().values(
                    report_data_usage_link_id=str(uuid4()),
                    report_id=report_id,
                    usage_id=item["usage_id"],
                    created_at=now,
                )
            )
        # Ensure market_state_input usage row + lineage + link to report
        ms_usage = _ensure_market_state_input_usage(
            db,
            stock_code=stock_code,
            report_trade_day=trade_day,
            market_state_row=market_state_row,
        )
        _ensure_report_usage_link(db, report_id=report_id, usage_id=ms_usage["usage_id"], created_at=now)
        for tier, payload in trade_instruction_by_tier.items():
            db.execute(
                trade_instruction_table.insert().values(
                    trade_instruction_id=str(uuid4()),
                    report_id=report_id,
                    capital_tier=tier,
                    status=payload["status"],
                    position_ratio=payload["position_ratio"],
                    skip_reason=payload["skip_reason"],
                    created_at=now,
                )
            )

        output_issues = _load_public_report_output_issues(db, report_id=report_id)
        if output_issues:
            _purge_report_generation_bundle(
                db,
                report_id=report_id,
                purge_reason=_REPORT_DATA_INCOMPLETE,
            )
            db.commit()
            logger.warning(
                "report_generation_failed_public_payload_gate stock=%s trade_date=%s issues=%s",
                stock_code,
                target_trade_date,
                output_issues,
            )
            raise ReportGenerationServiceError(422, _REPORT_DATA_INCOMPLETE)

        db.execute(
            task_table.update()
            .where(task_table.c.task_id == task_id)
            .values(
                status="Completed",
                quality_flag=quality_flag,
                status_reason=status_reason,
                llm_fallback_level=model_result["llm_fallback_level"],
                risk_audit_status=model_result["risk_audit_status"],
                risk_audit_skip_reason=model_result["risk_audit_skip_reason"],
                market_state_trade_date=_as_date(market_state_row.get("trade_date")) or trade_day,
                finished_at=utc_now(),
                updated_at=utc_now(),
            )
        )
        db.commit()
        final_report_committed = True
        return _load_report_result(db, report_id)
    except IntegrityError:
        db.rollback()
        return _resolve_generation_conflict(
            db,
            idempotency_key=normalized_key,
            stock_code=stock_code,
            trade_date=target_trade_date,
        )
    except OperationalError as exc:
        db.rollback()
        if "locked" in str(exc).lower():
            # Mark task as Failed instead of leaving as Processing
            # so next invocation can retry without _expire_stale_tasks delay
            if task_inserted:
                try:
                    db.execute(
                        task_table.update()
                        .where(task_table.c.task_id == task_id)
                        .values(
                            status="Failed",
                            status_reason="sqlite_locked",
                            finished_at=utc_now(),
                            updated_at=utc_now(),
                        )
                    )
                    db.commit()
                except Exception:
                    pass
            return _resolve_generation_conflict(
                db,
                idempotency_key=normalized_key,
                stock_code=stock_code,
                trade_date=target_trade_date,
            )
        raise
    except ReportGenerationServiceError as exc:
        if final_report_committed:
            raise
        if task_inserted:
            db.rollback()
            db.execute(
                task_table.update()
                .where(task_table.c.task_id == task_id)
                .values(
                    status="Failed",
                    status_reason=exc.error_code,
                    finished_at=utc_now(),
                    updated_at=utc_now(),
                )
            )
            db.commit()
        else:
            db.rollback()
        raise
    except Exception as exc:
        if final_report_committed:
            raise ReportGenerationServiceError(500, "VALIDATION_FAILED") from exc
        if task_inserted:
            db.rollback()
            db.execute(
                task_table.update()
                .where(task_table.c.task_id == task_id)
                .values(
                    status="Failed",
                    status_reason="llm_all_failed",
                    llm_fallback_level="failed",
                    finished_at=utc_now(),
                    updated_at=utc_now(),
                )
            )
            db.commit()
        else:
            db.rollback()
        raise ReportGenerationServiceError(500, "LLM_ALL_FAILED") from exc
    finally:
        if acquired:
            generation_lock.release()


# ---------------------------------------------------------------------------
# 批量并发研报生成 — 利用 NewAPI 12路 relay token 并发竞速
# ---------------------------------------------------------------------------

def _preselect_one_per_strategy_type(
    db_factory,
    *,
    stock_codes: list[str],
    trade_date: str | None = None,
) -> tuple[list[str], dict[str, str]]:
    """Pre-screen stock_codes and select at most one per strategy type (A/B/C).

    Returns a tuple of:
    - list of at most 3 stock codes (each with a distinct strategy type)
    - selected_strategy_type_map: {stock_code: strategy_type} for every selected code,
      so the final generation step stays pinned to the same A/B/C split even if
      generate_report_ssot() would naturally drift to a different strategy type

    Priority: first occurrence of each natural type wins.

    OPT-14: 兜底指派 — 若自然分类后缺少某类型（A/C 尤其容易缺席），
    从剩余候选股中补选并强制赋类型，确保在有≥3只候选时三份研报齐全。
    Used by generate_reports_batch(one_per_strategy_type=True) to enforce the
    "每次最多生成3个研报，每种类型各一个" rule (docs/core/06 §7.1).
    """
    target, trade_day = _resolve_trade_date(trade_date)
    selected: dict[str, str] = {}  # strategy_type -> stock_code
    selected_strategy_type_map: dict[str, str] = {}
    used_codes: set[str] = set()

    for stock_code in stock_codes:
        if len(selected) >= 3:
            break
        db = db_factory()
        try:
            kline_row = _query_one(
                db,
                """
                SELECT stock_code, trade_date, open, high, low, close,
                       atr_pct, ma5, ma20, volatility_20d, is_suspended
                FROM kline_daily
                WHERE stock_code = :stock_code AND trade_date = :trade_date
                LIMIT 1
                """,
                {"stock_code": stock_code, "trade_date": trade_day},
            )
            if not kline_row:
                continue
            strategy_type = _determine_strategy_type(
                db,
                stock_code=stock_code,
                trade_day=trade_day,
                kline_row=kline_row,
            )
            if strategy_type not in selected:
                selected[strategy_type] = stock_code
                selected_strategy_type_map[stock_code] = strategy_type
                used_codes.add(stock_code)
        except Exception as exc:
            logger.debug("preselect_strategy_type_skip stock=%s err=%s", stock_code, exc)
        finally:
            db.close()

    # OPT-14: 兜底指派 — 若缺少某类型，从剩余候选股中补选并强制赋类型
    missing_types = [t for t in ("A", "B", "C") if t not in selected]
    if missing_types:
        remaining = [c for c in stock_codes if c not in used_codes]
        for missing_type in missing_types:
            if not remaining:
                break
            fallback_code = remaining.pop(0)
            selected[missing_type] = fallback_code
            selected_strategy_type_map[fallback_code] = missing_type
            used_codes.add(fallback_code)
            logger.info(
                "preselect_fallback_assign stock=%s forced_strategy_type=%s",
                fallback_code, missing_type,
            )

    return list(selected.values()), selected_strategy_type_map


def generate_reports_batch(
    db_factory,
    *,
    stock_codes: list[str],
    trade_date: str | None = None,
    skip_pool_check: bool = False,
    force_same_day_rebuild: bool = False,
    max_concurrent_override: int | None = None,
    one_per_strategy_type: bool = False,
    strategy_type_override_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Batch-generate reports for multiple stock codes concurrently.

    Uses asyncio.Semaphore to limit concurrency to settings.report_batch_max_concurrent.
    Each report invocation uses its own DB session from db_factory (callable returning Session).

    When one_per_strategy_type=True, pre-selects at most one stock per strategy type (A/B/C),
    limiting to a maximum of 3 reports per run (docs/core/06 §7.1).
    """
    # 策略类型限制: 每次最多生成3个研报，每种类型（A/B/C）各一个
    original_candidate_count = len(stock_codes)
    _preselected_strategy_type_map: dict[str, str] = {}
    if one_per_strategy_type and stock_codes:
        stock_codes, _preselected_strategy_type_map = _preselect_one_per_strategy_type(
            db_factory,
            stock_codes=stock_codes,
            trade_date=trade_date,
        )
    # OPT-14: 合并用户指定 override 与自动兜底 override（用户优先）
    _merged_override_map: dict[str, str] = {
        **_preselected_strategy_type_map,
        **(strategy_type_override_map or {}),
    }

    default_concurrency = max(1, int(getattr(settings, "report_batch_max_concurrent", 6)))
    if max_concurrent_override is None:
        max_concurrent = default_concurrency
    else:
        try:
            max_concurrent = max(1, int(max_concurrent_override))
        except (TypeError, ValueError):
            max_concurrent = default_concurrency
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _gen_one(code: str) -> dict[str, Any]:
        async with semaphore:
            loop = asyncio.get_event_loop()
            # OPT-12/OPT-14: 从合并后的策略覆盖映射中取强制策略类型
            _override_fst = _merged_override_map.get(code)
            try:
                result = await loop.run_in_executor(
                    None,
                    lambda: _gen_one_sync(
                        db_factory, code, trade_date, skip_pool_check, force_same_day_rebuild,
                        forced_strategy_type=_override_fst,
                    ),
                )
                return {"stock_code": code, "status": "ok", "result": result}
            except ReportGenerationServiceError as exc:
                return {
                    "stock_code": code,
                    "status": "error",
                    "error_code": getattr(exc, "error_code", str(exc)),
                    "status_code": getattr(exc, "status_code", None),
                }
            except Exception as exc:
                return {"stock_code": code, "status": "error", "error_code": str(exc)[:200]}

    async def _run_all() -> list[dict[str, Any]]:
        tasks = [_gen_one(code) for code in stock_codes]
        return await asyncio.gather(*tasks)

    import time as _time
    t0 = _time.time()
    # OPT-08 guard: 空列表时直接返回，避免 timeout_sec=0 导致即时 RuntimeError
    if not stock_codes:
        return {
            "total": 0,
            "succeeded": 0,
            "failed": 0,
            "elapsed_s": 0.0,
            "max_concurrent": max_concurrent,
            "details": [],
            **({"one_per_strategy_type": True, "original_candidate_count": original_candidate_count,
                "preselected_count": 0, "strategy_distribution": {"A": [], "B": [], "C": []}}
               if one_per_strategy_type else {}),
        }
    results = _run_llm_coro(lambda: _run_all(), timeout_sec=max(60, len(stock_codes) * 120))
    elapsed = round(_time.time() - t0, 2)

    succeeded = [r for r in results if r["status"] == "ok"]
    failed = [r for r in results if r["status"] != "ok"]
    batch_result: dict[str, Any] = {
        "total": len(stock_codes),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "elapsed_s": elapsed,
        "max_concurrent": max_concurrent,
        "details": results,
    }
    if one_per_strategy_type:
        batch_result["one_per_strategy_type"] = True
        batch_result["original_candidate_count"] = original_candidate_count
        batch_result["preselected_count"] = len(stock_codes)
        # OPT-08: 审计策略分布，方便看板核对 A/B/C 各 1 份
        _dist: dict[str, list[str]] = {"A": [], "B": [], "C": []}
        for r in results:
            if r.get("status") == "ok":
                _r_data = r.get("result") or {}
                _st = str(_r_data.get("strategy_type") or "").upper()
                if _st in _dist:
                    _dist[_st].append(r.get("stock_code", ""))
        batch_result["strategy_distribution"] = {k: v for k, v in _dist.items()}
    return batch_result


def _gen_one_sync(db_factory, stock_code, trade_date, skip_pool_check, force_same_day_rebuild, forced_strategy_type=None):
    db = db_factory()
    try:
        # Keep manual/internal batch generation aligned with the scheduler path:
        # supplement non-report datasets before entering the SSOT report gate.
        try:
            from app.services.stock_snapshot_service import collect_non_report_usage_sync

            collect_non_report_usage_sync(
                db,
                stock_code=stock_code,
                trade_date=trade_date,
            )
        except Exception as exc:
            logger.warning("batch_non_report_collect_failed stock=%s err=%s", stock_code, exc)
        return generate_report_ssot(
            db,
            stock_code=stock_code,
            trade_date=trade_date,
            skip_pool_check=skip_pool_check,
            force_same_day_rebuild=force_same_day_rebuild,
            forced_strategy_type=forced_strategy_type,
        )
    finally:
        db.close()
