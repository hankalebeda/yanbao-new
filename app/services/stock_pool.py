"""FR-01 股票池服务。

提供：
1. 股票池刷新（评分、核心池/候补池拆分、回退）
2. 精确视图/有效视图/公开视图读取
3. 日常调度用股票池读取
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from threading import Lock
from typing import Any, Callable
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.db import Base

logger = logging.getLogger(__name__)

_STOCK_CODE_RE = re.compile(r"^\d{6}\.(SH|SZ)$")
_ST_NAMES = re.compile(r"^[*]?ST(?![A-Za-z])|退市?", re.I)

DEFAULT_FILTER_PARAMS: dict[str, Any] = {
    "target_pool_size": 200,
    "standby_pool_size": 50,
    "min_listing_days": 365,
    "min_market_cap_cny": 5_000_000_000,
    "min_avg_amount_20d_cny": 30_000_000,
    "max_single_industry_weight": 0.15,
}

STANDBY_POOL_SIZE = 50
MIN_LOW_VOL_CORE_COUNT = 10
_MIN_CORE_ROWS_VALID = 1
_IN_PROGRESS: set[str] = set()
_IN_PROGRESS_LOCK = Lock()


class PoolRefreshConflict(RuntimeError):
    pass


class PoolColdStartError(RuntimeError):
    pass


@dataclass(slots=True)
class Candidate:
    stock_code: str
    stock_name: str
    industry: str | None
    is_suspended: bool
    score: float
    factor_values: dict[str, float]
    low_vol_candidate: bool = False


@dataclass(slots=True)
class PoolTaskView:
    task_id: str
    trade_date: date
    status: str
    pool_version: int
    fallback_from: date | None
    status_reason: str | None
    updated_at: datetime | None
    finished_at: datetime | None


@dataclass(slots=True)
class PoolSnapshotRow:
    stock_code: str
    pool_role: str
    rank_no: int
    score: float
    is_suspended: bool


@dataclass(slots=True)
class PoolView:
    task: PoolTaskView
    core_rows: list[PoolSnapshotRow]
    standby_rows: list[PoolSnapshotRow]
    evicted_rows: list[PoolSnapshotRow]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_list(raw: str) -> list[str]:
    return [x.strip().upper() for x in (raw or "").split(",") if x.strip() and _STOCK_CODE_RE.match(x.strip().upper())]


def _to_date(value: str | date | None) -> date:
    if isinstance(value, date):
        return value
    if value:
        return date.fromisoformat(value)
    from app.services.trade_calendar import latest_trade_date_str

    return date.fromisoformat(latest_trade_date_str())


def _normalize_filter_params(filter_params: dict[str, Any] | None = None) -> dict[str, Any]:
    out = dict(DEFAULT_FILTER_PARAMS)
    if filter_params:
        out.update(filter_params)
    out["target_pool_size"] = int(out["target_pool_size"])
    out["standby_pool_size"] = int(out["standby_pool_size"])
    out["min_listing_days"] = int(out["min_listing_days"])
    out["min_market_cap_cny"] = float(out["min_market_cap_cny"])
    out["min_avg_amount_20d_cny"] = float(out["min_avg_amount_20d_cny"])
    out["max_single_industry_weight"] = float(out["max_single_industry_weight"])
    return out


def _market_cap_comfort(market_cap_cny: float | int | None) -> float:
    if market_cap_cny is None or market_cap_cny <= 0:
        return 0.0
    cap = float(market_cap_cny)
    if cap < 5_000_000_000:
        return 0.0
    if cap < 100_000_000_000:
        return 0.6
    if cap < 1_000_000_000_000:
        return 0.9
    if cap < 5_000_000_000_000:
        return 0.3
    return 0.2


def _history_map(db: Session, trade_date: date, stock_codes: list[str] | None = None) -> dict[str, dict[str, Any]]:
    params: dict[str, Any] = {"trade_date": trade_date}
    filter_sql = ""
    if stock_codes:
        binds = []
        for idx, code in enumerate(stock_codes):
            key = f"code_{idx}"
            params[key] = code
            binds.append(f":{key}")
        filter_sql = f" AND k.stock_code IN ({', '.join(binds)})"

    rows = db.execute(
        text(
            f"""
            SELECT
                k.stock_code,
                k.close,
                k.ma5,
                k.ma10,
                k.ma20,
                k.amount,
                k.volatility_20d,
                k.turnover_rate,
                k.is_suspended,
                s.stock_name,
                s.industry,
                s.list_date,
                s.circulating_shares,
                s.is_st,
                s.is_delisted
            FROM kline_daily k
            JOIN stock_master s ON s.stock_code = k.stock_code
            WHERE k.trade_date = :trade_date
            {filter_sql}
            """
        ),
        params,
    ).mappings().all()
    return {str(row["stock_code"]): dict(row) for row in rows}


def _build_candidates(db: Session, trade_date: date, params: dict[str, Any] | None = None) -> list[Candidate]:
    cfg = _normalize_filter_params(params)
    history = _history_map(db, trade_date)
    out: list[Candidate] = []
    for code, row in history.items():
        if row.get("is_delisted"):
            continue
        if row.get("is_st"):
            continue
        stock_name = str(row.get("stock_name") or code)
        if _ST_NAMES.search(stock_name):
            continue

        list_date = row.get("list_date")
        if isinstance(list_date, str):
            list_date = date.fromisoformat(list_date)
        if not isinstance(list_date, date):
            continue
        if (trade_date - list_date).days < cfg["min_listing_days"]:
            continue

        close = float(row.get("close") or 0)
        shares = float(row.get("circulating_shares") or 0)
        amount = float(row.get("amount") or 0)
        if close <= 0 or shares <= 0 or amount <= 0:
            continue

        market_cap = close * shares
        if market_cap < cfg["min_market_cap_cny"]:
            continue
        if amount < cfg["min_avg_amount_20d_cny"]:
            continue

        ma5 = float(row.get("ma5") or 0)
        ma10 = float(row.get("ma10") or 0)
        ma20 = float(row.get("ma20") or 0)
        volatility = float(row.get("volatility_20d") or 0)
        turnover = float(row.get("turnover_rate") or 0)

        trend_score = 0.0
        if ma20 > 0:
            trend_score = (close - ma20) / ma20
        momentum_score = 0.0
        if ma10 > 0:
            momentum_score = (ma5 - ma10) / ma10
        liquidity_score = min(1.0, amount / 500_000_000)
        cap_score = _market_cap_comfort(market_cap)
        stability_score = max(0.0, 1.0 - min(volatility, 0.2) / 0.2)
        turnover_score = min(1.0, turnover / 5.0) if turnover > 0 else 0.0

        final_score = (
            trend_score * 30
            + momentum_score * 20
            + liquidity_score * 20
            + cap_score * 15
            + stability_score * 10
            + turnover_score * 5
        )

        out.append(
            Candidate(
                stock_code=code,
                stock_name=stock_name,
                industry=row.get("industry"),
                is_suspended=bool(row.get("is_suspended")),
                score=round(float(final_score), 6),
                factor_values={
                    "trend_score": round(trend_score, 6),
                    "momentum_score": round(momentum_score, 6),
                    "liquidity_score": round(liquidity_score, 6),
                    "cap_score": round(cap_score, 6),
                    "stability_score": round(stability_score, 6),
                    "turnover_score": round(turnover_score, 6),
                },
                low_vol_candidate=volatility <= 0.03,
            )
        )

    out.sort(key=lambda item: (item.score, item.stock_code), reverse=True)
    return out


def _split_core_and_standby(candidates: list[Candidate], params: dict[str, Any] | None = None) -> tuple[list[Candidate], list[Candidate]]:
    cfg = _normalize_filter_params(params)
    target_pool_size = int(cfg["target_pool_size"])
    standby_size = int(cfg["standby_pool_size"])

    sorted_candidates = sorted(candidates, key=lambda item: (item.score, item.stock_code), reverse=True)
    core = list(sorted_candidates[:target_pool_size])
    low_vol_count = sum(1 for item in core if item.low_vol_candidate)
    if low_vol_count < MIN_LOW_VOL_CORE_COUNT:
        low_vol_pool = [item for item in sorted_candidates[target_pool_size:] if item.low_vol_candidate]
        while low_vol_count < MIN_LOW_VOL_CORE_COUNT and low_vol_pool and core:
            promoted = low_vol_pool.pop(0)
            demoted_idx = next((idx for idx in range(len(core) - 1, -1, -1) if not core[idx].low_vol_candidate), None)
            if demoted_idx is None:
                break
            core[demoted_idx] = promoted
            low_vol_count += 1
        core.sort(key=lambda item: (item.score, item.stock_code), reverse=True)

    core_codes = {item.stock_code for item in core}
    standby = [item for item in sorted_candidates if item.stock_code not in core_codes][:standby_size]
    return core, standby


def _load_task_and_rows(db: Session, task_id: str) -> PoolView | None:
    task_t = Base.metadata.tables.get("stock_pool_refresh_task")
    snapshot_t = Base.metadata.tables.get("stock_pool_snapshot")
    if task_t is None or snapshot_t is None:
        return None

    task_row = db.execute(task_t.select().where(task_t.c.task_id == task_id)).mappings().first()
    if not task_row:
        return None

    rows = db.execute(
        snapshot_t.select()
        .where(snapshot_t.c.refresh_task_id == task_id)
        .order_by(snapshot_t.c.pool_role.asc(), snapshot_t.c.rank_no.asc(), snapshot_t.c.stock_code.asc())
    ).mappings().all()

    core_rows = [
        PoolSnapshotRow(
            stock_code=str(row["stock_code"]),
            pool_role=str(row["pool_role"]),
            rank_no=int(row["rank_no"] or 0),
            score=float(row["score"] or 0.0),
            is_suspended=bool(row.get("is_suspended")),
        )
        for row in rows
        if row.get("pool_role") == "core"
    ]
    standby_rows = [
        PoolSnapshotRow(
            stock_code=str(row["stock_code"]),
            pool_role=str(row["pool_role"]),
            rank_no=int(row["rank_no"] or 0),
            score=float(row["score"] or 0.0),
            is_suspended=bool(row.get("is_suspended")),
        )
        for row in rows
        if row.get("pool_role") == "standby"
    ]
    evicted_rows = [
        PoolSnapshotRow(
            stock_code=str(row["stock_code"]),
            pool_role=str(row["pool_role"]),
            rank_no=int(row["rank_no"] or 0),
            score=float(row["score"] or 0.0),
            is_suspended=bool(row.get("is_suspended")),
        )
        for row in rows
        if row.get("pool_role") == "evicted"
    ]

    td = task_row.get("trade_date")
    if isinstance(td, str):
        td = date.fromisoformat(td)
    fb = task_row.get("fallback_from")
    if isinstance(fb, str):
        fb = date.fromisoformat(fb)

    task = PoolTaskView(
        task_id=str(task_row["task_id"]),
        trade_date=td,
        status=str(task_row.get("status") or ""),
        pool_version=int(task_row.get("pool_version") or 0),
        fallback_from=fb,
        status_reason=task_row.get("status_reason"),
        updated_at=task_row.get("updated_at"),
        finished_at=task_row.get("finished_at"),
    )
    return PoolView(task=task, core_rows=core_rows, standby_rows=standby_rows, evicted_rows=evicted_rows)


def _is_valid_runtime_pool_view(view: PoolView | None) -> bool:
    if view is None:
        return False
    status = (view.task.status or "").upper()
    if status not in {"COMPLETED", "FALLBACK"}:
        return False
    if len(view.core_rows) < _MIN_CORE_ROWS_VALID:
        return False
    if len(view.standby_rows) < 1:
        return False
    return True


def _resolve_public_runtime_anchor_view(db: Session, view: PoolView | None) -> PoolView | None:
    if not _is_valid_runtime_pool_view(view):
        return None
    status = (view.task.status or "").upper()
    if status != "FALLBACK":
        return view

    fallback_from = getattr(view.task, "fallback_from", None)
    if fallback_from is None:
        return None

    fallback_view = get_exact_pool_view(db, fallback_from, allow_fallback_as_runtime_anchor=False)
    if _is_valid_runtime_pool_view(fallback_view):
        return fallback_view

    fallback_effective_view = get_effective_pool_view(db, fallback_from)
    if _is_valid_runtime_pool_view(fallback_effective_view):
        return fallback_effective_view
    return None


def get_exact_pool_view(db: Session, trade_date: str | date, allow_fallback_as_runtime_anchor: bool = False) -> PoolView | None:
    td = _to_date(trade_date)
    task_t = Base.metadata.tables.get("stock_pool_refresh_task")
    if task_t is None:
        return None

    task_row = db.execute(
        task_t.select()
        .where(task_t.c.trade_date == td)
        .order_by(task_t.c.created_at.desc())
    ).mappings().first()
    if not task_row:
        return None
    view = _load_task_and_rows(db, str(task_row["task_id"]))
    if view is None:
        return None
    status = (view.task.status or "").upper()
    if status == "FALLBACK" and not allow_fallback_as_runtime_anchor:
        return None
    if status == "COMPLETED":
        from app.services.trade_calendar import latest_trade_date_str

        latest_trade_day = date.fromisoformat(latest_trade_date_str())
        if td >= latest_trade_day:
            eligibility = evaluate_public_task_eligibility(db, view.task.task_id)
            if not eligibility.get("eligible"):
                return None
    return view if _is_valid_runtime_pool_view(view) else None


def get_effective_pool_view(db: Session, trade_date: str | date) -> PoolView | None:
    td = _to_date(trade_date)
    exact = get_exact_pool_view(db, td, allow_fallback_as_runtime_anchor=True)
    if _is_valid_runtime_pool_view(exact):
        return exact

    task_t = Base.metadata.tables.get("stock_pool_refresh_task")
    if task_t is None:
        return None
    rows = db.execute(
        task_t.select()
        .where(task_t.c.trade_date < td)
        .order_by(task_t.c.trade_date.desc(), task_t.c.created_at.desc())
    ).mappings().all()
    for row in rows:
        view = _load_task_and_rows(db, str(row["task_id"]))
        if _is_valid_runtime_pool_view(view):
            return view
    return None


def get_public_pool_view(db: Session, max_trade_date: str | date | None = None) -> PoolView | None:
    task_t = Base.metadata.tables.get("stock_pool_refresh_task")
    if task_t is None:
        return None

    effective_max_trade_date = max_trade_date
    if effective_max_trade_date is None:
        from app.services.trade_calendar import latest_trade_date_str

        effective_max_trade_date = latest_trade_date_str()

    sel = task_t.select()
    if effective_max_trade_date is not None:
        sel = sel.where(task_t.c.trade_date <= _to_date(effective_max_trade_date))
    rows = db.execute(
        sel.order_by(task_t.c.trade_date.desc(), task_t.c.created_at.desc())
    ).mappings().all()
    for idx, row in enumerate(rows):
        raw_view = _load_task_and_rows(db, str(row["task_id"]))
        if raw_view is None:
            continue
        raw_status = (raw_view.task.status or "").upper()

        if raw_status == "COMPLETED":
            # Only hard-gate the newest candidate. Older snapshots can still be
            # used as stable fallback anchors when the latest batch is ineligible.
            if idx == 0:
                eligibility = evaluate_public_task_eligibility(db, raw_view.task.task_id)
                if not eligibility.get("eligible"):
                    continue
            if _is_valid_runtime_pool_view(raw_view):
                return raw_view
            continue

        view = _resolve_public_runtime_anchor_view(db, raw_view)
        if _is_valid_runtime_pool_view(view):
            return view
    return None


def is_excluded_by_isolation(stock_code: str, company_name: str | None) -> bool:
    name = (company_name or "").strip()
    if name and _ST_NAMES.search(name):
        return True
    return False


def filter_by_isolation(stock_codes: list[str], name_resolver: Callable[[str], str | None] | None = None) -> list[str]:
    out: list[str] = []
    for stock_code in stock_codes:
        name = name_resolver(stock_code) if name_resolver else None
        if not is_excluded_by_isolation(stock_code, name):
            out.append(stock_code)
    return out


def get_daily_stock_pool(
    trade_date: str | None = None,
    tier: int | None = None,
    name_resolver: Callable[[str], str | None] | None = None,
    exact_trade_date: bool = False,
    allow_same_day_fallback: bool = False,
) -> list[str]:
    view = None
    db = _resolve_db_session()
    try:
        if trade_date:
            if exact_trade_date:
                view = get_exact_pool_view(db=db, trade_date=trade_date, allow_fallback_as_runtime_anchor=allow_same_day_fallback)
            else:
                view = get_effective_pool_view(db=db, trade_date=trade_date)
        else:
            view = get_public_pool_view(db=db)
    finally:
        db.close()

    if view is not None:
        pool = [row.stock_code for row in sorted(view.core_rows, key=lambda row: ((row.rank_no or 0), row.stock_code))]
    else:
        raw = getattr(settings, "stock_pool", None) or ""
        pool = filter_by_isolation(_parse_list(raw), name_resolver=name_resolver)

    if tier is None:
        return pool
    if tier == 1:
        return pool[:50]
    if tier == 2:
        tier2_pool = pool[50:]
        if not tier2_pool:
            return []
        from app.services.trade_calendar import latest_trade_date_str, trade_days_in_range

        dt = trade_date or latest_trade_date_str()
        ordinal = len(trade_days_in_range("2020-01-01", dt))
        batch_mod = getattr(settings, "tier2_batch_mod", 5)
        return [c for i, c in enumerate(tier2_pool) if (ordinal + i) % batch_mod == 0]
    return pool


def _resolve_db_session() -> Session:
    from app.core.db import SessionLocal

    return SessionLocal()


def refresh_stock_pool(
    db: Session,
    trade_date: str | date | None = None,
    force_rebuild: bool = False,
    request_id: str | None = None,
    filter_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    td = _to_date(trade_date)
    td_key = td.isoformat()
    with _IN_PROGRESS_LOCK:
        if td_key in _IN_PROGRESS:
            raise PoolRefreshConflict(td_key)
        _IN_PROGRESS.add(td_key)

    try:
        task_t = Base.metadata.tables["stock_pool_refresh_task"]
        snapshot_t = Base.metadata.tables["stock_pool_snapshot"]
        score_t = Base.metadata.tables.get("stock_score")
        exact_task_row = db.execute(
            task_t.select()
            .where(task_t.c.trade_date == td)
            .order_by(task_t.c.updated_at.desc(), task_t.c.created_at.desc())
        ).mappings().first()

        if not force_rebuild:
            existing = get_exact_pool_view(db, td, allow_fallback_as_runtime_anchor=True)
            if existing is not None and (existing.task.status or "").upper() != "FALLBACK":
                return {
                    "task_id": existing.task.task_id,
                    "trade_date": td.isoformat(),
                    "status": existing.task.status,
                    "pool_version": existing.task.pool_version,
                    "fallback_from": existing.task.fallback_from.isoformat() if existing.task.fallback_from else None,
                    "status_reason": existing.task.status_reason,
                    "core_pool_size": len(existing.core_rows),
                    "standby_pool_size": len(existing.standby_rows),
                    "evicted_stocks": [],
                }

        params = _normalize_filter_params(filter_params)
        build_failed_reason: str | None = None
        try:
            candidates = _build_candidates(db, td, params)
        except Exception as exc:
            candidates = []
            build_failed_reason = str(exc)
        fallback_view = get_effective_pool_view(db, td)
        fallback_reason: str | None = None

        if build_failed_reason or len(candidates) < int(params["target_pool_size"]):
            fallback_reason = build_failed_reason or "KLINE_COVERAGE_INSUFFICIENT"

        core: list[Candidate] = []
        standby: list[Candidate] = []
        status = "COMPLETED"
        fallback_from: date | None = None

        if fallback_reason:
            if fallback_view is None:
                raise PoolColdStartError("fallback_unavailable")
            status = "FALLBACK"
            fallback_from = fallback_view.task.trade_date
            core = [
                Candidate(
                    stock_code=row.stock_code,
                    stock_name=row.stock_code,
                    industry=None,
                    is_suspended=row.is_suspended,
                    score=row.score,
                    factor_values={},
                )
                for row in fallback_view.core_rows
            ]
            standby = [
                Candidate(
                    stock_code=row.stock_code,
                    stock_name=row.stock_code,
                    industry=None,
                    is_suspended=row.is_suspended,
                    score=row.score,
                    factor_values={},
                )
                for row in fallback_view.standby_rows[:STANDBY_POOL_SIZE]
            ]
        else:
            try:
                core, standby = _split_core_and_standby(candidates, params)
            except Exception as exc:
                build_failed_reason = str(exc)
                if fallback_view is None:
                    raise PoolColdStartError("fallback_unavailable") from exc
                status = "FALLBACK"
                fallback_from = fallback_view.task.trade_date
                core = [
                    Candidate(
                        stock_code=row.stock_code,
                        stock_name=row.stock_code,
                        industry=None,
                        is_suspended=row.is_suspended,
                        score=row.score,
                        factor_values={},
                    )
                    for row in fallback_view.core_rows
                ]
                standby = [
                    Candidate(
                        stock_code=row.stock_code,
                        stock_name=row.stock_code,
                        industry=None,
                        is_suspended=row.is_suspended,
                        score=row.score,
                        factor_values={},
                    )
                    for row in fallback_view.standby_rows[:STANDBY_POOL_SIZE]
                ]

        latest_version = db.execute(text("SELECT COALESCE(MAX(pool_version), 0) FROM stock_pool_refresh_task")).scalar() or 0
        if exact_task_row is not None:
            pool_version = int(exact_task_row.get("pool_version") or (int(latest_version) + 1))
            task_id = str(exact_task_row["task_id"])
        else:
            pool_version = int(latest_version) + 1
            task_id = str(uuid4())
        now = _now_utc()

        prev_view = get_effective_pool_view(db, td)
        prev_core = {row.stock_code for row in (prev_view.core_rows if prev_view else [])}
        new_core = {item.stock_code for item in core}
        evicted_stocks = sorted(prev_core - new_core)
        status_reason = fallback_reason or build_failed_reason

        if exact_task_row is not None:
            db.execute(snapshot_t.delete().where(snapshot_t.c.refresh_task_id == task_id))
            if score_t is not None:
                db.execute(score_t.delete().where(score_t.c.pool_date == td.isoformat()))
            db.execute(
                task_t.update()
                .where(task_t.c.task_id == task_id)
                .values(
                    status=status,
                    pool_version=pool_version,
                    fallback_from=fallback_from,
                    filter_params_json=json.dumps(params, ensure_ascii=False),
                    core_pool_size=len(core),
                    standby_pool_size=len(standby),
                    evicted_stocks_json=json.dumps(evicted_stocks, ensure_ascii=False),
                    status_reason=status_reason,
                    request_id=request_id,
                    started_at=now,
                    finished_at=now,
                    updated_at=now,
                )
            )
        else:
            db.execute(
                task_t.insert().values(
                    task_id=task_id,
                    trade_date=td,
                    status=status,
                    pool_version=pool_version,
                    fallback_from=fallback_from,
                    filter_params_json=json.dumps(params, ensure_ascii=False),
                    core_pool_size=len(core),
                    standby_pool_size=len(standby),
                    evicted_stocks_json=json.dumps(evicted_stocks, ensure_ascii=False),
                    status_reason=status_reason,
                    request_id=request_id,
                    started_at=now,
                    finished_at=now,
                    updated_at=now,
                    created_at=now,
                )
            )

        for rank, candidate in enumerate(core, start=1):
            db.execute(
                snapshot_t.insert().values(
                    pool_snapshot_id=str(uuid4()),
                    refresh_task_id=task_id,
                    trade_date=td,
                    pool_version=pool_version,
                    stock_code=candidate.stock_code,
                    pool_role="core",
                    rank_no=rank,
                    score=round(float(candidate.score), 4),
                    is_suspended=bool(candidate.is_suspended),
                    created_at=now,
                )
            )
        for rank, candidate in enumerate(standby[:STANDBY_POOL_SIZE], start=1):
            db.execute(
                snapshot_t.insert().values(
                    pool_snapshot_id=str(uuid4()),
                    refresh_task_id=task_id,
                    trade_date=td,
                    pool_version=pool_version,
                    stock_code=candidate.stock_code,
                    pool_role="standby",
                    rank_no=rank,
                    score=round(float(candidate.score), 4),
                    is_suspended=bool(candidate.is_suspended),
                    created_at=now,
                )
            )

        if score_t is not None:
            for candidate in core + standby[:STANDBY_POOL_SIZE]:
                fv = candidate.factor_values
                db.execute(
                    score_t.insert().values(
                        score_id=str(uuid4()),
                        pool_date=td.isoformat(),
                        stock_code=candidate.stock_code,
                        score=str(round(float(candidate.score), 6)),
                        factor_momentum=str(round(fv.get("momentum_score", 0.0), 6)),
                        factor_market_cap=str(round(fv.get("cap_score", 0.0), 6)),
                        factor_liquidity=str(round(fv.get("liquidity_score", 0.0), 6)),
                        factor_ma20_slope=str(round(fv.get("trend_score", 0.0), 6)),
                        factor_earnings="0",
                        factor_turnover=str(round(fv.get("turnover_score", 0.0), 6)),
                        factor_rsi="0",
                        factor_52w_high="0",
                        in_core_pool="1" if candidate in core else "0",
                        in_standby_pool="0" if candidate in core else "1",
                        created_at=now.isoformat(),
                    )
                )

        db.commit()
        return {
            "task_id": task_id,
            "trade_date": td.isoformat(),
            "status": status,
            "pool_version": pool_version,
            "fallback_from": fallback_from.isoformat() if fallback_from else None,
            "status_reason": status_reason,
            "core_pool_size": len(core),
            "standby_pool_size": len(standby[:STANDBY_POOL_SIZE]),
            "evicted_stocks": evicted_stocks,
        }
    finally:
        with _IN_PROGRESS_LOCK:
            _IN_PROGRESS.discard(td_key)


# ---------------------------------------------------------------------------
# kline 覆盖率摘要 — 被 ssot_read_model / runtime_anchor_service 调用
# ---------------------------------------------------------------------------

def get_trade_date_kline_coverage_summary(
    db: Session, trade_date: str | date, *, stock_codes: list[str] | None = None,
) -> dict[str, Any] | None:
    """Return kline coverage stats for a given trade_date (or None).

    When *stock_codes* is supplied the universe is scoped to those codes
    rather than the full stock_master table, so that a few extra master entries
    created by a test (or the real universe) don't skew the coverage percentage.
    """
    td = _to_date(trade_date)
    td_str = td.isoformat()

    try:
        kline_t = Base.metadata.tables.get("kline_daily")
        master_t = Base.metadata.tables.get("stock_master")
        if kline_t is None or master_t is None:
            return None

        if stock_codes:
            total_stocks = len(stock_codes)
            if total_stocks == 0:
                return None
            placeholders = ", ".join(f":sc{i}" for i in range(total_stocks))
            params: dict = {f"sc{i}": c for i, c in enumerate(stock_codes)}
            params["td"] = td_str
            covered = db.execute(
                text(
                    f"SELECT COUNT(DISTINCT stock_code) FROM kline_daily "
                    f"WHERE trade_date = :td AND stock_code IN ({placeholders})"
                ),
                params,
            ).scalar() or 0
        else:
            total_stocks = db.execute(
                text("SELECT COUNT(DISTINCT stock_code) FROM stock_master")
            ).scalar() or 0
            if total_stocks == 0:
                return None
            covered = db.execute(
                text("SELECT COUNT(DISTINCT stock_code) FROM kline_daily WHERE trade_date = :td"),
                {"td": td_str},
            ).scalar() or 0

        if covered == 0:
            return None

        coverage_pct = covered / total_stocks if total_stocks > 0 else 0.0
        threshold_pct = 0.80  # 约定 80% 为可用阈值

        return {
            "trade_date": td_str,
            "total_stocks": total_stocks,
            "covered_stocks": covered,
            "missing_stocks": total_stocks - covered,
            "available_count": covered,
            "universe_count": total_stocks,
            "missing_count": total_stocks - covered,
            "coverage_pct": round(coverage_pct, 4),
            "threshold_pct": threshold_pct,
            "sufficient": coverage_pct >= threshold_pct,
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 公共任务资格评估 — 被 runtime_anchor_service 调用
# ---------------------------------------------------------------------------

def evaluate_public_task_eligibility(
    db: Session, task_id: Any,
) -> dict[str, Any]:
    """Evaluate whether a pool-refresh task qualifies as public runtime anchor."""
    view = _load_task_and_rows(db, getattr(task_id, "task_id", task_id))
    if view is None:
        return {"eligible": False, "reason": "task_not_found"}
    status = str(view.task.status or "").upper()
    if status not in ("COMPLETED", "FALLBACK"):
        return {"eligible": False, "reason": f"status_{status.lower()}"}

    task_meta = db.execute(
        text(
            """
            SELECT core_pool_size, standby_pool_size, trade_date
            FROM stock_pool_refresh_task
            WHERE task_id = :task_id
            LIMIT 1
            """
        ),
        {"task_id": view.task.task_id},
    ).mappings().first()

    expected_core = int((task_meta or {}).get("core_pool_size") or 0)
    expected_standby = int((task_meta or {}).get("standby_pool_size") or 0)
    if expected_core > 0 and len(view.core_rows) < expected_core:
        return {"eligible": False, "reason": "core_pool_size_mismatch"}
    if expected_standby > 0 and len(view.standby_rows) < expected_standby:
        return {"eligible": False, "reason": "standby_pool_missing"}

    pool_stock_codes = [r.stock_code for r in view.core_rows]
    coverage_summary = get_trade_date_kline_coverage_summary(
        db, view.task.trade_date, stock_codes=pool_stock_codes or None,
    )
    coverage_pct = coverage_summary.get("coverage_pct") if coverage_summary else None
    if status == "FALLBACK":
        if coverage_summary is not None and not coverage_summary.get("sufficient", True):
            return {
                "eligible": False,
                "reason": "KLINE_COVERAGE_INSUFFICIENT",
                "kline_coverage": coverage_pct,
            }
        task_status_reason = str(getattr(view.task, "status_reason", "") or "").strip()
        return {
            "eligible": False,
            "reason": task_status_reason or "fallback_task_not_runtime_anchor",
            "kline_coverage": coverage_pct,
        }
    if coverage_summary is not None and not coverage_summary.get("sufficient", True):
        return {
            "eligible": False,
            "reason": "KLINE_COVERAGE_INSUFFICIENT",
            "kline_coverage": coverage_pct,
        }
    if len(view.core_rows) < _MIN_CORE_ROWS_VALID:
        return {"eligible": False, "reason": "core_pool_too_small"}
    return {"eligible": True, "reason": None}
