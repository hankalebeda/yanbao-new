from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from math import sqrt
from uuid import uuid4

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models import (
    DataBatch,
    DataBatchError,
    DataBatchLineage,
    DataSourceCircuitState,
    KlineDaily,
    MarketHotspotItem,
    MarketHotspotItemSource,
    MarketHotspotItemStockLink,
    StockMaster,
)
from app.services.usage_lineage import stable_upsert_usage_row

logger = logging.getLogger(__name__)

HOTSPOT_SOURCE_PRIORITY = (
    "eastmoney",
    "xueqiu",
    "cls",
    "baidu_hot",
    "weibo",
    "douyin",
    "kuaishou",
)
QUALITY_FLAGS = {"ok", "stale_ok", "missing", "degraded"}
SUMMARY_STATUS = {"ok", "missing", "degraded"}


@dataclass(frozen=True)
class HotspotContribution:
    source_name: str
    source_rank: int
    topic_title: str
    source_url: str
    fetch_time: datetime
    news_event_type: str | None
    hotspot_tags: tuple[str, ...]
    stock_codes: tuple[str, ...]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_trade_date(value: str | date | None) -> date:
    if isinstance(value, date):
        return value
    if value:
        return date.fromisoformat(value)
    return _now_utc().date()


def _as_datetime(value) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if value is None:
        return _now_utc()
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_float(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _next_batch_seq(db: Session, source_name: str, trade_date: date, batch_scope: str) -> int:
    latest = (
        db.query(DataBatch)
        .filter(
            DataBatch.source_name == source_name,
            DataBatch.trade_date == trade_date,
            DataBatch.batch_scope == batch_scope,
        )
        .order_by(DataBatch.batch_seq.desc())
        .first()
    )
    return 1 if latest is None else int(latest.batch_seq) + 1


def _create_batch(
    db: Session,
    *,
    source_name: str,
    trade_date: date,
    batch_scope: str,
    batch_status: str,
    quality_flag: str,
    covered_stock_count: int | None = None,
    core_pool_covered_count: int | None = None,
    records_total: int | None = None,
    records_success: int | None = None,
    records_failed: int | None = None,
    status_reason: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> DataBatch:
    now = _now_utc()
    batch = DataBatch(
        batch_id=str(uuid4()),
        source_name=source_name,
        trade_date=trade_date,
        batch_scope=batch_scope,
        batch_seq=_next_batch_seq(db, source_name, trade_date, batch_scope),
        batch_status=batch_status,
        quality_flag=quality_flag,
        covered_stock_count=covered_stock_count,
        core_pool_covered_count=core_pool_covered_count,
        records_total=records_total,
        records_success=records_success,
        records_failed=records_failed,
        status_reason=status_reason,
        trigger_task_run_id=None,
        started_at=started_at or now,
        finished_at=finished_at or now,
        updated_at=now,
        created_at=now,
    )
    db.add(batch)
    db.flush()
    return batch


def _create_lineage(db: Session, child_batch_id: str, parent_batch_id: str, lineage_role: str) -> None:
    db.add(
        DataBatchLineage(
            batch_lineage_id=str(uuid4()),
            child_batch_id=child_batch_id,
            parent_batch_id=parent_batch_id,
            lineage_role=lineage_role,
            created_at=_now_utc(),
        )
    )


def _log_batch_error(
    db: Session,
    *,
    batch_id: str,
    stock_code: str | None,
    record_key: str,
    error_stage: str,
    error_code: str,
    error_message: str,
) -> None:
    db.add(
        DataBatchError(
            batch_error_id=str(uuid4()),
            batch_id=batch_id,
            stock_code=stock_code,
            record_key=record_key,
            error_stage=error_stage,
            error_code=error_code,
            error_message=error_message,
            created_at=_now_utc(),
        )
    )


def _load_stock_map(db: Session, stock_codes: list[str] | None) -> dict[str, StockMaster]:
    query = db.query(StockMaster).filter(StockMaster.is_delisted.is_(False))
    if stock_codes:
        query = query.filter(StockMaster.stock_code.in_(stock_codes))
    rows = query.order_by(StockMaster.stock_code.asc()).all()
    return {row.stock_code: row for row in rows}


def _calc_atr_pct(history: list[dict]) -> float | None:
    if len(history) < 2:
        return None
    recent = history[-14:]
    true_ranges: list[float] = []
    previous_close = _as_float(recent[0].get("close"))
    for row in recent[1:]:
        high = _as_float(row.get("high"))
        low = _as_float(row.get("low"))
        close = _as_float(row.get("close"))
        if high is None or low is None or previous_close is None or close is None:
            return None
        true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        previous_close = close
    latest_close = _as_float(recent[-1].get("close"))
    if latest_close in (None, 0) or not true_ranges:
        return None
    # Return as percentage (e.g. 7.93 = 7.93%) to match DB convention
    return round(sum(true_ranges) / len(true_ranges) / latest_close * 100, 6)


def _moving_average(history: list[dict], window: int) -> float | None:
    closes = [_as_float(row.get("close")) for row in history[-window:]]
    valid = [value for value in closes if value is not None]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 4)


def _volatility_20d(history: list[dict]) -> float | None:
    recent = history[-21:]
    closes = [_as_float(row.get("close")) for row in recent]
    if len(closes) < 2 or any(value is None or value <= 0 for value in closes):
        return None
    returns = []
    for previous, current in zip(closes, closes[1:]):
        returns.append((float(current) / float(previous)) - 1.0)
    if not returns:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / len(returns)
    return round(sqrt(variance), 6)


def _normalize_kline_row(stock: StockMaster, history: list[dict]) -> dict:
    latest = history[-1]
    volume = _as_float(latest.get("volume"))
    turnover_rate = None
    shares = _as_float(getattr(stock, "circulating_shares", None))
    if shares and volume is not None:
        turnover_rate = round(volume / shares, 6)
    return {
        "trade_date": _to_trade_date(latest.get("trade_date")),
        "open": round(_as_float(latest.get("open")) or 0, 4),
        "high": round(_as_float(latest.get("high")) or 0, 4),
        "low": round(_as_float(latest.get("low")) or 0, 4),
        "close": round(_as_float(latest.get("close")) or 0, 4),
        "volume": round(volume or 0, 2),
        "amount": round(_as_float(latest.get("amount")) or 0, 2),
        "adjust_type": "front_adjusted",
        "atr_pct": _calc_atr_pct(history),
        "turnover_rate": turnover_rate,
        "ma5": _moving_average(history, 5),
        "ma10": _moving_average(history, 10),
        "ma20": _moving_average(history, 20),
        "ma60": _moving_average(history, 60),
        "volatility_20d": _volatility_20d(history),
        "hs300_return_20d": _as_float(latest.get("hs300_return_20d")),
        "is_suspended": bool(latest.get("is_suspended")),
    }


def _load_previous_kline(db: Session, stock_code: str, trade_date: date) -> KlineDaily | None:
    return (
        db.query(KlineDaily)
        .filter(KlineDaily.stock_code == stock_code, KlineDaily.trade_date < trade_date)
        .order_by(KlineDaily.trade_date.desc())
        .first()
    )


def _upsert_circuit_state(db: Session, source_name: str) -> DataSourceCircuitState:
    state = db.get(DataSourceCircuitState, source_name)
    if state is None:
        state = DataSourceCircuitState(
            source_name=source_name,
            circuit_state="CLOSED",
            consecutive_failures=0,
            circuit_open_at=None,
            cooldown_until=None,
            last_probe_at=None,
            last_failure_reason=None,
            updated_at=_now_utc(),
            created_at=_now_utc(),
        )
        db.add(state)
        db.flush()
    return state


def _source_call_allowed(state: DataSourceCircuitState, now: datetime) -> bool:
    cooldown_until = state.cooldown_until
    if cooldown_until and cooldown_until.tzinfo is None:
        cooldown_until = cooldown_until.replace(tzinfo=timezone.utc)
    if state.circuit_state == "OPEN" and cooldown_until and now < cooldown_until:
        return False
    if state.circuit_state == "OPEN" and (cooldown_until is None or now >= cooldown_until):
        state.circuit_state = "HALF_OPEN"
        state.last_probe_at = now
        state.updated_at = now
    return True


def _record_source_success(state: DataSourceCircuitState, now: datetime) -> bool:
    was_open = state.circuit_state in {"OPEN", "HALF_OPEN"}
    state.circuit_state = "CLOSED"
    state.consecutive_failures = 0
    state.circuit_open_at = None
    state.cooldown_until = None
    state.last_probe_at = now
    state.last_failure_reason = None
    state.updated_at = now
    return was_open


def _record_source_failure(state: DataSourceCircuitState, now: datetime, reason: str) -> bool:
    was_open = state.circuit_state == "OPEN"
    if state.circuit_state == "HALF_OPEN":
        state.consecutive_failures = int(getattr(settings, "source_fail_open_threshold", 3))
    else:
        state.consecutive_failures = int(state.consecutive_failures or 0) + 1
    state.last_failure_reason = reason
    if state.consecutive_failures >= int(getattr(settings, "source_fail_open_threshold", 3)):
        state.circuit_state = "OPEN"
        state.circuit_open_at = now
        state.cooldown_until = now + timedelta(seconds=int(getattr(settings, "source_circuit_cooldown_seconds", 300)))
    else:
        state.circuit_state = "CLOSED"
    state.updated_at = now
    return (not was_open) and state.circuit_state == "OPEN"


def _emit_circuit_breaker_alert(
    *,
    alert_type: str,
    source_name: str,
    source_kind: str,
    state: DataSourceCircuitState,
    reason: str | None,
    now: datetime,
) -> None:
    try:
        from app.services.notification import emit_operational_alert

        status, status_reason, channel = emit_operational_alert(
            alert_type=alert_type,
            fr_id="FR-04",
            message=f"{source_kind} source {source_name} {alert_type.lower()}",
            payload={
                "source_name": source_name,
                "source_kind": source_kind,
                "circuit_state": state.circuit_state,
                "consecutive_failures": int(state.consecutive_failures or 0),
                "last_failure_reason": reason or state.last_failure_reason,
            },
            timestamp=now,
        )
        logger.warning(
            "circuit_alert source=%s kind=%s alert_type=%s status=%s channel=%s reason=%s",
            source_name,
            source_kind,
            alert_type,
            status,
            channel,
            status_reason,
        )
    except Exception:
        logger.exception(
            "circuit_alert_dispatch_failed source=%s kind=%s alert_type=%s",
            source_name,
            source_kind,
            alert_type,
        )


def _overall_kline_quality(
    total_stocks: int,
    covered_count: int,
    core_pool_covered_count: int,
    core_pool_size: int,
    fallback_count: int,
) -> tuple[str, str | None]:
    if core_pool_covered_count < core_pool_size:
        return "degraded", "core_pool_coverage_missing"
    if total_stocks <= 0:
        return "degraded", "empty_universe"
    coverage_ratio = covered_count / total_stocks
    if fallback_count > 0 and coverage_ratio >= 0.95:
        return "stale_ok", "fallback_t_minus_1"
    if coverage_ratio >= 0.95:
        return ("ok", None) if covered_count == total_stocks else ("stale_ok", "partial_commit")
    return "degraded", "coverage_below_threshold"


def _merge_hotspots(source_results: dict[str, list[HotspotContribution]]) -> list[dict]:
    grouped: dict[str, list[HotspotContribution]] = {}
    for contributions in source_results.values():
        for item in contributions:
            grouped.setdefault(item.topic_title.strip().lower(), []).append(item)

    merged_items: list[dict] = []
    for merge_key, contributions in grouped.items():
        primary = min(
            contributions,
            key=lambda item: (item.source_rank, HOTSPOT_SOURCE_PRIORITY.index(item.source_name), item.topic_title),
        )
        tags: list[str] = []
        stock_codes: list[str] = []
        for contribution in contributions:
            for tag in contribution.hotspot_tags:
                if tag not in tags:
                    tags.append(tag)
            for stock_code in contribution.stock_codes:
                if stock_code not in stock_codes:
                    stock_codes.append(stock_code)
        merged_items.append(
            {
                "merge_key": merge_key,
                "topic_title": primary.topic_title,
                "source_name": primary.source_name,
                "source_rank": primary.source_rank,
                "source_url": primary.source_url,
                "fetch_time": primary.fetch_time,
                "news_event_type": primary.news_event_type,
                "hotspot_tags": tags,
                "stock_codes": stock_codes,
                "contributions": contributions,
            }
        )

    merged_items.sort(
        key=lambda item: (
            item["source_rank"],
            HOTSPOT_SOURCE_PRIORITY.index(item["source_name"]),
            item["topic_title"],
        )
    )
    return merged_items[:50]


def _create_usage_row(
    db: Session,
    *,
    trade_date: date,
    stock_code: str,
    dataset_name: str,
    source_name: str,
    batch_id: str,
    fetch_time: datetime,
    status: str,
    status_reason: str | None = None,
) -> None:
    if status not in QUALITY_FLAGS:
        raise ValueError(f"invalid_usage_status:{status}")
    if dataset_name in {"northbound_summary", "etf_flow_summary"} and status not in SUMMARY_STATUS:
        raise ValueError(f"invalid_summary_status:{status}")
    stable_upsert_usage_row(
        db,
        trade_date=trade_date,
        stock_code=stock_code,
        dataset_name=dataset_name,
        source_name=source_name,
        batch_id=batch_id,
        fetch_time=fetch_time,
        status=status,
        status_reason=status_reason,
        created_at=_now_utc(),
    )


def ingest_market_data(
    db: Session,
    *,
    trade_date: str | date | None = None,
    stock_codes: list[str] | None = None,
    core_pool_codes: list[str] | None = None,
    kline_source_name: str = "tdx_local",
    fetch_kline_history=None,
    fetch_hotspot_by_source=None,
    fetch_northbound_summary=None,
    fetch_etf_flow_summary=None,
    now: datetime | None = None,
) -> dict:
    target_date = _to_trade_date(trade_date)
    now = _as_datetime(now)
    core_pool_codes = list(core_pool_codes or [])
    stock_map = _load_stock_map(db, stock_codes)
    universe_codes = list(stock_map)
    total_stocks = len(universe_codes)

    if fetch_kline_history is None and not universe_codes:
        pass  # no kline fetcher and no stocks: skip kline block entirely

    # Guard: if fetcher not provided but stocks exist, skip kline loop (P0-09 fix)
    _skip_kline_loop = fetch_kline_history is None and bool(universe_codes)

    if universe_codes and not _skip_kline_loop:
        (
            db.query(KlineDaily)
            .filter(KlineDaily.trade_date == target_date, KlineDaily.stock_code.in_(universe_codes))
            .delete(synchronize_session=False)
        )

    kline_batch = _create_batch(
        db,
        source_name=kline_source_name,
        trade_date=target_date,
        batch_scope="full_market",
        batch_status="RUNNING",
        quality_flag="ok",
        started_at=now,
        finished_at=now,
    )

    covered_count = 0
    core_pool_covered_count = 0
    failed_count = 0
    fallback_count = 0
    fallback_parent_ids: set[str] = set()

    for stock_code in (universe_codes if not _skip_kline_loop else []):
        stock = stock_map[stock_code]
        try:
            history = fetch_kline_history(stock_code, target_date)
            if not history:
                raise RuntimeError("empty_history")
            normalized = _normalize_kline_row(stock, history)
            db.add(
                KlineDaily(
                    kline_id=str(uuid4()),
                    stock_code=stock_code,
                    trade_date=target_date,
                    open=normalized["open"],
                    high=normalized["high"],
                    low=normalized["low"],
                    close=normalized["close"],
                    volume=normalized["volume"],
                    amount=normalized["amount"],
                    adjust_type=normalized["adjust_type"],
                    atr_pct=normalized["atr_pct"],
                    turnover_rate=normalized["turnover_rate"],
                    ma5=normalized["ma5"],
                    ma10=normalized["ma10"],
                    ma20=normalized["ma20"],
                    ma60=normalized["ma60"],
                    volatility_20d=normalized["volatility_20d"],
                    hs300_return_20d=normalized["hs300_return_20d"],
                    is_suspended=normalized["is_suspended"],
                    source_batch_id=kline_batch.batch_id,
                    created_at=_now_utc(),
                )
            )
            _create_usage_row(
                db,
                trade_date=target_date,
                stock_code=stock_code,
                dataset_name="kline_daily",
                source_name=kline_source_name,
                batch_id=kline_batch.batch_id,
                fetch_time=now,
                status="ok",
            )
            covered_count += 1
            if stock_code in core_pool_codes:
                core_pool_covered_count += 1
        except Exception as exc:
            previous = _load_previous_kline(db, stock_code, target_date) if stock_code in core_pool_codes else None
            if previous is not None:
                db.add(
                    KlineDaily(
                        kline_id=str(uuid4()),
                        stock_code=stock_code,
                        trade_date=target_date,
                        open=previous.open,
                        high=previous.high,
                        low=previous.low,
                        close=previous.close,
                        volume=previous.volume,
                        amount=previous.amount,
                        adjust_type=previous.adjust_type,
                        atr_pct=previous.atr_pct,
                        turnover_rate=previous.turnover_rate,
                        ma5=previous.ma5,
                        ma10=previous.ma10,
                        ma20=previous.ma20,
                        ma60=previous.ma60,
                        volatility_20d=previous.volatility_20d,
                        hs300_return_20d=previous.hs300_return_20d,
                        is_suspended=previous.is_suspended,
                        source_batch_id=kline_batch.batch_id,
                        created_at=_now_utc(),
                    )
                )
                fallback_parent_ids.add(previous.source_batch_id)
                _create_usage_row(
                    db,
                    trade_date=target_date,
                    stock_code=stock_code,
                    dataset_name="kline_daily",
                    source_name=kline_source_name,
                    batch_id=kline_batch.batch_id,
                    fetch_time=now,
                    status="stale_ok",
                    status_reason="fallback_t_minus_1",
                )
                fallback_count += 1
                covered_count += 1
                core_pool_covered_count += 1
            else:
                failed_count += 1
                _log_batch_error(
                    db,
                    batch_id=kline_batch.batch_id,
                    stock_code=stock_code,
                    record_key=f"{stock_code}:{target_date.isoformat()}",
                    error_stage="collect",
                    error_code="KLINE_FETCH_FAILED",
                    error_message=str(exc),
                )

    for parent_batch_id in fallback_parent_ids:
        _create_lineage(db, kline_batch.batch_id, parent_batch_id, "FALLBACK_FROM")

    kline_quality_flag, kline_reason = _overall_kline_quality(
        total_stocks, covered_count, core_pool_covered_count, len(core_pool_codes), fallback_count
    )
    # P0-09: do not mark SUCCESS when fetcher was missing or all records failed
    if _skip_kline_loop:
        kline_batch.batch_status = "PARTIAL_SUCCESS"
        kline_quality_flag = "degraded"
        kline_reason = kline_reason or "fetcher_not_provided"
    elif covered_count == 0 and total_stocks > 0:
        kline_batch.batch_status = "FAILED"
    else:
        kline_batch.batch_status = "SUCCESS" if failed_count == 0 else "PARTIAL_SUCCESS"
    kline_batch.quality_flag = kline_quality_flag
    kline_batch.covered_stock_count = covered_count
    kline_batch.core_pool_covered_count = core_pool_covered_count
    kline_batch.records_total = total_stocks
    kline_batch.records_success = covered_count
    kline_batch.records_failed = failed_count
    kline_batch.status_reason = kline_reason
    kline_batch.finished_at = _now_utc()
    kline_batch.updated_at = _now_utc()

    hotspot_source_batches: dict[str, DataBatch] = {}
    hotspot_source_results: dict[str, list[HotspotContribution]] = {}
    for source_name in HOTSPOT_SOURCE_PRIORITY:
        state = _upsert_circuit_state(db, source_name)
        if not _source_call_allowed(state, now):
            hotspot_source_batches[source_name] = _create_batch(
                db,
                source_name=source_name,
                trade_date=target_date,
                batch_scope="full_market",
                batch_status="FAILED",
                quality_flag="degraded",
                records_total=0,
                records_success=0,
                records_failed=0,
                status_reason="circuit_open",
                started_at=now,
                finished_at=now,
            )
            continue

        if fetch_hotspot_by_source is None:
            hotspot_source_batches[source_name] = _create_batch(
                db,
                source_name=source_name,
                trade_date=target_date,
                batch_scope="full_market",
                batch_status="FAILED",
                quality_flag="missing",
                records_total=0,
                records_success=0,
                records_failed=0,
                status_reason="fetcher_not_provided",
                started_at=now,
                finished_at=now,
            )
            continue

        try:
            raw_items = fetch_hotspot_by_source(source_name, target_date)
            contributions = [
                HotspotContribution(
                    source_name=source_name,
                    source_rank=int(item.get("rank") or item.get("source_rank") or index),
                    topic_title=str(item.get("topic_title") or ""),
                    source_url=str(item.get("source_url") or ""),
                    fetch_time=_as_datetime(item.get("fetch_time") or now),
                    news_event_type=item.get("news_event_type"),
                    hotspot_tags=tuple(item.get("hotspot_tags") or []),
                    stock_codes=tuple(item.get("stock_codes") or []),
                )
                for index, item in enumerate(raw_items or [], start=1)
                if item.get("topic_title") and item.get("source_url")
            ]
            circuit_closed = _record_source_success(state, now)
            if circuit_closed:
                _emit_circuit_breaker_alert(
                    alert_type="CIRCUIT_BREAKER_CLOSE",
                    source_name=source_name,
                    source_kind="hotspot",
                    state=state,
                    reason=None,
                    now=now,
                )
            hotspot_source_results[source_name] = contributions
            batch_ok = len(contributions) > 0
            hotspot_source_batches[source_name] = _create_batch(
                db,
                source_name=source_name,
                trade_date=target_date,
                batch_scope="full_market",
                batch_status="SUCCESS" if batch_ok else "PARTIAL_SUCCESS",
                quality_flag="ok" if batch_ok else "degraded",
                records_total=len(contributions),
                records_success=len(contributions),
                records_failed=0,
                status_reason=None if batch_ok else "no_items_fetched",
                started_at=now,
                finished_at=now,
            )
        except Exception as exc:
            circuit_opened = _record_source_failure(state, now, str(exc))
            if circuit_opened:
                _emit_circuit_breaker_alert(
                    alert_type="CIRCUIT_BREAKER_OPEN",
                    source_name=source_name,
                    source_kind="hotspot",
                    state=state,
                    reason=str(exc),
                    now=now,
                )
            hotspot_source_batches[source_name] = _create_batch(
                db,
                source_name=source_name,
                trade_date=target_date,
                batch_scope="full_market",
                batch_status="FAILED",
                quality_flag="degraded",
                records_total=0,
                records_success=0,
                records_failed=1,
                status_reason=str(exc),
                started_at=now,
                finished_at=now,
            )
        db.flush()

    successful_sources = [name for name in HOTSPOT_SOURCE_PRIORITY if hotspot_source_results.get(name)]
    if len(successful_sources) >= 3:
        hotspot_quality_flag = "ok"
    elif successful_sources:
        hotspot_quality_flag = "stale_ok"
    else:
        hotspot_quality_flag = "degraded"

    merged_items = _merge_hotspots(hotspot_source_results)
    persistable_hotspots: list[dict] = []
    linked_stock_codes: set[str] = set()
    hotspot_topics_by_stock: dict[str, list[str]] = {}
    for item in merged_items:
        valid_stock_codes: list[str] = []
        for stock_code in item["stock_codes"]:
            if stock_code not in stock_map or stock_code in valid_stock_codes:
                continue
            valid_stock_codes.append(stock_code)
        if not valid_stock_codes:
            continue
        persisted_item = dict(item)
        persisted_item["stock_codes"] = valid_stock_codes
        persistable_hotspots.append(persisted_item)
        for stock_code in valid_stock_codes:
            linked_stock_codes.add(stock_code)
            hotspot_topics_by_stock.setdefault(stock_code, []).append(item["topic_title"])

    primary_source = successful_sources[0] if successful_sources else "eastmoney"
    hotspot_status_reason = None if persistable_hotspots else (
        "no_linked_hotspot_topics" if merged_items else "all_hotspot_sources_unavailable"
    )
    hotspot_batch = _create_batch(
        db,
        source_name=primary_source,
        trade_date=target_date,
        batch_scope="hotspot_merged",
        batch_status="SUCCESS" if persistable_hotspots else "FAILED",
        quality_flag=hotspot_quality_flag if persistable_hotspots else "missing",
        covered_stock_count=len(linked_stock_codes),
        core_pool_covered_count=len(linked_stock_codes.intersection(core_pool_codes)),
        records_total=len(persistable_hotspots),
        records_success=len(persistable_hotspots),
        records_failed=0 if persistable_hotspots else max(1, len(merged_items)),
        status_reason=hotspot_status_reason,
        started_at=now,
        finished_at=now,
    )
    for source_name in successful_sources:
        _create_lineage(db, hotspot_batch.batch_id, hotspot_source_batches[source_name].batch_id, "MERGED_FROM")

    hotspot_top50: list[dict] = []
    for rank, item in enumerate(persistable_hotspots, start=1):
        hotspot_item = MarketHotspotItem(
            hotspot_item_id=str(uuid4()),
            batch_id=hotspot_batch.batch_id,
            source_name=item["source_name"],
            merged_rank=rank,
            source_rank=item["source_rank"],
            topic_title=item["topic_title"],
            news_event_type=item["news_event_type"],
            hotspot_tags_json=item["hotspot_tags"],
            source_url=item["source_url"],
            fetch_time=item["fetch_time"],
            quality_flag=hotspot_quality_flag,
            created_at=_now_utc(),
        )
        db.add(hotspot_item)
        db.flush()
        for contribution in item["contributions"]:
            db.add(
                MarketHotspotItemSource(
                    hotspot_item_source_id=str(uuid4()),
                    hotspot_item_id=hotspot_item.hotspot_item_id,
                    batch_id=hotspot_source_batches[contribution.source_name].batch_id,
                    source_name=contribution.source_name,
                    source_rank=contribution.source_rank,
                    source_url=contribution.source_url,
                    fetch_time=contribution.fetch_time,
                    quality_flag="ok",
                    created_at=_now_utc(),
                )
            )
        for stock_code in item["stock_codes"]:
            db.add(
                MarketHotspotItemStockLink(
                    hotspot_item_stock_link_id=str(uuid4()),
                    hotspot_item_id=hotspot_item.hotspot_item_id,
                    stock_code=stock_code,
                    relation_role="primary",
                    match_confidence=0.95,
                    created_at=_now_utc(),
                )
            )

        hotspot_top50.append(
            {
                "rank": rank,
                "topic_title": item["topic_title"],
                "source_name": item["source_name"],
                "source_url": item["source_url"],
                "fetch_time": item["fetch_time"].isoformat(),
                "quality_flag": hotspot_quality_flag,
            }
        )

    hotspot_usage_codes = set(core_pool_codes) | linked_stock_codes
    for stock_code in sorted(hotspot_usage_codes):
        linked_topics = hotspot_topics_by_stock.get(stock_code) or []
        _create_usage_row(
            db,
            trade_date=target_date,
            stock_code=stock_code,
            dataset_name="hotspot_top50",
            source_name=primary_source,
            batch_id=hotspot_batch.batch_id,
            fetch_time=now,
            status=hotspot_quality_flag if linked_topics else "missing",
            status_reason=None if linked_topics else "no_hotspot_link",
        )

    def _collect_summary(source_name: str, fetcher, dataset_name: str):
        if fetcher is None:
            batch = _create_batch(
                db,
                source_name=source_name,
                trade_date=target_date,
                batch_scope="summary",
                batch_status="FAILED",
                quality_flag="missing",
                records_total=0,
                records_success=0,
                records_failed=0,
                status_reason="fetcher_not_provided",
                started_at=now,
                finished_at=now,
            )
            summary = {"status": "missing", "reason": "fetcher_not_provided", "fetch_time": now.isoformat()}
            for stock_code in core_pool_codes:
                _create_usage_row(
                    db,
                    trade_date=target_date,
                    stock_code=stock_code,
                    dataset_name=dataset_name,
                    source_name=source_name,
                    batch_id=batch.batch_id,
                    fetch_time=now,
                    status="missing",
                    status_reason="fetcher_not_provided",
                )
            return summary
        try:
            payload = fetcher(target_date)
            if not payload:
                status = "missing"
                reason = f"{source_name}_missing"
                batch = _create_batch(
                    db,
                    source_name=source_name,
                    trade_date=target_date,
                    batch_scope="summary",
                    batch_status="FAILED",
                    quality_flag="missing",
                    records_total=1,
                    records_success=0,
                    records_failed=1,
                    status_reason=reason,
                    started_at=now,
                    finished_at=now,
                )
                summary = {"status": status, "reason": reason, "fetch_time": now.isoformat()}
            else:
                status = str(payload.get("status") or "ok")
                if status not in SUMMARY_STATUS:
                    status = "degraded"
                reason = payload.get("reason")
                batch = _create_batch(
                    db,
                    source_name=source_name,
                    trade_date=target_date,
                    batch_scope="summary",
                    batch_status="SUCCESS",
                    quality_flag=status,
                    records_total=1,
                    records_success=1,
                    records_failed=0,
                    status_reason=reason,
                    started_at=now,
                    finished_at=now,
                )
                summary = dict(payload)
                summary["status"] = status
                summary["reason"] = reason
                summary["fetch_time"] = _as_datetime(payload.get("fetch_time") or now).isoformat()
        except Exception as exc:
            status = "degraded"
            reason = str(exc)
            batch = _create_batch(
                db,
                source_name=source_name,
                trade_date=target_date,
                batch_scope="summary",
                batch_status="FAILED",
                quality_flag="degraded",
                records_total=1,
                records_success=0,
                records_failed=1,
                status_reason=reason,
                started_at=now,
                finished_at=now,
            )
            summary = {"status": status, "reason": reason, "fetch_time": now.isoformat()}

        for stock_code in core_pool_codes:
            _create_usage_row(
                db,
                trade_date=target_date,
                stock_code=stock_code,
                dataset_name=dataset_name,
                source_name=source_name,
                batch_id=batch.batch_id,
                fetch_time=_as_datetime(summary["fetch_time"]),
                status=summary["status"],
                status_reason=summary.get("reason"),
            )
        return summary

    northbound_summary = _collect_summary("northbound", fetch_northbound_summary, "northbound_summary")
    etf_flow_summary = _collect_summary("etf_flow", fetch_etf_flow_summary, "etf_flow_summary")

    overall_quality_flag = "ok"
    reasons: list[str] = []
    if kline_quality_flag == "degraded" or hotspot_quality_flag == "degraded":
        overall_quality_flag = "degraded"
    elif kline_quality_flag != "ok" or hotspot_quality_flag != "ok":
        overall_quality_flag = "stale_ok"
    if northbound_summary["status"] != "ok" or etf_flow_summary["status"] != "ok":
        overall_quality_flag = "stale_ok" if overall_quality_flag == "ok" else overall_quality_flag
    if kline_reason:
        reasons.append(kline_reason)
    if hotspot_quality_flag != "ok":
        reasons.append("hotspot_quality_degraded")
    if northbound_summary["status"] != "ok":
        reasons.append(str(northbound_summary.get("reason") or "northbound_not_ok"))
    if etf_flow_summary["status"] != "ok":
        reasons.append(str(etf_flow_summary.get("reason") or "etf_flow_not_ok"))

    db.commit()
    return {
        "trade_date": target_date.isoformat(),
        "total_stocks": total_stocks,
        "covered_count": covered_count,
        "core_pool_covered_count": core_pool_covered_count,
        "quality_flag": overall_quality_flag,
        "status_reason": ";".join(reasons) if reasons else None,
        "hotspot_top50": hotspot_top50,
        "northbound_summary": northbound_summary,
        "etf_flow_summary": etf_flow_summary,
        "batch_ids": {"kline": kline_batch.batch_id, "hotspot": hotspot_batch.batch_id},
    }


def backfill_missing_kline_daily(
    db: Session,
    *,
    trade_date: str | date | None = None,
    stock_codes: list[str] | None = None,
    history_limit: int = 120,
    concurrency: int = 24,
    source_name: str = "eastmoney",
    fetch_recent_klines_async=None,
    now: datetime | None = None,
) -> dict:
    target_date = _to_trade_date(trade_date)
    now = _as_datetime(now)
    stock_map = _load_stock_map(db, stock_codes)
    universe_codes = list(stock_map)
    existing_codes = {
        row[0]
        for row in (
            db.query(KlineDaily.stock_code)
            .filter(KlineDaily.trade_date == target_date, KlineDaily.stock_code.in_(universe_codes))
            .all()
        )
    }
    missing_codes = [code for code in universe_codes if code not in existing_codes]

    batch = _create_batch(
        db,
        source_name=source_name,
        trade_date=target_date,
        batch_scope="backfill_missing",
        batch_status="RUNNING",
        quality_flag="ok",
        started_at=now,
        finished_at=now,
    )

    if not missing_codes:
        batch.batch_status = "SUCCESS"
        batch.covered_stock_count = 0
        batch.core_pool_covered_count = 0
        batch.records_total = 0
        batch.records_success = 0
        batch.records_failed = 0
        batch.status_reason = None
        batch.finished_at = _now_utc()
        batch.updated_at = _now_utc()
        db.commit()
        return {
            "trade_date": target_date.isoformat(),
            "source_name": source_name,
            "batch_id": batch.batch_id,
            "candidate_count": 0,
            "inserted_count": 0,
            "failed_count": 0,
            "skipped_existing_count": len(existing_codes),
            "quality_flag": "ok",
            "status_reason": None,
        }

    if fetch_recent_klines_async is None:
        from app.services.market_data import fetch_recent_klines as fetch_recent_klines_async

    target_date_str = target_date.isoformat()

    async def _collect_all() -> list[tuple[str, list[dict] | None, str | None]]:
        semaphore = asyncio.Semaphore(max(1, concurrency))

        async def _collect_one(stock_code: str) -> tuple[str, list[dict] | None, str | None]:
            async with semaphore:
                try:
                    rows = await fetch_recent_klines_async(stock_code, limit=max(60, int(history_limit or 120)))
                except Exception as exc:
                    return stock_code, None, str(exc)
            history = [
                {
                    "trade_date": str(item.get("date") or "")[:10],
                    "open": item.get("open"),
                    "high": item.get("high"),
                    "low": item.get("low"),
                    "close": item.get("close"),
                    "volume": item.get("volume"),
                    "amount": item.get("amount"),
                }
                for item in (rows or [])
                if str(item.get("date") or "")[:10] <= target_date_str
            ]
            if not history:
                return stock_code, None, "empty_history"
            if history[-1].get("trade_date") != target_date_str:
                return stock_code, None, "target_trade_date_missing"
            return stock_code, history, None

        return await asyncio.gather(*[_collect_one(stock_code) for stock_code in missing_codes])

    results = asyncio.run(_collect_all())

    inserted_count = 0
    failed_count = 0
    for stock_code, history, error in results:
        if error or not history:
            failed_count += 1
            _log_batch_error(
                db,
                batch_id=batch.batch_id,
                stock_code=stock_code,
                record_key=f"{stock_code}:{target_date_str}",
                error_stage="backfill_missing",
                error_code="KLINE_FETCH_FAILED",
                error_message=error or "empty_history",
            )
            continue

        normalized = _normalize_kline_row(stock_map[stock_code], history)
        db.add(
            KlineDaily(
                kline_id=str(uuid4()),
                stock_code=stock_code,
                trade_date=target_date,
                open=normalized["open"],
                high=normalized["high"],
                low=normalized["low"],
                close=normalized["close"],
                volume=normalized["volume"],
                amount=normalized["amount"],
                adjust_type=normalized["adjust_type"],
                atr_pct=normalized["atr_pct"],
                turnover_rate=normalized["turnover_rate"],
                ma5=normalized["ma5"],
                ma10=normalized["ma10"],
                ma20=normalized["ma20"],
                ma60=normalized["ma60"],
                volatility_20d=normalized["volatility_20d"],
                hs300_return_20d=normalized["hs300_return_20d"],
                is_suspended=normalized["is_suspended"],
                source_batch_id=batch.batch_id,
                created_at=_now_utc(),
            )
        )
        _create_usage_row(
            db,
            trade_date=target_date,
            stock_code=stock_code,
            dataset_name="kline_daily",
            source_name=source_name,
            batch_id=batch.batch_id,
            fetch_time=now,
            status="ok",
        )
        inserted_count += 1

    quality_flag, status_reason = _overall_kline_quality(len(missing_codes), inserted_count, 0, 0, 0)
    batch.batch_status = "SUCCESS" if failed_count == 0 else "PARTIAL_SUCCESS"
    batch.quality_flag = quality_flag
    batch.covered_stock_count = inserted_count
    batch.core_pool_covered_count = 0
    batch.records_total = len(missing_codes)
    batch.records_success = inserted_count
    batch.records_failed = failed_count
    batch.status_reason = status_reason
    batch.finished_at = _now_utc()
    batch.updated_at = _now_utc()
    db.commit()
    return {
        "trade_date": target_date.isoformat(),
        "source_name": source_name,
        "batch_id": batch.batch_id,
        "candidate_count": len(missing_codes),
        "inserted_count": inserted_count,
        "failed_count": failed_count,
        "skipped_existing_count": len(existing_codes),
        "quality_flag": quality_flag,
        "status_reason": status_reason,
    }
