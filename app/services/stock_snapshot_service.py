from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.models import Base
from app.services.capital_usage_collector import persist_capital_usage
from app.services.capital_flow import fetch_capital_dimensions
from app.services.company_data import (
    fetch_company_overview,
    fetch_industry_competition,
    fetch_valuation_snapshot,
)
from app.services.etf_flow_data import fetch_etf_flow_summary_global
from app.services.multisource_ingest import _create_batch
from app.services.news_policy import fetch_policy_news, fetch_stock_news
from app.services.northbound_data import fetch_northbound_summary
from app.services.stock_profile_collector import persist_stock_profile
from app.services.tdx_local_data import build_tdx_local_features
from app.services.trade_calendar import latest_trade_date_str
from app.services.usage_lineage import infer_usage_batch_id, stable_upsert_usage_row


_SUCCESS_USAGE_STATUSES = {"ok", "stale_ok", "proxy_ok", "realtime_only"}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text_value = str(value).strip()
    return text_value or None


def _iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    text_value = str(value).strip()
    return text_value or None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text_value = str(value).strip()
        if not text_value:
            return None
        return float(text_value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _window_return(rows_desc: list[dict[str, Any]], window_days: int) -> float | None:
    if len(rows_desc) <= window_days:
        return None
    latest = _to_float(rows_desc[0].get("close"))
    base = _to_float(rows_desc[window_days].get("close"))
    if latest is None or base in (None, 0):
        return None
    return round((latest - base) / base * 100.0, 4)


def _load_kline_rows(db: Session, *, stock_code: str, trade_day: date, limit: int = 21) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in db.execute(
            text(
                """
                SELECT trade_date, open, high, low, close, volume, amount,
                       turnover_rate, ma5, ma10, ma20, ma60, atr_pct,
                       volatility_20d, hs300_return_20d, is_suspended
                FROM kline_daily
                WHERE stock_code = :stock_code AND trade_date <= :trade_date
                ORDER BY trade_date DESC
                LIMIT :row_limit
                """
            ),
            {"stock_code": stock_code, "trade_date": trade_day, "row_limit": limit},
        ).mappings().all()
    ]


def _build_market_snapshot(rows_desc: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows_desc:
        return {}
    latest = rows_desc[0]
    previous = rows_desc[1] if len(rows_desc) > 1 else None
    close = _to_float(latest.get("close"))
    prev_close = _to_float(previous.get("close")) if previous else None
    pct_change = None
    if close is not None and prev_close not in (None, 0):
        pct_change = round((close - prev_close) / prev_close * 100.0, 4)
    return {
        "trade_date": _iso_date(latest.get("trade_date")),
        "last_price": close,
        "prev_close": prev_close,
        "pct_change": pct_change,
        "open": _to_float(latest.get("open")),
        "high": _to_float(latest.get("high")),
        "low": _to_float(latest.get("low")),
        "volume": _to_float(latest.get("volume")),
        "amount": _to_float(latest.get("amount")),
        "turnover_rate": _to_float(latest.get("turnover_rate")),
        "ma5": _to_float(latest.get("ma5")),
        "ma10": _to_float(latest.get("ma10")),
        "ma20": _to_float(latest.get("ma20")),
        "ma60": _to_float(latest.get("ma60")),
        "atr_pct": _to_float(latest.get("atr_pct")),
        "volatility_20d": _to_float(latest.get("volatility_20d")),
        "hs300_return_20d": _to_float(latest.get("hs300_return_20d")),
        "ret5": _window_return(rows_desc, 5),
        "ret20": _window_return(rows_desc, 20),
        "is_suspended": bool(latest.get("is_suspended")),
        "sample_days": len(rows_desc),
    }


def _load_stock_master_row(db: Session, *, stock_code: str) -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT stock_code, stock_name, industry, exchange, list_date,
                   circulating_shares, is_st, is_suspended
            FROM stock_master
            WHERE stock_code = :stock_code
            LIMIT 1
            """
        ),
        {"stock_code": stock_code},
    ).mappings().first()
    return dict(row) if row else None


def _load_market_state_row(db: Session, *, trade_day: date) -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT trade_date, reference_date, market_state, state_reason,
                   cache_status, market_state_degraded,
                   a_type_pct, b_type_pct, c_type_pct,
                   kline_batch_id, hotspot_batch_id, computed_at
            FROM market_state_cache
            WHERE trade_date <= :trade_date
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ),
        {"trade_date": trade_day},
    ).mappings().first()
    if not row:
        return None
    payload = dict(row)
    payload["trade_date"] = _iso_date(payload.get("trade_date"))
    payload["reference_date"] = _iso_date(payload.get("reference_date"))
    computed_at = payload.get("computed_at")
    if isinstance(computed_at, str):
        try:
            payload["computed_at"] = datetime.fromisoformat(computed_at)
        except ValueError:
            payload["computed_at"] = _now_utc().replace(tzinfo=None)
    payload["market_state_degraded"] = bool(payload.get("market_state_degraded"))
    return payload


def _load_hotspot_payload(db: Session, *, stock_code: str, limit: int = 10) -> dict[str, Any]:
    hotspot_item = Base.metadata.tables.get("market_hotspot_item")
    hotspot_link = Base.metadata.tables.get("market_hotspot_item_stock_link")
    if hotspot_item is None or hotspot_link is None:
        return {"status": "missing", "count": 0, "items": []}

    threshold = _now_utc() - timedelta(hours=48)
    rows = db.execute(
        select(
            hotspot_item.c.hotspot_item_id.label("hotspot_item_id"),
            hotspot_item.c.topic_title.label("topic_title"),
            hotspot_item.c.source_name.label("source_name"),
            hotspot_item.c.source_url.label("source_url"),
            hotspot_item.c.fetch_time.label("fetch_time"),
            hotspot_item.c.merged_rank.label("merged_rank"),
            hotspot_item.c.news_event_type.label("news_event_type"),
            hotspot_link.c.match_confidence.label("match_confidence"),
        )
        .select_from(
            hotspot_link.join(
                hotspot_item,
                hotspot_link.c.hotspot_item_id == hotspot_item.c.hotspot_item_id,
            )
        )
        .where(hotspot_link.c.stock_code == stock_code)
        .where(hotspot_item.c.fetch_time >= threshold)
        .order_by(hotspot_item.c.merged_rank.asc(), hotspot_item.c.fetch_time.desc())
        .limit(limit)
    ).mappings().all()

    items = [
        {
            "hotspot_item_id": str(row.get("hotspot_item_id") or ""),
            "topic_title": row.get("topic_title"),
            "source_name": row.get("source_name"),
            "source_url": row.get("source_url"),
            "fetch_time": _iso_datetime(row.get("fetch_time")),
            "merged_rank": _to_int(row.get("merged_rank")),
            "match_confidence": _to_float(row.get("match_confidence")),
            "news_event_type": row.get("news_event_type"),
        }
        for row in rows
    ]
    return {
        "status": "ok" if items else "missing",
        "count": len(items),
        "items": items,
        "latest_fetch_time": items[0].get("fetch_time") if items else None,
    }


def _normalize_news_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items or []:
        normalized.append(
            {
                "title": item.get("title"),
                "source_name": item.get("source_name"),
                "source_url": item.get("source_url"),
                "fetch_time": _iso_datetime(item.get("fetch_time")),
                "category": item.get("category"),
            }
        )
    return normalized


def _build_financial_analysis(
    *,
    market_snapshot: dict[str, Any],
    valuation: dict[str, Any],
    tdx_local_features: dict[str, Any],
    capital_data: dict[str, Any],
) -> dict[str, Any]:
    analysis = {
        "total_shares": valuation.get("total_shares"),
        "float_shares": valuation.get("float_shares"),
        "total_market_cap": valuation.get("total_market_cap"),
        "float_market_cap": valuation.get("float_market_cap"),
        "pe_ttm": valuation.get("pe_ttm"),
        "pb": valuation.get("pb"),
        "listed_days": valuation.get("listed_days"),
        "ret5": market_snapshot.get("ret5"),
        "ret20": market_snapshot.get("ret20"),
        "sample_days": market_snapshot.get("sample_days"),
        "tdx_local_features": dict(tdx_local_features.get("features") or {}),
        "main_force_status": (capital_data.get("main_force") or {}).get("status"),
        "dragon_tiger_status": (capital_data.get("dragon_tiger") or {}).get("status"),
        "margin_financing_status": (capital_data.get("margin_financing") or {}).get("status"),
        "northbound_status": (capital_data.get("northbound") or {}).get("status"),
        "etf_flow_status": (capital_data.get("etf_flow") or {}).get("status"),
    }
    analysis["data_status"] = (
        "ok"
        if any(analysis.get(key) is not None for key in ("total_market_cap", "float_market_cap", "pe_ttm", "pb"))
        else "missing"
    )
    return analysis


def _build_data_sources(
    *,
    market_snapshot: dict[str, Any],
    company_overview: dict[str, Any],
    valuation: dict[str, Any],
    industry_competition: dict[str, Any],
    hotspot_payload: dict[str, Any],
    capital_data: dict[str, Any],
    news_items: list[dict[str, Any]],
    policy_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "dataset": "market_snapshot",
            "source_name": "kline_daily",
            "status": "ok" if market_snapshot.get("trade_date") else "missing",
            "purpose": "latest_price_and_technical_context",
        },
        {
            "dataset": "company_overview",
            "source_name": "eastmoney_company_survey",
            "status": "ok" if company_overview.get("company_name") else "missing",
            "purpose": "issuer_identity_and_profile",
        },
        {
            "dataset": "valuation",
            "source_name": "eastmoney_push2_stock_get",
            "status": "ok" if valuation.get("total_market_cap") is not None or valuation.get("pe_ttm") is not None else "missing",
            "purpose": "valuation_and_share_structure",
        },
        {
            "dataset": "industry_competition",
            "source_name": "eastmoney_industry_board",
            "status": "ok" if industry_competition.get("peers") else "partial",
            "purpose": "peer_comparison",
        },
        {
            "dataset": "hotspot",
            "source_name": "market_hotspot_item",
            "status": hotspot_payload.get("status") or "missing",
            "purpose": "event_and_attention_signal",
        },
        {
            "dataset": "capital_data",
            "source_name": "eastmoney_plus_akshare",
            "status": "ok"
            if any((capital_data.get(name) or {}).get("status") in {"ok", "stale_ok", "realtime_only", "proxy_ok"} for name in ("main_force", "dragon_tiger", "margin_financing", "northbound", "etf_flow"))
            else "missing",
            "purpose": "capital_flow_and_participation",
        },
        {
            "dataset": "news_policy",
            "source_name": "eastmoney_and_cls",
            "status": "ok" if news_items or policy_items else "missing",
            "purpose": "external_event_context",
        },
    ]


def _data_status_from_sources(sources: list[dict[str, Any]]) -> str:
    ok_count = sum(1 for item in sources if item.get("status") == "ok")
    partial_count = sum(1 for item in sources if item.get("status") == "partial")
    if ok_count >= max(1, len(sources) - 1):
        return "ok"
    if ok_count or partial_count:
        return "partial"
    return "missing"


def _summary_source_name(dataset_name: str, summary: dict[str, Any]) -> str:
    reason = str(summary.get("reason") or "").lower()
    if dataset_name == "northbound_summary":
        return "akshare_hsgt_hist" if "akshare" in reason else "northbound_summary"
    if dataset_name == "etf_flow_summary":
        if "akshare" in reason:
            return str(summary.get("reason"))
        return "etf_flow_summary"
    return dataset_name


def _summary_status_reason(summary: dict[str, Any]) -> str:
    status = str(summary.get("status") or "missing").lower()
    if status in {"ok", "stale_ok"}:
        return json.dumps(summary, ensure_ascii=False, default=str)
    return str(summary.get("reason") or status)


def _load_parent_batch_ids(db: Session, *, child_batch_id: str) -> set[str]:
    lineage_table = Base.metadata.tables["data_batch_lineage"]
    rows = db.execute(
        select(lineage_table.c.parent_batch_id).where(
            lineage_table.c.child_batch_id == child_batch_id,
            lineage_table.c.lineage_role == "MERGED_FROM",
        )
    ).all()
    return {str(row[0]) for row in rows if row and row[0]}


def _upsert_data_usage_fact(
    db: Session,
    *,
    usage_id: str,
    batch_id: str,
    trade_day: date,
    stock_code: str,
    source_name: str,
    fetch_time: datetime,
    status: str,
    status_reason: str | None,
) -> None:
    fact_table = Base.metadata.tables.get("data_usage_fact")
    if fact_table is None:
        return

    values = {
        "batch_id": batch_id,
        "trade_date": trade_day.isoformat(),
        "stock_code": stock_code,
        "source_name": source_name,
        "fetch_time": _iso_datetime(fetch_time),
        "status": status,
        "status_reason": status_reason,
        "created_at": _iso_datetime(fetch_time),
    }
    existing = db.execute(
        select(fact_table.c.usage_id).where(fact_table.c.usage_id == usage_id)
    ).mappings().first()
    if existing:
        db.execute(
            fact_table.update()
            .where(fact_table.c.usage_id == usage_id)
            .values(**values)
        )
        return
    db.execute(
        fact_table.insert().values(
            usage_id=usage_id,
            **values,
        )
    )


def _upsert_report_data_usage(
    db: Session,
    *,
    stock_code: str,
    trade_day: date,
    dataset_name: str,
    source_name: str,
    batch_id: str,
    fetch_time: datetime,
    status: str,
    status_reason: str | None,
) -> str:
    return stable_upsert_usage_row(
        db,
        trade_date=trade_day,
        stock_code=stock_code,
        dataset_name=dataset_name,
        source_name=source_name,
        batch_id=batch_id,
        fetch_time=fetch_time,
        status=status,
        status_reason=status_reason,
        created_at=fetch_time,
    )


def _resolve_market_state_parent_batch_ids(
    db: Session,
    *,
    market_state_trade_day: date,
    reference_trade_day: date | None,
    market_state_row: dict[str, Any],
) -> dict[str, str]:
    market_state_table = Base.metadata.tables["market_state_cache"]
    resolved: dict[str, str] = {}
    updates: dict[str, str] = {}
    for field_name, dataset_name in (
        ("kline_batch_id", "kline_daily"),
        ("hotspot_batch_id", "hotspot_top50"),
    ):
        current_batch_id = str(market_state_row.get(field_name) or "").strip()
        if not current_batch_id:
            current_batch_id = str(
                infer_usage_batch_id(
                    db,
                    trade_date=market_state_trade_day,
                    dataset_name=dataset_name,
                    fallback_trade_date=reference_trade_day,
                )
                or ""
            ).strip()
            if current_batch_id:
                updates[field_name] = current_batch_id
        if current_batch_id:
            resolved[field_name] = current_batch_id

    if updates:
        db.execute(
            market_state_table.update()
            .where(market_state_table.c.trade_date == market_state_trade_day)
            .values(**updates)
        )
        market_state_row.update(updates)
    return resolved


def _ensure_market_state_input_usage(
    db: Session,
    *,
    stock_code: str,
    usage_trade_day: date,
    market_state_row: dict[str, Any],
) -> dict[str, Any]:
    batch_table = Base.metadata.tables["data_batch"]
    lineage_table = Base.metadata.tables["data_batch_lineage"]
    market_state_trade_day = date.fromisoformat(str(market_state_row["trade_date"]))
    now = market_state_row.get("computed_at") or _now_utc().replace(tzinfo=None)
    reference_trade_day = None
    if market_state_row.get("reference_date"):
        reference_trade_day = date.fromisoformat(str(market_state_row["reference_date"]))
    parent_batch_map = _resolve_market_state_parent_batch_ids(
        db,
        market_state_trade_day=market_state_trade_day,
        reference_trade_day=reference_trade_day,
        market_state_row=market_state_row,
    )
    desired_parent_batch_ids = set(parent_batch_map.values())
    derived_batch_rows = db.execute(
        batch_table.select().where(
            batch_table.c.source_name == "market_state_cache",
            batch_table.c.trade_date == market_state_trade_day,
            batch_table.c.batch_scope == "market_state_derived",
        )
    ).mappings().all()
    derived_batch = None
    for candidate in derived_batch_rows:
        if _load_parent_batch_ids(db, child_batch_id=str(candidate["batch_id"])) == desired_parent_batch_ids:
            derived_batch = candidate
            break

    status = "degraded" if market_state_row.get("market_state_degraded") else "ok"
    status_reason = (
        str(market_state_row.get("state_reason") or "market_state_degraded")
        if status == "degraded"
        else None
    )
    if derived_batch:
        batch_id = str(derived_batch["batch_id"])
        db.execute(
            batch_table.update()
            .where(batch_table.c.batch_id == batch_id)
            .values(
                batch_status="SUCCESS",
                quality_flag=status,
                covered_stock_count=1,
                core_pool_covered_count=1,
                records_total=1,
                records_success=1,
                records_failed=0,
                status_reason=status_reason,
                finished_at=now,
                updated_at=now,
            )
        )
    else:
        derived_batch = _create_batch(
            db,
            source_name="market_state_cache",
            trade_date=market_state_trade_day,
            batch_scope="market_state_derived",
            batch_status="SUCCESS",
            quality_flag=status,
            covered_stock_count=1,
            core_pool_covered_count=1,
            records_total=1,
            records_success=1,
            records_failed=0,
            status_reason=status_reason,
            started_at=now,
            finished_at=now,
        )
        batch_id = str(derived_batch.batch_id)

    for parent_batch_id in sorted(desired_parent_batch_ids):
        exists = db.execute(
            lineage_table.select().where(
                lineage_table.c.child_batch_id == batch_id,
                lineage_table.c.parent_batch_id == parent_batch_id,
                lineage_table.c.lineage_role == "MERGED_FROM",
            )
        ).mappings().first()
        if exists:
            continue
        db.execute(
            lineage_table.insert().values(
                batch_lineage_id=str(uuid4()),
                child_batch_id=batch_id,
                parent_batch_id=parent_batch_id,
                lineage_role="MERGED_FROM",
                created_at=now,
            )
        )

    usage_id = _upsert_report_data_usage(
        db,
        stock_code=stock_code,
        trade_day=usage_trade_day,
        dataset_name="market_state_input",
        source_name="market_state_cache",
        batch_id=batch_id,
        fetch_time=now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now,
        status=status,
        status_reason=status_reason,
    )
    return {
        "usage_id": usage_id,
        "batch_id": batch_id,
        "status": status,
        "reason": status_reason,
        "market_state_trade_date": market_state_trade_day.isoformat(),
    }


async def build_stock_snapshot_payload(
    db: Session,
    *,
    stock_code: str,
    trade_date: str | None = None,
) -> dict[str, Any]:
    trade_text = trade_date or latest_trade_date_str()
    trade_day = date.fromisoformat(trade_text)
    stock_master = _load_stock_master_row(db, stock_code=stock_code) or {}
    kline_rows = _load_kline_rows(db, stock_code=stock_code, trade_day=trade_day, limit=21)
    market_snapshot = _build_market_snapshot(kline_rows)
    market_state = _load_market_state_row(db, trade_day=trade_day)
    hotspot_payload = _load_hotspot_payload(db, stock_code=stock_code, limit=10)
    tdx_local_features = build_tdx_local_features(stock_code)

    company_overview, valuation, capital_dimensions, policy_items = await asyncio.gather(
        fetch_company_overview(stock_code),
        fetch_valuation_snapshot(stock_code),
        fetch_capital_dimensions(stock_code),
        fetch_policy_news(limit=5),
    )

    company_name = (
        company_overview.get("company_name")
        or stock_master.get("stock_name")
        or valuation.get("stock_name")
        or stock_code
    )
    industry_name = company_overview.get("industry") or valuation.get("industry") or stock_master.get("industry")
    if not company_overview.get("company_name"):
        company_overview["company_name"] = company_name
    if not company_overview.get("stock_code"):
        company_overview["stock_code"] = stock_code
    if not company_overview.get("industry") and industry_name:
        company_overview["industry"] = industry_name

    news_keywords = [industry_name, valuation.get("region")]
    news_items, industry_competition = await asyncio.gather(
        fetch_stock_news(
            stock_code,
            stock_name=company_name,
            keywords=[keyword for keyword in news_keywords if keyword],
            limit=5,
        ),
        fetch_industry_competition(stock_code, industry_name),
    )

    northbound_summary = fetch_northbound_summary(stock_code) or {
        "status": "missing",
        "reason": "northbound_data_unavailable",
    }
    etf_flow_summary = fetch_etf_flow_summary_global(trade_day)
    capital_data = {
        "main_force": ((capital_dimensions.get("capital_flow") or {}).get("main_force") or {"status": "missing"}),
        "dragon_tiger": capital_dimensions.get("dragon_tiger") or {"status": "missing"},
        "margin_financing": capital_dimensions.get("margin_financing") or {"status": "missing"},
        "northbound": northbound_summary,
        "etf_flow": etf_flow_summary,
        "errors": list(capital_dimensions.get("errors") or []),
    }
    financial_analysis = _build_financial_analysis(
        market_snapshot=market_snapshot,
        valuation=valuation,
        tdx_local_features=tdx_local_features,
        capital_data=capital_data,
    )
    normalized_news_items = _normalize_news_items(news_items)
    normalized_policy_items = _normalize_news_items(policy_items)
    data_sources = _build_data_sources(
        market_snapshot=market_snapshot,
        company_overview=company_overview,
        valuation=valuation,
        industry_competition=industry_competition,
        hotspot_payload=hotspot_payload,
        capital_data=capital_data,
        news_items=normalized_news_items,
        policy_items=normalized_policy_items,
    )

    return {
        "stock_code": stock_code,
        "stock_name": company_name,
        "trade_date": trade_text,
        "company_overview": company_overview,
        "market_snapshot": market_snapshot,
        "market_state": market_state,
        "valuation": valuation,
        "financial_analysis": financial_analysis,
        "industry_competition": {
            "industry_name": industry_competition.get("industry_name") or industry_name,
            "industry_board_code": industry_competition.get("industry_board_code"),
            "industry_board_name": industry_competition.get("industry_board_name") or industry_competition.get("industry_name"),
            "peers": list(industry_competition.get("peers") or []),
            "peer_count": len(industry_competition.get("peers") or []),
            "data_status": "ok" if industry_competition.get("peers") else ("partial" if industry_name else "missing"),
        },
        "capital_data": capital_data,
        "hotspot": hotspot_payload,
        "news_policy": {
            "news_count": len(normalized_news_items),
            "policy_count": len(normalized_policy_items),
            "news_items": normalized_news_items,
            "policy_items": normalized_policy_items,
        },
        "data_sources": data_sources,
        "data_status": _data_status_from_sources(data_sources),
    }


async def collect_non_report_usage(
    db: Session,
    *,
    stock_code: str,
    trade_date: str | None = None,
) -> dict[str, Any]:
    trade_text = trade_date or latest_trade_date_str()
    trade_day = date.fromisoformat(trade_text)
    now = _now_utc()

    capital_batch = _create_batch(
        db,
        source_name="supplemental_capital",
        trade_date=trade_day,
        batch_scope="stock_supplemental",
        batch_status="RUNNING",
        quality_flag="ok",
        records_total=3,
        records_success=0,
        records_failed=0,
        started_at=now.replace(tzinfo=None),
        finished_at=now.replace(tzinfo=None),
    )
    capital_usage = await persist_capital_usage(
        db,
        stock_code=stock_code,
        trade_date=trade_text,
        batch_id=capital_batch.batch_id,
    )
    capital_results = capital_usage.get("per_dataset") or {}
    capital_statuses = [
        str(item.get("persisted_status") or "missing").lower()
        for item in capital_results.values()
    ]
    capital_batch.records_success = sum(
        1 for status in capital_statuses if status in _SUCCESS_USAGE_STATUSES
    )
    capital_batch.records_failed = max(0, len(capital_statuses) - int(capital_batch.records_success or 0))
    if capital_batch.records_success == len(capital_statuses) and capital_statuses:
        capital_batch.batch_status = "SUCCESS"
    elif capital_batch.records_success:
        capital_batch.batch_status = "PARTIAL_SUCCESS"
    else:
        capital_batch.batch_status = "FAILED"
    if capital_batch.records_success == 0:
        capital_batch.quality_flag = "missing"
    elif all(status == "ok" for status in capital_statuses):
        capital_batch.quality_flag = "ok"
    else:
        capital_batch.quality_flag = "stale_ok"
    if capital_batch.records_failed:
        capital_batch.status_reason = "capital_usage_partial_missing"
    elif capital_batch.records_success:
        capital_batch.status_reason = None
    else:
        capital_batch.status_reason = "capital_usage_collect_failed"
    capital_batch.finished_at = _now_utc().replace(tzinfo=None)

    profile_batch = _create_batch(
        db,
        source_name="stock_profile",
        trade_date=trade_day,
        batch_scope="stock_supplemental",
        batch_status="RUNNING",
        quality_flag="ok",
        records_total=1,
        records_success=0,
        records_failed=0,
        started_at=now.replace(tzinfo=None),
        finished_at=now.replace(tzinfo=None),
    )
    stock_profile = persist_stock_profile(
        db,
        stock_code=stock_code,
        trade_date=trade_text,
        batch_id=profile_batch.batch_id,
    )
    profile_status = str(stock_profile.get("persisted_status") or "missing").lower()
    profile_ok = profile_status in {"ok", "stale_ok"}
    profile_batch.records_success = 1 if profile_ok else 0
    profile_batch.records_failed = 0 if profile_ok else 1
    profile_batch.batch_status = "SUCCESS" if profile_ok else "FAILED"
    profile_batch.quality_flag = profile_status if profile_ok else "missing"
    profile_batch.status_reason = None if profile_ok else str(stock_profile.get("reason") or "stock_profile_collect_failed")
    profile_batch.finished_at = _now_utc().replace(tzinfo=None)

    summary_results: dict[str, Any] = {}
    summary_fetchers = {
        "northbound_summary": fetch_northbound_summary(stock_code) or {"status": "missing", "reason": "northbound_data_unavailable"},
        "etf_flow_summary": fetch_etf_flow_summary_global(trade_day),
    }
    for dataset_name, summary in summary_fetchers.items():
        fetch_time = _now_utc()
        status = str(summary.get("status") or "missing").lower()
        status_reason = _summary_status_reason(summary)
        source_name = _summary_source_name(dataset_name, summary)
        batch = _create_batch(
            db,
            source_name=source_name[:32],
            trade_date=trade_day,
            batch_scope="summary",
            batch_status="SUCCESS" if status in {"ok", "stale_ok"} else "FAILED",
            quality_flag=status if status in {"ok", "stale_ok", "missing", "degraded"} else "missing",
            records_total=1,
            records_success=1 if status in {"ok", "stale_ok"} else 0,
            records_failed=0 if status in {"ok", "stale_ok"} else 1,
            status_reason=None if status in {"ok", "stale_ok"} else status_reason,
            started_at=fetch_time.replace(tzinfo=None),
            finished_at=fetch_time.replace(tzinfo=None),
        )
        _upsert_report_data_usage(
            db,
            stock_code=stock_code,
            trade_day=trade_day,
            dataset_name=dataset_name,
            source_name=source_name,
            batch_id=batch.batch_id,
            fetch_time=fetch_time,
            status=status if status in {"ok", "stale_ok", "missing", "degraded"} else "missing",
            status_reason=status_reason,
        )
        summary_results[dataset_name] = {
            "status": status,
            "reason": summary.get("reason"),
            "batch_id": batch.batch_id,
        }

    market_state_row = _load_market_state_row(db, trade_day=trade_day)
    if market_state_row:
        summary_results["market_state_input"] = _ensure_market_state_input_usage(
            db,
            stock_code=stock_code,
            usage_trade_day=trade_day,
            market_state_row=market_state_row,
        )

    db.commit()
    return {
        "stock_code": stock_code,
        "trade_date": trade_text,
        "stock_profile": {
            **stock_profile,
            "batch_id": profile_batch.batch_id,
        },
        "capital_usage": {
            **capital_usage,
            "batch_id": capital_batch.batch_id,
        },
        **summary_results,
    }


def collect_non_report_usage_sync(
    db: Session,
    *,
    stock_code: str,
    trade_date: str | None = None,
) -> dict[str, Any]:
    """Synchronous wrapper for collect_non_report_usage.

    Creates a new event loop in the current thread to run the async function.
    Safe to call from synchronous scheduler contexts (APScheduler background threads)
    where no event loop is running. Do NOT call from within an already-running
    asyncio event loop — use `await collect_non_report_usage(...)` instead.
    """
    import asyncio

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            collect_non_report_usage(db, stock_code=stock_code, trade_date=trade_date)
        )
    except Exception as exc:
        return {"stock_code": stock_code, "status": "error", "error": str(exc)}
    finally:
        loop.close()


def materialize_non_report_usage_for_pool(
    db: Session,
    *,
    stock_codes: list[str],
    trade_date: str | date | None = None,
) -> dict[str, Any]:
    trade_text = trade_date.isoformat() if isinstance(trade_date, date) else (trade_date or latest_trade_date_str())
    deduped_codes: list[str] = []
    seen: set[str] = set()
    for stock_code in stock_codes:
        normalized = str(stock_code or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped_codes.append(normalized)

    failed_stocks: list[str] = []
    northbound_status_counts: dict[str, int] = {}
    margin_status_counts: dict[str, int] = {}
    market_state_input_written = 0

    for stock_code in deduped_codes:
        result = collect_non_report_usage_sync(
            db,
            stock_code=stock_code,
            trade_date=trade_text,
        )
        if str(result.get("status") or "").lower() == "error":
            db.rollback()
            failed_stocks.append(stock_code)
            continue

        if result.get("market_state_input"):
            market_state_input_written += 1

        northbound_status = str((result.get("northbound_summary") or {}).get("status") or "missing").lower()
        northbound_status_counts[northbound_status] = northbound_status_counts.get(northbound_status, 0) + 1

        margin_status = str(
            ((((result.get("capital_usage") or {}).get("per_dataset") or {}).get("margin_financing") or {}).get("persisted_status"))
            or "missing"
        ).lower()
        margin_status_counts[margin_status] = margin_status_counts.get(margin_status, 0) + 1

    return {
        "trade_date": trade_text,
        "total_stocks": len(deduped_codes),
        "succeeded": len(deduped_codes) - len(failed_stocks),
        "failed": len(failed_stocks),
        "failed_stocks": failed_stocks,
        "market_state_input_written": market_state_input_written,
        "northbound_status_counts": northbound_status_counts,
        "margin_financing_status_counts": margin_status_counts,
    }