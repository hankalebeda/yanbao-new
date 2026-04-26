from __future__ import annotations

import pytest
from datetime import date, datetime, timedelta, timezone

pytestmark = pytest.mark.feature("FR-04")

from app.models import Base
from app.services.multisource_ingest import ingest_market_data


def _seed_stock_master(db_session, *, total_stocks: int) -> list[str]:
    stock_master = Base.metadata.tables["stock_master"]
    trade_day = date(2026, 3, 9)
    codes: list[str] = []
    for index in range(total_stocks):
        code = f"{600000 + index:06d}.SH"
        codes.append(code)
        db_session.execute(
            stock_master.insert().values(
                stock_code=code,
                stock_name=f"STOCK{index:04d}",
                exchange="SH",
                industry=f"IND{index % 20}",
                list_date=trade_day - timedelta(days=800 + index),
                circulating_shares=1_000_000_000 + index * 1_000_000,
                is_st=False,
                is_suspended=False,
                is_delisted=False,
            )
        )
    db_session.commit()
    return codes


def _history_for(stock_code: str, trade_day: date) -> list[dict]:
    base = 8 + (int(stock_code[:6]) - 600000) * 0.002
    rows = []
    for offset in range(25):
        current_day = trade_day - timedelta(days=24 - offset)
        close_price = round(base + offset * 0.03, 4)
        rows.append(
            {
                "trade_date": current_day.isoformat(),
                "open": round(close_price - 0.05, 4),
                "high": round(close_price + 0.08, 4),
                "low": round(close_price - 0.08, 4),
                "close": close_price,
                "volume": 10_000_000 + offset * 10_000,
                "amount": (10_000_000 + offset * 10_000) * close_price,
                "is_suspended": False,
            }
        )
    return rows


def _hotspot_items(source_name: str, trade_day: date, core_pool_codes: list[str]) -> list[dict]:
    fetch_time = datetime.combine(trade_day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=9)
    return [
        {
            "rank": rank,
            "topic_title": f"主题{rank:02d}",
            "source_url": f"https://{source_name}.example.com/topic/{rank}",
            "fetch_time": fetch_time.isoformat(),
            "news_event_type": "policy" if rank % 10 == 0 else None,
            "hotspot_tags": ["policy"] if rank % 10 == 0 else ["watchlist"],
            "stock_codes": [core_pool_codes[(rank - 1) % len(core_pool_codes)]],
        }
        for rank in range(1, 51)
    ]


def _ok_summary() -> dict:
    return {
        "status": "ok",
        "reason": None,
        "fetch_time": datetime.now(timezone.utc).isoformat(),
    }


def test_fr04_partial_commit_gate_isolates_tail_failures_without_rolling_back_core_pool(db_session):
    trade_day = date(2026, 3, 9)
    all_codes = _seed_stock_master(db_session, total_stocks=5000)
    core_pool_codes = all_codes[:200]
    failed_codes = set(all_codes[-100:])

    def fetch_kline_history(stock_code: str, current_trade_day: date):
        if stock_code in failed_codes:
            raise RuntimeError("tail_kline_missing")
        return _history_for(stock_code, current_trade_day)

    def fetch_hotspot_by_source(source_name: str, current_trade_day: date):
        if source_name in {"eastmoney", "xueqiu", "cls"}:
            return _hotspot_items(source_name, current_trade_day, core_pool_codes)
        raise RuntimeError(f"{source_name}_disabled")

    result = ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_kline_history,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=lambda current_trade_day: _ok_summary(),
        fetch_etf_flow_summary=lambda current_trade_day: _ok_summary(),
        now=datetime(2026, 3, 9, 9, 0, tzinfo=timezone.utc),
    )

    assert result["total_stocks"] == 5000
    assert result["covered_count"] == 4900
    assert result["core_pool_covered_count"] == 200
    assert result["quality_flag"] == "stale_ok"
    assert "partial_commit" in (result["status_reason"] or "")

    data_batch = Base.metadata.tables["data_batch"]
    kline_batch = db_session.execute(
        data_batch.select().where(
            data_batch.c.trade_date == trade_day,
            data_batch.c.source_name == "tdx_local",
            data_batch.c.batch_scope == "full_market",
        ).order_by(data_batch.c.created_at.desc())
    ).mappings().first()
    assert kline_batch is not None
    assert kline_batch["batch_status"] == "PARTIAL_SUCCESS"
    assert kline_batch["records_total"] == 5000
    assert kline_batch["records_success"] == 4900
    assert kline_batch["records_failed"] == 100
    assert kline_batch["core_pool_covered_count"] == 200
    assert kline_batch["status_reason"] == "partial_commit"

    error_rows = db_session.execute(
        Base.metadata.tables["data_batch_error"]
        .select()
        .where(Base.metadata.tables["data_batch_error"].c.batch_id == kline_batch["batch_id"])
    ).mappings().all()
    assert len(error_rows) == 100
    assert {row["stock_code"] for row in error_rows} == failed_codes
    assert failed_codes.isdisjoint(core_pool_codes)

    persisted_codes = {
        row["stock_code"]
        for row in db_session.execute(
            Base.metadata.tables["kline_daily"]
            .select()
            .where(Base.metadata.tables["kline_daily"].c.trade_date == trade_day)
        ).mappings().all()
    }
    assert len(persisted_codes) == 4900
    assert failed_codes.isdisjoint(persisted_codes)
    assert set(core_pool_codes).issubset(persisted_codes)

    usage_rows = db_session.execute(
        Base.metadata.tables["report_data_usage"]
        .select()
        .where(
            Base.metadata.tables["report_data_usage"].c.trade_date == trade_day,
            Base.metadata.tables["report_data_usage"].c.dataset_name == "kline_daily",
        )
    ).mappings().all()
    assert len(usage_rows) == 4900
    assert failed_codes.isdisjoint({row["stock_code"] for row in usage_rows})
