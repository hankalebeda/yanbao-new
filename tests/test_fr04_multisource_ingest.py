from __future__ import annotations

import pytest
from datetime import date, datetime, timedelta, timezone

pytestmark = [
    pytest.mark.feature("FR-04"),
    pytest.mark.feature("FR04-DATA-01"),
    pytest.mark.feature("FR04-DATA-03"),
    pytest.mark.feature("FR04-DATA-04"),
    pytest.mark.feature("FR04-DATA-07"),
]

from app.models import Base
import app.services.market_data as market_data_module
from app.services import scheduler as scheduler_module
from app.services.multisource_ingest import backfill_missing_kline_daily, ingest_market_data


def _seed_stock_master(db_session, *, total_stocks: int = 210):
    stock_master = Base.metadata.tables["stock_master"]
    trade_day = date(2026, 3, 9)
    codes: list[str] = []
    for index in range(total_stocks):
        code = f"{600000 + index:06d}.SH"
        codes.append(code)
        db_session.execute(
            stock_master.insert().values(
                stock_code=code,
                stock_name=f"STOCK{index:03d}",
                exchange="SH",
                industry=f"IND{index % 10}",
                list_date=trade_day - timedelta(days=800 + index),
                circulating_shares=1_000_000_000 + index * 2_000_000,
                is_st=False,
                is_suspended=False,
                is_delisted=False,
            )
        )
    db_session.commit()
    return codes


def _history_for(stock_code: str, trade_day: date, *, slope: float = 0.05):
    base = 8 + (int(stock_code[:6]) - 600000) * 0.03
    rows = []
    for offset in range(25):
        current_day = trade_day - timedelta(days=24 - offset)
        close_price = round(base + offset * slope, 4)
        rows.append(
            {
                "trade_date": current_day.isoformat(),
                "open": round(close_price - 0.05, 4),
                "high": round(close_price + 0.08, 4),
                "low": round(close_price - 0.08, 4),
                "close": close_price,
                "volume": 20_000_000 + offset * 50_000,
                "amount": (20_000_000 + offset * 50_000) * close_price,
                "is_suspended": False,
            }
        )
    return rows


def _hotspot_items(source_name: str, trade_day: date, core_pool_codes: list[str], *, count: int = 50):
    fetch_time = datetime.combine(trade_day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=9)
    items = []
    for rank in range(1, count + 1):
        items.append(
            {
                "rank": rank,
                "topic_title": f"主题{rank:02d}",
                "source_url": f"https://{source_name}.example.com/topic/{rank}",
                "fetch_time": fetch_time.isoformat(),
                "news_event_type": "policy" if rank % 10 == 0 else None,
                "hotspot_tags": ["policy"] if rank % 10 == 0 else ["watchlist"],
                "stock_codes": [core_pool_codes[(rank - 1) % len(core_pool_codes)]],
            }
        )
    return items


def test_fr04_normalized_output(db_session):
    trade_day = date(2026, 3, 9)
    all_codes = _seed_stock_master(db_session)
    core_pool_codes = all_codes[:200]
    failed_codes = set(all_codes[200:210])

    def fetch_kline_history(stock_code: str, trade_date: date):
        if stock_code in failed_codes:
            raise RuntimeError("tail_kline_missing")
        return _history_for(stock_code, trade_date, slope=0.04 + (int(stock_code[:6]) - 600000) * 0.0002)

    def fetch_hotspot_by_source(source_name: str, trade_date: date):
        if source_name in {"eastmoney", "xueqiu", "cls"}:
            return _hotspot_items(source_name, trade_date, core_pool_codes)
        raise RuntimeError(f"{source_name}_disabled")

    def fetch_northbound_summary(trade_date: date):
        return {
            "status": "ok",
            "reason": None,
            "net_inflow_1d": 12.3,
            "net_inflow_3d": 18.5,
            "net_inflow_5d": 25.1,
            "net_inflow_10d": 31.4,
            "net_inflow_20d": 44.2,
            "streak_days": 3,
            "fetch_time": datetime.now(timezone.utc).isoformat(),
        }

    def fetch_etf_flow_summary(trade_date: date):
        return {
            "status": "ok",
            "reason": None,
            "net_creation_redemption_5d": 6.2,
            "net_creation_redemption_20d": 12.8,
            "fetch_time": datetime.now(timezone.utc).isoformat(),
        }

    result = ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_kline_history,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_northbound_summary,
        fetch_etf_flow_summary=fetch_etf_flow_summary,
        now=datetime(2026, 3, 9, 9, 0, tzinfo=timezone.utc),
    )

    assert result["covered_count"] >= int(result["total_stocks"] * 0.95)
    assert result["core_pool_covered_count"] == 200
    assert len(result["hotspot_top50"]) == 50
    assert [item["rank"] for item in result["hotspot_top50"]] == list(range(1, 51))
    assert result["quality_flag"] in ("ok", "stale_ok", "missing", "degraded")
    assert result["northbound_summary"]["status"] == "ok"
    assert result["etf_flow_summary"]["status"] == "ok"

    kline_daily = Base.metadata.tables["kline_daily"]
    usage_table = Base.metadata.tables["report_data_usage"]
    fact_table = Base.metadata.tables["data_usage_fact"]
    row = db_session.execute(
        kline_daily.select().where(
            kline_daily.c.stock_code == core_pool_codes[0],
            kline_daily.c.trade_date == trade_day,
        )
    ).fetchone()
    assert row is not None
    assert row.adjust_type == "front_adjusted"
    assert row.atr_pct is not None
    assert row.turnover_rate is not None
    assert row.ma20 is not None
    assert row.volatility_20d is not None

    usage_rows = db_session.execute(
        usage_table.select().where(usage_table.c.trade_date == trade_day)
    ).mappings().all()
    fact_rows = db_session.execute(
        fact_table.select().where(
            fact_table.c.usage_id.in_([row["usage_id"] for row in usage_rows])
        )
    ).mappings().all()
    assert len(fact_rows) == len(usage_rows)

    hotspot_rows = db_session.execute(
        usage_table.select().where(
            usage_table.c.trade_date == trade_day,
            usage_table.c.dataset_name == "hotspot_top50",
            usage_table.c.stock_code.in_(core_pool_codes),
        )
    ).mappings().all()
    assert len(hotspot_rows) == len(core_pool_codes)
    hotspot_status_counts = {
        status: sum(1 for row in hotspot_rows if row["status"] == status)
        for status in {row["status"] for row in hotspot_rows}
    }
    assert hotspot_status_counts == {"ok": 50, "missing": 150}


def test_fr04_hotspot_source_name_enum(db_session):
    trade_day = date(2026, 3, 9)
    all_codes = _seed_stock_master(db_session, total_stocks=205)
    core_pool_codes = all_codes[:200]
    source_calls = {"eastmoney": 0}

    def fetch_kline_history(stock_code: str, trade_date: date):
        return _history_for(stock_code, trade_date)

    def fetch_hotspot_by_source(source_name: str, trade_date: date):
        if source_name == "eastmoney":
            source_calls["eastmoney"] += 1
            if source_calls["eastmoney"] <= 3:
                raise RuntimeError("eastmoney_timeout")
            if source_calls["eastmoney"] == 4:
                raise RuntimeError("eastmoney_half_open_failed")
            return _hotspot_items(source_name, trade_date, core_pool_codes)
        if source_name in {"xueqiu", "cls"}:
            return _hotspot_items(source_name, trade_date, core_pool_codes)
        raise RuntimeError(f"{source_name}_disabled")

    def fetch_northbound_summary(trade_date: date):
        return {"status": "ok", "reason": None, "fetch_time": datetime.now(timezone.utc).isoformat()}

    def fetch_etf_flow_summary(trade_date: date):
        return {"status": "ok", "reason": None, "fetch_time": datetime.now(timezone.utc).isoformat()}

    for minute in (0, 1, 2):
        ingest_market_data(
            db_session,
            trade_date=trade_day,
            stock_codes=all_codes,
            core_pool_codes=core_pool_codes,
            fetch_kline_history=fetch_kline_history,
            fetch_hotspot_by_source=fetch_hotspot_by_source,
            fetch_northbound_summary=fetch_northbound_summary,
            fetch_etf_flow_summary=fetch_etf_flow_summary,
            now=datetime(2026, 3, 9, 9, minute, tzinfo=timezone.utc),
        )

    circuit = Base.metadata.tables["data_source_circuit_state"]
    circuit_row = db_session.execute(
        circuit.select().where(circuit.c.source_name == "eastmoney")
    ).fetchone()
    assert circuit_row is not None
    assert circuit_row.circuit_state == "OPEN"
    assert circuit_row.circuit_open_at is not None

    ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_kline_history,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_northbound_summary,
        fetch_etf_flow_summary=fetch_etf_flow_summary,
        now=datetime(2026, 3, 9, 9, 3, tzinfo=timezone.utc),
    )
    assert source_calls["eastmoney"] == 3

    ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_kline_history,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_northbound_summary,
        fetch_etf_flow_summary=fetch_etf_flow_summary,
        now=datetime(2026, 3, 9, 9, 8, tzinfo=timezone.utc),
    )
    assert source_calls["eastmoney"] == 4
    circuit_row = db_session.execute(
        circuit.select().where(circuit.c.source_name == "eastmoney")
    ).fetchone()
    assert circuit_row.circuit_state == "OPEN"

    result = ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_kline_history,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_northbound_summary,
        fetch_etf_flow_summary=fetch_etf_flow_summary,
        now=datetime(2026, 3, 9, 9, 14, tzinfo=timezone.utc),
    )
    assert source_calls["eastmoney"] == 5
    circuit_row = db_session.execute(
        circuit.select().where(circuit.c.source_name == "eastmoney")
    ).fetchone()
    assert circuit_row.circuit_state == "CLOSED"

    allowed_sources = {"eastmoney", "xueqiu", "cls", "baidu_hot", "weibo", "douyin", "kuaishou"}
    assert {item["source_name"] for item in result["hotspot_top50"]}.issubset(allowed_sources)

    hotspot_item_source = Base.metadata.tables["market_hotspot_item_source"]
    source_rows = db_session.execute(hotspot_item_source.select()).fetchall()
    assert source_rows
    assert {row.source_name for row in source_rows}.issubset(allowed_sources)


def test_fr04_quality_flag_traceable(db_session):
    trade_day = date(2026, 3, 9)
    all_codes = _seed_stock_master(db_session)
    core_pool_codes = all_codes[:200]
    failed_codes = set(all_codes[200:210])

    def fetch_kline_history(stock_code: str, trade_date: date):
        if stock_code in failed_codes:
            raise RuntimeError("tail_store_failed")
        return _history_for(stock_code, trade_date)

    def fetch_hotspot_by_source(source_name: str, trade_date: date):
        if source_name in {"eastmoney", "xueqiu"}:
            return _hotspot_items(source_name, trade_date, core_pool_codes)
        raise RuntimeError(f"{source_name}_down")

    def fetch_northbound_summary(trade_date: date):
        return None

    def fetch_etf_flow_summary(trade_date: date):
        raise RuntimeError("etf_upstream_timeout")

    result = ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_kline_history,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_northbound_summary,
        fetch_etf_flow_summary=fetch_etf_flow_summary,
        now=datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
    )

    assert result["quality_flag"] in ("ok", "stale_ok", "missing", "degraded")
    assert result["northbound_summary"]["status"] in ("missing", "degraded")
    assert result["etf_flow_summary"]["status"] in ("missing", "degraded")

    data_batch_error = Base.metadata.tables["data_batch_error"]
    error_rows = db_session.execute(data_batch_error.select()).fetchall()
    assert len(error_rows) == len(failed_codes)
    assert {row.stock_code for row in error_rows} == failed_codes

    kline_daily = Base.metadata.tables["kline_daily"]
    persisted_codes = {
        row.stock_code
        for row in db_session.execute(
            kline_daily.select().where(kline_daily.c.trade_date == trade_day)
        ).fetchall()
    }
    assert failed_codes.isdisjoint(persisted_codes)
    assert set(core_pool_codes).issubset(persisted_codes)

    report_data_usage = Base.metadata.tables["report_data_usage"]
    summary_rows = db_session.execute(
        report_data_usage.select().where(
            report_data_usage.c.trade_date == trade_day,
            report_data_usage.c.stock_code == core_pool_codes[0],
            report_data_usage.c.dataset_name.in_(("northbound_summary", "etf_flow_summary")),
        )
    ).fetchall()
    assert len(summary_rows) == 2
    for row in summary_rows:
        assert row.status in ("missing", "degraded")
        assert row.status_reason


def test_fr04_hotspot_filters_unlinked_topics_and_materializes_missing_pool_usage(db_session):
    trade_day = date(2026, 3, 9)
    all_codes = _seed_stock_master(db_session, total_stocks=2)
    core_pool_codes = all_codes[:2]

    def fetch_kline_history(stock_code: str, current_trade_date: date):
        return _history_for(stock_code, current_trade_date)

    def fetch_hotspot_by_source(source_name: str, current_trade_date: date):
        if source_name != "eastmoney":
            return []
        return [
            {
                "rank": 1,
                "topic_title": "无股票映射的泛话题",
                "source_url": "https://eastmoney.example.com/topic/1",
                "fetch_time": datetime(2026, 3, 9, 9, 0, tzinfo=timezone.utc).isoformat(),
                "news_event_type": None,
                "hotspot_tags": ["watchlist"],
                "stock_codes": [],
            },
            {
                "rank": 2,
                "topic_title": "600000.SH 财报超预期",
                "source_url": "https://eastmoney.example.com/topic/2",
                "fetch_time": datetime(2026, 3, 9, 9, 1, tzinfo=timezone.utc).isoformat(),
                "news_event_type": "earnings",
                "hotspot_tags": ["earnings"],
                "stock_codes": [core_pool_codes[0]],
            },
        ]

    def fetch_summary(current_trade_date: date):
        return {"status": "ok", "reason": None, "fetch_time": datetime.now(timezone.utc).isoformat()}

    result = ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_kline_history,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_summary,
        fetch_etf_flow_summary=fetch_summary,
        now=datetime(2026, 3, 9, 9, 0, tzinfo=timezone.utc),
    )

    hotspot_item = Base.metadata.tables["market_hotspot_item"]
    hotspot_link = Base.metadata.tables["market_hotspot_item_stock_link"]
    usage_table = Base.metadata.tables["report_data_usage"]

    persisted_items = db_session.execute(hotspot_item.select()).mappings().all()
    assert len(persisted_items) == 1
    assert persisted_items[0]["topic_title"] == "600000.SH 财报超预期"

    persisted_links = db_session.execute(hotspot_link.select()).mappings().all()
    assert len(persisted_links) == 1
    assert persisted_links[0]["stock_code"] == core_pool_codes[0]

    hotspot_rows = db_session.execute(
        usage_table.select().where(
            usage_table.c.trade_date == trade_day,
            usage_table.c.dataset_name == "hotspot_top50",
            usage_table.c.stock_code.in_(core_pool_codes),
        ).order_by(usage_table.c.stock_code.asc())
    ).mappings().all()
    assert [(row["stock_code"], row["status"], row["status_reason"]) for row in hotspot_rows] == [
        (core_pool_codes[0], "stale_ok", None),
        (core_pool_codes[1], "missing", "no_hotspot_link"),
    ]
    assert len(result["hotspot_top50"]) == 1


def test_fr04_replay_reuses_usage_ids_for_same_logical_rows(db_session):
    trade_day = date(2026, 3, 9)
    all_codes = _seed_stock_master(db_session, total_stocks=2)
    core_pool_codes = all_codes[:2]

    def fetch_kline_history(stock_code: str, current_trade_date: date):
        return _history_for(stock_code, current_trade_date)

    def fetch_hotspot_by_source(source_name: str, current_trade_date: date):
        if source_name != "eastmoney":
            return []
        return _hotspot_items(source_name, current_trade_date, core_pool_codes, count=2)

    def fetch_summary(current_trade_date: date):
        return {"status": "ok", "reason": None, "fetch_time": datetime.now(timezone.utc).isoformat()}

    usage_table = Base.metadata.tables["report_data_usage"]
    fact_table = Base.metadata.tables["data_usage_fact"]

    ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_kline_history,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_summary,
        fetch_etf_flow_summary=fetch_summary,
        now=datetime(2026, 3, 9, 9, 0, tzinfo=timezone.utc),
    )

    first_rows = db_session.execute(
        usage_table.select().where(
            usage_table.c.trade_date == trade_day,
            usage_table.c.dataset_name.in_(("hotspot_top50", "northbound_summary", "etf_flow_summary")),
            usage_table.c.stock_code.in_(core_pool_codes),
        )
    ).mappings().all()
    first_usage_map = {
        (row["stock_code"], row["dataset_name"]): row["usage_id"]
        for row in first_rows
    }

    ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_kline_history,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_summary,
        fetch_etf_flow_summary=fetch_summary,
        now=datetime(2026, 3, 9, 9, 30, tzinfo=timezone.utc),
    )

    replay_rows = db_session.execute(
        usage_table.select().where(
            usage_table.c.trade_date == trade_day,
            usage_table.c.dataset_name.in_(("hotspot_top50", "northbound_summary", "etf_flow_summary")),
            usage_table.c.stock_code.in_(core_pool_codes),
        )
    ).mappings().all()
    replay_usage_map = {
        (row["stock_code"], row["dataset_name"]): row["usage_id"]
        for row in replay_rows
    }
    fact_rows = db_session.execute(
        fact_table.select().where(
            fact_table.c.usage_id.in_(list(replay_usage_map.values()))
        )
    ).mappings().all()

    assert len(replay_rows) == len(first_rows) == 6
    assert replay_usage_map == first_usage_map
    assert len(fact_rows) == len(replay_usage_map)


def test_fr04_replay_collapses_preexisting_duplicate_usage_rows(db_session):
    trade_day = date(2026, 3, 9)
    all_codes = _seed_stock_master(db_session, total_stocks=1)
    core_pool_codes = all_codes[:1]

    def fetch_kline_history(stock_code: str, current_trade_date: date):
        return _history_for(stock_code, current_trade_date)

    def fetch_hotspot_by_source(source_name: str, current_trade_date: date):
        if source_name != "eastmoney":
            return []
        return _hotspot_items(source_name, current_trade_date, core_pool_codes, count=1)

    def fetch_summary(current_trade_date: date):
        return {"status": "ok", "reason": None, "fetch_time": datetime.now(timezone.utc).isoformat()}

    usage_table = Base.metadata.tables["report_data_usage"]
    fact_table = Base.metadata.tables["data_usage_fact"]
    now = datetime(2026, 3, 9, 8, 0, tzinfo=timezone.utc).replace(tzinfo=None)

    db_session.execute(
        usage_table.insert().values(
            usage_id="dup-a",
            trade_date=trade_day,
            stock_code=core_pool_codes[0],
            dataset_name="northbound_summary",
            source_name="northbound_summary",
            batch_id="dup-batch-a",
            fetch_time=now,
            status="missing",
            status_reason="northbound_data_unavailable",
            created_at=now,
        )
    )
    db_session.execute(
        usage_table.insert().values(
            usage_id="dup-b",
            trade_date=trade_day,
            stock_code=core_pool_codes[0],
            dataset_name="northbound_summary",
            source_name="northbound_summary",
            batch_id="dup-batch-b",
            fetch_time=now + timedelta(minutes=1),
            status="missing",
            status_reason="northbound_data_unavailable",
            created_at=now + timedelta(minutes=1),
        )
    )
    db_session.execute(
        fact_table.insert().values(
            usage_id="dup-a",
            batch_id="dup-batch-a",
            trade_date=trade_day.isoformat(),
            stock_code=core_pool_codes[0],
            source_name="northbound_summary",
            fetch_time=now.isoformat(),
            status="missing",
            status_reason="northbound_data_unavailable",
            created_at=now.isoformat(),
        )
    )
    db_session.commit()

    ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_kline_history,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_summary,
        fetch_etf_flow_summary=fetch_summary,
        now=datetime(2026, 3, 9, 9, 0, tzinfo=timezone.utc),
    )

    northbound_rows = db_session.execute(
        usage_table.select().where(
            usage_table.c.trade_date == trade_day,
            usage_table.c.stock_code == core_pool_codes[0],
            usage_table.c.dataset_name == "northbound_summary",
        )
    ).mappings().all()
    fact_rows = db_session.execute(
        fact_table.select().where(
            fact_table.c.usage_id.in_([row["usage_id"] for row in northbound_rows])
        )
    ).mappings().all()

    assert len(northbound_rows) == 1
    assert northbound_rows[0]["status"] == "ok"
    assert len(fact_rows) == 1


def test_fr04_kline_fallback_batch_is_not_marked_ok(db_session):
    trade_day = date(2026, 3, 9)
    previous_day = trade_day - timedelta(days=1)
    all_codes = _seed_stock_master(db_session)
    core_pool_codes = all_codes[:200]
    fallback_code = core_pool_codes[0]

    def fetch_ok(stock_code: str, current_trade_date: date):
        return _history_for(stock_code, current_trade_date)

    def fetch_with_one_fallback(stock_code: str, current_trade_date: date):
        if stock_code == fallback_code:
            raise RuntimeError("tdx_empty_today")
        return _history_for(stock_code, current_trade_date)

    def fetch_hotspot_by_source(source_name: str, current_trade_date: date):
        if source_name in {"eastmoney", "xueqiu", "cls"}:
            return _hotspot_items(source_name, current_trade_date, core_pool_codes)
        raise RuntimeError(f"{source_name}_disabled")

    def fetch_summary(current_trade_date: date):
        return {"status": "ok", "reason": None, "fetch_time": datetime.now(timezone.utc).isoformat()}

    ingest_market_data(
        db_session,
        trade_date=previous_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_ok,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_summary,
        fetch_etf_flow_summary=fetch_summary,
        now=datetime(2026, 3, 8, 9, 0, tzinfo=timezone.utc),
    )

    result = ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=all_codes,
        core_pool_codes=core_pool_codes,
        fetch_kline_history=fetch_with_one_fallback,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=fetch_summary,
        fetch_etf_flow_summary=fetch_summary,
        now=datetime(2026, 3, 9, 9, 0, tzinfo=timezone.utc),
    )

    assert result["quality_flag"] == "stale_ok"
    assert result["status_reason"] == "fallback_t_minus_1"

    data_batch = Base.metadata.tables["data_batch"]
    kline_batch = db_session.execute(
        data_batch.select().where(
            data_batch.c.trade_date == trade_day,
            data_batch.c.source_name == "tdx_local",
            data_batch.c.batch_scope == "full_market",
        ).order_by(data_batch.c.created_at.desc())
    ).fetchone()
    assert kline_batch is not None
    assert kline_batch.quality_flag == "stale_ok"
    assert kline_batch.status_reason == "fallback_t_minus_1"


def test_fr04_handler_uses_eastmoney_when_tdx_missing(db_session, monkeypatch):
    trade_day = date(2026, 3, 20)
    captured: dict[str, object] = {}

    async def fake_fetch_recent_klines(stock_code: str, limit: int = 60):
        return [
            {"date": "2026-03-18", "open": 8.0, "high": 8.3, "low": 7.9, "close": 8.1, "volume": 1000.0, "amount": 8100.0},
            {"date": "2026-03-19", "open": 8.1, "high": 8.4, "low": 8.0, "close": 8.2, "volume": 1100.0, "amount": 9020.0},
            {"date": "2026-03-20", "open": 8.2, "high": 8.5, "low": 8.1, "close": 8.3, "volume": 1200.0, "amount": 9960.0},
        ]

    def fake_ingest(db, **kwargs):
        captured["kline_source_name"] = kwargs["kline_source_name"]
        captured["history"] = kwargs["fetch_kline_history"]("600000.SH", trade_day)
        return {"quality_flag": "ok", "status_reason": None}

    monkeypatch.setattr("app.services.stock_pool.get_daily_stock_pool", lambda **kwargs: ["600000.SH"])
    monkeypatch.setattr("app.services.tdx_local_data.load_tdx_day_records", lambda stock_code: [])
    monkeypatch.setattr("app.services.market_data.fetch_recent_klines", fake_fetch_recent_klines)
    monkeypatch.setattr("app.services.multisource_ingest.ingest_market_data", fake_ingest)

    result = scheduler_module._handler_fr04_data_collect(trade_day)

    assert result["quality_flag"] == "ok"
    assert captured["kline_source_name"] == "eastmoney"
    history = captured["history"]
    assert isinstance(history, list) and history
    assert history[-1]["trade_date"] == "2026-03-20"
    assert history[-1]["close"] == 8.3


def test_fr04_handler_refreshes_pool_when_exact_snapshot_missing(db_session, monkeypatch):
    trade_day = date(2026, 3, 20)
    captured: dict[str, object] = {"pool_calls": 0, "refresh_calls": 0}

    _seed_stock_master(db_session, total_stocks=1)

    async def fake_fetch_recent_klines(stock_code: str, limit: int = 60):
        return [
            {"date": "2026-03-18", "open": 8.0, "high": 8.3, "low": 7.9, "close": 8.1, "volume": 1000.0, "amount": 8100.0},
            {"date": "2026-03-19", "open": 8.1, "high": 8.4, "low": 8.0, "close": 8.2, "volume": 1100.0, "amount": 9020.0},
            {"date": "2026-03-20", "open": 8.2, "high": 8.5, "low": 8.1, "close": 8.3, "volume": 1200.0, "amount": 9960.0},
        ]

    def fake_load_internal_exact_core_pool_codes(trade_date=None, *, allow_same_day_fallback=False):
        captured["pool_calls"] += 1
        if captured["pool_calls"] == 1:
            return []
        return ["600000.SH"]

    def fake_refresh_stock_pool(db, trade_date=None, force_rebuild=False, **kwargs):
        captured["refresh_calls"] += 1
        return {"status": "FALLBACK", "trade_date": str(trade_date)}

    def fake_ingest(db, **kwargs):
        captured["kline_source_name"] = kwargs["kline_source_name"]
        return {"quality_flag": "ok", "status_reason": None}

    monkeypatch.setattr("app.services.scheduler._load_internal_exact_core_pool_codes", fake_load_internal_exact_core_pool_codes)
    monkeypatch.setattr("app.services.scheduler.refresh_stock_pool", fake_refresh_stock_pool)
    monkeypatch.setattr("app.services.tdx_local_data.load_tdx_day_records", lambda stock_code: [])
    monkeypatch.setattr("app.services.market_data.fetch_recent_klines", fake_fetch_recent_klines)
    monkeypatch.setattr("app.services.multisource_ingest.ingest_market_data", fake_ingest)

    result = scheduler_module._handler_fr04_data_collect(trade_day)

    assert result["quality_flag"] == "ok"
    assert captured["refresh_calls"] == 1
    assert captured["pool_calls"] >= 2
    assert captured["kline_source_name"] == "eastmoney"


def test_fr04_handler_wires_realtime_hotspot_fetcher(db_session, monkeypatch):
    trade_day = date(2026, 3, 20)
    captured: dict[str, object] = {}

    _seed_stock_master(db_session, total_stocks=1)

    async def fake_fetch_recent_klines(stock_code: str, limit: int = 60):
        return [
            {"date": "2026-03-18", "open": 8.0, "high": 8.3, "low": 7.9, "close": 8.1, "volume": 1000.0, "amount": 8100.0},
            {"date": "2026-03-19", "open": 8.1, "high": 8.4, "low": 8.0, "close": 8.2, "volume": 1100.0, "amount": 9020.0},
            {"date": "2026-03-20", "open": 8.2, "high": 8.5, "low": 8.1, "close": 8.3, "volume": 1200.0, "amount": 9960.0},
        ]

    async def fake_fetch_weibo_hot(top_n: int = 50):
        return [
            {
                "topic_id": "topic-weibo-1",
                "platform": "weibo",
                "rank": 1,
                "title": "600000.SH 政策利好",
                "raw_heat": "1000",
                "fetch_time": datetime(2026, 3, 20, 9, 0, tzinfo=timezone.utc),
                "source_url": "https://weibo.example.com/topic/1",
                "heat_score": 1.0,
            }
        ]

    async def fake_fetch_douyin_hot(top_n: int = 50):
        return [
            {
                "topic_id": "topic-douyin-1",
                "platform": "douyin",
                "rank": 2,
                "title": "600000.SH 产业突破",
                "raw_heat": "900",
                "fetch_time": datetime(2026, 3, 20, 9, 1, tzinfo=timezone.utc),
                "source_url": "https://douyin.example.com/topic/1",
                "heat_score": 0.9,
            }
        ]

    async def fake_fetch_eastmoney_hot(top_n: int = 50):
        return [
            {
                "topic_id": "topic-em-1",
                "platform": "eastmoney",
                "rank": 1,
                "title": "600000.SH 财报超预期",
                "raw_heat": "50000",
                "fetch_time": datetime(2026, 3, 20, 9, 2, tzinfo=timezone.utc),
                "source_url": "https://stock.eastmoney.com/a/topic1.html",
                "heat_score": 0.98,
            }
        ]

    def fake_fetch_northbound_summary(trade_date: date):
        return {"status": "ok", "reason": "scheduler_nb_ok", "fetch_time": datetime(2026, 3, 20, 9, 3, tzinfo=timezone.utc).isoformat()}

    def fake_fetch_etf_flow_summary_global(trade_date: date):
        return {"status": "ok", "reason": "scheduler_etf_ok", "fetch_time": datetime(2026, 3, 20, 9, 4, tzinfo=timezone.utc).isoformat()}

    def fake_ingest(db, **kwargs):
        fetch_hotspot_by_source = kwargs["fetch_hotspot_by_source"]
        captured["weibo"] = fetch_hotspot_by_source("weibo", trade_day)
        captured["douyin"] = fetch_hotspot_by_source("douyin", trade_day)
        captured["eastmoney"] = fetch_hotspot_by_source("eastmoney", trade_day)
        captured["northbound_summary"] = kwargs["fetch_northbound_summary"](trade_day)
        captured["etf_flow_summary"] = kwargs["fetch_etf_flow_summary"](trade_day)
        return {"quality_flag": "stale_ok", "status_reason": "hotspot_source_partial"}

    monkeypatch.setattr("app.services.scheduler.get_daily_stock_pool", lambda **kwargs: ["600000.SH"])
    monkeypatch.setattr("app.services.tdx_local_data.load_tdx_day_records", lambda stock_code: [])
    monkeypatch.setattr("app.services.market_data.fetch_recent_klines", fake_fetch_recent_klines)
    monkeypatch.setattr("app.services.scheduler.fetch_weibo_hot", fake_fetch_weibo_hot)
    monkeypatch.setattr("app.services.scheduler.fetch_douyin_hot", fake_fetch_douyin_hot)
    monkeypatch.setattr("app.services.scheduler.fetch_eastmoney_hot", fake_fetch_eastmoney_hot)
    monkeypatch.setattr("app.services.scheduler.fetch_northbound_summary", fake_fetch_northbound_summary)
    monkeypatch.setattr("app.services.scheduler.fetch_etf_flow_summary_global", fake_fetch_etf_flow_summary_global)
    monkeypatch.setattr("app.services.multisource_ingest.ingest_market_data", fake_ingest)

    result = scheduler_module._handler_fr04_data_collect(trade_day)

    assert result["quality_flag"] == "stale_ok"
    # eastmoney 热点匹配到 600000.SH
    assert captured["eastmoney"] == [
        {
            "rank": 1,
            "topic_title": "600000.SH 财报超预期",
            "source_url": "https://stock.eastmoney.com/a/topic1.html",
            "fetch_time": datetime(2026, 3, 20, 9, 2, tzinfo=timezone.utc),
            "news_event_type": "earnings",
            "hotspot_tags": ["earnings"],
            "stock_codes": ["600000.SH"],
        }
    ]
    assert captured["weibo"] == [
        {
            "rank": 1,
            "topic_title": "600000.SH 政策利好",
            "source_url": "https://weibo.example.com/topic/1",
            "fetch_time": datetime(2026, 3, 20, 9, 0, tzinfo=timezone.utc),
            "news_event_type": "policy",
            "hotspot_tags": ["policy"],
            "stock_codes": ["600000.SH"],
        }
    ]
    assert captured["douyin"] == [
        {
            "rank": 2,
            "topic_title": "600000.SH 产业突破",
            "source_url": "https://douyin.example.com/topic/1",
            "fetch_time": datetime(2026, 3, 20, 9, 1, tzinfo=timezone.utc),
            "news_event_type": "industry_chain",
            "hotspot_tags": ["industry_chain"],
            "stock_codes": ["600000.SH"],
        }
    ]
    assert captured["northbound_summary"]["reason"] == "scheduler_nb_ok"
    assert captured["etf_flow_summary"]["reason"] == "scheduler_etf_ok"


def test_fr04_handler_fail_closes_when_northbound_fetcher_is_stock_level(db_session, monkeypatch):
    trade_day = date(2026, 3, 20)
    captured: dict[str, object] = {}

    _seed_stock_master(db_session, total_stocks=1)

    async def fake_fetch_recent_klines(stock_code: str, limit: int = 60):
        return [
            {"date": "2026-03-18", "open": 8.0, "high": 8.3, "low": 7.9, "close": 8.1, "volume": 1000.0, "amount": 8100.0},
            {"date": "2026-03-19", "open": 8.1, "high": 8.4, "low": 8.0, "close": 8.2, "volume": 1100.0, "amount": 9020.0},
            {"date": "2026-03-20", "open": 8.2, "high": 8.5, "low": 8.1, "close": 8.3, "volume": 1200.0, "amount": 9960.0},
        ]

    def fake_stock_level_northbound_fetcher(stock_code: str):
        return {"status": "ok", "reason": f"stock_level:{stock_code}"}

    def fake_fetch_etf_flow_summary_global(current_trade_date: date):
        return {"status": "ok", "reason": "scheduler_etf_ok", "fetch_time": datetime(2026, 3, 20, 9, 4, tzinfo=timezone.utc).isoformat()}

    def fake_ingest(db, **kwargs):
        captured["northbound_summary"] = kwargs["fetch_northbound_summary"](trade_day)
        captured["etf_flow_summary"] = kwargs["fetch_etf_flow_summary"](trade_day)
        return {"quality_flag": "stale_ok", "status_reason": "northbound_fail_closed"}

    monkeypatch.setattr("app.services.scheduler.get_daily_stock_pool", lambda **kwargs: ["600000.SH"])
    monkeypatch.setattr("app.services.tdx_local_data.load_tdx_day_records", lambda stock_code: [])
    monkeypatch.setattr("app.services.market_data.fetch_recent_klines", fake_fetch_recent_klines)
    monkeypatch.setattr("app.services.scheduler.fetch_northbound_summary", fake_stock_level_northbound_fetcher)
    monkeypatch.setattr("app.services.scheduler.fetch_etf_flow_summary_global", fake_fetch_etf_flow_summary_global)
    monkeypatch.setattr("app.services.multisource_ingest.ingest_market_data", fake_ingest)

    result = scheduler_module._handler_fr04_data_collect(trade_day)

    assert result["quality_flag"] == "stale_ok"
    assert captured["northbound_summary"]["status"] == "missing"
    assert captured["northbound_summary"]["reason"] == "stock_level_fetcher_not_applicable_to_fr04_summary"
    assert captured["etf_flow_summary"]["reason"] == "scheduler_etf_ok"


def test_fr04_handler_limits_ingest_scope_to_core_pool(db_session, monkeypatch):
    trade_day = date(2026, 3, 20)
    captured: dict[str, object] = {}

    _seed_stock_master(db_session, total_stocks=3)

    async def fake_fetch_recent_klines(stock_code: str, limit: int = 60):
        return [
            {"date": "2026-03-18", "open": 8.0, "high": 8.3, "low": 7.9, "close": 8.1, "volume": 1000.0, "amount": 8100.0},
            {"date": "2026-03-19", "open": 8.1, "high": 8.4, "low": 8.0, "close": 8.2, "volume": 1100.0, "amount": 9020.0},
            {"date": "2026-03-20", "open": 8.2, "high": 8.5, "low": 8.1, "close": 8.3, "volume": 1200.0, "amount": 9960.0},
        ]

    def fake_ingest(db, **kwargs):
        captured["stock_codes"] = list(kwargs.get("stock_codes") or [])
        captured["core_pool_codes"] = list(kwargs.get("core_pool_codes") or [])
        return {"quality_flag": "ok", "status_reason": None}

    def fake_load_internal_exact_core_pool_codes(trade_date=None, *, allow_same_day_fallback=False):
        captured["pool_call"] = {"trade_date": trade_date, "allow_same_day_fallback": allow_same_day_fallback}
        return ["600000.SH", "600001.SH"]

    monkeypatch.setattr("app.services.scheduler._load_internal_exact_core_pool_codes", fake_load_internal_exact_core_pool_codes)
    monkeypatch.setattr("app.services.tdx_local_data.load_tdx_day_records", lambda stock_code: [])
    monkeypatch.setattr("app.services.market_data.fetch_recent_klines", fake_fetch_recent_klines)
    monkeypatch.setattr("app.services.multisource_ingest.ingest_market_data", fake_ingest)

    result = scheduler_module._handler_fr04_data_collect(trade_day)

    assert result["quality_flag"] == "ok"
    assert captured["pool_call"] == {"trade_date": trade_day, "allow_same_day_fallback": False}
    assert captured["stock_codes"] == ["600000.SH", "600001.SH"]
    assert captured["core_pool_codes"] == ["600000.SH", "600001.SH"]


def test_fr04_backfill_missing_kline_daily_only_inserts_missing_rows(db_session):
    trade_day = date(2026, 3, 20)
    codes = _seed_stock_master(db_session, total_stocks=3)
    existing_code = codes[0]
    missing_codes = codes[1:]

    existing_result = ingest_market_data(
        db_session,
        trade_date=trade_day,
        stock_codes=[existing_code],
        core_pool_codes=[],
        fetch_kline_history=lambda stock_code, current_trade_date: _history_for(stock_code, current_trade_date),
        now=datetime(2026, 3, 20, 9, 0, tzinfo=timezone.utc),
    )
    assert existing_result["covered_count"] == 1

    fetched_codes: list[str] = []

    async def fake_fetch_recent_klines(stock_code: str, limit: int = 60):
        fetched_codes.append(stock_code)
        rows = _history_for(stock_code, trade_day)
        return [
            {
                "date": row["trade_date"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "amount": row["amount"],
            }
            for row in rows
        ]

    result = backfill_missing_kline_daily(
        db_session,
        trade_date=trade_day,
        stock_codes=codes,
        fetch_recent_klines_async=fake_fetch_recent_klines,
        concurrency=2,
        now=datetime(2026, 3, 20, 9, 5, tzinfo=timezone.utc),
    )

    assert set(fetched_codes) == set(missing_codes)
    assert result["candidate_count"] == 2
    assert result["inserted_count"] == 2
    assert result["failed_count"] == 0
    assert result["skipped_existing_count"] == 1
    assert result["quality_flag"] == "ok"

    kline_daily = Base.metadata.tables["kline_daily"]
    rows = db_session.execute(
        kline_daily.select().where(kline_daily.c.trade_date == trade_day)
    ).fetchall()
    assert len(rows) == 3
    assert {row.stock_code for row in rows} == set(codes)


def test_fr04_backfill_missing_kline_daily_skips_rows_without_target_trade_date(db_session):
    trade_day = date(2026, 3, 20)
    codes = _seed_stock_master(db_session, total_stocks=1)

    async def fake_fetch_recent_klines(stock_code: str, limit: int = 60):
        rows = _history_for(stock_code, trade_day - timedelta(days=1))
        return [
            {
                "date": row["trade_date"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "amount": row["amount"],
            }
            for row in rows
        ]

    result = backfill_missing_kline_daily(
        db_session,
        trade_date=trade_day,
        stock_codes=codes,
        fetch_recent_klines_async=fake_fetch_recent_klines,
        concurrency=1,
        now=datetime(2026, 3, 20, 9, 5, tzinfo=timezone.utc),
    )

    assert result["candidate_count"] == 1
    assert result["inserted_count"] == 0
    assert result["failed_count"] == 1
    assert result["quality_flag"] == "degraded"

    kline_daily = Base.metadata.tables["kline_daily"]
    rows = db_session.execute(
        kline_daily.select().where(kline_daily.c.trade_date == trade_day)
    ).fetchall()
    assert rows == []


def test_fr04_fetch_recent_klines_falls_back_to_smaller_window(monkeypatch):
    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class _FakeAsyncClient:
        requested_limits: list[str] = []

        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, params=None):
            limit = str((params or {}).get("lmt"))
            self.requested_limits.append(limit)
            if limit == "365":
                raise market_data_module.httpx.RemoteProtocolError("server disconnected")
            return _FakeResponse(
                {
                    "data": {
                        "klines": [
                            "2026-04-15,4.01,3.97,4.05,3.95,1171299,466921648.59",
                            "2026-04-16,3.97,3.97,4.00,3.94,363559,144110615.10",
                        ]
                    }
                }
            )

    monkeypatch.setattr(market_data_module.httpx, "AsyncClient", _FakeAsyncClient)

    import asyncio

    rows = asyncio.run(market_data_module.fetch_recent_klines("000002.SZ", limit=365))

    assert len(rows) == 2
    assert rows[-1]["date"] == "2026-04-16"
    assert _FakeAsyncClient.requested_limits[:4] == ["365", "365", "365", "120"]
