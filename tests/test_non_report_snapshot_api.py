from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

from app.models import Base


def _seed_stock_context(db_session, *, stock_code: str = "600519.SH", trade_day: date | None = None) -> None:
    trade_day = trade_day or date(2026, 4, 22)
    stock_master = Base.metadata.tables["stock_master"]
    kline_daily = Base.metadata.tables["kline_daily"]
    data_batch = Base.metadata.tables["data_batch"]
    market_state_cache = Base.metadata.tables["market_state_cache"]
    market_hotspot_item = Base.metadata.tables["market_hotspot_item"]
    market_hotspot_item_stock_link = Base.metadata.tables["market_hotspot_item_stock_link"]
    now = datetime.now(timezone.utc)

    db_session.execute(
        stock_master.insert().values(
            stock_code=stock_code,
            stock_name="贵州茅台",
            exchange="SH",
            industry="白酒",
            list_date=date(2001, 8, 27),
            circulating_shares=1_250_000_000,
            is_st=False,
            is_suspended=False,
            is_delisted=False,
            created_at=now,
            updated_at=now,
        )
    )

    for offset in range(21):
        current_day = trade_day - timedelta(days=offset)
        close_price = 1500 + offset
        db_session.execute(
            kline_daily.insert().values(
                kline_id=str(uuid4()),
                stock_code=stock_code,
                trade_date=current_day,
                open=close_price - 5,
                high=close_price + 10,
                low=close_price - 12,
                close=close_price,
                volume=1_000_000 + offset,
                amount=2_000_000_000 + offset * 1000,
                adjust_type="front_adjusted",
                atr_pct=2.15,
                turnover_rate=1.38,
                ma5=1498 + offset,
                ma10=1490 + offset,
                ma20=1480 + offset,
                ma60=1400 + offset,
                volatility_20d=1.88,
                hs300_return_20d=4.2,
                is_suspended=False,
                source_batch_id="seed-batch",
                created_at=now,
            )
        )

    db_session.execute(
        data_batch.insert().values(
            batch_id="seed-kline-batch",
            source_name="tdx_local",
            trade_date=trade_day,
            batch_scope="full_market",
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
    db_session.execute(
        data_batch.insert().values(
            batch_id="seed-hotspot-batch",
            source_name="eastmoney",
            trade_date=trade_day,
            batch_scope="hotspot_merged",
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

    db_session.execute(
        market_state_cache.insert().values(
            trade_date=trade_day,
            market_state="BULL",
            cache_status="FRESH",
            state_reason=None,
            reference_date=trade_day - timedelta(days=1),
            market_state_degraded=False,
            a_type_pct=0.3,
            b_type_pct=0.5,
            c_type_pct=0.2,
            kline_batch_id="seed-kline-batch",
            hotspot_batch_id="seed-hotspot-batch",
            computed_at=now,
            created_at=now,
        )
    )

    hotspot_id = str(uuid4())
    db_session.execute(
        market_hotspot_item.insert().values(
            hotspot_item_id=hotspot_id,
            batch_id="seed-hotspot-batch",
            source_name="eastmoney",
            merged_rank=1,
            source_rank=1,
            topic_title="白酒板块热度回升",
            news_event_type="industry_chain",
            hotspot_tags_json=["白酒", "消费"],
            source_url="https://example.com/hotspot",
            fetch_time=now,
            quality_flag="OK",
            created_at=now,
        )
    )
    db_session.execute(
        market_hotspot_item_stock_link.insert().values(
            hotspot_item_stock_link_id=str(uuid4()),
            hotspot_item_id=hotspot_id,
            stock_code=stock_code,
            relation_role="related",
            match_confidence=0.91,
            created_at=now,
        )
    )
    db_session.commit()


def test_public_stock_snapshot_returns_complete_non_report_payload(client, db_session, monkeypatch):
    from app.services import stock_snapshot_service

    _seed_stock_context(db_session)

    async def fake_company_overview(stock_code: str):
        return {
            "company_name": "贵州茅台",
            "stock_code": stock_code,
            "industry": "白酒",
            "listed_date": "2001-08-27",
            "website": "https://www.moutai.com.cn",
            "intro": "高端白酒龙头",
        }

    async def fake_valuation(stock_code: str):
        return {
            "stock_code": stock_code,
            "stock_name": "贵州茅台",
            "industry": "白酒",
            "region": "贵州",
            "pe_ttm": 23.4,
            "pb": 8.1,
            "total_market_cap": 1_900_000_000_000.0,
            "float_market_cap": 1_850_000_000_000.0,
            "total_shares": 1_256_000_000.0,
            "float_shares": 1_250_000_000.0,
            "listed_days": 9000,
            "fetch_time": datetime.now(timezone.utc).isoformat(),
        }

    async def fake_capital_dimensions(stock_code: str):
        return {
            "capital_flow": {
                "main_force": {
                    "status": "ok",
                    "net_inflow_1d": 120000000.0,
                    "net_inflow_5d": 350000000.0,
                }
            },
            "dragon_tiger": {"status": "ok", "lhb_count_30d": 2},
            "margin_financing": {"status": "ok", "latest_rzye": 8000000000.0},
            "errors": [],
        }

    async def fake_policy_news(limit: int = 5):
        return [
            {
                "title": "消费政策支持高端白酒",
                "source_name": "policy",
                "source_url": "https://example.com/policy",
                "fetch_time": datetime.now(timezone.utc).isoformat(),
                "category": "policy",
            }
        ]

    async def fake_stock_news(stock_code: str, stock_name: str | None = None, keywords: list[str] | None = None, limit: int = 5):
        return [
            {
                "title": "贵州茅台渠道动销改善",
                "source_name": "eastmoney_news",
                "source_url": "https://example.com/news",
                "fetch_time": datetime.now(timezone.utc).isoformat(),
                "category": "news",
            }
        ]

    async def fake_industry_competition(stock_code: str, industry_name: str | None):
        return {
            "industry_name": industry_name,
            "industry_board_code": "BK1277",
            "industry_board_name": "白酒Ⅱ",
            "peers": [{"stock_code": "000858.SZ", "stock_name": "五粮液", "market_cap": 600000000000.0}],
        }

    monkeypatch.setattr(stock_snapshot_service, "fetch_company_overview", fake_company_overview)
    monkeypatch.setattr(stock_snapshot_service, "fetch_valuation_snapshot", fake_valuation)
    monkeypatch.setattr(stock_snapshot_service, "fetch_capital_dimensions", fake_capital_dimensions)
    monkeypatch.setattr(stock_snapshot_service, "fetch_policy_news", fake_policy_news)
    monkeypatch.setattr(stock_snapshot_service, "fetch_stock_news", fake_stock_news)
    monkeypatch.setattr(stock_snapshot_service, "fetch_industry_competition", fake_industry_competition)
    monkeypatch.setattr(stock_snapshot_service, "fetch_northbound_summary", lambda stock_code: {"status": "ok", "reason": "akshare_stock_hsgt_individual_em", "net_inflow_5d": 180000000.0})
    monkeypatch.setattr(stock_snapshot_service, "fetch_etf_flow_summary_global", lambda trade_date: {"status": "ok", "reason": "akshare_fund_etf_daily", "net_creation_redemption_5d": 520000000.0})
    monkeypatch.setattr(stock_snapshot_service, "build_tdx_local_features", lambda stock_code: {"status": "ok", "features": {"ret5": 3.2, "ret20": 8.7}})

    response = client.get("/api/v1/stocks/600519.SH/snapshot?trade_date=2026-04-22")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["stock_code"] == "600519.SH"
    assert data["company_overview"]["company_name"] == "贵州茅台"
    assert data["valuation"]["pe_ttm"] == 23.4
    assert data["financial_analysis"]["data_status"] == "ok"
    assert data["industry_competition"]["peer_count"] == 1
    assert data["capital_data"]["main_force"]["status"] == "ok"
    assert data["capital_data"]["northbound"]["status"] == "ok"
    assert data["news_policy"]["news_count"] == 1
    assert data["news_policy"]["policy_count"] == 1
    assert data["hotspot"]["count"] == 1
    assert data["market_state"]["market_state"] == "BULL"
    assert data["data_status"] == "ok"


def test_internal_collect_non_report_data_persists_usage_with_linked_batches(
    client,
    db_session,
    internal_headers,
    monkeypatch,
):
    from app.services import stock_snapshot_service
    import app.services.capital_usage_collector as capital_usage_collector
    import app.services.stock_profile_collector as stock_profile_collector

    async def fake_capital_dimensions(stock_code: str):
        return {
            "capital_flow": {"main_force": {"status": "ok", "net_inflow_1d": 100.0, "net_inflow_5d": 300.0}},
            "dragon_tiger": {"status": "ok", "lhb_count_30d": 1, "net_buy_total": 200.0},
            "margin_financing": {"status": "realtime_only", "latest_rzye": 500.0, "latest_rqye": 100.0},
        }

    monkeypatch.setattr(capital_usage_collector, "fetch_capital_dimensions", fake_capital_dimensions)
    monkeypatch.setattr(stock_profile_collector, "fetch_stock_profile", lambda stock_code: {
        "status": "ok",
        "snapshot": {
            "pe_ttm": 21.4,
            "pb": 7.2,
            "roe_pct": 28.1,
            "total_mv": 1760000000000.0,
            "circulating_mv": 1700000000000.0,
            "industry": "白酒",
            "region": "贵州",
            "list_date": "2001-08-27",
            "total_shares": 1256000000.0,
            "circulating_shares": 1250000000.0,
        },
    })
    monkeypatch.setattr(stock_snapshot_service, "fetch_northbound_summary", lambda stock_code: {
        "status": "ok",
        "reason": "akshare_stock_hsgt_individual_em",
        "net_inflow_1d": 1000.0,
        "net_inflow_5d": 5000.0,
    })
    monkeypatch.setattr(stock_snapshot_service, "fetch_etf_flow_summary_global", lambda trade_date: {
        "status": "ok",
        "reason": "akshare_fund_etf_daily",
        "net_creation_redemption_5d": 9000.0,
        "tracked_etf_count": 3,
    })

    headers = internal_headers()
    response = client.post(
        "/api/v1/internal/stocks/600519.SH/non-report-data/collect?trade_date=2026-04-22",
        headers=headers,
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["stock_profile"]["persisted_status"] == "ok"
    assert payload["capital_usage"]["per_dataset"]["main_force_flow"]["persisted_status"] == "ok"
    assert payload["northbound_summary"]["status"] == "ok"
    assert payload["etf_flow_summary"]["status"] == "ok"

    usage_table = Base.metadata.tables["report_data_usage"]
    rows = db_session.execute(
        usage_table.select().where(
            usage_table.c.stock_code == "600519.SH",
            usage_table.c.trade_date == date(2026, 4, 22),
            usage_table.c.dataset_name.in_(
                (
                    "main_force_flow",
                    "dragon_tiger_list",
                    "margin_financing",
                    "stock_profile",
                    "northbound_summary",
                    "etf_flow_summary",
                )
            ),
        )
    ).mappings().all()
    assert len(rows) == 6

    data_batch = Base.metadata.tables["data_batch"]
    batch_ids = [str(row.get("batch_id") or "") for row in rows]
    linked_count = db_session.execute(
        data_batch.select().where(data_batch.c.batch_id.in_(batch_ids))
    ).fetchall()
    assert len(linked_count) == len(set(batch_ids))

    fact_table = Base.metadata.tables["data_usage_fact"]
    fact_rows = db_session.execute(
        fact_table.select().where(
            fact_table.c.usage_id.in_([row["usage_id"] for row in rows])
        )
    ).mappings().all()
    assert len(fact_rows) == len(rows)


def test_internal_collect_non_report_data_persists_market_state_truth_lineage(
    client,
    db_session,
    internal_headers,
    monkeypatch,
):
    from app.services import stock_snapshot_service
    import app.services.capital_usage_collector as capital_usage_collector
    import app.services.stock_profile_collector as stock_profile_collector

    _seed_stock_context(db_session)

    async def fake_capital_dimensions(stock_code: str):
        return {
            "capital_flow": {"main_force": {"status": "ok", "net_inflow_1d": 100.0}},
            "dragon_tiger": {"status": "ok", "lhb_count_30d": 1},
            "margin_financing": {"status": "ok", "latest_rzye": 500.0, "latest_rqye": 100.0},
        }

    monkeypatch.setattr(capital_usage_collector, "fetch_capital_dimensions", fake_capital_dimensions)
    monkeypatch.setattr(stock_profile_collector, "fetch_stock_profile", lambda stock_code: {
        "status": "ok",
        "snapshot": {"pe_ttm": 21.4, "pb": 7.2, "industry": "白酒"},
    })
    monkeypatch.setattr(stock_snapshot_service, "fetch_northbound_summary", lambda stock_code: {
        "status": "ok",
        "reason": "akshare_stock_hsgt_individual_em",
        "net_inflow_5d": 5000.0,
    })
    monkeypatch.setattr(stock_snapshot_service, "fetch_etf_flow_summary_global", lambda trade_date: {
        "status": "ok",
        "reason": "akshare_fund_etf_daily",
        "net_creation_redemption_5d": 9000.0,
    })

    response = client.post(
        "/api/v1/internal/stocks/600519.SH/non-report-data/collect?trade_date=2026-04-22",
        headers=internal_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["market_state_input"]["status"] == "ok"
    assert payload["market_state_input"]["market_state_trade_date"] == "2026-04-22"

    usage_table = Base.metadata.tables["report_data_usage"]
    batch_table = Base.metadata.tables["data_batch"]
    lineage_table = Base.metadata.tables["data_batch_lineage"]
    fact_table = Base.metadata.tables["data_usage_fact"]

    usage_row = db_session.execute(
        usage_table.select().where(
            usage_table.c.stock_code == "600519.SH",
            usage_table.c.trade_date == date(2026, 4, 22),
            usage_table.c.dataset_name == "market_state_input",
            usage_table.c.source_name == "market_state_cache",
        )
    ).mappings().one()
    batch_row = db_session.execute(
        batch_table.select().where(batch_table.c.batch_id == usage_row["batch_id"])
    ).mappings().one()
    fact_row = db_session.execute(
        fact_table.select().where(fact_table.c.usage_id == usage_row["usage_id"])
    ).mappings().one()
    lineage_rows = db_session.execute(
        lineage_table.select().where(
            lineage_table.c.child_batch_id == usage_row["batch_id"],
            lineage_table.c.lineage_role == "MERGED_FROM",
        )
    ).mappings().all()

    assert batch_row["source_name"] == "market_state_cache"
    assert batch_row["batch_scope"] == "market_state_derived"
    assert usage_row["status"] == "ok"
    assert fact_row["batch_id"] == usage_row["batch_id"]
    assert {str(row["parent_batch_id"]) for row in lineage_rows} == {
        "seed-kline-batch",
        "seed-hotspot-batch",
    }


def test_internal_collect_non_report_data_infers_market_state_parent_batches_from_usage_rows(
    client,
    db_session,
    internal_headers,
    monkeypatch,
):
    from app.services import stock_snapshot_service
    import app.services.capital_usage_collector as capital_usage_collector
    import app.services.stock_profile_collector as stock_profile_collector

    trade_day = date(2026, 4, 22)
    _seed_stock_context(db_session, trade_day=trade_day)

    market_state_cache = Base.metadata.tables["market_state_cache"]
    usage_table = Base.metadata.tables["report_data_usage"]
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    db_session.execute(
        market_state_cache.update()
        .where(market_state_cache.c.trade_date == trade_day)
        .values(kline_batch_id=None, hotspot_batch_id=None)
    )
    db_session.execute(
        usage_table.insert().values(
            usage_id="seed-kline-usage",
            trade_date=trade_day,
            stock_code="600519.SH",
            dataset_name="kline_daily",
            source_name="tdx_local",
            batch_id="seed-kline-batch",
            fetch_time=now,
            status="ok",
            status_reason=None,
            created_at=now,
        )
    )
    db_session.execute(
        usage_table.insert().values(
            usage_id="seed-hotspot-usage",
            trade_date=trade_day,
            stock_code="600519.SH",
            dataset_name="hotspot_top50",
            source_name="eastmoney",
            batch_id="seed-hotspot-batch",
            fetch_time=now,
            status="ok",
            status_reason=None,
            created_at=now,
        )
    )
    db_session.commit()

    async def fake_capital_dimensions(stock_code: str):
        return {
            "capital_flow": {"main_force": {"status": "ok", "net_inflow_1d": 100.0}},
            "dragon_tiger": {"status": "ok", "lhb_count_30d": 1},
            "margin_financing": {"status": "ok", "latest_rzye": 500.0, "latest_rqye": 100.0},
        }

    monkeypatch.setattr(capital_usage_collector, "fetch_capital_dimensions", fake_capital_dimensions)
    monkeypatch.setattr(stock_profile_collector, "fetch_stock_profile", lambda stock_code: {
        "status": "ok",
        "snapshot": {"pe_ttm": 21.4, "pb": 7.2, "industry": "白酒"},
    })
    monkeypatch.setattr(stock_snapshot_service, "fetch_northbound_summary", lambda stock_code: {
        "status": "ok",
        "reason": "akshare_stock_hsgt_individual_em",
        "net_inflow_5d": 5000.0,
    })
    monkeypatch.setattr(stock_snapshot_service, "fetch_etf_flow_summary_global", lambda trade_date: {
        "status": "ok",
        "reason": "akshare_fund_etf_daily",
        "net_creation_redemption_5d": 9000.0,
    })

    response = client.post(
        "/api/v1/internal/stocks/600519.SH/non-report-data/collect?trade_date=2026-04-22",
        headers=internal_headers(),
    )

    assert response.status_code == 200

    refreshed_row = db_session.execute(
        market_state_cache.select().where(market_state_cache.c.trade_date == trade_day)
    ).mappings().one()
    assert refreshed_row["kline_batch_id"] == "seed-kline-batch"
    assert refreshed_row["hotspot_batch_id"] == "seed-hotspot-batch"


def test_internal_collect_non_report_data_keeps_proxy_reason_for_margin_financing(
    client,
    db_session,
    internal_headers,
    monkeypatch,
):
    from app.services import stock_snapshot_service
    import app.services.capital_usage_collector as capital_usage_collector
    import app.services.stock_profile_collector as stock_profile_collector

    _seed_stock_context(db_session)

    async def fake_capital_dimensions(stock_code: str):
        return {
            "capital_flow": {"main_force": {"status": "ok", "net_inflow_1d": 100.0}},
            "dragon_tiger": {"status": "ok", "lhb_count_30d": 1},
            "margin_financing": {
                "status": "stale_ok",
                "reason": "remote_unavailable_use_local_tdx_proxy",
                "proxy": True,
                "latest_rzye": 500.0,
                "latest_rqye": 100.0,
            },
        }

    monkeypatch.setattr(capital_usage_collector, "fetch_capital_dimensions", fake_capital_dimensions)
    monkeypatch.setattr(stock_profile_collector, "fetch_stock_profile", lambda stock_code: {
        "status": "ok",
        "snapshot": {"pe_ttm": 21.4, "pb": 7.2, "industry": "白酒"},
    })
    monkeypatch.setattr(stock_snapshot_service, "fetch_northbound_summary", lambda stock_code: {
        "status": "ok",
        "reason": "akshare_stock_hsgt_individual_em",
        "net_inflow_5d": 5000.0,
    })
    monkeypatch.setattr(stock_snapshot_service, "fetch_etf_flow_summary_global", lambda trade_date: {
        "status": "ok",
        "reason": "akshare_fund_etf_daily",
        "net_creation_redemption_5d": 9000.0,
    })

    response = client.post(
        "/api/v1/internal/stocks/600519.SH/non-report-data/collect?trade_date=2026-04-22",
        headers=internal_headers(),
    )

    assert response.status_code == 200

    usage_table = Base.metadata.tables["report_data_usage"]
    margin_row = db_session.execute(
        usage_table.select().where(
            usage_table.c.stock_code == "600519.SH",
            usage_table.c.trade_date == date(2026, 4, 22),
            usage_table.c.dataset_name == "margin_financing",
        )
    ).mappings().one()
    snapshot = json.loads(str(margin_row["status_reason"])[len("capital_snapshot:"):])

    assert margin_row["status"] == "proxy_ok"
    assert snapshot["persisted_status"] == "proxy_ok"
    assert snapshot["source_reason"] == "remote_unavailable_use_local_tdx_proxy"
    assert snapshot["proxy"] is True


def test_internal_collect_non_report_data_preserves_proxy_and_zero_value_capital_statuses(
    client,
    db_session,
    internal_headers,
    monkeypatch,
):
    from app.services import stock_snapshot_service
    import app.services.capital_usage_collector as capital_usage_collector
    import app.services.stock_profile_collector as stock_profile_collector

    async def fake_capital_dimensions(stock_code: str):
        return {
            "capital_flow": {
                "main_force": {
                    "status": "stale_ok",
                    "reason": "remote_unavailable_use_local_tdx_proxy",
                    "proxy": True,
                    "net_inflow_1d": 100.0,
                    "net_inflow_5d": 300.0,
                }
            },
            "dragon_tiger": {
                "status": "ok",
                "reason": "no_recent_lhb_records",
                "lhb_count_30d": 0,
                "lhb_count_90d": 0,
                "net_buy_total": 0.0,
                "source": "eastmoney",
            },
            "margin_financing": {
                "status": "realtime_only",
                "latest_rzye": 500.0,
                "latest_rqye": 100.0,
            },
        }

    monkeypatch.setattr(capital_usage_collector, "fetch_capital_dimensions", fake_capital_dimensions)
    monkeypatch.setattr(stock_profile_collector, "fetch_stock_profile", lambda stock_code: {
        "status": "ok",
        "snapshot": {
            "pe_ttm": 21.4,
            "pb": 7.2,
            "roe_pct": 28.1,
            "total_mv": 1760000000000.0,
            "circulating_mv": 1700000000000.0,
            "industry": "白酒",
            "region": "贵州",
            "list_date": "2001-08-27",
        },
    })
    monkeypatch.setattr(stock_snapshot_service, "fetch_northbound_summary", lambda stock_code: {
        "status": "ok",
        "reason": "akshare_stock_hsgt_individual_em",
        "net_inflow_1d": 1000.0,
        "net_inflow_5d": 5000.0,
    })
    monkeypatch.setattr(stock_snapshot_service, "fetch_etf_flow_summary_global", lambda trade_date: {
        "status": "ok",
        "reason": "akshare_fund_etf_daily",
        "net_creation_redemption_5d": 9000.0,
        "tracked_etf_count": 3,
    })

    response = client.post(
        "/api/v1/internal/stocks/600519.SH/non-report-data/collect?trade_date=2026-04-22",
        headers=internal_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["capital_usage"]["per_dataset"]["main_force_flow"]["persisted_status"] == "proxy_ok"
    assert payload["capital_usage"]["per_dataset"]["dragon_tiger_list"]["persisted_status"] == "ok"
    assert payload["capital_usage"]["per_dataset"]["margin_financing"]["persisted_status"] == "realtime_only"

    usage_table = Base.metadata.tables["report_data_usage"]
    rows = db_session.execute(
        usage_table.select().where(
            usage_table.c.stock_code == "600519.SH",
            usage_table.c.trade_date == date(2026, 4, 22),
            usage_table.c.dataset_name.in_(("main_force_flow", "dragon_tiger_list", "margin_financing")),
        )
    ).mappings().all()
    row_map = {row["dataset_name"]: row for row in rows}

    assert row_map["main_force_flow"]["status"] == "proxy_ok"
    assert row_map["dragon_tiger_list"]["status"] == "ok"
    assert row_map["margin_financing"]["status"] == "realtime_only"
    assert "capital_snapshot:" in str(row_map["main_force_flow"]["status_reason"])
    assert "capital_snapshot:" in str(row_map["dragon_tiger_list"]["status_reason"])
    assert "capital_snapshot:" in str(row_map["margin_financing"]["status_reason"])

    data_batch = Base.metadata.tables["data_batch"]
    capital_batch = db_session.execute(
        data_batch.select().where(data_batch.c.batch_id == payload["capital_usage"]["batch_id"])
    ).mappings().one()
    assert capital_batch["batch_status"] == "SUCCESS"
    assert capital_batch["records_success"] == 3
    assert capital_batch["quality_flag"] == "stale_ok"


def test_persist_stock_profile_reuses_latest_non_future_snapshot_when_live_fetch_fails(db_session, monkeypatch):
    import app.services.stock_profile_collector as stock_profile_collector

    usage_table = Base.metadata.tables["report_data_usage"]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    older_snapshot = {
        "pe_ttm": 21.42,
        "pb": 7.21,
        "roe_pct": 32.53,
        "total_mv": 1763196462720.0,
        "circulating_mv": 1763196462720.0,
        "industry": "白酒",
        "region": "贵州",
        "list_date": "2001-08-27",
    }
    future_snapshot = {
        "pe_ttm": 99.99,
        "pb": 99.99,
        "roe_pct": 99.99,
        "total_mv": 999999999999.0,
        "circulating_mv": 999999999999.0,
        "industry": "未来行业",
        "region": "未来地区",
        "list_date": "2099-01-01",
    }
    db_session.execute(
        usage_table.insert().values(
            usage_id=str(uuid4()),
            trade_date=date(2026, 4, 16),
            stock_code="600519.SH",
            dataset_name="stock_profile",
            source_name="eastmoney_push2_stock_get",
            batch_id=str(uuid4()),
            fetch_time=now,
            status="ok",
            status_reason="profile_snapshot:" + json.dumps(older_snapshot, ensure_ascii=False),
            created_at=now,
        )
    )
    db_session.execute(
        usage_table.insert().values(
            usage_id=str(uuid4()),
            trade_date=date(2026, 4, 23),
            stock_code="600519.SH",
            dataset_name="stock_profile",
            source_name="eastmoney_push2_stock_get",
            batch_id=str(uuid4()),
            fetch_time=now,
            status="ok",
            status_reason="profile_snapshot:" + json.dumps(future_snapshot, ensure_ascii=False),
            created_at=now,
        )
    )
    db_session.commit()

    monkeypatch.setattr(
        stock_profile_collector,
        "fetch_stock_profile",
        lambda stock_code: {"status": "failed", "reason": "RemoteProtocolError:Server disconnected without sending a response."},
    )

    result = stock_profile_collector.persist_stock_profile(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-04-21",
        batch_id="batch-fallback",
    )

    row = db_session.execute(
        usage_table.select().where(
            usage_table.c.trade_date == date(2026, 4, 21),
            usage_table.c.stock_code == "600519.SH",
            usage_table.c.dataset_name == "stock_profile",
        )
    ).mappings().one()

    assert result["persisted_status"] == "stale_ok"
    assert result["reason"] == "reused_latest_ok_snapshot:2026-04-16"
    assert result["snapshot"]["pe_ttm"] == older_snapshot["pe_ttm"]
    assert result["snapshot"]["industry"] == older_snapshot["industry"]
    assert result["snapshot"]["_source_trade_date"] == "2026-04-16"
    assert row["status"] == "stale_ok"
    assert "99.99" not in str(row["status_reason"])


def test_materialize_non_report_usage_for_pool_aggregates_truth_statuses(db_session, monkeypatch):
    from app.services import stock_snapshot_service

    captured_calls: list[tuple[str, str | None]] = []

    def fake_collect_non_report_usage_sync(db, *, stock_code: str, trade_date: str | None = None):
        captured_calls.append((stock_code, trade_date))
        if stock_code == "000001.SZ":
            return {
                "stock_code": stock_code,
                "trade_date": trade_date,
                "market_state_input": {"status": "ok"},
                "northbound_summary": {"status": "missing"},
                "capital_usage": {"per_dataset": {"margin_financing": {"persisted_status": "proxy_ok"}}},
            }
        return {
            "stock_code": stock_code,
            "trade_date": trade_date,
            "market_state_input": {"status": "ok"},
            "northbound_summary": {"status": "ok"},
            "capital_usage": {"per_dataset": {"margin_financing": {"persisted_status": "realtime_only"}}},
        }

    monkeypatch.setattr(stock_snapshot_service, "collect_non_report_usage_sync", fake_collect_non_report_usage_sync)

    result = stock_snapshot_service.materialize_non_report_usage_for_pool(
        db_session,
        stock_codes=["600519.SH", "000001.SZ", "600519.SH"],
        trade_date="2026-04-22",
    )

    assert captured_calls == [
        ("600519.SH", "2026-04-22"),
        ("000001.SZ", "2026-04-22"),
    ]
    assert result == {
        "trade_date": "2026-04-22",
        "total_stocks": 2,
        "succeeded": 2,
        "failed": 0,
        "failed_stocks": [],
        "market_state_input_written": 2,
        "northbound_status_counts": {"ok": 1, "missing": 1},
        "margin_financing_status_counts": {"realtime_only": 1, "proxy_ok": 1},
    }