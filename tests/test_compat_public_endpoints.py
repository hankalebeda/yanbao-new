from __future__ import annotations

import pytest

from tests.helpers_ssot import (
    insert_market_state_cache,
    insert_pool_snapshot,
    insert_report_bundle_ssot,
    insert_stock_master,
)

pytestmark = [
    pytest.mark.feature("FR10-HOME-01"),
    pytest.mark.test_kind("api"),
]


def _seed_home_basics(db_session) -> None:
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    insert_pool_snapshot(db_session, trade_date="2026-03-17", stock_codes=["600519.SH", "000001.SZ"])
    insert_market_state_cache(db_session, trade_date="2026-03-17", market_state="BULL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-17",
        confidence=0.86,
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-17",
        confidence=0.73,
    )


def test_compat_reports_featured_returns_ok_items_only(client, db_session):
    _seed_home_basics(db_session)

    response = client.get("/api/v1/reports/featured?limit=2")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    items = body["data"]["items"]
    assert len(items) == 2
    assert all(item.get("quality_flag") == "ok" for item in items)


def test_compat_hot_stocks_alias_returns_items(client, db_session):
    _seed_home_basics(db_session)

    response = client.get("/api/v1/hot-stocks?limit=2")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["source"] == "pool"
    assert len(body["data"]["items"]) == 2


def test_compat_market_overview_returns_home_core_fields(client, db_session):
    _seed_home_basics(db_session)

    response = client.get("/api/v1/market-overview")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    data = body["data"]
    assert data["market_state"] == "BULL"
    assert data["pool_size"] == 2
    assert data["today_report_count"] == 2
    assert isinstance(data.get("hot_stocks"), list)
