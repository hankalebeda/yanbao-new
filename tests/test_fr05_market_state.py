from datetime import date, datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

pytestmark = [
    pytest.mark.feature("FR-05"),
    pytest.mark.feature("FR05-MKT-01"),
    pytest.mark.feature("FR05-MKT-04"),
]

from app.models import MarketStateCache
from app.services.market_state import MarketStateMetrics


def test_fr05_market_state_enum(client, db_session, monkeypatch):
    import app.services.market_state as market_state

    current = datetime(2026, 3, 10, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai"))

    monkeypatch.setattr(market_state, "_now_cn", lambda: current)
    monkeypatch.setattr(market_state, "is_trade_day", lambda dt=None: True)
    monkeypatch.setattr(market_state, "_previous_trade_date", lambda service_date: date(2026, 3, 9))
    monkeypatch.setattr(
        market_state,
        "_load_reference_metrics",
        lambda db, reference_date: MarketStateMetrics(
            reference_date=reference_date,
            hs300_ma5=4020.0,
            hs300_ma20=4000.0,
            hs300_ma20_5d_ago=3950.0,
            hs300_return_20d=0.081,
            kline_batch_id=str(uuid4()),
            hotspot_batch_id=str(uuid4()),
        ),
    )

    response = client.get("/api/v1/market/state")

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["request_id"]
    assert payload["data"]["market_state"] in ("BULL", "NEUTRAL", "BEAR")
    assert payload["data"]["market_state"] == "BULL"
    assert payload["data"]["market_state_date"] == "2026-03-10"
    assert payload["data"]["reference_date"] == "2026-03-09"
    assert payload["data"]["state_reason"] not in (None, "")

    db_session.expire_all()
    row = db_session.get(MarketStateCache, date(2026, 3, 10))
    assert row is not None
    assert row.cache_status == "FRESH"
    assert row.state_reason not in (None, "")


def test_fr05_state_reason_on_fallback(client, db_session, monkeypatch):
    import app.services.market_state as market_state

    ghost_time = datetime(2026, 3, 9, 8, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    monkeypatch.setattr(market_state, "_now_cn", lambda: ghost_time)
    monkeypatch.setattr(market_state, "is_trade_day", lambda dt=None: True)

    cold_start = client.get("/api/v1/market/state")
    cold_payload = cold_start.json()["data"]
    assert cold_start.status_code == 200
    assert cold_payload["market_state"] == "NEUTRAL"
    assert cold_payload["state_reason"] == "COLD_START_FALLBACK"
    assert cold_payload["reference_date"] is None

    live_time = datetime(2026, 3, 9, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    monkeypatch.setattr(market_state, "_now_cn", lambda: live_time)
    monkeypatch.setattr(market_state, "_previous_trade_date", lambda service_date: date(2026, 3, 6))
    monkeypatch.setattr(market_state, "_load_reference_metrics", lambda db, reference_date: None)

    degraded = client.get("/api/v1/market/state")
    degraded_payload = degraded.json()["data"]
    assert degraded.status_code == 200
    assert degraded_payload["market_state"] == "NEUTRAL"
    assert degraded_payload["reference_date"] == "2026-03-06"
    assert "market_state_degraded=true" in degraded_payload["state_reason"]

    db_session.expire_all()
    row = db_session.get(MarketStateCache, date(2026, 3, 9))
    assert row is not None
    assert row.cache_status == "DEGRADED_NEUTRAL"
    assert row.market_state_degraded is True


def test_fr05_ghost_period_uses_previous_cache_row(client, db_session, monkeypatch):
    import app.services.market_state as market_state

    prior_row = MarketStateCache(
        trade_date=date(2026, 3, 8),
        market_state="BULL",
        cache_status="FRESH",
        state_reason="computed_from_reference_date=2026-03-07;cache_status=FRESH",
        reference_date=date(2026, 3, 7),
        market_state_degraded=False,
        computed_at=datetime(2026, 3, 8, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        created_at=datetime(2026, 3, 8, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    db_session.add(prior_row)
    db_session.commit()

    ghost_time = datetime(2026, 3, 9, 8, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    monkeypatch.setattr(market_state, "_now_cn", lambda: ghost_time)
    monkeypatch.setattr(market_state, "is_trade_day", lambda dt=None: True)

    response = client.get("/api/v1/market/state")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["market_state"] == "BULL"
    assert payload["trade_date"] == "2026-03-08"
    assert payload["market_state_date"] == "2026-03-08"
    assert payload["reference_date"] == "2026-03-07"
    assert payload["state_reason"] == "computed_from_reference_date=2026-03-07;cache_status=FRESH"

    db_session.expire_all()
    assert db_session.get(MarketStateCache, date(2026, 3, 9)) is None


def test_fr05_existing_cache_backfills_missing_reason(client, db_session, monkeypatch):
    import app.services.market_state as market_state

    current = datetime(2026, 3, 10, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    monkeypatch.setattr(market_state, "_now_cn", lambda: current)
    monkeypatch.setattr(market_state, "is_trade_day", lambda dt=None: True)

    row = MarketStateCache(
        trade_date=date(2026, 3, 10),
        market_state="NEUTRAL",
        cache_status="FRESH",
        state_reason=None,
        reference_date=date(2026, 3, 9),
        market_state_degraded=False,
        computed_at=current,
        created_at=current,
    )
    db_session.add(row)
    db_session.commit()

    response = client.get("/api/v1/market/state")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["state_reason"] == "computed_from_reference_date=2026-03-09;cache_status=FRESH"

    db_session.expire_all()
    refreshed = db_session.get(MarketStateCache, date(2026, 3, 10))
    assert refreshed is not None
    assert refreshed.state_reason == "computed_from_reference_date=2026-03-09;cache_status=FRESH"


def test_fr05_bear_priority():
    import app.services.market_state as market_state

    assert market_state._resolve_market_state(is_bull=True, is_bear=True) == "BEAR"


def test_fr05_scheduler_handler_passes_trade_date(monkeypatch):
    import app.services.scheduler as scheduler

    captured = {}

    def fake_calc_and_cache_market_state(*, trade_date=None):
        captured["trade_date"] = trade_date
        return "BULL"

    monkeypatch.setattr(scheduler, "calc_and_cache_market_state", fake_calc_and_cache_market_state)

    result = scheduler._handler_fr05_market_state(date(2026, 3, 10))

    assert result == {"market_state": "BULL"}
    assert captured["trade_date"] == date(2026, 3, 10)


def test_fr05_truth_materialize_handler_passes_trade_date_and_pool(monkeypatch):
    import app.services.scheduler as scheduler
    import app.services.stock_snapshot_service as stock_snapshot_service

    captured: dict[str, object] = {}

    class DummyDb:
        def close(self):
            captured["closed"] = True

    def fake_materialize_non_report_usage_for_pool(db, *, stock_codes, trade_date=None):
        captured["stock_codes"] = list(stock_codes)
        captured["trade_date"] = trade_date
        return {"succeeded": 2, "failed": 0}

    def fake_load_internal_exact_core_pool_codes(trade_date=None, *, allow_same_day_fallback=False):
        captured["pool_call"] = {"trade_date": trade_date, "allow_same_day_fallback": allow_same_day_fallback}
        return ["600519.SH", "000001.SZ"]

    monkeypatch.setattr(scheduler, "_load_internal_exact_core_pool_codes", fake_load_internal_exact_core_pool_codes)
    monkeypatch.setattr(scheduler, "SessionLocal", lambda: DummyDb())
    monkeypatch.setattr(stock_snapshot_service, "materialize_non_report_usage_for_pool", fake_materialize_non_report_usage_for_pool)

    result = scheduler._handler_fr05_non_report_truth_materialize(date(2026, 3, 10))

    assert result == {"succeeded": 2, "failed": 0}
    assert captured["stock_codes"] == ["600519.SH", "000001.SZ"]
    assert captured["trade_date"] == date(2026, 3, 10)
    assert captured["pool_call"] == {"trade_date": date(2026, 3, 10), "allow_same_day_fallback": False}
    assert captured["closed"] is True


def test_fr05_truth_materialize_handler_refreshes_pool_when_exact_day_missing(monkeypatch):
    import app.services.scheduler as scheduler
    import app.services.stock_snapshot_service as stock_snapshot_service

    captured: dict[str, object] = {"pool_calls": 0}

    class DummyDb:
        def close(self):
            captured["closed"] = True

    def fake_load_internal_exact_core_pool_codes(trade_date=None, *, allow_same_day_fallback=False):
        captured["pool_calls"] += 1
        if captured["pool_calls"] == 1:
            return []
        return ["600519.SH"]

    def fake_refresh_stock_pool(db, trade_date=None, force_rebuild=False):
        captured["refresh_called"] = (trade_date, force_rebuild)
        return {"trade_date": trade_date or date(2026, 3, 10).isoformat()}

    def fake_materialize_non_report_usage_for_pool(db, *, stock_codes, trade_date=None):
        captured["stock_codes"] = list(stock_codes)
        captured["trade_date"] = trade_date
        return {"succeeded": 1, "failed": 0}

    monkeypatch.setattr(scheduler, "_load_internal_exact_core_pool_codes", fake_load_internal_exact_core_pool_codes)
    monkeypatch.setattr(scheduler, "refresh_stock_pool", fake_refresh_stock_pool)
    monkeypatch.setattr(scheduler, "SessionLocal", lambda: DummyDb())
    monkeypatch.setattr(stock_snapshot_service, "materialize_non_report_usage_for_pool", fake_materialize_non_report_usage_for_pool)

    result = scheduler._handler_fr05_non_report_truth_materialize(date(2026, 3, 10), force=True)

    assert result == {"succeeded": 1, "failed": 0}
    assert captured["refresh_called"] == (date(2026, 3, 10), True)
    assert captured["stock_codes"] == ["600519.SH"]


@pytest.mark.feature("FR05-MKT-02")
def test_fr05_bootstrap_market_state_step_passes_trade_date(monkeypatch):
    import app.services.market_state as market_state
    import scripts.bootstrap_real_data as bootstrap_real_data

    captured = {}

    def fake_calc_and_cache_market_state(*, trade_date=None):
        captured["trade_date"] = trade_date
        return "NEUTRAL"

    monkeypatch.setattr(market_state, "calc_and_cache_market_state", fake_calc_and_cache_market_state)

    result = bootstrap_real_data.step_market_state(None, date(2026, 3, 11))

    assert result == "NEUTRAL"
    assert captured["trade_date"] == date(2026, 3, 11)
