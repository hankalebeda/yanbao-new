from __future__ import annotations

import pytest

pytestmark = pytest.mark.feature("FR-08")

from app.models import Base
from tests.helpers_ssot import insert_kline, insert_report_bundle_ssot, insert_stock_master


def _pro_headers(client, create_user) -> dict[str, str]:
    account = create_user(
        email="fr08-lifecycle@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )
    login = client.post("/auth/login", json={"email": account["user"].email, "password": account["password"]})
    assert login.status_code == 200
    return {"Authorization": f"Bearer {login.json()['data']['access_token']}"}


def _expected_trade_metrics(*, entry_price: float, exit_price: float, shares: int) -> dict[str, float]:
    buy_amount = entry_price * shares
    buy_commission = max(buy_amount * 0.00025, 5.0)
    buy_slippage = buy_amount * 0.0005
    buy_paid = buy_amount + buy_commission + buy_slippage

    sell_amount = exit_price * shares
    sell_commission = max(sell_amount * 0.00025, 5.0)
    stamp_duty = sell_amount * 0.0005
    sell_slippage = sell_amount * 0.0005
    sell_proceeds = sell_amount - sell_commission - stamp_duty - sell_slippage

    net_pnl = sell_proceeds - buy_paid
    return {
        "buy_paid": buy_paid,
        "sell_proceeds": sell_proceeds,
        "net_pnl": net_pnl,
        "net_return_pct": round(net_pnl / buy_paid, 6),
        "commission_total": round(buy_commission + sell_commission, 4),
        "stamp_duty": round(stamp_duty, 4),
        "slippage_total": round(buy_slippage + sell_slippage, 4),
    }


def test_fr08_position_lifecycle_e2e_collected(client, db_session, create_user):
    from app.services.sim_positioning_ssot import process_trade_date

    signal_day = "2026-03-06"
    open_day = "2026-03-09"
    close_day = "2026-03-10"
    expected_entry_price = 10.0
    expected_exit_price = 11.0
    expected_shares = 2000
    expected_metrics = _expected_trade_metrics(
        entry_price=expected_entry_price,
        exit_price=expected_exit_price,
        shares=expected_shares,
    )

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date=signal_day,
        confidence=0.91,
        signal_entry_price=10.0,
        target_price=11.0,
        stop_loss=9.0,
        trade_instructions={
            "10k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
            "100k": {"status": "EXECUTE", "position_ratio": 0.2, "skip_reason": None},
            "500k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
        },
    )
    headers = _pro_headers(client, create_user)

    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date=open_day,
        open_price=10.0,
        high_price=10.4,
        low_price=9.9,
        close_price=10.0,
    )
    process_trade_date(db_session, open_day)

    open_dashboard = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=headers)
    assert open_dashboard.status_code == 200
    open_positions = open_dashboard.json()["data"]["open_positions"]
    assert len(open_positions) == 1
    dashboard_open = open_positions[0]
    assert dashboard_open["report_id"] == report.report_id
    assert dashboard_open["position_status"] == "OPEN"
    assert dashboard_open["actual_entry_price"] == expected_entry_price
    assert dashboard_open["shares"] == expected_shares
    assert dashboard_open["exit_price"] is None
    assert dashboard_open["net_return_pct"] is None

    open_by_report = client.get(f"/api/v1/sim/positions/by-report/{report.report_id}", headers=headers)
    assert open_by_report.status_code == 200
    open_items = open_by_report.json()["data"]["items"]
    assert len(open_items) == 1
    open_item = open_items[0]
    assert open_item["status"] == "OPEN"
    assert open_item["position_status_raw"] == "OPEN"
    assert open_item["sim_open_date"] == open_day
    assert open_item["sim_open_price"] == expected_entry_price
    assert open_item["actual_entry_price"] == expected_entry_price
    assert open_item["sim_qty"] == expected_shares
    assert open_item["target_price_1"] == expected_exit_price
    assert open_item["stop_loss_price"] == 9.0
    assert open_item["sim_close_date"] is None
    assert open_item["sim_close_price"] is None
    assert open_item["sim_pnl_pct"] is None

    sim_position_table = Base.metadata.tables["sim_position"]
    open_row = db_session.execute(
        sim_position_table.select().where(sim_position_table.c.report_id == report.report_id)
    ).mappings().one()
    assert open_row["position_status"] == "OPEN"
    assert open_row["entry_date"].isoformat() == open_day
    assert open_row["actual_entry_price"] == expected_entry_price
    assert open_row["shares"] == expected_shares
    assert open_row["exit_price"] is None
    assert open_row["exit_date"] is None
    assert open_row["net_return_pct"] is None

    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date=close_day,
        open_price=10.7,
        high_price=11.2,
        low_price=10.4,
        close_price=11.1,
    )
    process_trade_date(db_session, close_day)

    closed_dashboard = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=headers)
    assert closed_dashboard.status_code == 200
    assert closed_dashboard.json()["data"]["open_positions"] == []

    closed_by_report = client.get(f"/api/v1/sim/positions/by-report/{report.report_id}", headers=headers)
    assert closed_by_report.status_code == 200
    closed_items = closed_by_report.json()["data"]["items"]
    assert len(closed_items) == 1
    closed_item = closed_items[0]
    assert closed_item["status"] == "TAKE_PROFIT"
    assert closed_item["position_status_raw"] == "TAKE_PROFIT"
    assert closed_item["sim_open_date"] == open_day
    assert closed_item["actual_entry_price"] == expected_entry_price
    assert closed_item["sim_qty"] == expected_shares
    assert closed_item["sim_close_date"] == close_day
    assert closed_item["sim_close_price"] == expected_exit_price
    assert closed_item["sim_pnl_pct"] == pytest.approx(expected_metrics["net_return_pct"] * 100, abs=1e-9)

    closed_row = db_session.execute(
        sim_position_table.select().where(sim_position_table.c.report_id == report.report_id)
    ).mappings().one()
    assert closed_row["position_status"] == "TAKE_PROFIT"
    assert closed_row["entry_date"].isoformat() == open_day
    assert closed_row["actual_entry_price"] == expected_entry_price
    assert closed_row["shares"] == expected_shares
    assert closed_row["exit_date"].isoformat() == close_day
    assert closed_row["exit_price"] == expected_exit_price
    assert closed_row["holding_days"] == 1
    assert float(closed_row["net_return_pct"]) == pytest.approx(expected_metrics["net_return_pct"], abs=1e-9)
    assert float(closed_row["commission_total"]) == pytest.approx(expected_metrics["commission_total"], abs=1e-9)
    assert float(closed_row["stamp_duty"]) == pytest.approx(expected_metrics["stamp_duty"], abs=1e-9)
    assert float(closed_row["slippage_total"]) == pytest.approx(expected_metrics["slippage_total"], abs=1e-9)

    account_table = Base.metadata.tables["sim_account"]
    account_row = db_session.execute(
        account_table.select().where(account_table.c.capital_tier == "100k")
    ).mappings().one()
    assert float(account_row["cash_available"]) == pytest.approx(100000 + expected_metrics["net_pnl"], abs=1e-6)
    assert float(account_row["total_asset"]) == pytest.approx(100000 + expected_metrics["net_pnl"], abs=1e-6)
    assert account_row["active_position_count"] == 0

    reports_response = client.get("/api/v1/reports", headers=headers)
    assert reports_response.status_code == 200
    report_item = next(item for item in reports_response.json()["data"]["items"] if item["report_id"] == report.report_id)
    assert report_item["position_status"] == "TAKE_PROFIT"

    business_events = db_session.execute(
        Base.metadata.tables["business_event"]
        .select()
        .where(Base.metadata.tables["business_event"].c.event_type == "POSITION_CLOSED")
    ).mappings().all()
    assert len(business_events) == 1

    outbox_rows = db_session.execute(
        Base.metadata.tables["outbox_event"]
        .select()
        .where(Base.metadata.tables["outbox_event"].c.business_event_id == business_events[0]["business_event_id"])
    ).mappings().all()
    assert len(outbox_rows) == 1


def test_fr08_account_snapshot_e2e_stop_loss(client, db_session, create_user):
    from app.services.runtime_materialization import materialize_sim_dashboard_snapshots
    from app.services.sim_positioning_ssot import process_trade_date

    signal_day = "2026-03-06"
    open_day = "2026-03-09"
    stop_day = "2026-03-10"

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date=signal_day,
        confidence=0.91,
        signal_entry_price=10.0,
        target_price=11.0,
        stop_loss=9.0,
        trade_instructions={
            "10k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
            "100k": {"status": "EXECUTE", "position_ratio": 0.2, "skip_reason": None},
            "500k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
        },
    )
    headers = _pro_headers(client, create_user)

    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date=open_day,
        open_price=10.0,
        high_price=10.4,
        low_price=9.9,
        close_price=10.0,
    )
    process_trade_date(db_session, open_day)

    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date=stop_day,
        open_price=9.8,
        high_price=10.1,
        low_price=8.8,
        close_price=9.0,
    )
    process_trade_date(db_session, stop_day)
    materialize_sim_dashboard_snapshots(db_session, snapshot_date=stop_day, capital_tiers=["100k"])
    db_session.commit()

    by_report = client.get(f"/api/v1/sim/positions/by-report/{report.report_id}", headers=headers)
    assert by_report.status_code == 200
    items = by_report.json()["data"]["items"]
    assert len(items) == 1
    assert items[0]["status"] == "STOP_LOSS"
    assert items[0]["sim_close_date"] == stop_day

    snapshots = client.get(f"/api/v1/sim/account/snapshots?capital_tier=100k&date_from={stop_day}&date_to={stop_day}", headers=headers)
    assert snapshots.status_code == 200
    payload = snapshots.json()["data"]
    assert payload["total"] == 1
    snapshot = payload["items"][0]
    assert snapshot["snapshot_date"] == stop_day
    assert snapshot["capital_tier"] == "100k"
    assert snapshot["open_positions"] == 0
    assert snapshot["settled_trades"] == 1
    assert snapshot["total_asset"] < 100000


def test_fr08_account_snapshot_e2e_delisted(client, db_session, create_user):
    from sqlalchemy import text

    from app.services.runtime_materialization import materialize_sim_dashboard_snapshots
    from app.services.sim_positioning_ssot import process_trade_date

    signal_day = "2026-03-06"
    open_day = "2026-03-09"
    delisted_day = "2026-03-10"

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date=signal_day,
        confidence=0.91,
        signal_entry_price=10.0,
        target_price=11.0,
        stop_loss=9.0,
        trade_instructions={
            "10k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
            "100k": {"status": "EXECUTE", "position_ratio": 0.2, "skip_reason": None},
            "500k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
        },
    )
    headers = _pro_headers(client, create_user)

    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date=open_day,
        open_price=10.0,
        high_price=10.4,
        low_price=9.9,
        close_price=10.0,
    )
    process_trade_date(db_session, open_day)

    db_session.execute(text("UPDATE stock_master SET is_delisted = 1 WHERE stock_code = :stock_code"), {"stock_code": "600519.SH"})
    db_session.commit()

    process_trade_date(db_session, delisted_day)
    materialize_sim_dashboard_snapshots(db_session, snapshot_date=delisted_day, capital_tiers=["100k"])
    db_session.commit()

    by_report = client.get(f"/api/v1/sim/positions/by-report/{report.report_id}", headers=headers)
    assert by_report.status_code == 200
    items = by_report.json()["data"]["items"]
    assert len(items) == 1
    assert items[0]["status"] == "DELISTED_LIQUIDATED"
    assert items[0]["sim_close_date"] == delisted_day

    snapshots = client.get(f"/api/v1/sim/account/snapshots?capital_tier=100k&date_from={delisted_day}&date_to={delisted_day}", headers=headers)
    assert snapshots.status_code == 200
    payload = snapshots.json()["data"]
    assert payload["total"] == 1
    snapshot = payload["items"][0]
    assert snapshot["snapshot_date"] == delisted_day
    assert snapshot["open_positions"] == 0
    assert snapshot["settled_trades"] == 1
    assert snapshot["total_asset"] < 80000
    assert snapshot["total_asset"] > 0
