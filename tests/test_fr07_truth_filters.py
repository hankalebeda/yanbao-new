from __future__ import annotations

import time
from datetime import date

import pytest

pytestmark = pytest.mark.feature("FR-07")

from app.models import Base
from app.services.settlement_ssot import rebuild_fr07_snapshot
from tests.helpers_ssot import insert_kline, insert_report_bundle_ssot, insert_settlement_result, insert_stock_master


def _admin_headers(client, create_user) -> dict[str, str]:
    user_info = create_user(
        email="admin-fr07-filter@example.com",
        password="Password123",
        role="admin",
        tier="Enterprise",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": user_info["user"].email, "password": user_info["password"]},
    )
    assert login.status_code == 200
    return {"Authorization": f"Bearer {login.json()['data']['access_token']}"}


def _wait_for_task_terminal(db_session, task_id: str, *, timeout_seconds: float = 5.0) -> dict[str, object]:
    task_table = Base.metadata.tables["settlement_task"]
    deadline = time.monotonic() + timeout_seconds
    while True:
        db_session.expire_all()
        row = db_session.execute(
            task_table.select().where(task_table.c.task_id == task_id)
        ).mappings().first()
        if row and str(row.get("status") or "").upper() in {"COMPLETED", "FAILED"}:
            return dict(row)
        if time.monotonic() >= deadline:
            raise AssertionError(f"settlement task did not reach terminal state: {task_id}")
        time.sleep(0.05)


def _strategy_row(db_session, *, snapshot_date: str, window_days: int, strategy_type: str) -> dict[str, object]:
    table = Base.metadata.tables["strategy_metric_snapshot"]
    row = db_session.execute(
        table.select().where(
            table.c.snapshot_date == date.fromisoformat(snapshot_date),
            table.c.window_days == window_days,
            table.c.strategy_type == strategy_type,
        )
    ).mappings().one()
    return dict(row)


def _baseline_row(db_session, *, snapshot_date: str, window_days: int, baseline_type: str) -> dict[str, object]:
    table = Base.metadata.tables["baseline_metric_snapshot"]
    row = db_session.execute(
        table.select().where(
            table.c.snapshot_date == date.fromisoformat(snapshot_date),
            table.c.window_days == window_days,
            table.c.baseline_type == baseline_type,
        )
    ).mappings().one()
    return dict(row)


def test_fr07_only_quality_ok_reports_are_eligible_for_settlement(client, db_session, create_user):
    trade_date = "2026-03-10"
    signal_date = "2026-03-01"
    reports: dict[str, str] = {}

    for stock_code, quality_flag, published in (
        ("600519.SH", "ok", True),
        ("000001.SZ", "degraded", True),
        ("000002.SZ", "stale_ok", True),
        ("000003.SZ", "ok", False),
    ):
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_code)
        report = insert_report_bundle_ssot(
            db_session,
            stock_code=stock_code,
            stock_name=stock_code,
            trade_date=signal_date,
            strategy_type="A",
            quality_flag=quality_flag,
            published=published,
        )
        reports[stock_code] = report.report_id
        insert_kline(
            db_session,
            stock_code=stock_code,
            trade_date=trade_date,
            open_price=10.0,
            high_price=10.6,
            low_price=9.8,
            close_price=10.4,
        )

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": trade_date, "window_days": 7, "target_scope": "all", "force": True},
        headers=_admin_headers(client, create_user),
    )
    assert response.status_code == 202
    task = _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])
    assert task["status"] == "COMPLETED"

    result_table = Base.metadata.tables["settlement_result"]
    rows = db_session.execute(
        result_table.select().where(result_table.c.window_days == 7)
    ).mappings().all()
    assert [row["report_id"] for row in rows] == [reports["600519.SH"]]
    assert rows[0]["quality_flag"] == "ok"

    strategy_row = _strategy_row(
        db_session,
        snapshot_date=trade_date,
        window_days=7,
        strategy_type="A",
    )
    assert strategy_row["sample_size"] == 1
    assert float(strategy_row["coverage_pct"]) == 1.0


def test_fr07_signal_validity_warning_uses_new_baseline_truth(db_session, monkeypatch):
    trade_date = "2026-03-10"
    window_days = 7
    signal_date = "2026-03-01"
    returns = [-0.01] * 35

    for index, net_return_pct in enumerate(returns):
        stock_code = f"601{index:03d}.SH"
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_code)
        report = insert_report_bundle_ssot(
            db_session,
            stock_code=stock_code,
            stock_name=stock_code,
            trade_date=signal_date,
            strategy_type="A",
            quality_flag="ok",
            published=True,
        )
        insert_settlement_result(
            db_session,
            report_id=report.report_id,
            stock_code=stock_code,
            signal_date=signal_date,
            window_days=window_days,
            strategy_type="A",
            exit_trade_date=trade_date,
            net_return_pct=net_return_pct,
        )

    market_candidates = [
        {
            "template_index": index,
            "stock_code": f"300{index:03d}.SZ",
            "signal_date": date.fromisoformat(signal_date),
            "exit_trade_date": date.fromisoformat(trade_date),
            "net_return_pct": 0.01,
        }
        for index in range(len(returns))
    ]

    monkeypatch.setattr(
        "app.services.settlement_ssot.load_random_baseline_market_returns",
        lambda *args, **kwargs: list(market_candidates),
    )

    summary = rebuild_fr07_snapshot(
        db_session,
        trade_day=date.fromisoformat(trade_date),
        window_days=window_days,
        purge_invalid=True,
    )
    assert summary["signal_validity_warning"] is True

    strategy_row = _strategy_row(
        db_session,
        snapshot_date=trade_date,
        window_days=window_days,
        strategy_type="A",
    )
    baseline_row = _baseline_row(
        db_session,
        snapshot_date=trade_date,
        window_days=window_days,
        baseline_type="baseline_random",
    )

    assert strategy_row["signal_validity_warning"] is True
    assert float(strategy_row["cumulative_return_pct"]) < float(baseline_row["cumulative_return_pct"])
