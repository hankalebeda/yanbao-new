from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

from sqlalchemy import text

from app.models import Base
from app.services.dashboard_query import get_dashboard_stats_payload_ssot
from app.services.runtime_truth_guard import (
    normalize_snapshot_truth,
    report_truth_gate,
    soft_delete_stray_unpublished_reports,
    truth_counters,
)
from app.services.ssot_read_model import (
    _build_dashboard_strategy_metrics_from_snapshot_rows,
    get_report_view_payload_ssot,
    get_sim_dashboard_payload_ssot,
)
from tests.helpers_ssot import (
    insert_baseline_metric_snapshot,
    insert_kline,
    insert_market_state_cache,
    insert_open_position,
    insert_pool_snapshot,
    insert_report_bundle_ssot,
    insert_sim_dashboard_snapshot,
    insert_sim_equity_curve_point,
    insert_stock_master,
    insert_strategy_metric_snapshot,
)


def _auth_headers(client, create_user, *, email: str, role: str = "admin", tier: str = "Enterprise") -> dict[str, str]:
    user_info = create_user(email=email, password="Password123", role=role, tier=tier, email_verified=True)
    login = client.post("/auth/login", json={"email": user_info["user"].email, "password": user_info["password"]})
    assert login.status_code == 200
    access_token = login.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {access_token}"}


def test_canonical_report_fail_closes_when_recent_failure_lookup_raises(client, db_session, monkeypatch):
    import app.main as app_main

    insert_stock_master(db_session, stock_code="000625.SZ", stock_name="TEST")
    monkeypatch.setattr(
        app_main,
        "recent_report_failure",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    response = client.get("/report/000625.SZ")

    assert response.status_code == 404
    assert "服务器错误" not in response.text


def test_runtime_truth_guard_soft_deletes_weekend_unpublished_reports(db_session):
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        published=True,
    )
    stray = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-07",
        published=False,
        ensure_pool_snapshot=False,
    )
    db_session.execute(
        text("DELETE FROM market_state_cache WHERE trade_date = '2026-03-07'")
    )
    db_session.flush()

    counters_before = truth_counters(db_session)
    deleted_count = soft_delete_stray_unpublished_reports(db_session, runtime_trade_date="2026-03-06")
    db_session.commit()
    counters_after = truth_counters(db_session)
    row = db_session.execute(
        text("SELECT is_deleted FROM report WHERE report_id = :report_id"),
        {"report_id": stray.report_id},
    ).first()

    assert counters_before["weekend_unpublished_report_count"] == 1
    assert deleted_count == 1
    assert row[0] == 1
    assert counters_after["weekend_unpublished_report_count"] == 0


def test_dashboard_stats_keeps_computing_when_snapshot_rows_exist_but_facts_not_closed(db_session):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date="2026-03-06", stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        recommendation="BUY",
        strategy_type="B",
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="B",
        window_days=60,
        data_status="READY",
        sample_size=29,
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="C",
        window_days=60,
        data_status="READY",
        sample_size=29,
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        baseline_type="baseline_random",
        window_days=60,
        sample_size=29,
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        baseline_type="baseline_ma_cross",
        window_days=60,
        sample_size=29,
    )

    payload = get_dashboard_stats_payload_ssot(db_session, window_days=60)
    assert payload["data_status"] == "COMPUTING"
    assert payload["status_reason"] == "stats_not_ready"
    gate = report_truth_gate(db_session, trade_date="2026-03-06")
    assert gate["passed"] is True


def test_dashboard_strategy_alpha_filters_absurd_snapshot_values():
    payload = _build_dashboard_strategy_metrics_from_snapshot_rows(
        [
            {
                "strategy_type": "B",
                "sample_size": 42,
                "coverage_pct": 0.8,
                "win_rate": 0.62,
                "profit_loss_ratio": 1.9,
                "alpha_annual": 9.023116782925264e28,
                "max_drawdown_pct": -0.08,
                "cumulative_return_pct": 0.21,
                "signal_validity_warning": False,
                "display_hint": None,
            }
        ]
    )

    assert payload["B"]["alpha_annual"] is None
    assert payload["B"]["sample_size"] == 42


def test_normalize_snapshot_truth_degrades_absurd_source_alpha(db_session):
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="B",
        window_days=30,
        data_status="READY",
        sample_size=42,
        coverage_pct=0.8,
        alpha_annual=35.22,
        display_hint=None,
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        baseline_type="baseline_random",
        window_days=30,
        sample_size=42,
        alpha_annual=35.22,
    )

    updates = normalize_snapshot_truth(db_session)
    db_session.commit()

    strategy_row = db_session.execute(
        text(
            """
            SELECT data_status, alpha_annual, display_hint
            FROM strategy_metric_snapshot
            WHERE snapshot_date = '2026-03-06'
              AND strategy_type = 'B'
              AND window_days = 30
            """
        )
    ).mappings().one()
    baseline_row = db_session.execute(
        text(
            """
            SELECT alpha_annual
            FROM baseline_metric_snapshot
            WHERE snapshot_date = '2026-03-06'
              AND baseline_type = 'baseline_random'
              AND window_days = 30
            """
        )
    ).mappings().one()

    assert updates["strategy_metric_snapshot"] == 1
    assert updates["baseline_metric_snapshot"] == 1
    assert strategy_row["data_status"] == "DEGRADED"
    assert strategy_row["alpha_annual"] is None
    assert strategy_row["display_hint"] == "abnormal_alpha_annual"
    assert baseline_row["alpha_annual"] is None


def test_report_view_market_snapshot_prefers_consistent_front_adjusted_rows(db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        signal_entry_price=120.0,
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-05",
        open_price=118.0,
        high_price=119.0,
        low_price=117.0,
        close_price=118.0,
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-06",
        open_price=119.0,
        high_price=121.0,
        low_price=118.0,
        close_price=120.0,
    )

    kline_table = Base.metadata.tables["kline_daily"]
    created_at = datetime.now(timezone.utc)
    for trade_date_text, close_price in (("2026-03-06", 1480.0), ("2026-03-05", 1460.0)):
        trade_day = date.fromisoformat(trade_date_text)
        db_session.execute(
            kline_table.insert().values(
                kline_id=str(uuid4()),
                stock_code="600519.SH",
                trade_date=trade_day,
                open=close_price,
                high=close_price,
                low=close_price,
                close=close_price,
                volume=100000.0,
                amount=close_price * 100000.0,
                adjust_type="back_adjusted",
                atr_pct=0.03,
                turnover_rate=0.02,
                ma5=close_price,
                ma10=close_price,
                ma20=close_price,
                ma60=close_price,
                volatility_20d=0.02,
                hs300_return_20d=0.01,
                is_suspended=False,
                source_batch_id=str(uuid4()),
                created_at=created_at,
            )
        )
    db_session.commit()

    payload = get_report_view_payload_ssot(db_session, report.report_id)

    assert payload is not None
    snapshot = payload["market_snapshot"]
    assert snapshot["last_price"] == 120.0
    assert snapshot["prev_close"] == 118.0
    assert snapshot["pct_change"] == 1.69


def test_sim_dashboard_downgrades_ready_snapshot_when_sample_and_baseline_are_missing(db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        recommendation="BUY",
        strategy_type="B",
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        capital_tier="100k",
        signal_date="2026-03-06",
        entry_date="2026-03-06",
        actual_entry_price=100.0,
        signal_entry_price=100.0,
        position_ratio=0.2,
        shares=100,
    )
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-06", equity=100500.0)
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date="2026-03-06",
        data_status="READY",
        sample_size=0,
        display_hint="baseline_pending",
    )

    normalize_snapshot_truth(db_session)
    db_session.commit()
    payload = get_sim_dashboard_payload_ssot(db_session, capital_tier="100k")

    assert payload["data_status"] == "DEGRADED"
    assert payload["status_reason"] in {"sim_sample_lt_30", "sim_baseline_pending", "sim_snapshot_lagging"}
    assert payload["display_hint"] in {"sample_lt_30", "baseline_pending"}


def test_admin_system_status_exposes_truth_gaps(client, db_session, create_user):
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        published=True,
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-07",
        published=False,
        ensure_pool_snapshot=False,
    )
    db_session.execute(
        text("DELETE FROM market_state_cache WHERE trade_date = '2026-03-07'")
    )
    db_session.flush()
    headers = _auth_headers(client, create_user, email="truth-gap-admin@example.com")

    response = client.get("/api/v1/admin/system-status", headers=headers)

    assert response.status_code == 200
    truth = response.json()["data"]["truth_gaps"]
    payment = response.json()["data"]["payment_capability"]
    membership_truth = response.json()["data"]["membership_truth"]
    assert truth["runtime_trade_date"] == "2026-03-06"
    assert truth["weekend_unpublished_report_count"] == 1
    assert "hotspot_truth_mismatch" in truth
    assert "published_outside_core_count" in truth
    assert "billing_order_count" in truth
    assert "report_feedback_count" in truth
    assert "provider_status" in payment
    assert "browser_checkout_ready" in payment
    assert membership_truth["paid_tier_null_expiry"]["count"] >= 0


def test_report_truth_gate_rejects_published_reports_outside_core_pool(db_session):
    insert_pool_snapshot(db_session, trade_date="2026-03-06", stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-06",
        published=True,
        ensure_pool_snapshot=False,
    )

    gate = report_truth_gate(db_session, trade_date="2026-03-06")

    assert gate["passed"] is False
    assert gate["published_outside_core_count"] == 1
    assert "published_outside_core_pool" in gate["reasons"]


def test_report_truth_gate_requires_real_hotspot_rows_when_hotspot_usage_is_ok(db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        published=True,
    )
    market_state_row = db_session.execute(
        text(
            """
            SELECT hotspot_batch_id
            FROM market_state_cache
            WHERE trade_date = :trade_date
            """
        ),
        {"trade_date": "2026-03-06"},
    ).mappings().first()
    assert market_state_row is not None
    hotspot_batch_id = str(market_state_row["hotspot_batch_id"])
    now = datetime(2026, 3, 6, 15, 0, tzinfo=timezone.utc)
    usage_id = str(uuid4())

    db_session.execute(
        Base.metadata.tables["report_data_usage"].insert().values(
            usage_id=usage_id,
            trade_date=datetime(2026, 3, 6, tzinfo=timezone.utc).date(),
            stock_code="600519.SH",
            dataset_name="hotspot_top50",
            source_name="eastmoney",
            batch_id=hotspot_batch_id,
            fetch_time=now,
            status="ok",
            status_reason=None,
            created_at=now,
        )
    )
    db_session.execute(
        Base.metadata.tables["report_data_usage_link"].insert().values(
            report_data_usage_link_id=str(uuid4()),
            report_id=report.report_id,
            usage_id=usage_id,
            created_at=now,
        )
    )
    db_session.commit()

    gate = report_truth_gate(db_session, trade_date="2026-03-06")

    assert gate["passed"] is False
    assert gate["hotspot_usage_ok_count"] == 1
    assert gate["hotspot_item_count"] == 0
    assert gate["hotspot_link_count"] == 0
    assert gate["hotspot_truth_mismatch"] is True
    assert "hotspot_truth_mismatch" in gate["reasons"]

    hotspot_item_id = str(uuid4())
    db_session.execute(
        Base.metadata.tables["market_hotspot_item"].insert().values(
            hotspot_item_id=hotspot_item_id,
            batch_id=hotspot_batch_id,
            source_name="weibo",
            merged_rank=1,
            source_rank=1,
            topic_title="茅台热搜",
            source_url="https://example.com/hotspot/moutai",
            fetch_time=now,
            quality_flag="ok",
            created_at=now,
        )
    )
    db_session.execute(
        Base.metadata.tables["market_hotspot_item_stock_link"].insert().values(
            hotspot_item_stock_link_id=str(uuid4()),
            hotspot_item_id=hotspot_item_id,
            stock_code="600519.SH",
            relation_role="primary",
            match_confidence=0.99,
            created_at=now,
        )
    )
    db_session.commit()

    gate_after_hotspot_insert = report_truth_gate(db_session, trade_date="2026-03-06")

    assert gate_after_hotspot_insert["passed"] is True
    assert gate_after_hotspot_insert["hotspot_item_count"] == 1
    assert gate_after_hotspot_insert["hotspot_link_count"] == 1
    assert gate_after_hotspot_insert["hotspot_truth_mismatch"] is False
