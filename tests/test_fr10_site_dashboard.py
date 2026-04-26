from __future__ import annotations

from datetime import date, timedelta
from uuid import uuid4

import pytest

from app.models import Base
from tests.helpers_ssot import (
    insert_baseline_metric_snapshot,
    insert_baseline_equity_curve_point,
    insert_kline,
    insert_market_state_cache,
    insert_open_position,
    insert_pool_snapshot,
    insert_report_bundle_ssot,
    insert_settlement_result,
    insert_sim_dashboard_snapshot,
    insert_sim_equity_curve_point,
    insert_stock_master,
    insert_strategy_metric_snapshot,
    utc_now,
)

pytestmark = [
    pytest.mark.feature("FR10-HOME-01"),
    pytest.mark.feature("FR10-DETAIL-01"),
    pytest.mark.feature("FR10-BOARD-01"),
    pytest.mark.feature("FR10-BOARD-02"),
]

import app.services.stock_pool as _stock_pool_mod


@pytest.fixture(autouse=True)
def _lower_min_core_rows(monkeypatch):
    monkeypatch.setattr(_stock_pool_mod, "_MIN_CORE_ROWS_VALID", 1)


_DASHBOARD_WINDOWS = (1, 7, 14, 30, 60)
_DASHBOARD_STRATEGIES = ("A", "B", "C")


def _login_headers(client, create_user, *, email: str, tier: str, role: str = "user") -> dict[str, str]:
    user_info = create_user(
        email=email,
        password="Password123",
        tier=tier,
        role=role,
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": user_info["user"].email, "password": user_info["password"]},
    )
    assert login.status_code == 200
    return {"Authorization": f"Bearer {login.json()['data']['access_token']}"}


def _dashboard_window_bounds(anchor_trade_date: str, window_days: int) -> tuple[str, str]:
    window_end = date.fromisoformat(anchor_trade_date)
    window_start = window_end - timedelta(days=window_days - 1)
    return window_start.isoformat(), window_end.isoformat()


def _seed_complete_dashboard_snapshot_batch(
    db_session,
    *,
    snapshot_date: str,
    report_specs: list[dict[str, str]],
) -> None:
    report_specs_by_date: dict[str, list[dict[str, str]]] = {}
    for spec in report_specs:
        report_specs_by_date.setdefault(spec["trade_date"], []).append(spec)
    for trade_date_value, specs_for_day in report_specs_by_date.items():
        insert_pool_snapshot(
            db_session,
            trade_date=trade_date_value,
            stock_codes=[spec["stock_code"] for spec in specs_for_day],
        )

    report_rows = []
    for spec in report_specs:
        report = insert_report_bundle_ssot(
            db_session,
            stock_code=spec["stock_code"],
            stock_name=spec["stock_name"],
            trade_date=spec["trade_date"],
            strategy_type=spec.get("strategy_type", "B"),
            recommendation=spec.get("recommendation", "BUY"),
        )
        report_rows.append((report, spec))

    for report, spec in report_rows:
        signal_date = str(report.trade_date)
        for window_days in _DASHBOARD_WINDOWS:
            window_start, window_end = _dashboard_window_bounds(snapshot_date, window_days)
            if not (window_start <= signal_date <= window_end):
                continue
            insert_settlement_result(
                db_session,
                report_id=report.report_id,
                stock_code=report.stock_code,
                signal_date=signal_date,
                window_days=window_days,
                strategy_type=spec.get("strategy_type", "B"),
                net_return_pct=0.03,
                exit_trade_date=snapshot_date,
            )

    for window_days in _DASHBOARD_WINDOWS:
        window_start, window_end = _dashboard_window_bounds(snapshot_date, window_days)
        sample_counts = {key: 0 for key in _DASHBOARD_STRATEGIES}
        buy_counts = {key: 0 for key in _DASHBOARD_STRATEGIES}
        for report, spec in report_rows:
            signal_date = str(report.trade_date)
            if not (window_start <= signal_date <= window_end):
                continue
            strategy_type = spec.get("strategy_type", "B")
            sample_counts[strategy_type] += 1
            if spec.get("recommendation", "BUY") == "BUY":
                buy_counts[strategy_type] += 1

        total_sample = sum(sample_counts.values())
        strategy_hint = "sample_lt_30" if 0 < total_sample < 30 else None
        baseline_hint = "sample_lt_30" if 0 < total_sample < 30 else None
        for strategy_type in _DASHBOARD_STRATEGIES:
            sample_size = sample_counts[strategy_type]
            coverage_pct = (
                round(sample_size / buy_counts[strategy_type], 6)
                if buy_counts[strategy_type] > 0
                else 0.0
            )
            insert_strategy_metric_snapshot(
                db_session,
                snapshot_date=snapshot_date,
                strategy_type=strategy_type,
                window_days=window_days,
                sample_size=sample_size,
                coverage_pct=coverage_pct,
                win_rate=None if sample_size < 30 else 0.6,
                profit_loss_ratio=None if sample_size < 30 else 1.8,
                alpha_annual=None if sample_size < 30 else 0.12,
                max_drawdown_pct=None if sample_size < 30 else -0.08,
                cumulative_return_pct=None if sample_size < 30 else round(sample_size * 0.02, 6),
                signal_validity_warning=False,
                display_hint=strategy_hint if sample_size > 0 else None,
            )
        for baseline_type, cumulative_return_pct in (
            ("baseline_random", 0.02),
            ("baseline_ma_cross", 0.01),
        ):
            insert_baseline_metric_snapshot(
                db_session,
                snapshot_date=snapshot_date,
                baseline_type=baseline_type,
                window_days=window_days,
                sample_size=total_sample,
                win_rate=None if total_sample < 30 else 0.55,
                profit_loss_ratio=None if total_sample < 30 else 1.5,
                alpha_annual=None if total_sample < 30 else 0.08,
                max_drawdown_pct=None if total_sample < 30 else -0.09,
                cumulative_return_pct=None if total_sample < 30 else cumulative_return_pct,
                display_hint=baseline_hint if total_sample > 0 else None,
            )


def test_fr10_home_api_fields(client, db_session):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    insert_pool_snapshot(db_session, trade_date="2026-03-06", stock_codes=["600519.SH", "000001.SZ"])
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        confidence=0.86,
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-06",
        confidence=0.73,
    )

    response = client.get("/api/v1/home", headers={"X-Request-ID": "req-fr10-home"})

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-fr10-home"
    assert body["data"]["market_state"] == "BULL"
    assert body["data"]["pool_size"] == 2
    assert body["data"]["data_status"] == "COMPUTING"
    assert body["data"]["status_reason"] == "stats_not_ready"
    assert body["data"]["display_reason"] == body["data"]["public_performance"]["display_hint"]
    assert len(body["data"]["latest_reports"]) == 2
    assert {item["stock_code"] for item in body["data"]["latest_reports"]} == {"600519.SH", "000001.SZ"}
    assert len(body["data"]["hot_stocks"]) == 2
    assert {item["stock_code"] for item in body["data"]["hot_stocks"]} == {"600519.SH", "000001.SZ"}
    assert body["data"]["today_report_count"] == 2


def test_fr10_home_degrades_when_reports_are_newer_than_effective_pool(client, db_session):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    insert_pool_snapshot(db_session, trade_date="2026-03-09", stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date="2026-03-17", market_state="BEAR")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-09",
        confidence=0.81,
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-17",
        open_price=1800.0,
        high_price=1815.0,
        low_price=1795.0,
        close_price=1810.0,
    )
    insert_kline(
        db_session,
        stock_code="000001.SZ",
        trade_date="2026-03-17",
        open_price=10.0,
        high_price=10.5,
        low_price=9.8,
        close_price=10.2,
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-17",
        confidence=0.88,
        ensure_pool_snapshot=False,
    )

    response = client.get("/api/v1/home")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["data_status"] == "DEGRADED"
    assert data["status_reason"] == "home_source_inconsistent"
    assert data["pool_size"] == 1
    assert data["today_report_count"] == 1
    assert len(data["latest_reports"]) == 1
    assert {item["trade_date"] for item in data["latest_reports"]} == {"2026-03-09"}


def test_fr10_home_ignores_partial_fallback_day_when_resolving_public_anchor(client, db_session, monkeypatch):
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-17")

    for stock_code, stock_name in (
        ("600519.SH", "MOUTAI"),
        ("000001.SZ", "PINGAN"),
        ("300750.SZ", "CATL"),
        ("601318.SH", "PINGAN INS"),
    ):
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_name)

    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-09",
        stock_codes=["600519.SH", "000001.SZ"],
        pool_version=1,
    )
    insert_market_state_cache(db_session, trade_date="2026-03-17", market_state="NEUTRAL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-09",
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-09",
    )

    now = utc_now()
    fallback_task_id = str(uuid4())
    db_session.execute(
        Base.metadata.tables["stock_pool_refresh_task"].insert().values(
            task_id=fallback_task_id,
            trade_date=date(2026, 3, 17),
            status="FALLBACK",
            pool_version=2,
            fallback_from=date(2026, 3, 9),
            filter_params_json={"target_pool_size": 3},
            core_pool_size=3,
            standby_pool_size=1,
            evicted_stocks_json=[],
            status_reason="KLINE_COVERAGE_INSUFFICIENT",
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    for rank_no, stock_code in enumerate(("600519.SH", "000001.SZ", "300750.SZ"), start=1):
        db_session.execute(
            Base.metadata.tables["stock_pool_snapshot"].insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=fallback_task_id,
                trade_date=date(2026, 3, 17),
                pool_version=2,
                stock_code=stock_code,
                pool_role="core",
                rank_no=rank_no,
                score=100 - rank_no,
                is_suspended=False,
                created_at=now,
            )
        )
    db_session.execute(
        Base.metadata.tables["stock_pool_snapshot"].insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=fallback_task_id,
            trade_date=date(2026, 3, 17),
            pool_version=2,
            stock_code="601318.SH",
            pool_role="standby",
            rank_no=1,
            score=90,
            is_suspended=False,
            created_at=now,
        )
    )
    db_session.commit()

    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-17",
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-17",
    )

    response = client.get("/api/v1/home")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["trade_date"] == "2026-03-09"
    assert data["data_status"] == "DEGRADED"
    assert data["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert data["pool_size"] == 2
    assert data["today_report_count"] == 2
    assert {item["trade_date"] for item in data["latest_reports"]} == {"2026-03-09"}


def test_fr10_home_cache_invalidates_when_market_state_changes(client, db_session):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date="2026-03-06", stock_codes=["600519.SH"])
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        confidence=0.86,
    )
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")

    first = client.get("/api/v1/home")
    assert first.status_code == 200
    assert first.json()["data"]["market_state"] == "BULL"

    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BEAR")

    second = client.get("/api/v1/home")
    assert second.status_code == 200
    assert second.json()["data"]["market_state"] == "BEAR"


def test_fr10_home_cache_invalidates_when_hotspot_changes(client, db_session):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date="2026-03-06", stock_codes=["600519.SH"])
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
    )
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")

    first = client.get("/api/v1/home")
    assert first.status_code == 200
    first_hot = first.json()["data"]["hot_stocks"]
    assert len(first_hot) == 1
    assert first_hot[0]["stock_code"] == "600519.SH"
    assert first_hot[0]["topic_title"] is None

    market_state_row = db_session.execute(
        Base.metadata.tables["market_state_cache"].select()
        .where(Base.metadata.tables["market_state_cache"].c.trade_date == date(2026, 3, 6))
    ).mappings().first()
    assert market_state_row is not None
    hotspot_batch_id = str(market_state_row["hotspot_batch_id"])
    now = utc_now()
    hotspot_item_id = str(uuid4())
    db_session.execute(
        Base.metadata.tables["market_hotspot_item"].insert().values(
            hotspot_item_id=hotspot_item_id,
            batch_id=hotspot_batch_id,
            source_name="weibo",
            merged_rank=1,
            source_rank=1,
            topic_title="MOUTAI 热门",
            news_event_type="hotspot",
            hotspot_tags_json=["hotspot"],
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

    second = client.get("/api/v1/home")
    assert second.status_code == 200
    second_hot = second.json()["data"]["hot_stocks"]
    assert len(second_hot) == 1
    assert second_hot[0]["stock_code"] == "600519.SH"
    assert second_hot[0]["topic_title"] == "MOUTAI 热门"
    assert second_hot[0]["heat_score"] == 99


def test_fr10_home_cache_invalidates_when_public_pool_members_change_same_trade_date(client, db_session):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-06",
        stock_codes=["600519.SH"],
        pool_version=1,
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
    )
    insert_kline(db_session, stock_code="000001.SZ", trade_date="2026-03-06",
                 open_price=10.0, high_price=10.5, low_price=9.8, close_price=10.2)
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")

    first = client.get("/api/v1/home")
    assert first.status_code == 200
    assert [item["stock_code"] for item in first.json()["data"]["hot_stocks"]] == ["600519.SH"]

    refresh_task_table = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]
    refresh_task_id = db_session.execute(
        refresh_task_table.select().where(
            refresh_task_table.c.trade_date == date.fromisoformat("2026-03-06")
        )
    ).mappings().one()["task_id"]
    now = utc_now()
    db_session.execute(
        refresh_task_table.update().where(
            refresh_task_table.c.task_id == refresh_task_id
        ).values(
            pool_version=2,
            filter_params_json={"target_pool_size": 1},
            core_pool_size=1,
            standby_pool_size=1,
            updated_at=now,
            finished_at=now,
        )
    )
    db_session.execute(
        snapshot_table.delete().where(
            snapshot_table.c.refresh_task_id == refresh_task_id
        )
    )
    db_session.execute(
        snapshot_table.insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=refresh_task_id,
            trade_date=date.fromisoformat("2026-03-06"),
            pool_version=2,
            stock_code="000001.SZ",
            pool_role="core",
            rank_no=1,
            score=99,
            is_suspended=False,
            created_at=now,
        )
    )
    db_session.execute(
        snapshot_table.insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=refresh_task_id,
            trade_date=date.fromisoformat("2026-03-06"),
            pool_version=2,
            stock_code="990001.SH",
            pool_role="standby",
            rank_no=1,
            score=50,
            is_suspended=False,
            created_at=now,
        )
    )
    db_session.commit()
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-06",
    )

    second = client.get("/api/v1/home")
    assert second.status_code == 200
    assert [item["stock_code"] for item in second.json()["data"]["hot_stocks"]] == ["000001.SZ"]


def test_fr10_home_ignores_future_market_state_rows_for_payload_and_cache(client, db_session, monkeypatch):
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-20")
    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-20")

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date="2026-03-20", stock_codes=["600519.SH"])
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-20",
    )
    insert_market_state_cache(db_session, trade_date="2026-03-20", market_state="NEUTRAL")

    first = client.get("/api/v1/home")
    assert first.status_code == 200
    first_data = first.json()["data"]
    assert first_data["trade_date"] == "2026-03-20"
    assert first_data["market_state"] == "NEUTRAL"

    insert_market_state_cache(db_session, trade_date="2026-04-01", market_state="BEAR")

    second = client.get("/api/v1/home")
    assert second.status_code == 200
    second_data = second.json()["data"]
    assert second_data["trade_date"] == "2026-03-20"
    assert second_data["market_state"] == "NEUTRAL"


def test_fr10_home_cache_invalidates_when_public_performance_snapshot_changes(client, db_session):
    insert_market_state_cache(db_session, trade_date="2026-03-20", market_state="NEUTRAL")
    report_specs = []
    for idx in range(36):
        report_specs.append(
            {
                "stock_code": f"{600300 + idx:06d}.SH",
                "stock_name": f"SNAP{idx:03d}",
                "trade_date": "2026-03-20" if idx < 12 else "2026-03-19" if idx < 24 else "2026-03-18",
                "strategy_type": "B",
                "recommendation": "BUY",
            }
        )
    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-20",
        report_specs=report_specs,
    )

    first = client.get("/api/v1/home")
    assert first.status_code == 200
    first_public = first.json()["data"]["public_performance"]
    assert first_public["overall_win_rate"] == 0.6
    assert first_public["overall_profit_loss_ratio"] == 1.8

    later = utc_now()
    strategy_snapshot = Base.metadata.tables["strategy_metric_snapshot"]
    baseline_snapshot = Base.metadata.tables["baseline_metric_snapshot"]
    db_session.execute(
        strategy_snapshot.update().where(
            strategy_snapshot.c.snapshot_date == date.fromisoformat("2026-03-20"),
            strategy_snapshot.c.window_days == 30,
            strategy_snapshot.c.strategy_type == "B",
        ).values(
            win_rate=0.72,
            profit_loss_ratio=2.4,
            alpha_annual=0.18,
            cumulative_return_pct=0.42,
            created_at=later,
        )
    )
    db_session.execute(
        baseline_snapshot.update().where(
            baseline_snapshot.c.snapshot_date == date.fromisoformat("2026-03-20"),
            baseline_snapshot.c.window_days == 30,
            baseline_snapshot.c.baseline_type == "baseline_random",
        ).values(
            win_rate=0.5,
            profit_loss_ratio=1.2,
            alpha_annual=0.05,
            cumulative_return_pct=0.1,
            created_at=later,
        )
    )
    db_session.commit()

    second = client.get("/api/v1/home")
    dashboard = client.get("/api/v1/dashboard/stats?window_days=30")
    platform = client.get("/api/v1/platform/summary")

    assert second.status_code == 200
    assert dashboard.status_code == 200
    assert platform.status_code == 200

    second_public = second.json()["data"]["public_performance"]
    dashboard_data = dashboard.json()["data"]
    platform_data = platform.json()["data"]

    assert second_public["overall_win_rate"] == 0.72
    assert second_public["overall_profit_loss_ratio"] == 2.4
    assert second_public["overall_win_rate"] == dashboard_data["overall_win_rate"]
    assert second_public["overall_profit_loss_ratio"] == dashboard_data["overall_profit_loss_ratio"]
    assert second_public["alpha_vs_baseline"] == platform_data["alpha"]
    assert second_public["runtime_trade_date"] == dashboard_data["runtime_trade_date"]


def test_fr10_home_cache_invalidates_when_kline_coverage_changes(client, db_session, monkeypatch):
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar
    from tests.helpers_ssot import insert_kline

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-17")
    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-17")
    monkeypatch.setattr(read_model, "trade_days_in_range", lambda start, end: ["2026-03-09"])

    for stock_code, stock_name in (
        ("600519.SH", "MOUTAI"),
        ("000001.SZ", "PINGAN"),
        ("300750.SZ", "CATL"),
        ("601318.SH", "PINGAN INS"),
    ):
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_name)

    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-09",
        stock_codes=["600519.SH", "000001.SZ"],
        pool_version=1,
    )
    insert_market_state_cache(db_session, trade_date="2026-03-17", market_state="NEUTRAL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-09",
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-09",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-17",
        open_price=10.0,
        high_price=10.5,
        low_price=9.9,
        close_price=10.2,
    )

    now = utc_now()
    fallback_task_id = str(uuid4())
    db_session.execute(
        Base.metadata.tables["stock_pool_refresh_task"].insert().values(
            task_id=fallback_task_id,
            trade_date=date(2026, 3, 17),
            status="FALLBACK",
            pool_version=2,
            fallback_from=date(2026, 3, 9),
            filter_params_json={"target_pool_size": 3},
            core_pool_size=3,
            standby_pool_size=1,
            evicted_stocks_json=[],
            status_reason="KLINE_COVERAGE_INSUFFICIENT",
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    for rank_no, stock_code in enumerate(("600519.SH", "000001.SZ", "300750.SZ"), start=1):
        db_session.execute(
            Base.metadata.tables["stock_pool_snapshot"].insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=fallback_task_id,
                trade_date=date(2026, 3, 17),
                pool_version=2,
                stock_code=stock_code,
                pool_role="core",
                rank_no=rank_no,
                score=100 - rank_no,
                is_suspended=False,
                created_at=now,
            )
        )
    db_session.execute(
        Base.metadata.tables["stock_pool_snapshot"].insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=fallback_task_id,
            trade_date=date(2026, 3, 17),
            pool_version=2,
            stock_code="601318.SH",
            pool_role="standby",
            rank_no=1,
            score=90,
            is_suspended=False,
            created_at=now,
        )
    )
    db_session.commit()

    first = client.get("/api/v1/home")
    assert first.status_code == 200
    first_reason = first.json()["data"]["display_reason"]
    assert "1/4" in first_reason

    insert_kline(
        db_session,
        stock_code="000001.SZ",
        trade_date="2026-03-17",
        open_price=10.0,
        high_price=10.5,
        low_price=9.9,
        close_price=10.2,
    )
    insert_kline(
        db_session,
        stock_code="300750.SZ",
        trade_date="2026-03-17",
        open_price=10.0,
        high_price=10.5,
        low_price=9.9,
        close_price=10.2,
    )
    db_session.commit()

    second = client.get("/api/v1/home")
    assert second.status_code == 200
    second_reason = second.json()["data"]["display_reason"]
    assert "3/4" in second_reason
    assert second_reason != first_reason


def test_fr10_home_cache_ignores_future_pool_and_snapshot_rows(client, db_session, monkeypatch):
    import app.api.routes_business as routes_business
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-20")
    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-20")

    insert_market_state_cache(db_session, trade_date="2026-03-20", market_state="NEUTRAL")
    report_specs = []
    for idx in range(36):
        report_specs.append(
            {
                "stock_code": f"{600400 + idx:06d}.SH",
                "stock_name": f"FUT{idx:03d}",
                "trade_date": "2026-03-20" if idx < 12 else "2026-03-19" if idx < 24 else "2026-03-18",
                "strategy_type": "B",
                "recommendation": "BUY",
            }
        )
    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-20",
        report_specs=report_specs,
    )

    first = client.get("/api/v1/home")
    assert first.status_code == 200

    now = utc_now()
    future_task_id = str(uuid4())
    db_session.execute(
        Base.metadata.tables["stock_pool_refresh_task"].insert().values(
            task_id=future_task_id,
            trade_date=date(2026, 4, 1),
            status="FALLBACK",
            pool_version=9,
            fallback_from=date(2026, 3, 20),
            filter_params_json={"target_pool_size": 3},
            core_pool_size=3,
            standby_pool_size=1,
            evicted_stocks_json=[],
            status_reason="KLINE_COVERAGE_INSUFFICIENT",
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    for rank_no, stock_code in enumerate(("600400.SH", "600401.SH", "600402.SH"), start=1):
        db_session.execute(
            Base.metadata.tables["stock_pool_snapshot"].insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=future_task_id,
                trade_date=date(2026, 4, 1),
                pool_version=9,
                stock_code=stock_code,
                pool_role="core",
                rank_no=rank_no,
                score=100 - rank_no,
                is_suspended=False,
                created_at=now,
            )
        )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-04-01",
        strategy_type="A",
        window_days=30,
        win_rate=0.99,
        profit_loss_ratio=9.9,
        cumulative_return_pct=0.99,
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-04-01",
        strategy_type="B",
        window_days=30,
        win_rate=0.99,
        profit_loss_ratio=9.9,
        cumulative_return_pct=0.99,
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-04-01",
        strategy_type="C",
        window_days=30,
        win_rate=0.99,
        profit_loss_ratio=9.9,
        cumulative_return_pct=0.99,
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-04-01",
        baseline_type="baseline_random",
        window_days=30,
        win_rate=0.11,
        profit_loss_ratio=0.8,
        cumulative_return_pct=0.01,
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-04-01",
        baseline_type="baseline_ma_cross",
        window_days=30,
        win_rate=0.12,
        profit_loss_ratio=0.9,
        cumulative_return_pct=0.02,
    )

    def fail_if_recomputed(*args, **kwargs):
        raise AssertionError("home_cache_should_ignore_future_rows")

    monkeypatch.setattr(routes_business, "get_home_payload_ssot", fail_if_recomputed)

    second = client.get("/api/v1/home")
    assert second.status_code == 200
    assert second.json()["data"] == first.json()["data"]


def test_fr10_public_status_stays_on_stable_anchor_when_latest_pool_fallback_is_bad(
    client,
    db_session,
    create_user,
    monkeypatch,
):
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-17")
    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-17")
    monkeypatch.setattr(read_model, "trade_days_in_range", lambda start, end: ["2026-03-09"])

    insert_market_state_cache(db_session, trade_date="2026-03-17", market_state="NEUTRAL")
    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-09",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-09",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
        ],
    )

    now = utc_now()
    fallback_task_id = str(uuid4())
    db_session.execute(
        Base.metadata.tables["stock_pool_refresh_task"].insert().values(
            task_id=fallback_task_id,
            trade_date=date(2026, 3, 17),
            status="FALLBACK",
            pool_version=2,
            fallback_from=date(2026, 3, 9),
            filter_params_json={"target_pool_size": 3},
            core_pool_size=3,
            standby_pool_size=1,
            evicted_stocks_json=[],
            status_reason="KLINE_COVERAGE_INSUFFICIENT",
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    for rank_no, stock_code in enumerate(("600519.SH", "000001.SZ", "300750.SZ"), start=1):
        db_session.execute(
            Base.metadata.tables["stock_pool_snapshot"].insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=fallback_task_id,
                trade_date=date(2026, 3, 17),
                pool_version=2,
                stock_code=stock_code,
                pool_role="core",
                rank_no=rank_no,
                score=100 - rank_no,
                is_suspended=False,
                created_at=now,
            )
        )
    db_session.execute(
        Base.metadata.tables["stock_pool_snapshot"].insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=fallback_task_id,
            trade_date=date(2026, 3, 17),
            pool_version=2,
            stock_code="601318.SH",
            pool_role="standby",
            rank_no=1,
            score=90,
            is_suspended=False,
            created_at=now,
        )
    )
    db_session.commit()

    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-17",
    )

    pro_headers = _login_headers(
        client,
        create_user,
        email="kline-fallback-pro@example.com",
        tier="Pro",
    )
    admin_headers = _login_headers(
        client,
        create_user,
        email="kline-fallback-admin@example.com",
        tier="Enterprise",
        role="admin",
    )

    home_resp = client.get("/api/v1/home")
    dashboard_resp = client.get("/api/v1/dashboard/stats?window_days=30")
    platform_resp = client.get("/api/v1/platform/summary")
    admin_resp = client.get("/api/v1/admin/system-status", headers=admin_headers)
    sim_resp = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=pro_headers)

    assert home_resp.status_code == 200
    assert dashboard_resp.status_code == 200
    assert platform_resp.status_code == 200
    assert admin_resp.status_code == 200
    assert sim_resp.status_code == 200

    home = home_resp.json()["data"]
    dashboard = dashboard_resp.json()["data"]
    platform = platform_resp.json()["data"]
    admin = admin_resp.json()["data"]
    sim_public = sim_resp.json()["data"]["public_performance"]

    assert home["trade_date"] == "2026-03-09"
    assert home["data_status"] == "DEGRADED"
    assert home["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert home["display_reason"] == home["public_performance"]["display_hint"]

    for payload in (home["public_performance"], sim_public, admin["public_runtime"]):
        assert payload["data_status"] == "DEGRADED"
        assert payload["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
        assert payload["runtime_trade_date"] == "2026-03-09"
        assert payload["snapshot_date"] == "2026-03-09"
        assert payload["date_range"]["to"] == "2026-03-09"

    assert dashboard["data_status"] == "DEGRADED"
    assert dashboard["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert dashboard["runtime_trade_date"] == "2026-03-09"
    assert dashboard["stats_snapshot_date"] == "2026-03-09"
    assert dashboard["date_range"]["to"] == "2026-03-09"

    assert platform["data_status"] == "DEGRADED"
    assert platform["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert platform["runtime_trade_date"] == "2026-03-09"
    assert platform["snapshot_date"] == "2026-03-09"
    assert platform["period_end"] == "2026-03-09"

    assert admin["source_dates"]["runtime_trade_date"] == "2026-03-09"
    assert admin["source_dates"]["public_pool_trade_date"] == "2026-03-09"
    assert admin["source_dates"]["latest_complete_public_batch_trade_date"] == "2026-03-09"
    assert admin["source_dates"]["latest_published_report_trade_date"] == "2026-03-17"
    assert admin["public_runtime"]["task_status"] == "FALLBACK"
    assert admin["public_runtime"]["attempted_trade_date"] == "2026-03-17"
    assert admin["public_runtime"]["fallback_from"] == "2026-03-09"
    assert admin["public_runtime"]["kline_coverage"]["trade_date"] == "2026-03-17"


def test_fr10_public_status_fallback_issue_overrides_stats_not_ready_consistently(
    client,
    db_session,
    create_user,
    monkeypatch,
):
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-17")
    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-17")
    monkeypatch.setattr(read_model, "trade_days_in_range", lambda start, end: ["2026-03-09"])

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    insert_stock_master(db_session, stock_code="300750.SZ", stock_name="CATL")
    insert_stock_master(db_session, stock_code="601318.SH", stock_name="PINGAN INS")

    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-09",
        stock_codes=["600519.SH", "000001.SZ"],
        pool_version=1,
    )
    insert_market_state_cache(db_session, trade_date="2026-03-17", market_state="NEUTRAL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-09",
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-09",
    )

    now = utc_now()
    fallback_task_id = str(uuid4())
    db_session.execute(
        Base.metadata.tables["stock_pool_refresh_task"].insert().values(
            task_id=fallback_task_id,
            trade_date=date(2026, 3, 17),
            status="FALLBACK",
            pool_version=2,
            fallback_from=date(2026, 3, 9),
            filter_params_json={"target_pool_size": 3},
            core_pool_size=3,
            standby_pool_size=1,
            evicted_stocks_json=[],
            status_reason="KLINE_COVERAGE_INSUFFICIENT",
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    for rank_no, stock_code in enumerate(("600519.SH", "000001.SZ", "300750.SZ"), start=1):
        db_session.execute(
            Base.metadata.tables["stock_pool_snapshot"].insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=fallback_task_id,
                trade_date=date(2026, 3, 17),
                pool_version=2,
                stock_code=stock_code,
                pool_role="core",
                rank_no=rank_no,
                score=100 - rank_no,
                is_suspended=False,
                created_at=now,
            )
        )
    db_session.execute(
        Base.metadata.tables["stock_pool_snapshot"].insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=fallback_task_id,
            trade_date=date(2026, 3, 17),
            pool_version=2,
            stock_code="601318.SH",
            pool_role="standby",
            rank_no=1,
            score=90,
            is_suspended=False,
            created_at=now,
        )
    )
    db_session.commit()

    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-17",
    )

    pro_headers = _login_headers(
        client,
        create_user,
        email="kline-fallback-computing-pro@example.com",
        tier="Pro",
    )
    admin_headers = _login_headers(
        client,
        create_user,
        email="kline-fallback-computing-admin@example.com",
        tier="Enterprise",
        role="admin",
    )

    home_resp = client.get("/api/v1/home")
    dashboard_resp = client.get("/api/v1/dashboard/stats?window_days=30")
    platform_resp = client.get("/api/v1/platform/summary")
    admin_resp = client.get("/api/v1/admin/system-status", headers=admin_headers)
    sim_resp = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=pro_headers)

    assert home_resp.status_code == 200
    assert dashboard_resp.status_code == 200
    assert platform_resp.status_code == 200
    assert admin_resp.status_code == 200
    assert sim_resp.status_code == 200

    home = home_resp.json()["data"]
    dashboard = dashboard_resp.json()["data"]
    platform = platform_resp.json()["data"]
    admin = admin_resp.json()["data"]
    sim_public = sim_resp.json()["data"]["public_performance"]

    assert home["trade_date"] == "2026-03-09"
    assert home["data_status"] == "DEGRADED"
    assert home["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert home["display_reason"] == home["public_performance"]["display_hint"]

    for payload in (home["public_performance"], sim_public, admin["public_runtime"]):
        assert payload["data_status"] == "DEGRADED"
        assert payload["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
        assert payload["runtime_trade_date"] == "2026-03-09"
        assert payload["snapshot_date"] is None
        assert payload["date_range"]["to"] == "2026-03-09"

    assert dashboard["data_status"] == "DEGRADED"
    assert dashboard["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert dashboard["runtime_trade_date"] == "2026-03-09"
    assert dashboard["stats_snapshot_date"] is None
    assert dashboard["date_range"]["to"] == "2026-03-09"

    assert platform["data_status"] == "DEGRADED"
    assert platform["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert platform["runtime_trade_date"] == "2026-03-09"
    assert platform["snapshot_date"] is None
    assert platform["period_end"] == "2026-03-09"
    assert admin["public_runtime"]["task_status"] == "FALLBACK"
    assert admin["public_runtime"]["attempted_trade_date"] == "2026-03-17"
    assert admin["public_runtime"]["fallback_from"] == "2026-03-09"
    assert admin["public_runtime"]["kline_coverage"]["trade_date"] == "2026-03-17"


def test_fr10_home_public_status_ignores_market_state_degraded_when_public_runtime_ready(
    client,
    db_session,
    monkeypatch,
):
    import app.services.ssot_read_model as read_model

    monkeypatch.setattr(read_model, "_window_has_truncated_history", lambda *args, **kwargs: False)

    insert_market_state_cache(
        db_session,
        trade_date="2026-03-17",
        market_state="NEUTRAL",
        cache_status="DEGRADED_NEUTRAL",
        state_reason="market_state_degraded=true",
        market_state_degraded=True,
    )
    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-17",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-17",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
        ],
    )

    home_resp = client.get("/api/v1/home")
    dashboard_resp = client.get("/api/v1/dashboard/stats?window_days=30")
    platform_resp = client.get("/api/v1/platform/summary")

    assert home_resp.status_code == 200
    assert dashboard_resp.status_code == 200
    assert platform_resp.status_code == 200

    home = home_resp.json()["data"]
    dashboard = dashboard_resp.json()["data"]
    platform = platform_resp.json()["data"]
    public_performance = home["public_performance"]

    assert home["data_status"] == "READY"
    assert home["status_reason"] is None
    assert home["data_status"] == public_performance["data_status"] == dashboard["data_status"] == platform["data_status"]
    assert home["status_reason"] == public_performance["status_reason"] == dashboard["status_reason"] == platform["status_reason"]
    assert home["display_reason"] == public_performance["display_hint"]


def test_fr10_public_status_ignores_later_stats_snapshot_than_runtime_anchor(
    client,
    db_session,
    create_user,
    monkeypatch,
):
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-20")
    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-20")

    insert_market_state_cache(db_session, trade_date="2026-03-20", market_state="NEUTRAL")
    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-17",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-17",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
        ],
    )

    now = utc_now()
    fallback_task_id = str(uuid4())
    db_session.execute(
        Base.metadata.tables["stock_pool_refresh_task"].insert().values(
            task_id=fallback_task_id,
            trade_date=date(2026, 3, 20),
            status="FALLBACK",
            pool_version=2,
            fallback_from=date(2026, 3, 17),
            filter_params_json={"target_pool_size": 1},
            core_pool_size=1,
            standby_pool_size=0,
            evicted_stocks_json=[],
            status_reason="KLINE_COVERAGE_INSUFFICIENT",
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.execute(
        Base.metadata.tables["stock_pool_snapshot"].insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=fallback_task_id,
            trade_date=date(2026, 3, 20),
            pool_version=2,
            stock_code="600519.SH",
            pool_role="core",
            rank_no=1,
            score=99,
            is_suspended=False,
            created_at=now,
        )
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-20",
        strategy_type="B",
        window_days=30,
        sample_size=1,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        cumulative_return_pct=None,
        display_hint="sample_lt_30",
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-20",
        baseline_type="baseline_random",
        window_days=30,
        sample_size=1,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        cumulative_return_pct=None,
        display_hint="sample_lt_30",
    )

    admin_headers = _login_headers(
        client,
        create_user,
        email="future-stats-anchor-admin@example.com",
        tier="Enterprise",
        role="admin",
    )

    home_resp = client.get("/api/v1/home")
    dashboard_resp = client.get("/api/v1/dashboard/stats?window_days=30")
    platform_resp = client.get("/api/v1/platform/summary")
    admin_resp = client.get("/api/v1/admin/system-status", headers=admin_headers)

    assert home_resp.status_code == 200
    assert dashboard_resp.status_code == 200
    assert platform_resp.status_code == 200
    assert admin_resp.status_code == 200

    home = home_resp.json()["data"]
    dashboard = dashboard_resp.json()["data"]
    platform = platform_resp.json()["data"]
    admin = admin_resp.json()["data"]

    assert dashboard["runtime_trade_date"] == "2026-03-17"
    assert dashboard["stats_snapshot_date"] == "2026-03-17"
    assert home["public_performance"]["snapshot_date"] == "2026-03-17"
    assert platform["snapshot_date"] == "2026-03-17"
    assert admin["public_runtime"]["task_status"] == "FALLBACK"
    assert admin["public_runtime"]["snapshot_date"] == "2026-03-17"
    assert admin["public_runtime"]["attempted_trade_date"] == "2026-03-20"
    assert admin["source_dates"]["stats_snapshot_date"] == "2026-03-17"


def test_fr10_pool_stocks_uses_latest_effective_snapshot_only(client, db_session, monkeypatch):
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-13")

    for stock_code, stock_name in (
        ("600519.SH", "MOUTAI"),
        ("000001.SZ", "PINGAN"),
        ("300750.SZ", "CATL"),
        ("601318.SH", "PINGAN INS"),
    ):
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_name)

    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-12",
        stock_codes=["600519.SH", "000001.SZ"],
        pool_version=1,
    )
    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-13",
        stock_codes=["600519.SH", "300750.SZ"],
        pool_version=1,
    )

    response = client.get("/api/v1/pool/stocks")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["trade_date"] == "2026-03-13"
    assert body["data"]["total"] == 2
    assert [item["stock_code"] for item in body["data"]["items"]] == [
        "600519.SH",
        "300750.SZ",
    ]


def test_fr10_pool_stocks_can_pin_exact_trade_date_snapshot(client, db_session, monkeypatch):
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-13")

    for stock_code, stock_name in (
        ("600519.SH", "MOUTAI"),
        ("000001.SZ", "PINGAN"),
        ("300750.SZ", "CATL"),
    ):
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_name)

    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-12",
        stock_codes=["600519.SH", "000001.SZ"],
        pool_version=1,
    )
    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-13",
        stock_codes=["600519.SH", "300750.SZ"],
        pool_version=1,
    )

    response = client.get("/api/v1/pool/stocks?trade_date=2026-03-12")

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["trade_date"] == "2026-03-12"
    assert body["data"]["total"] == 2
    assert [item["stock_code"] for item in body["data"]["items"]] == [
        "600519.SH",
        "000001.SZ",
    ]


def test_fr10_public_pool_views_ignore_newer_corrupt_snapshot_task(client, db_session, create_user, monkeypatch):
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-13")

    for stock_code, stock_name in (
        ("600519.SH", "MOUTAI"),
        ("000001.SZ", "PINGAN"),
        ("300750.SZ", "CATL"),
    ):
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_name)
    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-12",
        stock_codes=["600519.SH", "000001.SZ"],
        pool_version=1,
    )
    insert_market_state_cache(db_session, trade_date="2026-03-13", market_state="BULL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-12",
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-12",
    )

    now = utc_now()
    refresh_task_table = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]
    bad_task_id = str(uuid4())
    db_session.execute(
        refresh_task_table.insert().values(
            task_id=bad_task_id,
            trade_date=date(2026, 3, 13),
            status="COMPLETED",
            pool_version=1,
            fallback_from=None,
            filter_params_json={},
            core_pool_size=2,
            standby_pool_size=0,
            evicted_stocks_json=[],
            status_reason=None,
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.execute(
        snapshot_table.insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=bad_task_id,
            trade_date=date(2026, 3, 13),
            pool_version=1,
            stock_code="300750.SZ",
            pool_role="core",
            rank_no=1,
            score=99.0,
            is_suspended=False,
            created_at=now,
        )
    )
    db_session.commit()

    admin_headers = _login_headers(
        client,
        create_user,
        email="admin-pool-fr10@example.com",
        tier="Enterprise",
        role="admin",
    )

    pool_response = client.get("/api/v1/pool/stocks")
    home_response = client.get("/api/v1/home")
    overview_response = client.get("/api/v1/admin/overview", headers=admin_headers)

    assert pool_response.status_code == 200
    assert home_response.status_code == 200
    assert overview_response.status_code == 200
    assert pool_response.json()["data"]["trade_date"] == "2026-03-12"
    assert pool_response.json()["data"]["total"] == 2
    assert home_response.json()["data"]["pool_size"] == 2
    assert overview_response.json()["data"]["pool_size"] == 2


def test_fr10_public_pool_views_ignore_low_coverage_completed_task(client, db_session, create_user, monkeypatch):
    import app.services.trade_calendar as trade_calendar
    from app.services.stock_pool import get_exact_pool_view

    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-13")

    for stock_code, stock_name in (
        ("600519.SH", "MOUTAI"),
        ("000001.SZ", "PINGAN"),
        ("300750.SZ", "CATL"),
    ):
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_name)
    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-12",
        stock_codes=["600519.SH", "000001.SZ"],
        pool_version=1,
    )
    insert_market_state_cache(db_session, trade_date="2026-03-13", market_state="BULL")
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-13",
        open_price=10.0,
        high_price=10.5,
        low_price=9.8,
        close_price=10.2,
    )

    now = utc_now()
    refresh_task_table = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]
    bad_task_id = str(uuid4())
    db_session.execute(
        refresh_task_table.insert().values(
            task_id=bad_task_id,
            trade_date=date(2026, 3, 13),
            status="COMPLETED",
            pool_version=1,
            fallback_from=None,
            filter_params_json={},
            core_pool_size=2,
            standby_pool_size=1,
            evicted_stocks_json=[],
            status_reason=None,
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    for rank_no, (stock_code, pool_role) in enumerate(
        (("600519.SH", "core"), ("000001.SZ", "core"), ("300750.SZ", "standby")),
        start=1,
    ):
        db_session.execute(
            snapshot_table.insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=bad_task_id,
                trade_date=date(2026, 3, 13),
                pool_version=1,
                stock_code=stock_code,
                pool_role=pool_role,
                rank_no=rank_no if pool_role == "core" else 1,
                score=100 - rank_no,
                is_suspended=False,
                created_at=now,
            )
        )
    db_session.commit()

    admin_headers = _login_headers(
        client,
        create_user,
        email="admin-low-coverage-fr10@example.com",
        tier="Enterprise",
        role="admin",
    )

    assert get_exact_pool_view(db_session, trade_date="2026-03-13") is None

    pool_response = client.get("/api/v1/pool/stocks")
    home_response = client.get("/api/v1/home")
    overview_response = client.get("/api/v1/admin/overview", headers=admin_headers)

    assert pool_response.status_code == 200
    assert home_response.status_code == 200
    assert overview_response.status_code == 200
    assert pool_response.json()["data"]["trade_date"] == "2026-03-12"
    assert home_response.json()["data"]["pool_size"] == 2
    assert overview_response.json()["data"]["pool_size"] == 2


def test_fr10_complete_public_batch_ignores_low_coverage_completed_task(
    client,
    db_session,
    create_user,
    monkeypatch,
):
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-13")
    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-13")

    for stock_code, stock_name in (
        ("600519.SH", "MOUTAI"),
        ("000001.SZ", "PINGAN"),
        ("300750.SZ", "CATL"),
    ):
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_name)
    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-12",
        stock_codes=["600519.SH"],
        pool_version=1,
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-12",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-13",
        open_price=10.0,
        high_price=10.5,
        low_price=9.8,
        close_price=10.2,
    )

    now = utc_now()
    refresh_task_table = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]
    bad_task_id = str(uuid4())
    db_session.execute(
        refresh_task_table.insert().values(
            task_id=bad_task_id,
            trade_date=date(2026, 3, 13),
            status="COMPLETED",
            pool_version=1,
            fallback_from=None,
            filter_params_json={},
            core_pool_size=3,
            standby_pool_size=0,
            evicted_stocks_json=[],
            status_reason=None,
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    # Pool with 3 core stocks but only 600519.SH has kline → 33% coverage
    for rank_no, stock_code in enumerate(["600519.SH", "000001.SZ", "300750.SZ"], start=1):
        db_session.execute(
            snapshot_table.insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=bad_task_id,
                trade_date=date(2026, 3, 13),
                pool_version=1,
                stock_code=stock_code,
                pool_role="core",
                rank_no=rank_no,
                score=100 - rank_no,
                is_suspended=False,
                created_at=now,
            )
        )
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-13",
        ensure_pool_snapshot=False,
    )

    assert read_model._latest_complete_public_batch_trade_date(db_session) == "2026-03-12"

    admin_headers = _login_headers(
        client,
        create_user,
        email="admin-low-coverage-completed-fr10@example.com",
        tier="Enterprise",
        role="admin",
    )

    home_resp = client.get("/api/v1/home")
    dashboard_resp = client.get("/api/v1/dashboard/stats?window_days=30")
    platform_resp = client.get("/api/v1/platform/summary")
    admin_resp = client.get("/api/v1/admin/system-status", headers=admin_headers)

    assert home_resp.status_code == 200
    assert dashboard_resp.status_code == 200
    assert platform_resp.status_code == 200
    assert admin_resp.status_code == 200

    home = home_resp.json()["data"]
    dashboard = dashboard_resp.json()["data"]
    platform = platform_resp.json()["data"]
    admin = admin_resp.json()["data"]

    assert home["trade_date"] == "2026-03-12"
    assert home["data_status"] == "DEGRADED"
    assert home["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert home["display_reason"] == home["public_performance"]["display_hint"]

    assert dashboard["runtime_trade_date"] == "2026-03-12"
    assert dashboard["data_status"] == "DEGRADED"
    assert dashboard["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"

    assert platform["runtime_trade_date"] == "2026-03-12"
    assert platform["data_status"] == "DEGRADED"
    assert platform["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"

    assert admin["public_runtime"]["runtime_trade_date"] == "2026-03-12"
    assert admin["public_runtime"]["task_status"] == "COMPLETED"
    assert admin["public_runtime"]["attempted_trade_date"] == "2026-03-13"
    assert admin["public_runtime"]["fallback_from"] is None
    assert admin["public_runtime"]["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert admin["public_runtime"]["kline_coverage"]["trade_date"] == "2026-03-13"


def test_fr10_sim_dashboard_capital_tier(client, db_session, create_user):
    insert_sim_dashboard_snapshot(db_session, capital_tier="100k", snapshot_date="2026-03-06")
    insert_sim_dashboard_snapshot(db_session, capital_tier="500k", snapshot_date="2026-03-06")
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="A",
        signal_validity_warning=True,
    )
    insert_strategy_metric_snapshot(db_session, snapshot_date="2026-03-06", strategy_type="B")
    insert_strategy_metric_snapshot(db_session, snapshot_date="2026-03-06", strategy_type="C")
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-04", equity=100000)
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-05", equity=101000)
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-06", equity=102500)
    insert_sim_equity_curve_point(db_session, capital_tier="500k", trade_date="2026-03-04", equity=500000)
    insert_sim_equity_curve_point(db_session, capital_tier="500k", trade_date="2026-03-05", equity=503000)
    insert_sim_equity_curve_point(db_session, capital_tier="500k", trade_date="2026-03-06", equity=508000)

    free_headers = _login_headers(
        client,
        create_user,
        email="free-fr10@example.com",
        tier="Free",
    )
    free_default = client.get("/api/v1/portfolio/sim-dashboard", headers=free_headers)
    assert free_default.status_code == 403
    assert free_default.json()["error_code"] == "TIER_NOT_AVAILABLE"

    free_forbidden = client.get(
        "/api/v1/portfolio/sim-dashboard?capital_tier=500k",
        headers=free_headers,
    )
    assert free_forbidden.status_code == 403
    assert free_forbidden.json()["error_code"] == "TIER_NOT_AVAILABLE"

    pro_headers = _login_headers(
        client,
        create_user,
        email="pro-fr10@example.com",
        tier="Pro",
    )
    pro_response = client.get(
        "/api/v1/portfolio/sim-dashboard?capital_tier=500k",
        headers=pro_headers,
    )
    assert pro_response.status_code == 200
    assert pro_response.json()["data"]["capital_tier"] == "500k"


def test_fr10_term_context_fields(client, db_session, create_user):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        signal_entry_price=123.45,
        atr_pct=0.032,
        stop_loss=117.28,
        target_price=138.88,
    )

    headers = _login_headers(
        client,
        create_user,
        email="unused@example.com",
        tier="Pro",
    )

    response = client.get(f"/api/v1/reports/{report.report_id}", headers=headers)

    assert response.status_code == 200
    body = response.json()
    term_context = body["data"]["term_context"]
    assert isinstance(term_context, dict)
    assert "ATR" in term_context
    assert "3.2%" in term_context["ATR"]
    assert term_context["signal_entry_price"] == "123.45"
    assert term_context["stop_loss"] == "117.28"
    assert term_context["target_price"] == "138.88"


def test_fr10_sim_dashboard_unknown_paid_membership_matches_html_gate(client, create_user):
    user_info = create_user(
        email="sim-unknown-paid@example.com",
        password="Password123",
        tier="Pro",
        tier_expires_at=None,
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": user_info["user"].email, "password": user_info["password"]},
    )
    assert login.status_code == 200
    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

    html_response = client.get("/portfolio/sim-dashboard", headers=headers)
    api_response = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=headers)

    assert html_response.status_code == 403
    assert api_response.status_code == 403
    assert api_response.json()["error_code"] == "TIER_NOT_AVAILABLE"


def test_fr10_equity_curve_continuous(client, db_session, create_user):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-03",
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code=report.stock_code,
        capital_tier="100k",
        signal_date="2026-03-03",
        entry_date="2026-03-04",
        actual_entry_price=100.0,
        signal_entry_price=100.0,
        position_ratio=0.3,
        shares=100,
    )
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date="2026-03-06",
        total_return_pct=0.025,
        sample_size=36,
        display_hint=None,
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="A",
        signal_validity_warning=True,
    )
    insert_strategy_metric_snapshot(db_session, snapshot_date="2026-03-06", strategy_type="B")
    insert_strategy_metric_snapshot(db_session, snapshot_date="2026-03-06", strategy_type="C")
    insert_sim_equity_curve_point(
        db_session,
        capital_tier="100k",
        trade_date="2026-03-04",
        equity=100000,
        cash_available=70000,
        position_market_value=30000,
    )
    insert_sim_equity_curve_point(
        db_session,
        capital_tier="100k",
        trade_date="2026-03-05",
        equity=100000,
        cash_available=70000,
        position_market_value=30000,
    )
    insert_sim_equity_curve_point(
        db_session,
        capital_tier="100k",
        trade_date="2026-03-06",
        equity=102500,
        cash_available=72500,
        position_market_value=30000,
    )
    for trade_date, equity in (
        ("2026-03-04", 100000),
        ("2026-03-05", 100800),
        ("2026-03-06", 101400),
    ):
        insert_baseline_equity_curve_point(
            db_session,
            capital_tier="100k",
            baseline_type="baseline_random",
            trade_date=trade_date,
            equity=equity,
        )
        insert_baseline_equity_curve_point(
            db_session,
            capital_tier="100k",
            baseline_type="baseline_ma_cross",
            trade_date=trade_date,
            equity=equity - 500,
        )
    headers = _login_headers(
        client,
        create_user,
        email="equity-fr10@example.com",
        tier="Pro",
    )
    response = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["is_simulated_only"] is True
    assert body["data"]["signal_validity_warning"] is False
    assert [item["date"] for item in body["data"]["equity_curve"]] == [
        "2026-03-04",
        "2026-03-05",
        "2026-03-06",
    ]
    assert body["data"]["baseline_random"] is not None
    assert body["data"]["baseline_ma_cross"] is not None
    assert len(body["data"]["open_positions"]) == 1


def test_fr10_dashboard_stats_stays_computing_without_settlements(client, db_session):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")
    insert_pool_snapshot(db_session, trade_date="2026-03-06", stock_codes=["600519.SH"])
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        confidence=0.81,
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="A",
        sample_size=0,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=None,
        display_hint="sample_lt_30",
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="B",
        sample_size=0,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=None,
        signal_validity_warning=True,
        display_hint="sample_lt_30",
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="C",
        sample_size=0,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=None,
        display_hint="sample_lt_30",
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        baseline_type="baseline_random",
        sample_size=0,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=None,
        display_hint="sample_lt_30",
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        baseline_type="baseline_ma_cross",
        sample_size=0,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=None,
        display_hint="sample_lt_30",
    )

    response = client.get("/api/v1/dashboard/stats?window_days=1")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["total_reports"] == 1
    assert data["total_settled"] == 0
    assert data["data_status"] == "COMPUTING"
    assert data["status_reason"] == "stats_not_ready"
    assert data["signal_validity_warning"] is False
    assert data["baseline_random"] is None
    assert data["baseline_ma_cross"] is None


def test_fr10_public_metrics_unknown_semantics_align_between_dashboard_platform_and_sim(
    client, db_session, create_user
):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date="2026-03-06", stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        confidence=0.8,
    )
    pro_headers = _login_headers(
        client,
        create_user,
        email="metric-align-pro@example.com",
        tier="Pro",
    )

    dashboard_resp = client.get("/api/v1/dashboard/stats?window_days=30")
    platform_resp = client.get("/api/v1/platform/summary")
    sim_resp = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=pro_headers)

    assert dashboard_resp.status_code == 200
    assert platform_resp.status_code == 200
    assert sim_resp.status_code == 200
    dashboard = dashboard_resp.json()["data"]
    platform = platform_resp.json()["data"]
    sim = sim_resp.json()["data"]

    assert dashboard["overall_win_rate"] is None
    assert dashboard["overall_profit_loss_ratio"] is None
    assert platform["win_rate"] is None
    assert platform["pnl_ratio"] is None
    assert sim["win_rate"] is None
    assert sim["profit_loss_ratio"] is None


def test_fr10_public_status_aligns_across_home_dashboard_platform_admin_and_sim_when_stats_not_ready(
    client,
    db_session,
    create_user,
):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date="2026-03-06", stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        confidence=0.8,
    )

    pro_headers = _login_headers(
        client,
        create_user,
        email="stats-not-ready-pro@example.com",
        tier="Pro",
    )
    admin_headers = _login_headers(
        client,
        create_user,
        email="stats-not-ready-admin@example.com",
        tier="Enterprise",
        role="admin",
    )

    home_resp = client.get("/api/v1/home")
    dashboard_resp = client.get("/api/v1/dashboard/stats?window_days=30")
    platform_resp = client.get("/api/v1/platform/summary")
    admin_resp = client.get("/api/v1/admin/system-status", headers=admin_headers)
    sim_resp = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=pro_headers)

    assert home_resp.status_code == 200
    assert dashboard_resp.status_code == 200
    assert platform_resp.status_code == 200
    assert admin_resp.status_code == 200
    assert sim_resp.status_code == 200

    home = home_resp.json()["data"]
    dashboard = dashboard_resp.json()["data"]
    platform = platform_resp.json()["data"]
    admin = admin_resp.json()["data"]
    sim_public = sim_resp.json()["data"]["public_performance"]

    assert home["data_status"] == "COMPUTING"
    assert home["status_reason"] == "stats_not_ready"
    assert home["display_reason"] == home["public_performance"]["display_hint"]

    for payload in (
        home["public_performance"],
        dashboard,
        platform,
        admin["public_runtime"],
        sim_public,
    ):
        assert payload["data_status"] == "COMPUTING"
        assert payload["status_reason"] == "stats_not_ready"

    assert home["trade_date"] == "2026-03-06"
    assert home["public_performance"]["runtime_trade_date"] == "2026-03-06"
    assert dashboard["runtime_trade_date"] == "2026-03-06"
    assert platform["runtime_trade_date"] == "2026-03-06"
    assert admin["public_runtime"]["runtime_trade_date"] == "2026-03-06"
    assert sim_public["runtime_trade_date"] == "2026-03-06"


def test_fr10_public_performance_truth_aligns_between_home_dashboard_platform_and_sim(
    client, db_session, create_user
):
    insert_market_state_cache(db_session, trade_date="2026-03-17", market_state="NEUTRAL")
    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-17",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-16",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
            {
                "stock_code": "000001.SZ",
                "stock_name": "PINGAN",
                "trade_date": "2026-03-17",
                "strategy_type": "C",
                "recommendation": "BUY",
            },
        ],
    )
    pro_headers = _login_headers(
        client,
        create_user,
        email="public-truth-pro@example.com",
        tier="Pro",
    )

    home_resp = client.get("/api/v1/home")
    dashboard_resp = client.get("/api/v1/dashboard/stats?window_days=30")
    platform_resp = client.get("/api/v1/platform/summary")
    sim_resp = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=pro_headers)

    assert home_resp.status_code == 200
    assert dashboard_resp.status_code == 200
    assert platform_resp.status_code == 200
    assert sim_resp.status_code == 200

    home_public = home_resp.json()["data"]["public_performance"]
    dashboard = dashboard_resp.json()["data"]
    platform = platform_resp.json()["data"]
    sim_public = sim_resp.json()["data"]["public_performance"]

    assert dashboard["data_status"] == "DEGRADED"
    assert dashboard["status_reason"] == "stats_history_truncated"
    assert dashboard["display_hint"] == "历史窗口覆盖不足"
    assert dashboard["overall_win_rate"] is None
    assert dashboard["overall_profit_loss_ratio"] is None
    assert dashboard["baseline_random"] is None
    assert dashboard["baseline_ma_cross"] is None

    for payload in (home_public, sim_public):
        assert payload["window_days"] == 30
        assert payload["data_status"] == dashboard["data_status"]
        assert payload["status_reason"] == dashboard["status_reason"]
        assert payload["display_hint"] == dashboard["display_hint"]
        assert payload["runtime_trade_date"] == dashboard["runtime_trade_date"]
        assert payload["snapshot_date"] == dashboard["stats_snapshot_date"]
        assert payload["date_range"] == dashboard["date_range"]
        assert payload["overall_win_rate"] == dashboard["overall_win_rate"]
        assert payload["overall_profit_loss_ratio"] == dashboard["overall_profit_loss_ratio"]
        assert payload["alpha_vs_baseline"] is None
        assert payload["sample_size"] == dashboard["total_settled"]
        assert payload["total_settled"] == dashboard["total_settled"]
        assert payload["total_reports"] == dashboard["total_reports"]
        assert payload["baseline_random"] is None
        assert payload["baseline_ma_cross"] is None

    assert platform["win_rate"] == dashboard["overall_win_rate"]
    assert platform["pnl_ratio"] == dashboard["overall_profit_loss_ratio"]
    assert platform["alpha"] is None
    assert platform["baseline_random"] is None
    assert platform["baseline_ma_cross"] is None
    assert platform["total_trades"] == dashboard["total_settled"]
    assert platform["period_start"] == dashboard["date_range"]["from"]
    assert platform["period_end"] == dashboard["date_range"]["to"]
    assert platform["data_status"] == dashboard["data_status"]
    assert platform["status_reason"] == dashboard["status_reason"]
    assert platform["display_hint"] == dashboard["display_hint"]
    assert platform["runtime_trade_date"] == dashboard["runtime_trade_date"]
    assert platform["snapshot_date"] == dashboard["stats_snapshot_date"]
    assert platform["cold_start"] is True
    assert platform["cold_start_message"] == dashboard["display_hint"]


def test_fr10_public_performance_preserves_ready_snapshot_under_source_degraded_history(
    client, db_session, create_user, monkeypatch
):
    import app.services.ssot_read_model as read_model

    insert_market_state_cache(db_session, trade_date="2026-03-20", market_state="NEUTRAL")
    report_specs = []
    for idx in range(36):
        report_specs.append(
            {
                "stock_code": f"{600100 + idx:06d}.SH",
                "stock_name": f"STOCK{idx:03d}",
                "trade_date": "2026-03-20" if idx < 12 else "2026-03-19" if idx < 24 else "2026-03-18",
                "strategy_type": "B",
                "recommendation": "BUY",
            }
        )
    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-20",
        report_specs=report_specs,
    )
    monkeypatch.setattr(read_model, "_window_has_truncated_history", lambda *args, **kwargs: True)
    pro_headers = _login_headers(
        client,
        create_user,
        email="public-source-degraded@example.com",
        tier="Pro",
    )

    home_resp = client.get("/api/v1/home")
    dashboard_resp = client.get("/api/v1/dashboard/stats?window_days=30")
    platform_resp = client.get("/api/v1/platform/summary")
    sim_resp = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=pro_headers)

    assert home_resp.status_code == 200
    assert dashboard_resp.status_code == 200
    assert platform_resp.status_code == 200
    assert sim_resp.status_code == 200

    home_public = home_resp.json()["data"]["public_performance"]
    dashboard = dashboard_resp.json()["data"]
    platform = platform_resp.json()["data"]
    sim_public = sim_resp.json()["data"]["public_performance"]

    assert dashboard["data_status"] == "DEGRADED"
    assert dashboard["status_reason"] == "stats_source_degraded"
    assert dashboard["display_hint"] == "部分历史批次未完整回补，沿用当前统计快照"
    assert dashboard["overall_win_rate"] is not None
    assert dashboard["overall_profit_loss_ratio"] is not None
    assert dashboard["overall_cumulative_return_pct"] is not None
    assert dashboard["baseline_random"] is not None
    assert dashboard["baseline_ma_cross"] is not None

    for payload in (home_public, sim_public):
        assert payload["window_days"] == 30
        assert payload["data_status"] == dashboard["data_status"]
        assert payload["status_reason"] == dashboard["status_reason"]
        assert payload["display_hint"] == dashboard["display_hint"]
        assert payload["runtime_trade_date"] == dashboard["runtime_trade_date"]
        assert payload["snapshot_date"] == dashboard["stats_snapshot_date"]
        assert payload["date_range"] == dashboard["date_range"]
        assert payload["overall_win_rate"] == dashboard["overall_win_rate"]
        assert payload["overall_profit_loss_ratio"] == dashboard["overall_profit_loss_ratio"]
        assert payload["overall_cumulative_return_pct"] == dashboard["overall_cumulative_return_pct"]
        assert payload["total_settled"] == dashboard["total_settled"]
        assert payload["total_reports"] == dashboard["total_reports"]
        assert payload["sample_size"] == dashboard["total_settled"]
        assert payload["baseline_random"] == dashboard["baseline_random"]
        assert payload["baseline_ma_cross"] == dashboard["baseline_ma_cross"]

    assert platform["win_rate"] == dashboard["overall_win_rate"]
    assert platform["pnl_ratio"] == dashboard["overall_profit_loss_ratio"]
    assert platform["alpha"] == home_public["alpha_vs_baseline"]
    assert platform["baseline_random"] == dashboard["baseline_random"]
    assert platform["baseline_ma_cross"] == dashboard["baseline_ma_cross"]
    assert platform["total_trades"] == dashboard["total_settled"]
    assert platform["period_start"] == dashboard["date_range"]["from"]
    assert platform["period_end"] == dashboard["date_range"]["to"]
    assert platform["data_status"] == dashboard["data_status"]
    assert platform["status_reason"] == dashboard["status_reason"]
    assert platform["display_hint"] == dashboard["display_hint"]
    assert platform["runtime_trade_date"] == dashboard["runtime_trade_date"]
    assert platform["snapshot_date"] == dashboard["stats_snapshot_date"]
    assert platform["cold_start"] is False
    assert platform["cold_start_message"] is None


def test_fr10_dashboard_stats_counts_current_window_distinct_settled_only(client, db_session):
    report_one = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-05",
        strategy_type="B",
        recommendation="BUY",
    )
    report_two = insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-06",
        strategy_type="B",
        recommendation="BUY",
    )
    for report in (report_one, report_two):
        insert_settlement_result(
            db_session,
            report_id=report.report_id,
            stock_code=report.stock_code,
            signal_date=str(report.trade_date),
            window_days=30,
            strategy_type="B",
            net_return_pct=0.05,
            exit_trade_date="2026-03-10",
        )
        for other_window in (1, 7, 14, 60):
            insert_settlement_result(
                db_session,
                report_id=report.report_id,
                stock_code=report.stock_code,
                signal_date=str(report.trade_date),
                window_days=other_window,
                strategy_type="B",
                net_return_pct=0.03,
                exit_trade_date="2026-03-10",
            )

    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="A",
        sample_size=0,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=None,
        display_hint="sample_lt_30",
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="B",
        sample_size=2,
        coverage_pct=1.0,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=0.10,
        display_hint="sample_lt_30",
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="C",
        sample_size=0,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=None,
        display_hint="sample_lt_30",
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        baseline_type="baseline_random",
        sample_size=2,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=0.04,
        display_hint="sample_lt_30",
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        baseline_type="baseline_ma_cross",
        sample_size=2,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=0.03,
        display_hint="sample_lt_30",
    )

    response = client.get("/api/v1/dashboard/stats?window_days=30")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["total_reports"] == 2
    assert data["total_settled"] == 2
    assert data["by_strategy_type"]["B"]["sample_size"] == 2
    assert data["by_strategy_type"]["B"]["coverage_pct"] == 1.0
    assert data["signal_validity_warning"] is False


def test_fr10_dashboard_stats_returns_requested_natural_day_boundaries(client, db_session, monkeypatch):
    import app.services.ssot_read_model as read_model

    monkeypatch.setattr(read_model, "trade_days_in_range", lambda start, end: ["2026-03-09"])

    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-09",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-09",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
        ],
    )

    response = client.get("/api/v1/dashboard/stats?window_days=7")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["date_range"] == {"from": "2026-03-03", "to": "2026-03-09"}
    assert data["total_reports"] == 1
    assert data["total_settled"] == 1
    assert data["data_status"] == "READY"
    assert data["status_reason"] is None
    assert data["by_strategy_type"]["B"]["sample_size"] == 1


def test_fr10_dashboard_stats_ignores_future_snapshot_dates(client, db_session, monkeypatch):
    import app.services.ssot_read_model as read_model

    monkeypatch.setattr(read_model, "trade_days_in_range", lambda start, end: ["2026-03-05", "2026-03-06"])

    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-06",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-05",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
            {
                "stock_code": "000001.SZ",
                "stock_name": "PINGAN",
                "trade_date": "2026-03-06",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
        ],
    )

    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-09",
        strategy_type="B",
        sample_size=9,
        coverage_pct=0.9,
        win_rate=0.9,
        profit_loss_ratio=3.0,
        alpha_annual=0.3,
        max_drawdown_pct=-0.01,
        cumulative_return_pct=0.5,
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-09",
        baseline_type="baseline_random",
        sample_size=9,
        win_rate=0.8,
        profit_loss_ratio=2.0,
        alpha_annual=0.2,
        max_drawdown_pct=-0.02,
        cumulative_return_pct=0.4,
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-09",
        baseline_type="baseline_ma_cross",
        sample_size=9,
        win_rate=0.7,
        profit_loss_ratio=1.8,
        alpha_annual=0.18,
        max_drawdown_pct=-0.03,
        cumulative_return_pct=0.3,
    )

    response = client.get("/api/v1/dashboard/stats?window_days=30")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["date_range"] == {"from": "2026-02-05", "to": "2026-03-06"}
    assert data["total_reports"] == 2
    assert data["total_settled"] == 2
    assert data["data_status"] == "READY"
    assert data["status_reason"] is None
    assert data["by_strategy_type"]["B"]["sample_size"] == 2
    assert data["baseline_random"]["sample_size"] == 2


def test_fr10_dashboard_stats_marks_inconsistent_snapshot_as_computing(client, db_session, monkeypatch):
    import app.services.ssot_read_model as read_model

    monkeypatch.setattr(read_model, "trade_days_in_range", lambda start, end: ["2026-03-05", "2026-03-06"])

    insert_pool_snapshot(db_session, trade_date="2026-03-05", stock_codes=["600519.SH"])
    insert_pool_snapshot(db_session, trade_date="2026-03-06", stock_codes=["000001.SZ"])

    report_one = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-05",
        strategy_type="B",
        recommendation="BUY",
    )
    report_two = insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-06",
        strategy_type="B",
        recommendation="BUY",
    )
    insert_settlement_result(
        db_session,
        report_id=report_one.report_id,
        stock_code=report_one.stock_code,
        signal_date="2026-03-05",
        window_days=30,
        strategy_type="B",
        net_return_pct=-0.02,
        exit_trade_date="2026-03-10",
    )
    insert_settlement_result(
        db_session,
        report_id=report_two.report_id,
        stock_code=report_two.stock_code,
        signal_date="2026-03-06",
        window_days=30,
        strategy_type="B",
        net_return_pct=0.04,
        exit_trade_date="2026-03-10",
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        strategy_type="B",
        sample_size=1,
        coverage_pct=0.5,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=-0.02,
        signal_validity_warning=True,
        display_hint="sample_lt_30",
    )

    response = client.get("/api/v1/dashboard/stats?window_days=30")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["total_settled"] == 2
    assert data["data_status"] == "COMPUTING"
    assert data["status_reason"] == "stats_not_ready"
    assert data["display_hint"] == "统计快照计算中"
    assert data["signal_validity_warning"] is False
    assert data["baseline_random"] is None
    assert data["by_strategy_type"]["B"]["sample_size"] == 2
    assert data["by_strategy_type"]["B"]["coverage_pct"] == 1.0
    assert data["by_strategy_type"]["B"]["win_rate"] is None


def test_fr10_dashboard_stats_anchors_to_latest_complete_stats_batch(client, db_session, monkeypatch):
    import app.services.ssot_read_model as read_model

    monkeypatch.setattr(read_model, "trade_days_in_range", lambda start, end: ["2026-03-09"])

    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-09",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-09",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
        ],
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-17",
        strategy_type="B",
        recommendation="BUY",
    )

    response = client.get("/api/v1/dashboard/stats?window_days=30")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["date_range"] == {"from": "2026-02-16", "to": "2026-03-17"}
    assert data["runtime_trade_date"] == "2026-03-17"
    assert data["stats_snapshot_date"] == "2026-03-09"
    assert data["total_reports"] == 2
    assert data["total_settled"] == 1
    assert data["data_status"] == "COMPUTING"
    assert data["status_reason"] == "stats_not_ready"
    assert data["overall_win_rate"] is None
    assert data["by_strategy_type"]["B"]["sample_size"] == 1
    assert data["by_strategy_type"]["B"]["coverage_pct"] == 0.5
    assert data["by_strategy_type"]["B"]["profit_loss_ratio"] is None


def test_fr10_future_dated_public_rows_do_not_advance_runtime_anchor(client, db_session, monkeypatch):
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar_mod

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda: "2026-03-20")
    monkeypatch.setattr(trade_calendar_mod, "latest_trade_date_str", lambda: "2026-03-20")

    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-20",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-20",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
        ],
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-04-01",
        strategy_type="B",
        recommendation="BUY",
    )

    dashboard = client.get("/api/v1/dashboard/stats?window_days=30")
    home = client.get("/api/v1/home")

    assert dashboard.status_code == 200
    assert home.status_code == 200
    dashboard_data = dashboard.json()["data"]
    home_data = home.json()["data"]
    assert dashboard_data["runtime_trade_date"] == "2026-03-20"
    assert dashboard_data["date_range"]["to"] == "2026-03-20"
    assert dashboard_data["stats_snapshot_date"] != "2026-04-01"
    assert home_data["trade_date"] == "2026-03-20"
    assert home_data["public_performance"]["runtime_trade_date"] == "2026-03-20"
    assert home_data["public_performance"]["snapshot_date"] != "2026-04-01"


def test_fr10_public_pool_fail_closes_when_only_future_snapshot_exists(db_session, monkeypatch):
    import app.services.trade_calendar as trade_calendar
    from app.services.stock_pool import get_public_pool_view

    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-20")
    insert_pool_snapshot(db_session, trade_date="2026-04-01", stock_codes=["600519.SH"])

    assert get_public_pool_view(db_session) is None


def test_fr10_future_public_pool_rows_do_not_leak_to_platform_or_sim_dashboard(
    client,
    db_session,
    create_user,
    monkeypatch,
):
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-20")
    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-20")

    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-20",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-20",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
        ],
    )
    insert_pool_snapshot(db_session, trade_date="2026-04-01", stock_codes=["000001.SZ"])
    pro_headers = _login_headers(
        client,
        create_user,
        email="future-public-pool-pro@example.com",
        tier="Pro",
    )

    platform_resp = client.get("/api/v1/platform/summary")
    sim_resp = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=pro_headers)

    assert platform_resp.status_code == 200
    assert sim_resp.status_code == 200
    platform = platform_resp.json()["data"]
    sim_public = sim_resp.json()["data"]["public_performance"]
    assert platform["period_end"] == "2026-03-20"
    assert sim_public["runtime_trade_date"] == "2026-03-20"
    assert sim_public["snapshot_date"] == "2026-03-20"
    assert sim_public["date_range"]["to"] == "2026-03-20"


def test_fr10_runtime_anchor_ignores_underfilled_future_batch(db_session, monkeypatch):
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-31")
    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-31")

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date="2026-03-20", stock_codes=["600519.SH"])
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-20",
    )
    insert_market_state_cache(db_session, trade_date="2026-03-31", market_state="BEAR")

    now = utc_now()
    refresh_task_table = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]
    bad_task_id = str(uuid4())
    db_session.execute(
        refresh_task_table.insert().values(
            task_id=bad_task_id,
            trade_date=date(2026, 3, 31),
            status="COMPLETED",
            pool_version=2,
            fallback_from=None,
            filter_params_json={},
            core_pool_size=1,
            standby_pool_size=0,
            evicted_stocks_json=[],
            status_reason=None,
            request_id=str(uuid4()),
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.execute(
        snapshot_table.insert().values(
            pool_snapshot_id=str(uuid4()),
            refresh_task_id=bad_task_id,
            trade_date=date(2026, 3, 31),
            pool_version=2,
            stock_code="600519.SH",
            pool_role="core",
            rank_no=1,
            score=99.0,
            is_suspended=False,
            created_at=now,
        )
    )
    db_session.commit()

    anchors = read_model.get_runtime_anchor_dates_ssot(db_session)

    assert anchors["public_pool_trade_date"] == "2026-03-20"
    assert anchors["latest_complete_public_batch_trade_date"] == "2026-03-20"
    assert anchors["runtime_trade_date"] == "2026-03-20"


def test_fr10_dashboard_stats_degrades_when_history_is_truncated(client, db_session):
    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-17",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-16",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
            {
                "stock_code": "000001.SZ",
                "stock_name": "PINGAN",
                "trade_date": "2026-03-17",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
        ],
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-02",
        open_price=10.0,
        high_price=10.5,
        low_price=9.9,
        close_price=10.2,
    )
    db_session.commit()

    response = client.get("/api/v1/dashboard/stats?window_days=60")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["date_range"] == {"from": "2026-01-17", "to": "2026-03-17"}
    assert data["data_status"] == "DEGRADED"
    assert data["status_reason"] == "stats_history_truncated"
    assert data["display_hint"] == "历史窗口覆盖不足"
    assert data["baseline_random"] is None
    assert data["baseline_ma_cross"] is None
    assert data["by_strategy_type"]["B"]["sample_size"] == 2
    assert data["by_strategy_type"]["B"]["coverage_pct"] == 1.0


def test_fr10_dashboard_stats_degrades_when_expected_trade_days_have_no_trace(client, db_session):
    _seed_complete_dashboard_snapshot_batch(
        db_session,
        snapshot_date="2026-03-17",
        report_specs=[
            {
                "stock_code": "600519.SH",
                "stock_name": "MOUTAI",
                "trade_date": "2026-03-13",
                "strategy_type": "B",
                "recommendation": "BUY",
            },
            {
                "stock_code": "000001.SZ",
                "stock_name": "PINGAN",
                "trade_date": "2026-03-17",
                "strategy_type": "C",
                "recommendation": "BUY",
            },
        ],
    )

    response = client.get("/api/v1/dashboard/stats?window_days=60")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["date_range"] == {"from": "2026-01-17", "to": "2026-03-17"}
    assert data["data_status"] == "DEGRADED"
    assert data["status_reason"] == "stats_history_truncated"
    assert data["baseline_random"] is None
    assert data["baseline_ma_cross"] is None
    assert data["by_strategy_type"]["B"]["sample_size"] == 1
    assert data["by_strategy_type"]["C"]["sample_size"] == 1


def test_fr10_dashboard_stats_rejects_invalid_window(client):
    response = client.get("/api/v1/dashboard/stats?window_days=15")

    assert response.status_code == 422


def test_fr10_dashboard_html_contains_warning_and_coverage_markers(client):
    response = client.get("/dashboard")

    assert response.status_code == 200
    html = response.text
    assert 'id="dashboard-warning"' in html
    assert "覆盖率" in html
    assert "样本积累中" in html


def test_fr10_sim_dashboard_ignores_stale_snapshot_without_real_sim_data(client, db_session, create_user):
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date="2026-03-06",
        data_status="READY",
        total_return_pct=0.092806,
        sample_size=0,
        display_hint="sample_lt_30",
    )
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-05", equity=109280.56)
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-06", equity=109280.56)

    headers = _login_headers(
        client,
        create_user,
        email="sim-empty-fr10@example.com",
        tier="Pro",
    )
    response = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=headers)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["data_status"] == "COMPUTING"
    assert data["status_reason"] == "sim_dashboard_not_ready"
    assert data["snapshot_date"] == "2026-03-06"
    assert data["equity_curve"] == []
    assert data["total_return_pct"] == 0.0
    assert data["open_positions"] == []


def test_fr10_sim_dashboard_ready_snapshot_keeps_status_reason_empty(client, db_session, create_user):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-17",
        strategy_type="B",
        recommendation="BUY",
    )
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date="2026-03-17",
        data_status="READY",
        status_reason=None,
        total_return_pct=0.052,
        win_rate=0.55,
        profit_loss_ratio=1.6,
        sample_size=0,
        display_hint="sample_lt_30",
    )
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-16", equity=100000.0)
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-17", equity=105200.0)
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code=report.stock_code,
        capital_tier="100k",
        signal_date="2026-03-17",
        entry_date="2026-03-17",
        actual_entry_price=100.0,
        signal_entry_price=100.0,
        position_ratio=0.2,
        shares=100,
    )

    headers = _login_headers(
        client,
        create_user,
        email="sim-ready-fr10@example.com",
        tier="Pro",
    )
    response = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=headers)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["data_status"] == "READY"
    assert data["status_reason"] is None
    assert data["snapshot_date"] == "2026-03-17"
    assert data["sample_size"] == 0


def test_fr10_sim_dashboard_clamps_negative_holding_days_and_hides_zero_baselines(client, db_session, create_user):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-17",
        strategy_type="B",
        recommendation="BUY",
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code=report.stock_code,
        capital_tier="100k",
        signal_date="2026-03-17",
        entry_date="2026-03-16",
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
    )
    db_session.execute(
        Base.metadata.tables["sim_position"]
        .update()
        .where(Base.metadata.tables["sim_position"].c.report_id == report.report_id)
        .values(holding_days=-6)
    )
    db_session.commit()
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date="2026-03-17",
        data_status="READY",
        sample_size=19,
        display_hint="sample_lt_30",
    )
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-17", equity=100000.0)
    insert_baseline_equity_curve_point(
        db_session,
        capital_tier="100k",
        baseline_type="baseline_random",
        trade_date="2026-03-17",
        equity=0.0,
    )
    insert_baseline_equity_curve_point(
        db_session,
        capital_tier="100k",
        baseline_type="baseline_ma_cross",
        trade_date="2026-03-17",
        equity=0.0,
    )

    headers = _login_headers(
        client,
        create_user,
        email="sim-clamp@example.com",
        tier="Pro",
    )
    response = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=headers)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["open_positions"][0]["holding_days"] >= 0
    assert data["baseline_random"] is None
    assert data["baseline_ma_cross"] is None
    assert data["display_hint"] == "基线对照数据计算中"
