from __future__ import annotations

from datetime import date
from pathlib import Path
from uuid import uuid4

from app.models import Base, StockPoolRefreshTask
from app.services.dashboard_query import get_public_performance_payload_ssot
from app.services.observability import runtime_metrics_summary
from app.services.runtime_anchor_service import RuntimeAnchorService
from app.services.stock_pool import evaluate_public_task_eligibility
from tests.helpers_ssot import (
    insert_baseline_metric_snapshot,
    insert_kline,
    insert_market_state_cache,
    insert_pool_snapshot,
    insert_report_bundle_ssot,
    insert_sim_dashboard_snapshot,
    insert_stock_master,
    insert_strategy_metric_snapshot,
    utc_now,
)

import pytest
from app.services import stock_pool as _stock_pool_mod


@pytest.fixture(autouse=True)
def _lower_min_core_rows(isolated_app, monkeypatch):
    monkeypatch.setattr(_stock_pool_mod, "_MIN_CORE_ROWS_VALID", 2)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _login_admin(client, create_user) -> dict[str, str]:
    account = create_user(
        email="runtime-anchor-admin@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": account["user"].email, "password": account["password"]},
    )
    assert login.status_code == 200
    return {"Authorization": f"Bearer {login.json()['data']['access_token']}"}


def _seed_stable_public_anchor(db_session) -> None:
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-06",
        stock_codes=["600519.SH", "000001.SZ"],
        pool_version=1,
    )
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-06",
    )


def _insert_bad_completed_batch(db_session, *, trade_date: str) -> None:
    insert_market_state_cache(db_session, trade_date=trade_date, market_state="BEAR")
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date,
        open_price=1800.0,
        high_price=1810.0,
        low_price=1790.0,
        close_price=1805.0,
    )
    task_id = str(uuid4())
    now = utc_now()
    db_session.execute(
        Base.metadata.tables["stock_pool_refresh_task"].insert().values(
            task_id=task_id,
            trade_date=date.fromisoformat(trade_date),
            status="COMPLETED",
            pool_version=2,
            fallback_from=None,
            filter_params_json={"target_pool_size": 200},
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
    # 3 core stocks, only 600519.SH has kline → 33% coverage < 80% threshold
    for idx, sc in enumerate(["600519.SH", "000001.SZ", "300750.SZ"], start=1):
        db_session.execute(
            Base.metadata.tables["stock_pool_snapshot"].insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=task_id,
                trade_date=date.fromisoformat(trade_date),
                pool_version=2,
                stock_code=sc,
                pool_role="core",
                rank_no=idx,
                score=99,
                is_suspended=False,
                created_at=now,
            )
        )
    db_session.commit()


def test_runtime_anchor_service_aligns_public_payloads(client, db_session, create_user):
    _seed_stable_public_anchor(db_session)
    admin_headers = _login_admin(client, create_user)

    service = RuntimeAnchorService(db_session)
    anchors = service.runtime_anchor_dates()
    runtime_status = service.public_runtime_status()
    versions = service.public_versions(window_days=30)

    home = client.get("/api/v1/home").json()["data"]
    dashboard = client.get("/api/v1/dashboard/stats?window_days=30").json()["data"]
    platform = client.get("/api/v1/platform/summary").json()["data"]
    admin = client.get("/api/v1/admin/system-status", headers=admin_headers).json()["data"]
    public_performance = get_public_performance_payload_ssot(
        db_session,
        window_days=30,
        runtime_anchor_service=service,
    )

    assert anchors["runtime_trade_date"] == "2026-03-06"
    assert runtime_status["trade_date"] == "2026-03-06"
    assert home["public_performance"]["runtime_trade_date"] == "2026-03-06"
    assert dashboard["runtime_trade_date"] == "2026-03-06"
    assert platform["runtime_trade_date"] == "2026-03-06"
    assert admin["public_runtime"]["runtime_trade_date"] == "2026-03-06"

    assert runtime_status["status_reason"] is None
    assert public_performance["status_reason"] == "stats_not_ready"
    assert home["public_performance"]["status_reason"] == "stats_not_ready"
    assert dashboard["status_reason"] == "stats_not_ready"
    assert platform["status_reason"] == "stats_not_ready"
    assert admin["public_runtime"]["status_reason"] == "stats_not_ready"
    assert admin["source_dates"]["runtime_trade_date"] == anchors["runtime_trade_date"]

    assert versions.public_runtime_version
    assert versions.public_snapshot_version


def test_home_route_cache_key_delegates_to_runtime_anchor_service(client, monkeypatch):
    import app.api.routes_business as routes_business

    state = {"version": "v1", "calls": 0}

    class _FakeRuntimeAnchorService:
        def __init__(self, db):
            self.db = db

        def home_cache_key(self, *, viewer_tier=None, viewer_role=None, window_days=30):
            return ("fake-home-cache", viewer_tier or "Free", (viewer_role or "").lower(), window_days, state["version"])

    def _fake_home_payload(db, *, viewer_tier=None, viewer_role=None, runtime_anchor_service=None):
        assert runtime_anchor_service is not None
        state["calls"] += 1
        return {
            "latest_reports": [],
            "hot_stocks": [],
            "market_state": "NEUTRAL",
            "trade_date": "2026-03-06",
            "pool_size": 0,
            "data_status": "READY",
            "status_reason": None,
            "display_reason": None,
            "today_report_count": 0,
            "public_performance": {
                "runtime_trade_date": "2026-03-06",
                "data_status": "READY",
                "status_reason": None,
                "display_hint": None,
            },
        }

    monkeypatch.setattr(routes_business, "RuntimeAnchorService", _FakeRuntimeAnchorService)
    monkeypatch.setattr(routes_business, "get_home_payload_ssot", _fake_home_payload)
    routes_business._home_cache.update({"data": None, "cache_key": None, "ts": 0.0})

    first = client.get("/api/v1/home")
    second = client.get("/api/v1/home")

    assert first.status_code == 200
    assert second.status_code == 200
    assert state["calls"] == 1

    state["version"] = "v2"
    third = client.get("/api/v1/home")

    assert third.status_code == 200
    assert state["calls"] == 2
    routes_business._home_cache.update({"data": None, "cache_key": None, "ts": 0.0})


def test_bad_completed_batch_isolated_before_public_anchor_and_versions_split(
    client,
    db_session,
    create_user,
):
    _seed_stable_public_anchor(db_session)
    service_before = RuntimeAnchorService(db_session)
    versions_before = service_before.public_versions(window_days=30)

    _insert_bad_completed_batch(db_session, trade_date="2026-03-07")
    latest_task = (
        db_session.query(StockPoolRefreshTask)
        .filter(StockPoolRefreshTask.trade_date == date(2026, 3, 7))
        .order_by(StockPoolRefreshTask.updated_at.desc(), StockPoolRefreshTask.created_at.desc())
        .first()
    )
    eligibility = evaluate_public_task_eligibility(db_session, latest_task)
    assert eligibility["eligible"] is False
    assert eligibility["reason"] == "KLINE_COVERAGE_INSUFFICIENT"

    service = RuntimeAnchorService(db_session)
    anchors = service.runtime_anchor_dates()
    runtime_status = service.public_runtime_status()
    versions_after_runtime = service.public_versions(window_days=30)

    assert anchors["runtime_trade_date"] == "2026-03-06"
    assert runtime_status["trade_date"] == "2026-03-06"
    assert runtime_status["attempted_trade_date"] == "2026-03-07"
    assert runtime_status["task_status"] == "COMPLETED"
    assert runtime_status["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert versions_after_runtime.public_runtime_version != versions_before.public_runtime_version
    assert versions_after_runtime.public_snapshot_version == versions_before.public_snapshot_version

    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date="2026-03-07",
        sample_size=30,
    )
    versions_after_snapshot = RuntimeAnchorService(db_session).public_versions(window_days=30)
    assert versions_after_snapshot.public_runtime_version == versions_after_runtime.public_runtime_version
    assert versions_after_snapshot.public_snapshot_version != versions_after_runtime.public_snapshot_version

    admin_headers = _login_admin(client, create_user)
    home = client.get("/api/v1/home").json()["data"]
    dashboard = client.get("/api/v1/dashboard/stats?window_days=30").json()["data"]
    platform = client.get("/api/v1/platform/summary").json()["data"]
    admin = client.get("/api/v1/admin/system-status", headers=admin_headers).json()["data"]

    assert home["public_performance"]["runtime_trade_date"] == "2026-03-06"
    assert dashboard["runtime_trade_date"] == "2026-03-06"
    assert platform["runtime_trade_date"] == "2026-03-06"
    assert admin["public_runtime"]["runtime_trade_date"] == "2026-03-06"
    assert admin["public_runtime"]["attempted_trade_date"] == "2026-03-07"
    assert admin["public_runtime"]["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"


def test_public_snapshot_version_changes_when_snapshot_rows_change_same_day(db_session):
    _seed_stable_public_anchor(db_session)
    for strategy_type in ("A", "B", "C"):
        insert_strategy_metric_snapshot(
            db_session,
            snapshot_date="2026-03-06",
            strategy_type=strategy_type,
            window_days=30,
            sample_size=30,
            coverage_pct=1.0,
            win_rate=0.6,
            profit_loss_ratio=1.8,
            alpha_annual=0.12,
            max_drawdown_pct=-0.08,
            cumulative_return_pct=0.2,
            signal_validity_warning=False,
        )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        baseline_type="baseline_random",
        window_days=30,
        sample_size=30,
        win_rate=0.55,
        profit_loss_ratio=1.5,
        alpha_annual=0.08,
        max_drawdown_pct=-0.09,
        cumulative_return_pct=0.1,
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-06",
        baseline_type="baseline_ma_cross",
        window_days=30,
        sample_size=30,
        win_rate=0.54,
        profit_loss_ratio=1.45,
        alpha_annual=0.07,
        max_drawdown_pct=-0.1,
        cumulative_return_pct=0.08,
    )
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date="2026-03-06",
        sample_size=30,
    )

    versions_before = RuntimeAnchorService(db_session).public_versions(window_days=30)
    strategy_table = Base.metadata.tables["strategy_metric_snapshot"]
    db_session.execute(
        strategy_table.update().where(
            strategy_table.c.snapshot_date == date(2026, 3, 6),
            strategy_table.c.window_days == 30,
            strategy_table.c.strategy_type == "B",
        ).values(
            win_rate=0.72,
            profit_loss_ratio=2.4,
            alpha_annual=0.18,
            cumulative_return_pct=0.42,
            created_at=utc_now(),
        )
    )
    db_session.commit()

    versions_after = RuntimeAnchorService(db_session).public_versions(window_days=30)
    assert versions_after.public_runtime_version == versions_before.public_runtime_version
    assert versions_after.public_snapshot_version != versions_before.public_snapshot_version


def test_home_market_state_stays_on_runtime_anchor_when_latest_attempt_is_bad(client, db_session):
    _seed_stable_public_anchor(db_session)
    _insert_bad_completed_batch(db_session, trade_date="2026-03-07")

    service = RuntimeAnchorService(db_session)
    runtime_market_row = service.runtime_market_state_row()
    home = client.get("/api/v1/home").json()["data"]

    assert service.runtime_trade_date() == "2026-03-06"
    assert runtime_market_row is not None
    assert str(runtime_market_row["trade_date"])[:10] == "2026-03-06"
    assert home["trade_date"] == "2026-03-06"
    assert home["market_state"] == "BULL"


def test_runtime_metrics_summary_keeps_public_runtime_reason_and_missing_pipeline_truth(db_session):
    _seed_stable_public_anchor(db_session)
    _insert_bad_completed_batch(db_session, trade_date="2026-03-07")

    metrics = runtime_metrics_summary(db_session)

    assert metrics["dashboard_30d"]["data_status"] == "DEGRADED"
    assert metrics["dashboard_30d"]["status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert metrics["settlement_pipeline"]["pipeline_status"] == "NOT_RUN"
    assert metrics["settlement_pipeline"]["pipeline_run_total"] == 0
    assert metrics["settlement_pipeline"]["matching_pipeline_run_total"] == 0
    assert "settlement_pipeline_not_completed" in metrics["data_quality"]["flags"]
    assert "public_runtime_degraded" in metrics["data_quality"]["flags"]
    assert "dashboard_stats_not_ready" not in metrics["data_quality"]["flags"]
    assert metrics["data_quality"]["dashboard_status_reason"] == "KLINE_COVERAGE_INSUFFICIENT"
    assert metrics["data_quality"]["pipeline_run_total"] == 0
    assert metrics["data_quality"]["matching_pipeline_run_total"] == 0


def test_main_py_stays_thin_for_report_flow():
    source = (REPO_ROOT / "app" / "main.py").read_text(encoding="utf-8")

    forbidden = [
        "async def _run_demo_generation",
        "def _demo_status_snapshot",
        "def _recent_demo_failure",
        "generate_report_ssot",
        "asyncio.create_task",
        "report_storage_mode",
        "get_latest_report_view_payload_ssot",
        "def _resolve_stock_code",
        "def _build_plain_fallback",
        "def _ensure_report_payload_for_view",
    ]
    for marker in forbidden:
        assert marker not in source

    required = [
        "build_report_template_context_for_user",
        "load_report_view_payload",
        "latest_report_id_for_code",
        "report_status_payload",
    ]
    for marker in required:
        assert marker in source


def test_repair_runtime_history_uses_runtime_anchor_service():
    source = (REPO_ROOT / "scripts" / "repair_runtime_history.py").read_text(encoding="utf-8")

    assert "RuntimeAnchorService" in source
    assert "app.services.ssot_read_model" not in source
