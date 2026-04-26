from fastapi import Request
from fastapi.testclient import TestClient

from app.api import routes_sim
from app.main import app
from app.models import User


async def _mock_require_sim_access(_request: Request) -> User:
    return User(
        user_id="ssot-read-test-user",
        email="ssot-read@test.local",
        password_hash="",
        membership_level="monthly",
        role="user",
    )


client = TestClient(app, base_url="http://127.0.0.1")


def test_reports_get_prefers_ssot_payload(monkeypatch):
    payload = {
        "report_id": "report-ssot-1",
        "stock_code": "600519.SH",
        "trade_date": "2026-04-07",
        "recommendation": "BUY",
        "confidence": 0.88,
    }
    monkeypatch.setattr("app.api.routes_business.get_report_api_payload_ssot", lambda db, report_id, **kwargs: payload)

    response = client.get("/api/v1/reports/report-ssot-1")

    assert response.status_code == 200
    assert response.json()["data"]["report_id"] == "report-ssot-1"
    assert response.json()["data"]["stock_code"] == "600519.SH"


def test_reports_list_prefers_ssot_read_model(monkeypatch):
    monkeypatch.setattr(
        "app.api.routes_business.list_report_summaries_ssot",
        lambda db, **kwargs: {
            "items": [{"report_id": "report-ssot-list-1", "stock_code": "600519.SH", "source": "real"}],
            "page": 1,
            "page_size": 1,
            "total": 1,
            "data_status": "READY",
            "status_reason": None,
            "degraded_banner": None,
        },
    )

    response = client.get("/api/v1/reports?page=1&page_size=10&position_status=OPEN")

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["total"] == 1
    assert body["items"][0]["report_id"] == "report-ssot-list-1"


def test_sim_positions_uses_ssot_read_model(monkeypatch):
    app.dependency_overrides[routes_sim._require_sim_access] = _mock_require_sim_access
    monkeypatch.setattr(
        "app.services.ssot_read_model.list_sim_positions_ssot",
        lambda db, **kwargs: {
            "items": [{"position_id": "pos-ssot-1", "status": "OPEN", "stock_code": "600519.SH"}],
            "page": 1,
            "page_size": 1,
            "total": 1,
        },
    )

    try:
        response = client.get("/api/v1/sim/positions")
    finally:
        app.dependency_overrides.pop(routes_sim._require_sim_access, None)

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["total"] == 1
    assert body["items"][0]["position_id"] == "pos-ssot-1"


def test_sim_position_detail_uses_ssot_read_model(monkeypatch):
    app.dependency_overrides[routes_sim._require_sim_access] = _mock_require_sim_access
    monkeypatch.setattr(
        "app.services.ssot_read_model.get_sim_position_ssot",
        lambda db, position_id: {"position_id": position_id, "status": "OPEN", "stock_code": "600519.SH"},
    )

    try:
        response = client.get("/api/v1/sim/positions/pos-ssot-2")
    finally:
        app.dependency_overrides.pop(routes_sim._require_sim_access, None)

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["position_id"] == "pos-ssot-2"
    assert body["stock_code"] == "600519.SH"


def test_platform_summary_uses_ssot_summary(monkeypatch):
    monkeypatch.setattr(
        "app.services.ssot_read_model.get_public_performance_payload_ssot",
        lambda db, **kwargs: {
            "overall_win_rate": 0.61,
            "overall_profit_loss_ratio": 1.8,
            "alpha_vs_baseline": 0.12,
            "total_settled": 42,
            "date_range": {"from": "2026-03-01", "to": "2026-04-07"},
            "data_status": "READY",
            "status_reason": None,
            "display_hint": None,
            "runtime_trade_date": "2026-04-07",
            "snapshot_date": "2026-04-07",
            "baseline_random": None,
            "baseline_ma_cross": None,
        },
    )

    response = client.get("/api/v1/platform/summary")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["win_rate"] == 0.61
    assert data["pnl_ratio"] == 1.8
    assert data["alpha"] == 0.12
    assert data["total_trades"] == 42
