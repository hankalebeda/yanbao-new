from __future__ import annotations

import json
import subprocess
from pathlib import Path


def _paid_headers(client, create_user, *, email: str = "sim-paid@test.com") -> dict[str, str]:
    user = create_user(
        email=email,
        password="Password123",
        role="user",
        tier="Pro",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": user["user"].email, "password": user["password"]},
    )
    assert login.status_code == 200
    token = login.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {token}"}


def test_e2e_sim_positions_paid_user_access(client, create_user, seed_report_bundle):
    report = seed_report_bundle(stock_code="600519.SH")
    headers = _paid_headers(client, create_user)

    response = client.get("/api/v1/sim/positions?stock_code=600519.SH", headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    items = body["data"]["items"]
    assert any(item["report_id"] == report.report_id for item in items)


def test_e2e_sim_positions_by_report(client, create_user, seed_report_bundle):
    report = seed_report_bundle(stock_code="000001.SZ")
    headers = _paid_headers(client, create_user, email="sim-by-report@test.com")

    response = client.get(
        f"/api/v1/sim/positions/by-report/{report.report_id}",
        headers=headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["items"][0]["report_id"] == report.report_id


def test_e2e_sim_account_summary_fields(client, create_user):
    headers = _paid_headers(client, create_user, email="sim-summary@test.com")

    response = client.get("/api/v1/sim/account/summary?capital_tier=10k", headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    data = body["data"]
    assert "total_trades" in data
    assert "cold_start" in data
    assert "cold_start_message" in data
    assert "strategy_paused" in data


def test_e2e_sim_dashboard_html(client, create_user):
    headers = _paid_headers(client, create_user, email="sim-dashboard@test.com")

    response = client.get("/portfolio/sim-dashboard", headers=headers)

    assert response.status_code == 200
    assert "sim-dashboard" in response.text or "模拟收益" in response.text


def test_market_state_public(client):
    response = client.get("/api/v1/market/state")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["market_state"] in ("BULL", "NEUTRAL", "BEAR")


def test_portfolio_sim_dashboard_api(client, create_user):
    headers = _paid_headers(client, create_user, email="sim-api@test.com")

    response = client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k", headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["capital_tier"] == "100k"


def test_walkforward_backtest_script_runs(tmp_path):
    root = Path(__file__).resolve().parents[1]
    script = root / "scripts" / "walkforward_backtest.py"
    output_json = tmp_path / "legacy-walkforward.json"

    result = subprocess.run(
        [
            "python",
            str(script),
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
            "--stock-codes",
            "600000.SH",
            "--capital-tier",
            "10w",
            "--output-json",
            str(output_json),
        ],
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=90,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["records"] == []
    assert payload["stats"]["closed_count"] == 0
