"""NFR-03: P0 contract alignment tests."""


def test_nfr03_contract_alignment_generate(client, db_session, create_user, seed_report_bundle):
    user = create_user(email="nfr03gen@example.com", role="user")
    seed_report_bundle(stock_code="600519.SH")

    login_resp = client.post(
        "/auth/login",
        json={"email": "nfr03gen@example.com", "password": "Password123"},
    )
    token = login_resp.json()["data"]["access_token"]

    resp = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    data = body.get("data", body)
    assert "report_id" in data
    assert "stock_code" in data
    assert "recommendation" in data
    assert "confidence" in data


def test_nfr03_contract_alignment_report_detail(client, db_session, create_user, seed_report_bundle):
    user = create_user(email="nfr03detail@example.com", role="user")
    report = seed_report_bundle(stock_code="600519.SH")

    login_resp = client.post(
        "/auth/login",
        json={"email": "nfr03detail@example.com", "password": "Password123"},
    )
    token = login_resp.json()["data"]["access_token"]

    resp = client.get(
        f"/api/v1/reports/{report.report_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    data = body.get("data", body)

    required_fields = [
        "report_id",
        "stock_code",
        "trade_date",
        "recommendation",
        "confidence",
        "strategy_type",
        "conclusion_text",
    ]
    for field in required_fields:
        assert field in data, f"report detail missing P0 field: {field}"


def test_nfr03_contract_alignment_sim_dashboard(client, db_session, create_user, seed_report_bundle):
    user = create_user(email="nfr03sim@example.com", role="user", tier="Pro")
    seed_report_bundle(stock_code="600519.SH")

    login_resp = client.post(
        "/auth/login",
        json={"email": "nfr03sim@example.com", "password": "Password123"},
    )
    token = login_resp.json()["data"]["access_token"]

    resp = client.get(
        "/api/v1/portfolio/sim-dashboard",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    data = body.get("data", body)

    assert isinstance(data, dict)
    assert body.get("success") is True
