from __future__ import annotations


def _assert_success_envelope(body: dict) -> None:
    assert body["success"] is True
    assert "request_id" in body
    assert "data" in body


def _login(client, email: str, password: str) -> dict[str, str]:
    resp = client.post("/auth/login", json={"email": email, "password": password})
    assert resp.status_code == 200
    return {"Authorization": f"Bearer {resp.json()['data']['access_token']}"}


def test_platform_summary_public(client):
    response = client.get("/api/v1/platform/summary")
    assert response.status_code == 200
    body = response.json()
    _assert_success_envelope(body)
    data = body["data"]
    assert "win_rate" in data
    assert "pnl_ratio" in data
    assert "alpha" in data
    assert "total_trades" in data


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    _assert_success_envelope(body)
    assert response.headers.get("X-Content-Type-Options") == "nosniff"
    assert response.headers.get("X-Frame-Options") == "DENY"


def test_page_routes_return_200(client):
    for path in ["/", "/reports", "/dashboard", "/login", "/register", "/forgot-password", "/subscribe"]:
        response = client.get(path)
        assert response.status_code == 200, f"GET {path} expected 200, got {response.status_code}"
        assert "<html" in response.text.lower() or "html" in response.headers.get("content-type", "")


def test_auth_register_succeeds(client):
    import uuid

    email = f"legacy-reg-{uuid.uuid4().hex[:10]}@example.com"
    response = client.post("/auth/register", json={"email": email, "password": "Test1234"})
    assert response.status_code in (200, 201)
    body = response.json()
    _assert_success_envelope(body)
    assert body["data"]["email"] == email
    assert body["data"]["tier"] == "Free"


def test_reports_list_and_filters(client, seed_report_bundle):
    report = seed_report_bundle(stock_name="Legacy%Report")

    response = client.get("/api/v1/reports?page=1&page_size=5")
    assert response.status_code == 200
    body = response.json()
    _assert_success_envelope(body)
    assert body["data"]["page"] == 1
    assert body["data"]["page_size"] == 5
    assert body["data"]["items"][0]["report_id"] == report.report_id

    q_response = client.get("/api/v1/reports?q=Legacy%25Report")
    assert q_response.status_code == 200
    assert q_response.json()["data"]["total"] == 1

    stock_name_response = client.get("/api/v1/reports?stock_name=Legacy%25Report")
    assert stock_name_response.status_code == 200
    assert stock_name_response.json()["data"]["total"] == 1


def test_report_detail_masks_and_pro_access(client, create_user, seed_report_bundle):
    report = seed_report_bundle()

    anonymous = client.get(f"/api/v1/reports/{report.report_id}")
    assert anonymous.status_code == 200
    anon_body = anonymous.json()
    _assert_success_envelope(anon_body)
    masked = anon_body["data"]["instruction_card"]["signal_entry_price"]
    assert masked != 123.45
    assert str(masked).endswith("**.**")

    user = create_user(
        email="legacy-pro@test.com",
        password="Password123",
        tier="Pro",
        email_verified=True,
    )
    headers = _login(client, user["user"].email, user["password"])
    pro_response = client.get(f"/api/v1/reports/{report.report_id}", headers=headers)
    assert pro_response.status_code == 200
    pro_body = pro_response.json()
    _assert_success_envelope(pro_body)
    assert pro_body["data"]["instruction_card"]["signal_entry_price"] == 123.45
    assert pro_body["data"]["sim_trade_instruction"]["100k"]["status"] == "EXECUTE"


def test_internal_routes_require_token(client, internal_headers):
    unauthorized = client.get("/api/v1/internal/llm/version")
    assert unauthorized.status_code == 401
    assert unauthorized.json()["error_code"] == "INTERNAL_UNAUTHORIZED"

    version = client.get(
        "/api/v1/internal/llm/version",
        headers=internal_headers("legacy-internal"),
    )
    assert version.status_code == 200
    _assert_success_envelope(version.json())

    fallback_status = client.get(
        "/api/v1/internal/source/fallback-status",
        headers=internal_headers("legacy-internal"),
    )
    assert fallback_status.status_code == 200
    _assert_success_envelope(fallback_status.json())


def test_internal_clear_endpoints(client, internal_headers):
    reports_clear = client.post(
        "/api/v1/internal/reports/clear",
        headers=internal_headers("legacy-clear"),
    )
    assert reports_clear.status_code == 200
    _assert_success_envelope(reports_clear.json())

    stats_clear = client.post(
        "/api/v1/internal/stats/clear",
        headers=internal_headers("legacy-clear"),
    )
    assert stats_clear.status_code == 200
    _assert_success_envelope(stats_clear.json())


def test_internal_llm_health(client, internal_headers, monkeypatch):
    from app.api import routes_internal

    async def fake_health():
        return {"status": "degraded", "tags": [], "latency_ms": 7, "error": "mock upstream unavailable"}

    monkeypatch.setattr(routes_internal.ollama_client, "health", fake_health)
    response = client.get(
        "/api/v1/internal/llm/health",
        headers=internal_headers("legacy-llm-health"),
    )
    assert response.status_code == 200
    body = response.json()
    _assert_success_envelope(body)
    assert body["degraded"] is True
    assert body["data"]["status"] == "degraded"


def test_not_found_envelope(client):
    response = client.get("/api/v1/reports/not_exists")
    assert response.status_code == 404
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "REPORT_NOT_AVAILABLE"


def test_request_id_roundtrip_header(client):
    req_id = "legacy-rid-001"
    response = client.get("/health", headers={"X-Request-ID": req_id})
    assert response.status_code == 200
    assert response.headers.get("X-Request-ID") == req_id
    assert response.json()["request_id"] == req_id


def test_demo_report_invalid_stock_code(client):
    response = client.get("/demo/report/ABC?cached_only=true")
    assert response.status_code == 404
