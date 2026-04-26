def test_nfr12_health_status_ok(client):
    response = client.get("/health", headers={"X-Request-ID": "req-health-ok"})

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-health-ok"
    assert response.headers["X-Request-ID"] == "req-health-ok"
    assert body["data"]["status"] in ("ok", "degraded", "unconfigured")
    assert "database_status" in body["data"]
    assert "scheduler_status" in body["data"]
    assert "llm_router_status" in body["data"]
    assert "checked_at" in body["data"]


def test_nfr12_health_status_degraded_when_scheduler_not_running(client, monkeypatch):
    """健康检查必须聚合 scheduler 状态，而不是只看数据库。"""
    import app.main as app_main
    import app.services.scheduler as scheduler_mod

    class _StoppedScheduler:
        running = False

    monkeypatch.setattr(app_main.settings, "enable_scheduler", True)
    monkeypatch.setattr(scheduler_mod, "scheduler", _StoppedScheduler())
    response = client.get("/health", headers={"X-Request-ID": "req-health-degraded"})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-health-degraded"
    assert body["data"]["scheduler_status"] == "degraded"
    assert body["data"]["status"] == "degraded"


def test_nfr12_health_status_reports_router_llm_ok_when_codex_pool_available(client, monkeypatch):
    import app.services.llm_router as llm_router_mod
    from app.services import codex_client

    monkeypatch.setattr(llm_router_mod.settings, "llm_backend", "router")
    monkeypatch.setattr(llm_router_mod.settings, "router_primary", "codex_api")
    monkeypatch.setattr(llm_router_mod.settings, "router_longctx", "codex_api")
    monkeypatch.setattr(llm_router_mod.settings, "router_bulk", "ollama")
    monkeypatch.setattr(
        codex_client,
        "discover_codex_provider_specs",
        lambda root=None: [
            codex_client.CodexProviderSpec(
                provider_name="relay-a",
                base_url="https://relay-a.example.com/v1",
                api_key="sk-relay-a",
                model="gpt-5.4",
            )
        ],
    )

    response = client.get("/health", headers={"X-Request-ID": "req-health-router-ok"})

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-health-router-ok"
    assert body["data"]["llm_router_status"] == "ok"


def test_nfr14_response_envelope_success(client, db_session, create_user, seed_report_bundle):
    """成功响应应包含标准 envelope: success=true, data!=null, request_id。"""
    create_user(role="user")
    seed_report_bundle()

    response = client.get("/api/v1/home", headers={"X-Request-ID": "req-envelope-success"})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-envelope-success"
    assert body["data"] is not None
    assert "error_code" not in body or body.get("error_code") is None


def test_nfr14_response_envelope_error(client):
    response = client.get("/auth/me", headers={"X-Request-ID": "req-auth-me-unauthorized"})

    assert response.status_code == 401
    body = response.json()
    assert body["success"] is False
    assert body["request_id"] == "req-auth-me-unauthorized"
    assert response.headers["X-Request-ID"] == "req-auth-me-unauthorized"
    assert body["error_code"] == "UNAUTHORIZED"
    assert body["error_message"] == "UNAUTHORIZED"
    assert body["data"] is None
