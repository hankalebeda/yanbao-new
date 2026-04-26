from app.core.config import settings


def test_internal_auth_rejects_legacy_internal_api_key_for_read_routes(client, monkeypatch):
    monkeypatch.setattr(settings, "internal_cron_token", "")
    monkeypatch.setattr(settings, "internal_api_key", "legacy-key")

    response = client.get(
        "/api/v1/internal/llm/version",
        headers={"X-Internal-Token": "legacy-key"},
    )

    assert response.status_code == 401
    assert response.json()["error_code"] == "UNAUTHORIZED"


def test_internal_auth_accepts_internal_cron_token_for_read_routes(client, monkeypatch):
    monkeypatch.setattr(settings, "internal_cron_token", "cron-token")
    monkeypatch.setattr(settings, "internal_api_key", "legacy-key")

    response = client.get(
        "/api/v1/internal/llm/version",
        headers={"X-Internal-Token": "cron-token"},
    )

    assert response.status_code == 200
    assert set(response.json()["data"].keys()) >= {"test_model", "prod_model"}


def test_internal_auth_accepts_control_plane_internal_token_alias_for_read_routes(client, monkeypatch):
    monkeypatch.setattr(settings, "internal_cron_token", "legacy-cron-token")
    monkeypatch.setattr(settings, "internal_api_key", "legacy-key")
    monkeypatch.setenv("INTERNAL_TOKEN", "canonical-control-plane-token")
    monkeypatch.delenv("INTERNAL_TOKEN_ALIASES", raising=False)

    response = client.get(
        "/api/v1/internal/llm/version",
        headers={"X-Internal-Token": "canonical-control-plane-token"},
    )

    assert response.status_code == 200
    assert set(response.json()["data"].keys()) >= {"test_model", "prod_model"}


def test_internal_llm_generate_route_is_retired(client, monkeypatch):
    monkeypatch.setattr(settings, "internal_cron_token", "cron-token")
    monkeypatch.setattr(settings, "internal_api_key", "legacy-key")

    response = client.post(
        "/api/v1/internal/llm/generate",
        headers={"X-Internal-Token": "cron-token"},
        json={"prompt": "hello", "use_prod_model": False},
    )

    assert response.status_code == 410
    assert response.json()["error_code"] == "ROUTE_RETIRED"
