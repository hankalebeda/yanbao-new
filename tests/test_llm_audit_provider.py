from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx

from app.core.config import Settings, settings
from app.services import codex_client
from app.services import llm_router


def test_debug_release_normalized_false():
    cfg = Settings(debug="release")
    assert cfg.debug is False


def test_audit_disabled_returns_skip_reason(monkeypatch):
    monkeypatch.setattr(settings, "llm_audit_enabled", False)
    result = asyncio.run(
        llm_router.run_audit_and_aggregate(
            main_vote="BUY",
            base_confidence=0.72,
            report_summary="test summary",
            timeout_sec=0,
        )
    )
    assert result["skip_reason"] == "audit_disabled"


def test_audit_prefers_codex_provider(monkeypatch):
    call_order: list[str] = []

    async def fake_call_model(model_name: str, prompt: str, temperature: float, use_cot: bool):
        call_order.append(model_name)
        return {"response": "vote=BUY severity=low", "source": model_name}

    monkeypatch.setattr(settings, "llm_audit_enabled", True)
    monkeypatch.setattr(settings, "llm_audit_provider", "codex_api")
    monkeypatch.setattr(settings, "llm_audit_fallback_chain", "ollama")
    monkeypatch.setattr(llm_router, "_call_model", fake_call_model)

    result = asyncio.run(
        llm_router.run_audit_and_aggregate(
            main_vote="BUY",
            base_confidence=0.72,
            report_summary="test summary",
            timeout_sec=0,
        )
    )

    assert call_order[:2] == ["codex_api", "ollama"]
    assert result["skip_reason"] is None
    assert result["audit_flag"] != "audit_skipped"


def test_codex_parallel_race_returns_fastest_primary_provider(monkeypatch):
    providers = [
        codex_client.CodexProviderSpec(
            provider_name="primary-a",
            base_url="https://primary-a.example.com/v1",
            api_key="sk-a",
            model="gpt-5.4",
        ),
        codex_client.CodexProviderSpec(
            provider_name="primary-b",
            base_url="https://primary-b.example.com/v1",
            api_key="sk-b",
            model="gpt-5.4",
        ),
        codex_client.CodexProviderSpec(
            provider_name="backup-a",
            base_url="https://backup.example.com/v1",
            api_key="sk-backup",
            model="gpt-5.2",
        ),
    ]

    class DummyClient:
        def __init__(self, provider_specs):
            self.providers = provider_specs
            self.healthy: list[str] = []
            self.failed: list[str] = []

        def _provider_order(self):
            return list(self.providers)

        def _provider_bucket(self, provider):
            return 0 if provider.model == "gpt-5.4" else 1

        def _mark_provider_healthy(self, provider):
            self.healthy.append(provider.provider_name)

        def _mark_provider_failed(self, provider):
            self.failed.append(provider.provider_name)

    dummy_client = DummyClient(providers)

    async def fake_single_provider(provider, prompt, temperature, **kwargs):
        assert prompt == "parallel prompt"
        if provider.provider_name == "primary-a":
            await asyncio.sleep(0.05)
            return {"response": "slow", "source": "codex_api", "pool_level": "primary"}
        if provider.provider_name == "primary-b":
            await asyncio.sleep(0.01)
            return {
                "response": "fast",
                "source": "codex_api",
                "pool_level": "primary",
                "provider_name": provider.provider_name,
            }
        raise AssertionError("backup provider should not join the primary race")

    monkeypatch.setattr(settings, "codex_api_parallel_enabled", True)
    monkeypatch.setattr(settings, "codex_api_parallel_max_providers", 2)
    monkeypatch.setattr(codex_client, "get_codex_client", lambda: dummy_client)
    monkeypatch.setattr(llm_router, "_call_codex_single_provider", fake_single_provider)

    result = asyncio.run(llm_router._call_codex_api_parallel("parallel prompt", 0.2))

    assert result is not None
    assert result["response"] == "fast"
    assert result["parallel_mode"] == "race"
    assert result["parallel_attempted_providers"] == ["primary-a", "primary-b"]
    assert result["parallel_winner_provider"] == "primary-b"
    assert dummy_client.healthy == ["primary-b"]
    assert dummy_client.failed == []


def test_shutdown_codex_client_tolerates_closed_event_loop(monkeypatch):
    class DummyClient:
        async def close(self):
            raise RuntimeError("Event loop is closed")

    monkeypatch.setattr(codex_client, "_client", DummyClient())
    asyncio.run(codex_client.shutdown_codex_client())
    assert codex_client._client is None


def _write_codex_provider(
    root: Path,
    provider_dir_name: str,
    *,
    model: str,
    review_model: str | None = None,
    fallback_models: list[str] | None = None,
    base_url: str,
    api_key: str,
    enabled: bool = True,
    reasoning_effort: str = "high",
) -> Path:
    provider_dir = root / provider_dir_name
    provider_dir.mkdir(parents=True, exist_ok=True)
    (provider_dir / "provider.json").write_text(
        json.dumps(
            {
                "name": provider_dir_name,
                "endpoint": base_url,
                "model": model,
                "review_model": review_model or model,
                "fallback_models": fallback_models or [],
                "enabled": enabled,
                "resource": "provider",
                "app": "codex",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (provider_dir / "auth.json").write_text(
        json.dumps({"OPENAI_API_KEY": api_key}, ensure_ascii=False),
        encoding="utf-8",
    )
    (provider_dir / "config.toml").write_text(
        "\n".join(
            [
                f'model = "{model}"',
                f'review_model = "{review_model or model}"',
                f'model_reasoning_effort = "{reasoning_effort}"',
                "",
                "[model_providers.OpenAI]",
                f'base_url = "{base_url}"',
                'wire_api = "responses"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return provider_dir


def test_discover_codex_provider_specs_orders_primary_before_backup(tmp_path, monkeypatch):
    _write_codex_provider(
        tmp_path,
        "zz-gpt52",
        model="gpt-5.2",
        base_url="https://backup.example.com/v1",
        api_key="sk-backup",
    )
    _write_codex_provider(
        tmp_path,
        "aa-gpt54",
        model="gpt-5.4",
        base_url="https://primary.example.com/v1",
        api_key="sk-primary",
    )
    _write_codex_provider(
        tmp_path,
        "bb-gpt54",
        model="gpt-5.4",
        base_url="https://primary2.example.com/v1",
        api_key="sk-primary-2",
    )
    (tmp_path / "portable_shadow").mkdir()

    monkeypatch.setattr(settings, "codex_provider_root", str(tmp_path))
    monkeypatch.setattr(settings, "codex_api_model", "gpt-5.4")
    monkeypatch.setattr(settings, "codex_api_fallback_model", "gpt-5.2")

    providers = codex_client.discover_codex_provider_specs()
    assert [provider.provider_name for provider in providers] == ["aa-gpt54", "bb-gpt54", "zz-gpt52"]
    assert providers[0].review_model == "gpt-5.4"
    assert providers[0].fallback_model == "gpt-5.2"


def test_discover_codex_provider_specs_reads_fallback_models_from_provider_metadata(tmp_path, monkeypatch):
    _write_codex_provider(
        tmp_path,
        "newapi-192.168.232.141-3000",
        model="gpt-5.4",
        review_model="gpt-5.3-codex",
        fallback_models=["gpt-5.4", "gpt-5.3-codex", "gpt-5.2"],
        base_url="http://192.168.232.141:3000/v1",
        api_key="sk-gateway",
    )

    monkeypatch.setattr(settings, "codex_provider_root", str(tmp_path))
    monkeypatch.setattr(settings, "codex_api_model", "")
    monkeypatch.setattr(settings, "codex_api_fallback_model", "gpt-5.2")
    monkeypatch.setattr(settings, "codex_api_base_url", "")
    monkeypatch.setattr(settings, "codex_api_key", "")

    providers = codex_client.discover_codex_provider_specs()

    assert len(providers) == 1
    assert providers[0].review_model == "gpt-5.3-codex"
    assert providers[0].fallback_models == ("gpt-5.4", "gpt-5.3-codex", "gpt-5.2")


def test_discover_codex_provider_specs_prefers_key_txt_api_key_over_auth_json(tmp_path, monkeypatch):
    _write_codex_provider(
        tmp_path,
        "newapi-192.168.232.141-3000",
        model="gpt-5.4",
        base_url="http://192.168.232.141:3000/v1",
        api_key="sk-stale-auth",
    )
    (tmp_path / "newapi-192.168.232.141-3000" / "key.txt").write_text(
        "http://192.168.232.141:3000/v1\nsk-live-keytxt\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(settings, "codex_provider_root", str(tmp_path))
    monkeypatch.setattr(settings, "codex_api_model", "")
    monkeypatch.setattr(settings, "codex_api_fallback_model", "")
    monkeypatch.setattr(settings, "codex_api_base_url", "")
    monkeypatch.setattr(settings, "codex_api_key", "")

    providers = codex_client.discover_codex_provider_specs()

    assert len(providers) == 1
    assert providers[0].provider_name == "newapi-192.168.232.141-3000"
    assert providers[0].api_key == "sk-live-keytxt"


def test_discover_codex_provider_specs_prefers_config_base_url_over_provider_json(tmp_path, monkeypatch):
    provider_dir = _write_codex_provider(
        tmp_path,
        "newapi-192.168.232.141-3000",
        model="gpt-5.4",
        base_url="http://192.168.232.141:3000/v1",
        api_key="sk-gateway",
    )
    (provider_dir / "provider.json").write_text(
        json.dumps(
            {
                "name": "newapi-192.168.232.141-3000",
                "endpoint": "https://ai.qaq.al/v1",
                "model": "gpt-5.4",
                "enabled": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(settings, "codex_provider_root", str(tmp_path))
    monkeypatch.setattr(settings, "codex_api_model", "")
    monkeypatch.setattr(settings, "codex_api_fallback_model", "")
    monkeypatch.setattr(settings, "codex_api_base_url", "")
    monkeypatch.setattr(settings, "codex_api_key", "")

    providers = codex_client.discover_codex_provider_specs()

    assert len(providers) == 1
    assert providers[0].base_url == "http://192.168.232.141:3000/v1"


def test_codex_client_settings_fallback_appended_after_discovered_provider_pool(monkeypatch):
    monkeypatch.setattr(settings, "codex_provider_root", "unused")
    monkeypatch.setattr(settings, "codex_api_base_url", "https://gateway.example.com/v1")
    monkeypatch.setattr(settings, "codex_api_key", "sk-gateway")
    monkeypatch.setattr(settings, "codex_api_model", "gpt-5.4")
    monkeypatch.setattr(settings, "codex_api_fallback_model", "gpt-5.2")
    monkeypatch.setattr(
        codex_client,
        "discover_codex_provider_specs",
        lambda root=None: [
            codex_client.CodexProviderSpec(
                provider_name="local-relay",
                base_url="https://relay-a.example.com/v1",
                api_key="sk-relay",
                model="gpt-5.4",
            ),
            codex_client.CodexProviderSpec(
                provider_name="gateway-shadow",
                base_url="https://gateway.example.com/v1",
                api_key="sk-shadow",
                model="gpt-5.4",
            ),
            codex_client.CodexProviderSpec(
                provider_name="backup-relay",
                base_url="https://relay-b.example.com/v1",
                api_key="sk-backup",
                model="gpt-5.2",
            ),
        ],
    )

    client = codex_client.CodexAPIClient()

    # v20: discovered cliproxy-style providers are tried first; settings_fallback
    # (historically pointing at a legacy/dead gateway) is appended last.
    assert [provider.provider_name for provider in client.providers] == [
        "local-relay",
        "backup-relay",
        "settings_fallback",
    ]
    assert client.providers[-1].api_key == "sk-gateway"


def test_get_primary_status_router_reports_ok_when_codex_pool_available(monkeypatch):
    monkeypatch.setattr(settings, "llm_backend", "router")
    monkeypatch.setattr(settings, "router_primary", "codex_api")
    monkeypatch.setattr(settings, "router_longctx", "codex_api")
    monkeypatch.setattr(settings, "router_bulk", "ollama")
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

    assert llm_router.get_primary_status() == "ok"


def test_get_primary_status_router_reports_degraded_when_primary_missing_but_ollama_available(monkeypatch):
    monkeypatch.setattr(settings, "llm_backend", "router")
    monkeypatch.setattr(settings, "router_primary", "codex_api")
    monkeypatch.setattr(settings, "router_longctx", "codex_api")
    monkeypatch.setattr(settings, "router_bulk", "ollama")
    monkeypatch.setattr(settings, "ollama_base_url", "http://127.0.0.1:11434")
    # 重置 direct API 配置，确保走 provider discovery 路径（而非 has_direct=True 短路）
    monkeypatch.setattr(settings, "codex_api_base_url", "")
    monkeypatch.setattr(settings, "codex_api_key", "")
    monkeypatch.setattr(codex_client, "discover_codex_provider_specs", lambda root=None: [])

    assert llm_router.get_primary_status() == "degraded"


def test_codex_client_fails_over_within_codex_provider_pool():
    class FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, object], text: str = ""):
            self.status_code = status_code
            self._payload = payload
            self.text = text or json.dumps(payload, ensure_ascii=False)
            self.request = httpx.Request("POST", "https://example.com/v1/responses")

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("boom", request=self.request, response=self)

    class FakeClient:
        def __init__(self, responses: list[FakeResponse]):
            self.responses = list(responses)
            self.calls: list[dict[str, object]] = []
            self.is_closed = False

        async def post(self, url: str, json: dict[str, object]):
            self.calls.append({"url": url, "json": json})
            return self.responses.pop(0)

        async def aclose(self):
            self.is_closed = True

    primary = codex_client.CodexProviderSpec(
        provider_name="primary-54",
        base_url="https://primary.example.com/v1",
        api_key="sk-primary",
        model="gpt-5.4",
    )
    backup = codex_client.CodexProviderSpec(
        provider_name="backup-52",
        base_url="https://backup.example.com/v1",
        api_key="sk-backup",
        model="gpt-5.2",
    )
    client = codex_client.CodexAPIClient(provider_specs=[primary, backup])
    fake_clients = {
        "primary-54": FakeClient([FakeResponse(500, {"error": "upstream failed"})]),
        "backup-52": FakeClient(
            [
                FakeResponse(
                    200,
                    {
                        "model": "gpt-5.2",
                        "output": [
                            {
                                "type": "message",
                                "content": [{"type": "output_text", "text": '{"ok": true}'}],
                            }
                        ],
                        "usage": {"input_tokens": 10, "output_tokens": 20},
                    },
                )
            ]
        ),
    }
    client._get_client = lambda provider: fake_clients[provider.provider_name]  # type: ignore[method-assign]

    result = asyncio.run(client.analyze("hello"))

    assert result["provider_name"] == "backup-52"
    assert result["pool_level"] == "backup"
    assert fake_clients["primary-54"].calls
    assert fake_clients["backup-52"].calls


def test_codex_client_rotates_same_tier_providers_across_requests():
    class FakeResponse:
        def __init__(self, provider_name: str):
            self.status_code = 200
            self._payload = {
                "model": "gpt-5.4",
                "output_text": json.dumps({"provider": provider_name}, ensure_ascii=False),
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }
            self.text = json.dumps(self._payload, ensure_ascii=False)
            self.request = httpx.Request("POST", "https://example.com/v1/responses")

        def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, provider_name: str):
            self.provider_name = provider_name
            self.calls: list[dict[str, object]] = []
            self.is_closed = False

        async def post(self, url: str, json: dict[str, object]):
            self.calls.append({"url": url, "json": json})
            return FakeResponse(self.provider_name)

        async def aclose(self):
            self.is_closed = True

    provider_a = codex_client.CodexProviderSpec(
        provider_name="primary-a",
        base_url="https://primary-a.example.com/v1",
        api_key="sk-primary-a",
        model="gpt-5.4",
    )
    provider_b = codex_client.CodexProviderSpec(
        provider_name="primary-b",
        base_url="https://primary-b.example.com/v1",
        api_key="sk-primary-b",
        model="gpt-5.4",
    )
    provider_c = codex_client.CodexProviderSpec(
        provider_name="backup-c",
        base_url="https://backup-c.example.com/v1",
        api_key="sk-backup-c",
        model="gpt-5.2",
    )
    client = codex_client.CodexAPIClient(provider_specs=[provider_a, provider_b, provider_c])
    fake_clients = {
        "primary-a": FakeClient("primary-a"),
        "primary-b": FakeClient("primary-b"),
        "backup-c": FakeClient("backup-c"),
    }
    client._get_client = lambda provider: fake_clients[provider.provider_name]  # type: ignore[method-assign]

    first = asyncio.run(client.analyze("hello"))
    second = asyncio.run(client.analyze("hello again"))

    assert first["provider_name"] == "primary-a"
    assert second["provider_name"] == "primary-b"
    assert len(fake_clients["primary-a"].calls) == 1
    assert len(fake_clients["primary-b"].calls) == 1
    assert not fake_clients["backup-c"].calls


def test_codex_client_cools_down_failed_provider_and_uses_next_site(monkeypatch):
    class FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, object], text: str = ""):
            self.status_code = status_code
            self._payload = payload
            self.text = text or json.dumps(payload, ensure_ascii=False)
            self.request = httpx.Request("POST", "https://example.com/v1/responses")

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("boom", request=self.request, response=self)

    class FakeClient:
        def __init__(self, responses: list[FakeResponse]):
            self.responses = list(responses)
            self.calls: list[dict[str, object]] = []
            self.is_closed = False

        async def post(self, url: str, json: dict[str, object]):
            self.calls.append({"url": url, "json": json})
            if not self.responses:
                raise AssertionError("unexpected extra request")
            return self.responses.pop(0)

        async def aclose(self):
            self.is_closed = True

    monkeypatch.setattr(settings, "codex_provider_failure_cooldown_seconds", 300)

    provider_a = codex_client.CodexProviderSpec(
        provider_name="primary-a",
        base_url="https://primary-a.example.com/v1",
        api_key="sk-primary-a",
        model="gpt-5.4",
    )
    provider_b = codex_client.CodexProviderSpec(
        provider_name="primary-b",
        base_url="https://primary-b.example.com/v1",
        api_key="sk-primary-b",
        model="gpt-5.4",
    )
    client = codex_client.CodexAPIClient(provider_specs=[provider_a, provider_b])
    fake_clients = {
        "primary-a": FakeClient([FakeResponse(500, {"error": "upstream failed"})]),
        "primary-b": FakeClient(
            [
                FakeResponse(200, {"model": "gpt-5.4", "output_text": '{"ok": 1}', "usage": {}}),
                FakeResponse(200, {"model": "gpt-5.4", "output_text": '{"ok": 2}', "usage": {}}),
            ]
        ),
    }
    client._get_client = lambda provider: fake_clients[provider.provider_name]  # type: ignore[method-assign]

    first = asyncio.run(client.analyze("hello"))
    client._provider_cursor_by_priority[0] = 0
    second = asyncio.run(client.analyze("hello again"))

    assert first["provider_name"] == "primary-b"
    assert second["provider_name"] == "primary-b"
    assert len(fake_clients["primary-a"].calls) == 1
    assert len(fake_clients["primary-b"].calls) == 2


def test_codex_client_tries_second_backup_relay_before_leaving_codex_pool():
    class FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, object], text: str = ""):
            self.status_code = status_code
            self._payload = payload
            self.text = text or json.dumps(payload, ensure_ascii=False)
            self.request = httpx.Request("POST", "https://example.com/v1/responses")

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("boom", request=self.request, response=self)

    class FakeClient:
        def __init__(self, responses: list[FakeResponse]):
            self.responses = list(responses)
            self.calls: list[dict[str, object]] = []
            self.is_closed = False

        async def post(self, url: str, json: dict[str, object]):
            self.calls.append({"url": url, "json": json})
            if not self.responses:
                raise AssertionError("unexpected extra request")
            return self.responses.pop(0)

        async def aclose(self):
            self.is_closed = True

    primary = codex_client.CodexProviderSpec(
        provider_name="primary-54",
        base_url="https://primary.example.com/v1",
        api_key="sk-primary",
        model="gpt-5.4",
    )
    backup_a = codex_client.CodexProviderSpec(
        provider_name="backup-a-52",
        base_url="https://backup-a.example.com/v1",
        api_key="sk-backup-a",
        model="gpt-5.2",
    )
    backup_b = codex_client.CodexProviderSpec(
        provider_name="backup-b-52",
        base_url="https://backup-b.example.com/v1",
        api_key="sk-backup-b",
        model="gpt-5.2",
    )
    client = codex_client.CodexAPIClient(provider_specs=[primary, backup_a, backup_b])
    fake_clients = {
        "primary-54": FakeClient([FakeResponse(500, {"error": "primary failed"})]),
        "backup-a-52": FakeClient([FakeResponse(500, {"error": "backup a failed"})]),
        "backup-b-52": FakeClient(
            [
                FakeResponse(
                    200,
                    {
                        "model": "gpt-5.2-2025-12-11",
                        "output_text": '{"ok": true, "relay": "backup-b-52"}',
                        "usage": {"input_tokens": 4, "output_tokens": 6},
                    },
                )
            ]
        ),
    }
    client._get_client = lambda provider: fake_clients[provider.provider_name]  # type: ignore[method-assign]

    result = asyncio.run(client.analyze("hello"))

    assert result["provider_name"] == "backup-b-52"
    assert result["pool_level"] == "backup"
    assert fake_clients["primary-54"].calls
    assert fake_clients["backup-a-52"].calls
    assert fake_clients["backup-b-52"].calls


def test_codex_client_uses_xhigh_then_falls_back_to_high_on_provider_rejection():
    class FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, object], text: str = ""):
            self.status_code = status_code
            self._payload = payload
            self.text = text or json.dumps(payload, ensure_ascii=False)
            self.request = httpx.Request("POST", "https://example.com/v1/responses")

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("boom", request=self.request, response=self)

    class FakeClient:
        def __init__(self, responses: list[FakeResponse]):
            self.responses = list(responses)
            self.calls: list[dict[str, object]] = []
            self.is_closed = False

        async def post(self, url: str, json: dict[str, object]):
            self.calls.append({"url": url, "json": json})
            return self.responses.pop(0)

        async def aclose(self):
            self.is_closed = True

    provider = codex_client.CodexProviderSpec(
        provider_name="primary-54",
        base_url="https://primary.example.com/v1",
        api_key="sk-primary",
        model="gpt-5.4",
        reasoning_effort="high",
    )
    client = codex_client.CodexAPIClient(provider_specs=[provider])
    fake_client = FakeClient(
        [
            FakeResponse(400, {"error": "unsupported reasoning"}),
            FakeResponse(
                200,
                {
                    "model": "gpt-5.4",
                    "output_text": '{"ok": true}',
                    "usage": {"input_tokens": 8, "output_tokens": 9},
                },
            ),
        ]
    )
    client._get_client = lambda _: fake_client  # type: ignore[method-assign]

    result = asyncio.run(client.analyze("hello", temperature=0.1))

    assert [call["json"]["reasoning"]["effort"] for call in fake_client.calls] == ["xhigh", "high"]
    assert all("temperature" not in call["json"] for call in fake_client.calls)
    assert result["pool_level"] == "primary"
    assert result["reasoning_effort"] == "high"


def test_codex_client_tries_gpt52_within_same_provider_after_gpt54_failure():
    class FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, object], text: str = ""):
            self.status_code = status_code
            self._payload = payload
            self.text = text or json.dumps(payload, ensure_ascii=False)
            self.request = httpx.Request("POST", "https://example.com/v1/responses")

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("boom", request=self.request, response=self)

    class FakeClient:
        def __init__(self, responses: list[FakeResponse]):
            self.responses = list(responses)
            self.calls: list[dict[str, object]] = []
            self.is_closed = False

        async def post(self, url: str, json: dict[str, object]):
            self.calls.append({"url": url, "json": json})
            return self.responses.pop(0)

        async def aclose(self):
            self.is_closed = True

    provider = codex_client.CodexProviderSpec(
        provider_name="relay-a",
        base_url="https://relay-a.example.com/v1",
        api_key="sk-relay-a",
        model="gpt-5.4",
        fallback_model="gpt-5.2",
        reasoning_effort="xhigh",
    )
    client = codex_client.CodexAPIClient(provider_specs=[provider])
    fake_client = FakeClient(
        [
            FakeResponse(504, {"error": "gateway timeout"}),
            FakeResponse(
                200,
                {
                    "model": "gpt-5.2-2025-12-11",
                    "output_text": '{"ok": true, "relay": "relay-a"}',
                    "usage": {"input_tokens": 6, "output_tokens": 9},
                },
            ),
        ]
    )
    client._get_client = lambda _: fake_client  # type: ignore[method-assign]

    result = asyncio.run(client.analyze("hello"))

    assert [call["json"]["model"] for call in fake_client.calls] == ["gpt-5.4", "gpt-5.2"]
    assert result["provider_name"] == "relay-a"
    assert result["pool_level"] == "backup"


def test_codex_client_tries_review_model_before_final_backup():
    class FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, object], text: str = ""):
            self.status_code = status_code
            self._payload = payload
            self.text = text or json.dumps(payload, ensure_ascii=False)
            self.request = httpx.Request("POST", "https://example.com/v1/responses")

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("boom", request=self.request, response=self)

    class FakeClient:
        def __init__(self, responses: list[FakeResponse]):
            self.responses = list(responses)
            self.calls: list[dict[str, object]] = []
            self.is_closed = False

        async def post(self, url: str, json: dict[str, object]):
            self.calls.append({"url": url, "json": json})
            return self.responses.pop(0)

        async def aclose(self):
            self.is_closed = True

    provider = codex_client.CodexProviderSpec(
        provider_name="relay-a",
        base_url="https://relay-a.example.com/v1",
        api_key="sk-relay-a",
        model="gpt-5.4",
        review_model="gpt-5.3-codex",
        fallback_model="gpt-5.2",
        fallback_models=("gpt-5.4", "gpt-5.3-codex", "gpt-5.2"),
        reasoning_effort="xhigh",
    )
    client = codex_client.CodexAPIClient(provider_specs=[provider])
    fake_client = FakeClient(
        [
            FakeResponse(504, {"error": "primary timeout"}),
            FakeResponse(503, {"error": "review unavailable"}),
            FakeResponse(
                200,
                {
                    "model": "gpt-5.2-2025-12-11",
                    "output_text": '{"ok": true, "relay": "relay-a"}',
                    "usage": {"input_tokens": 6, "output_tokens": 9},
                },
            ),
        ]
    )
    client._get_client = lambda _: fake_client  # type: ignore[method-assign]

    result = asyncio.run(client.analyze("hello"))

    assert [call["json"]["model"] for call in fake_client.calls] == ["gpt-5.4", "gpt-5.3-codex", "gpt-5.2"]
    assert result["provider_name"] == "relay-a"
    assert result["pool_level"] == "backup"


def test_codex_client_marks_review_model_result_as_backup():
    class FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, object], text: str = ""):
            self.status_code = status_code
            self._payload = payload
            self.text = text or json.dumps(payload, ensure_ascii=False)
            self.request = httpx.Request("POST", "https://example.com/v1/responses")

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("boom", request=self.request, response=self)

    class FakeClient:
        def __init__(self, responses: list[FakeResponse]):
            self.responses = list(responses)
            self.calls: list[dict[str, object]] = []
            self.is_closed = False

        async def post(self, url: str, json: dict[str, object]):
            self.calls.append({"url": url, "json": json})
            return self.responses.pop(0)

        async def aclose(self):
            self.is_closed = True

    provider = codex_client.CodexProviderSpec(
        provider_name="relay-a",
        base_url="https://relay-a.example.com/v1",
        api_key="sk-relay-a",
        model="gpt-5.4",
        review_model="gpt-5.3-codex",
        fallback_model="gpt-5.2",
        fallback_models=("gpt-5.4", "gpt-5.3-codex", "gpt-5.2"),
        reasoning_effort="xhigh",
    )
    client = codex_client.CodexAPIClient(provider_specs=[provider])
    fake_client = FakeClient(
        [
            FakeResponse(504, {"error": "primary timeout"}),
            FakeResponse(
                200,
                {
                    "model": "gpt-5.3-codex-2026-03-01",
                    "output_text": '{"ok": true, "relay": "relay-a"}',
                    "usage": {"input_tokens": 6, "output_tokens": 9},
                },
            ),
        ]
    )
    client._get_client = lambda _: fake_client  # type: ignore[method-assign]

    result = asyncio.run(client.analyze("hello"))

    assert [call["json"]["model"] for call in fake_client.calls] == ["gpt-5.4", "gpt-5.3-codex"]
    assert result["pool_level"] == "backup"


def test_codex_client_retries_next_model_after_decode_failure(caplog):
    class FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, object] | None, text: str = ""):
            self.status_code = status_code
            self._payload = payload
            self.text = text
            self.request = httpx.Request("POST", "https://example.com/v1/responses")

        def json(self):
            if self._payload is None:
                raise json.JSONDecodeError("Expecting value", self.text or "", 0)
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("boom", request=self.request, response=self)

    class FakeClient:
        def __init__(self, responses: list[FakeResponse]):
            self.responses = list(responses)
            self.calls: list[dict[str, object]] = []
            self.is_closed = False

        async def post(self, url: str, json: dict[str, object]):
            self.calls.append({"url": url, "json": json})
            return self.responses.pop(0)

        async def aclose(self):
            self.is_closed = True

    provider = codex_client.CodexProviderSpec(
        provider_name="relay-a",
        base_url="https://relay-a.example.com/v1",
        api_key="sk-relay-a",
        model="gpt-5.4",
        fallback_model="gpt-5.2",
        reasoning_effort="xhigh",
    )
    client = codex_client.CodexAPIClient(provider_specs=[provider])
    fake_client = FakeClient(
        [
            FakeResponse(200, None, text="<html>gateway up</html>"),
            FakeResponse(
                200,
                {
                    "model": "gpt-5.2-2025-12-11",
                    "output_text": '{"ok": true, "relay": "relay-a"}',
                    "usage": {"input_tokens": 6, "output_tokens": 9},
                },
            ),
        ]
    )
    client._get_client = lambda _: fake_client  # type: ignore[method-assign]

    with caplog.at_level("WARNING"):
        result = asyncio.run(client.analyze("hello"))

    assert [call["json"]["model"] for call in fake_client.calls] == ["gpt-5.4", "gpt-5.2"]
    assert "responses decode failed" in caplog.text
    assert "<html>gateway up</html>" in caplog.text
    assert result["pool_level"] == "backup"
