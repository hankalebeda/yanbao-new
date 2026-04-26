from __future__ import annotations

import json

import pytest

from app.services.codex_client import CodexProviderSpec
from scripts import probe_codex_gateway


def test_resolve_probe_targets_prefers_direct_settings(monkeypatch):
    monkeypatch.setattr(probe_codex_gateway.settings, "codex_api_base_url", "https://gateway.example.com/v1")
    monkeypatch.setattr(probe_codex_gateway.settings, "codex_api_key", "sk-direct")
    monkeypatch.setattr(probe_codex_gateway.settings, "codex_api_model", "gpt-5.4")
    monkeypatch.setattr(
        probe_codex_gateway,
        "discover_codex_provider_specs",
        lambda root=None: [
            CodexProviderSpec(
                provider_name="backup",
                base_url="https://backup.example.com/v1",
                api_key="sk-backup",
                model="gpt-5.2",
            )
        ],
    )

    providers = probe_codex_gateway.resolve_probe_targets()

    assert [provider.provider_name for provider in providers] == ["settings-direct", "backup"]


def test_probe_provider_reports_model_count(monkeypatch):
    provider = CodexProviderSpec(
        provider_name="primary",
        base_url="https://gateway.example.com/v1",
        api_key="sk-test",
        model="gpt-5.4",
    )

    class ModelsResponse:
        status_code = 200
        headers = {"content-type": "application/json"}

        def json(self):
            return {"data": [{"id": "gpt-5.4"}, {"id": "gpt-5.2"}]}

    class CompletionResponse:
        status_code = 200
        headers = {"content-type": "application/json"}

        def json(self):
            return {"choices": [{"message": {"role": "assistant", "content": "OK"}}], "usage": {"completion_tokens": 1}}

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers):
            assert url == "https://gateway.example.com/v1/models"
            assert headers["Authorization"] == "Bearer sk-test"
            return ModelsResponse()

        def post(self, url, headers, json):
            assert url == "https://gateway.example.com/v1/responses"
            return CompletionResponse()

    monkeypatch.setattr(probe_codex_gateway.httpx, "Client", DummyClient)

    result = probe_codex_gateway.probe_provider(provider, timeout_seconds=3.0)

    assert result["ok"] is True
    assert result["model_count"] == 2
    assert result["completion_probe"]["ok"] is True


def test_probe_provider_marks_models_ok_but_completion_empty_as_failure(monkeypatch):
    provider = CodexProviderSpec(
        provider_name="primary",
        base_url="https://gateway.example.com/v1",
        api_key="sk-test",
        model="gpt-5.4",
    )

    class ModelsResponse:
        status_code = 200
        headers = {"content-type": "application/json"}

        def json(self):
            return {"data": [{"id": "gpt-5.4"}]}

    class CompletionResponse:
        status_code = 200
        headers = {"content-type": "application/json"}
        text = ""

        def json(self):
            return {"output": [], "usage": {"completion_tokens": 0}}

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers):
            return ModelsResponse()

        def post(self, url, headers, json):
            assert url == "https://gateway.example.com/v1/responses"
            return CompletionResponse()

    monkeypatch.setattr(probe_codex_gateway.httpx, "Client", DummyClient)

    result = probe_codex_gateway.probe_provider(provider, timeout_seconds=3.0)

    assert result["models_ok"] is True
    assert result["completion_probe"]["ok"] is False
    assert result["ok"] is False


def test_main_returns_nonzero_when_all_targets_fail(monkeypatch, capsys):
    provider = CodexProviderSpec(
        provider_name="primary",
        base_url="https://gateway.example.com/v1",
        api_key="sk-test",
        model="gpt-5.4",
    )
    monkeypatch.setattr(probe_codex_gateway, "resolve_probe_targets", lambda **kwargs: [provider])
    monkeypatch.setattr(
        probe_codex_gateway,
        "probe_provider",
        lambda provider, timeout_seconds: {
            "provider_name": provider.provider_name,
            "base_url": provider.base_url,
            "ok": False,
            "status_code": 503,
        },
    )
    monkeypatch.setattr(
        probe_codex_gateway,
        "_parse_args",
        lambda: type("Args", (), {"provider_root": None, "timeout_seconds": 1.0, "provider": None})(),
    )

    exit_code = probe_codex_gateway.main()
    captured = capsys.readouterr().out.strip()
    payload = json.loads(captured)

    assert exit_code == 1
    assert payload["ok"] is False
    assert payload["failures"][0]["status_code"] == 503


def test_main_returns_two_when_no_targets(monkeypatch, capsys):
    monkeypatch.setattr(probe_codex_gateway, "resolve_probe_targets", lambda **kwargs: [])
    monkeypatch.setattr(
        probe_codex_gateway,
        "_parse_args",
        lambda: type("Args", (), {"provider_root": None, "timeout_seconds": 1.0, "provider": None})(),
    )

    exit_code = probe_codex_gateway.main()
    captured = capsys.readouterr().out.strip()
    payload = json.loads(captured)

    assert exit_code == 2
    assert payload["error"] == "no_probe_targets"


def test_probe_provider_bubbles_timeout(monkeypatch):
    provider = CodexProviderSpec(
        provider_name="primary",
        base_url="https://gateway.example.com/v1",
        api_key="sk-test",
        model="gpt-5.4",
    )

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers):
            raise TimeoutError("timed out")

    monkeypatch.setattr(probe_codex_gateway.httpx, "Client", DummyClient)

    with pytest.raises(TimeoutError):
        probe_codex_gateway.probe_provider(provider, timeout_seconds=3.0)
