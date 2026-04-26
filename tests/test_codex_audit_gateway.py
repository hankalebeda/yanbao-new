from __future__ import annotations

import json
from pathlib import Path

from app.core.config import settings
from app.services import codex_client
from scripts import codex_mesh


def _write_provider(root: Path, provider_name: str, *, api_key: str = "sk-test") -> None:
    provider_dir = root / provider_name
    provider_dir.mkdir(parents=True, exist_ok=True)
    (provider_dir / "provider.json").write_text(
        json.dumps(
            {
                "name": provider_name,
                "endpoint": f"https://{provider_name}/v1",
                "model": "gpt-5.4",
                "enabled": True,
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
                'model = "gpt-5.4"',
                'review_model = "gpt-5.2"',
                '[model_providers.OpenAI]',
                f'base_url = "https://{provider_name}/v1"',
                'wire_api = "responses"',
            ]
        ),
        encoding="utf-8",
    )


def test_discover_audit_codex_provider_specs_filters_to_canonical(tmp_path, monkeypatch):
    _write_provider(tmp_path, "newapi-192.168.232.141-3000")
    _write_provider(tmp_path, "ai.qaq.al")

    monkeypatch.setattr(settings, "codex_provider_root", str(tmp_path))
    monkeypatch.setattr(settings, "codex_api_base_url", "")
    monkeypatch.setattr(settings, "codex_api_key", "")
    monkeypatch.setattr(settings, "codex_audit_gateway_only", True)
    monkeypatch.setattr(settings, "codex_canonical_provider", "newapi-192.168.232.141-3000")

    providers = codex_client.discover_audit_codex_provider_specs()

    assert [provider.provider_name for provider in providers] == ["newapi-192.168.232.141-3000"]


def test_resolve_provider_allowlist_honors_gateway_only_env(monkeypatch):
    monkeypatch.setenv("CODEX_AUDIT_GATEWAY_ONLY", "true")
    monkeypatch.setenv("CODEX_CANONICAL_PROVIDER", "newapi-192.168.232.141-3000")
    monkeypatch.delenv("CODEX_STABLE_LANE", raising=False)
    monkeypatch.delenv("CODEX_READONLY_LANE", raising=False)
    monkeypatch.delenv("CODEX_READONLY_PROVIDER_ALLOWLIST", raising=False)

    assert codex_mesh.resolve_provider_allowlist() == ["newapi-192.168.232.141-3000"]
