from __future__ import annotations

import importlib
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.main import app

chatgpt_router = importlib.import_module("chatgpt_web.router")
deepseek_router = importlib.import_module("deepseek_web.router")
gemini_router = importlib.import_module("gemini_web.router")
qwen_router = importlib.import_module("qwen_web.router")
webai_router = importlib.import_module("webai.router")


def _make_fake_client(provider: str):
    class _FakeClient:
        _instance: "_FakeClient | None" = None

        def __init__(self) -> None:
            self._ready = True

        @classmethod
        async def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        async def analyze(self, prompt: str, timeout_ms: int = 120_000) -> dict[str, Any]:
            return {
                "response": f"{provider}:{prompt}",
                "elapsed_s": 0.01,
                "has_citation": False,
            }

        async def analyze_batch(self, items: list[dict[str, str]], timeout_ms: int = 120_000) -> list[dict[str, Any]]:
            out = []
            for item in items:
                out.append(
                    {
                        "code": item.get("code", ""),
                        "name": item.get("name", ""),
                        "response": f"{provider}:{item.get('prompt', '')}",
                        "elapsed_s": 0.01,
                        "has_citation": False,
                    }
                )
            return out

        async def close(self) -> None:
            self._ready = False
            self.__class__._instance = None

    return _FakeClient


@pytest.fixture()
def client_with_mocks(monkeypatch: pytest.MonkeyPatch):
    fake_chatgpt = _make_fake_client("chatgpt")
    fake_deepseek = _make_fake_client("deepseek")
    fake_gemini = _make_fake_client("gemini")
    fake_qwen = _make_fake_client("qwen")

    monkeypatch.setattr(chatgpt_router, "ChatGPTWebClient", fake_chatgpt)
    monkeypatch.setattr(deepseek_router, "DeepSeekWebClient", fake_deepseek)
    monkeypatch.setattr(gemini_router, "GeminiWebClient", fake_gemini)
    monkeypatch.setattr(qwen_router, "QwenWebClient", fake_qwen)

    monkeypatch.setattr(
        webai_router,
        "_PROVIDER_CLIENTS",
        {
            "chatgpt": fake_chatgpt,
            "deepseek": fake_deepseek,
            "gemini": fake_gemini,
            "qwen": fake_qwen,
        },
    )
    return TestClient(app, base_url="http://127.0.0.1")


@pytest.mark.parametrize(
    ("provider", "base_path"),
    [
        ("chatgpt", "/api/v1/chatgpt"),
        ("deepseek", "/api/v1/deepseek"),
        ("gemini", "/api/v1/gemini"),
        ("qwen", "/api/v1/qwen"),
    ],
)
def test_legacy_provider_routes_contract(client_with_mocks: TestClient, provider: str, base_path: str):
    single = client_with_mocks.post(
        f"{base_path}/analyze",
        json={"prompt": "ping", "timeout_s": 120},
    )
    assert single.status_code == 200
    body = single.json()
    assert body["code"] == 0
    assert "request_id" in body
    assert body["data"]["response"].startswith(f"{provider}:")
    assert "elapsed_s" in body["data"]
    assert "has_citation" in body["data"]

    batch = client_with_mocks.post(
        f"{base_path}/analyze/batch",
        json={
            "timeout_s": 120,
            "stocks": [
                {"code": "600519", "name": "A", "prompt": "p1"},
                {"code": "000858", "name": "B", "prompt": "p2"},
            ],
        },
    )
    assert batch.status_code == 200
    b = batch.json()
    assert b["code"] == 0
    assert b["data"]["count"] == 2
    assert len(b["data"]["results"]) == 2

    status = client_with_mocks.get(f"{base_path}/session/status")
    assert status.status_code == 200
    s = status.json()
    assert s["code"] == 0
    assert s["data"]["initialized"] is True
    assert s["data"]["ready"] is True

    close = client_with_mocks.delete(f"{base_path}/session")
    assert close.status_code == 200
    c = close.json()
    assert c["code"] == 0
    assert c["data"]["closed"] is True


def test_webai_unified_contract(client_with_mocks: TestClient):
    providers_resp = client_with_mocks.get("/api/v1/webai/providers")
    assert providers_resp.status_code == 200
    providers_body = providers_resp.json()
    assert providers_body["code"] == 0
    assert set(providers_body["data"]["providers"]) == {"chatgpt", "deepseek", "gemini", "qwen"}

    for provider in ["chatgpt", "deepseek", "gemini", "qwen"]:
        single = client_with_mocks.post(
            "/api/v1/webai/analyze",
            json={"provider": provider, "prompt": "hello", "timeout_s": 120},
        )
        assert single.status_code == 200
        body = single.json()
        assert body["code"] == 0
        assert body["data"]["provider"] == provider
        assert body["data"]["response"].startswith(f"{provider}:")

        batch = client_with_mocks.post(
            "/api/v1/webai/analyze/batch",
            json={
                "provider": provider,
                "timeout_s": 120,
                "stocks": [
                    {"code": "600519", "name": "A", "prompt": "p1"},
                    {"code": "000858", "name": "B", "prompt": "p2"},
                ],
            },
        )
        assert batch.status_code == 200
        b = batch.json()
        assert b["code"] == 0
        assert b["data"]["provider"] == provider
        assert b["data"]["count"] == 2
        assert len(b["data"]["results"]) == 2

        status_one = client_with_mocks.get(f"/api/v1/webai/session/status/{provider}")
        assert status_one.status_code == 200
        one = status_one.json()
        assert one["code"] == 0
        assert one["data"]["provider"] == provider

    status_all = client_with_mocks.get("/api/v1/webai/session/status")
    assert status_all.status_code == 200
    all_body = status_all.json()
    assert all_body["code"] == 0
    assert set(all_body["data"].keys()) == {"chatgpt", "deepseek", "gemini", "qwen"}

    close_all = client_with_mocks.delete("/api/v1/webai/session")
    assert close_all.status_code == 200
    close_body = close_all.json()
    assert close_body["code"] == 0
    assert close_body["data"]["closed"] is True
