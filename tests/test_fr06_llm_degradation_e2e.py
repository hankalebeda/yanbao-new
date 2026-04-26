"""FR06-LLM-05: LLM 降级链 E2E 测试（逐级降级验证）
SSOT: 01 §FR06-LLM-05, 03 §FR-06

验证：primary→backup→cli→local→failed 降级链真实触发，非 mock_llm 路径。
"""
from __future__ import annotations

import json

import pytest


def _ollama_resp(recommendation: str = "HOLD") -> dict:
    return {
        "response": json.dumps({
            "recommendation": recommendation,
            "confidence": 0.65,
            "conclusion_text": "ollama fallback conclusion text for testing, length ok",
            "reasoning_chain_md": "## Reasoning\nollama fallback chain fired after primary fail",
        }),
        "source": "ollama",
        "model": "ollama",
        "elapsed_s": 0.5,
        "usage": {},
    }


def _codex_resp(recommendation: str = "BUY", pool_level: str = "primary") -> dict:
    return {
        "response": json.dumps({
            "recommendation": recommendation,
            "confidence": 0.72,
            "conclusion_text": "codex primary conclusion text for testing, length ok",
            "reasoning_chain_md": "## Reasoning\ncodex primary chain used successfully",
        }),
        "source": "codex_api",
        "pool_level": pool_level,
        "model": "codex_api",
        "elapsed_s": 0.4,
        "usage": {},
    }


def _run_generation(**kwargs):
    """Helper to call run_generation_model with defaults."""
    from app.services.report_generation_ssot import run_generation_model
    defaults = {
        "stock_code": "600519.SH",
        "stock_name": "贵州茅台",
        "strategy_type": "B",
        "market_state": "BULL",
        "quality_flag": "ok",
        "prior_stats": None,
        "signal_entry_price": 1688.0,
        "used_data": [],
        "kline_row": {"close": 1688.0, "ma5": 1680.0, "ma20": 1650.0, "atr_pct": 0.02, "volatility_20d": 0.03},
    }
    defaults.update(kwargs)
    return run_generation_model(**defaults)


@pytest.mark.feature("FR06-LLM-05")
class TestLLMDegradationChainE2E:
    def test_primary_fail_triggers_ollama_local_fallback(self, monkeypatch):
        """codex_api 失败 → ollama 接管 → llm_fallback_level='local'。"""
        from app.services import llm_router
        from app.core.config import settings

        # Reset global circuit breaker before test
        llm_router._global_llm_circuit_breaker.record_success()

        async def fake_primary_fail(model_name, prompt, temperature, use_cot, **kwargs):
            raise RuntimeError("Primary LLM unavailable for testing")

        async def fake_ollama_ok(prompt, temperature, **kwargs):
            return _ollama_resp("BUY")

        monkeypatch.setattr(settings, "mock_llm", False)
        monkeypatch.setattr(settings, "max_llm_retries", 0)
        monkeypatch.setattr(llm_router, "_call_model", fake_primary_fail)
        monkeypatch.setattr(llm_router, "_call_ollama", fake_ollama_ok)

        data = _run_generation()

        assert data["llm_fallback_level"] == "local"
        assert data["recommendation"] in {"BUY", "HOLD", "SELL"}
        assert len(data["conclusion_text"]) >= 10

    def test_all_models_fail_returns_level_failed(self, monkeypatch):
        """所有模型失败 → llm_fallback_level='failed'，规则兜底。"""
        from app.services import llm_router
        from app.core.config import settings

        # Reset global circuit breaker before test
        llm_router._global_llm_circuit_breaker.record_success()

        async def fake_all_fail(model_name, prompt, temperature, use_cot, **kwargs):
            raise RuntimeError(f"{model_name} unavailable for testing")

        async def fake_ollama_fail(prompt, temperature, **kwargs):
            raise RuntimeError("Ollama unavailable for testing")

        monkeypatch.setattr(settings, "mock_llm", False)
        monkeypatch.setattr(settings, "max_llm_retries", 0)
        monkeypatch.setattr(llm_router, "_call_model", fake_all_fail)
        monkeypatch.setattr(llm_router, "_call_ollama", fake_ollama_fail)

        data = _run_generation(strategy_type="A", market_state="NEUTRAL")

        assert data["llm_fallback_level"] == "failed"
        assert data["recommendation"] in {"BUY", "HOLD", "SELL"}

    def test_codex_pool_backup_mapped_to_backup_level(self, monkeypatch):
        """codex_api pool_level=backup → llm_fallback_level='backup'（池内降级）。"""
        from app.services import llm_router
        from app.core.config import settings

        llm_router._global_llm_circuit_breaker.record_success()

        async def fake_codex_pool_backup(model_name, prompt, temperature, use_cot, **kwargs):
            return _codex_resp("HOLD", pool_level="backup")

        monkeypatch.setattr(settings, "mock_llm", False)
        monkeypatch.setattr(settings, "llm_backend", "router")
        monkeypatch.setattr(llm_router, "_call_model", fake_codex_pool_backup)

        data = _run_generation(strategy_type="C", market_state="BEAR", quality_flag="degraded")

        assert data["llm_fallback_level"] == "backup"
        assert data["recommendation"] in {"BUY", "HOLD", "SELL"}
        assert len(data["conclusion_text"]) >= 10

    def test_codex_pool_primary_mapped_to_primary_level(self, monkeypatch):
        """codex_api pool_level=primary → llm_fallback_level='primary'（正常路径）。"""
        from app.services import llm_router
        from app.core.config import settings

        llm_router._global_llm_circuit_breaker.record_success()

        async def fake_codex_primary(model_name, prompt, temperature, use_cot, **kwargs):
            return _codex_resp("BUY", pool_level="primary")

        monkeypatch.setattr(settings, "mock_llm", False)
        monkeypatch.setattr(settings, "llm_backend", "router")
        monkeypatch.setattr(llm_router, "_call_model", fake_codex_primary)

        data = _run_generation(strategy_type="A", market_state="BULL")

        assert data["llm_fallback_level"] == "primary"
        assert data["recommendation"] in {"BUY", "HOLD", "SELL"}

    def test_fallback_level_tracked_in_response_fields(self, monkeypatch):
        """降级后 response 包含 llm_fallback_level 字段（不为 None）。"""
        from app.services import llm_router
        from app.core.config import settings

        llm_router._global_llm_circuit_breaker.record_success()

        async def fake_primary_fail(model_name, prompt, temperature, use_cot, **kwargs):
            raise RuntimeError("Primary fail for field check")

        async def fake_ollama_ok(prompt, temperature, **kwargs):
            return _ollama_resp("HOLD")

        monkeypatch.setattr(settings, "mock_llm", False)
        monkeypatch.setattr(settings, "max_llm_retries", 0)
        monkeypatch.setattr(llm_router, "_call_model", fake_primary_fail)
        monkeypatch.setattr(llm_router, "_call_ollama", fake_ollama_ok)

        data = _run_generation()

        assert "llm_fallback_level" in data
        assert data["llm_fallback_level"] is not None
        assert data["llm_fallback_level"] in {"primary", "backup", "cli", "local", "failed"}
