"""FR06-LLM-05: LLM 降级链 E2E 测试（逐级降级验证）
SSOT: 01 §FR06-LLM-05, 03 §FR-06

验证：primary→backup→cli→local→failed 降级链真实触发，非 mock_llm 路径。
"""
from __future__ import annotations

import json

import pytest


def _grounded_llm_payload(*, recommendation: str, strategy_type: str) -> dict:
    strategy_type = str(strategy_type or "B").upper()
    strategy_details = {
        "A": {
            "signal": "事件催化延续，热点强度82分，MA5=1680.0继续站上MA20=1650.0",
            "validation": "事件与公告催化已核对，热点持续性和回撤风险均已复核。",
            "keyword": "事件催化",
        },
        "B": {
            "signal": "均线趋势保持多头，MA5=1680.0高于MA20=1650.0，5日涨幅6.8%",
            "validation": "趋势延续与均线结构已复核，未见明显空头破坏信号。",
            "keyword": "趋势均线",
        },
        "C": {
            "signal": "atr=2.0%，20日波动率=3.0%，低波震荡区间保持稳定",
            "validation": "低波结构与波动率分位已复核，震荡突破条件暂未失真。",
            "keyword": "低波波动率",
        },
    }
    detail = strategy_details[strategy_type]
    conclusion_text = (
        f"{detail['keyword']}判断支持本次{recommendation}结论：MA5=1680.0、MA20=1650.0、信号价1688.0与ATR=2.0%共同说明当前结构仍可解释。"
        f"结合20日波动率3.0%与价格回撤容忍度，模型认为关键证据并未失效；"
        f"{detail['signal']}，因此需要在结论中继续保留风险控制与执行纪律，避免脱离数值事实做空泛判断。"
    )
    reasoning_chain_md = "\n".join(
        [
            "## 技术面分析",
            f"围绕信号价1688.0、MA5=1680.0、MA20=1650.0展开，确认价格结构与均线/atr数值一致，{detail['signal']}。",
            "## 资金面分析",
            "当前测试样本未额外提供资金流字段，但仍基于价格与波动率数值说明仓位纪律，避免将缺失数据伪装为正向资金证据。",
            "## 多空矛盾判断",
            f"若短线回撤接近ATR=2.0%阈值，则需要重新审视{detail['keyword']}是否失效；目前1680.0与1650.0之间仍保留足够缓冲。",
            "## 风险因素",
            "主要风险包括事件兑现不足、趋势被跌破、或低波结构被放量打破，因此必须继续关注1688.0附近执行价与2.0%波动阈值。",
            "## 综合结论",
            f"综合上述五步，{detail['validation']} 结论保持{recommendation}，且 grounding 直接绑定 1688.0、1680.0、1650.0、2.0%、3.0% 五个数值。",
        ]
    )
    return {
        "recommendation": recommendation,
        "confidence": 0.72 if strategy_type != "C" else 0.68,
        "conclusion_text": conclusion_text,
        "reasoning_chain_md": reasoning_chain_md,
        "strategy_specific_evidence": {
            "strategy_type": strategy_type,
            "key_signal": detail["signal"],
            "validation_check": detail["validation"],
        },
    }


def _ollama_resp(recommendation: str = "HOLD", strategy_type: str = "B") -> dict:
    return {
        "response": json.dumps(_grounded_llm_payload(recommendation=recommendation, strategy_type=strategy_type), ensure_ascii=False),
        "source": "ollama",
        "model": "ollama",
        "elapsed_s": 0.5,
        "usage": {},
    }


def _codex_resp(
    recommendation: str = "BUY",
    pool_level: str = "primary",
    strategy_type: str = "B",
) -> dict:
    return {
        "response": json.dumps(
            _grounded_llm_payload(recommendation=recommendation, strategy_type=strategy_type),
            ensure_ascii=False,
        ),
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
            return _ollama_resp("BUY", strategy_type="B")

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
            return _codex_resp("HOLD", pool_level="backup", strategy_type="C")

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
            return _codex_resp("BUY", pool_level="primary", strategy_type="A")

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
            return _ollama_resp("HOLD", strategy_type="B")

        monkeypatch.setattr(settings, "mock_llm", False)
        monkeypatch.setattr(settings, "max_llm_retries", 0)
        monkeypatch.setattr(llm_router, "_call_model", fake_primary_fail)
        monkeypatch.setattr(llm_router, "_call_ollama", fake_ollama_ok)

        data = _run_generation()

        assert "llm_fallback_level" in data
        assert data["llm_fallback_level"] is not None
        assert data["llm_fallback_level"] in {"primary", "backup", "cli", "local", "failed"}
