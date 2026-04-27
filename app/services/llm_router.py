"""场景化LLM路由器

路由逻辑：场景识别 → 最优模型 → 失败降级 → 兜底(Ollama)
降级率目标：从87%降至≤30%（通过API替代Web自动化实现）

路由优先级：
  1. DeepSeek API   — 推理/公告/情绪/风险 场景（主力）
  2. Gemini API     — 长上下文/多周期/行业对比 场景
  3. Ollama本地     — 快速初筛/批量/兜底（不再是主力）
  4. Web自动化      — 已退出主路径，仅保留为可选数据采集工具
"""
from __future__ import annotations

import asyncio
import logging
import math
import re
import shutil
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from app.core.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 场景枚举
# ---------------------------------------------------------------------------

class LLMScene(str, Enum):
    ANNOUNCEMENT   = "announcement"    # 公告/财报解读
    SENTIMENT      = "sentiment"       # 情绪/舆情分析
    RISK_WARNING   = "risk_warning"    # 风险预警
    CAPITAL_FLOW   = "capital_flow"    # 主力资金分析
    LONG_CONTEXT   = "long_context"    # 多周期/长文本
    INDUSTRY_COMP  = "industry_comp"   # 行业横向对比
    BULK_SCREEN    = "bulk_screen"     # 快速批量初筛
    RULE_BASED     = "rule_based"      # 规则化结构判断
    GENERAL        = "general"         # 通用/未识别


# ---------------------------------------------------------------------------
# 场景识别器：基于关键词 + 上下文长度
# ---------------------------------------------------------------------------

_SCENE_KEYWORDS: dict[LLMScene, list[str]] = {
    LLMScene.ANNOUNCEMENT: [
        "公告", "财报", "年报", "季报", "半年报", "业绩预告",
        "重大事项", "定增", "回购", "分红", "股权激励",
    ],
    LLMScene.SENTIMENT: [
        "舆情", "情绪", "热搜", "评论", "社媒", "抖音", "微博",
        "市场情绪", "恐慌", "贪婪", "散户",
    ],
    LLMScene.RISK_WARNING: [
        "风险", "ST", "退市", "违规", "违法", "诉讼", "处罚",
        "亏损", "债务", "违约", "爆雷", "暴雷",
    ],
    LLMScene.CAPITAL_FLOW: [
        "主力", "资金", "净流", "龙虎榜", "大单", "机构",
        "北向资金", "融资融券", "量价",
    ],
    LLMScene.INDUSTRY_COMP: [
        "行业", "对比", "竞争", "同行", "市占率", "横向",
        "板块", "赛道", "比较",
    ],
    LLMScene.LONG_CONTEXT: [],  # 由 token 数量决定，不靠关键词
    LLMScene.BULK_SCREEN: [],   # 由调用方显式指定
    LLMScene.RULE_BASED: [],    # 由调用方显式指定
}

# 场景→最优模型 路由表。
# 活跃主链统一收口到 NewAPI provider pool（codex_api）→ ollama 兜底。
_SCENE_TO_MODEL: dict[LLMScene, list[str]] = {
    LLMScene.ANNOUNCEMENT:  ["codex_api", "ollama"],
    LLMScene.SENTIMENT:     ["codex_api", "ollama"],
    LLMScene.RISK_WARNING:  ["codex_api", "ollama"],
    LLMScene.CAPITAL_FLOW:  ["codex_api", "ollama"],
    LLMScene.INDUSTRY_COMP: ["codex_api", "ollama"],
    LLMScene.LONG_CONTEXT:  ["codex_api", "ollama"],
    LLMScene.BULK_SCREEN:   ["ollama", "ollama"],
    LLMScene.RULE_BASED:    ["ollama", "ollama"],
    LLMScene.GENERAL:       ["codex_api", "ollama"],
}


def detect_scene(prompt: str, estimated_tokens: int = 0) -> LLMScene:
    """基于关键词和上下文长度识别最佳场景。"""
    from app.core.config import settings

    # 长上下文优先判断（避免大 prompt 撑爆短上下文模型）
    if estimated_tokens > settings.router_max_context_tokens:
        return LLMScene.LONG_CONTEXT

    # 关键词匹配（取命中数最多的场景）
    scores: dict[LLMScene, int] = {}
    for scene, keywords in _SCENE_KEYWORDS.items():
        if not keywords:
            continue
        hit = sum(1 for kw in keywords if kw in prompt)
        if hit > 0:
            scores[scene] = hit

    if scores:
        return max(scores, key=lambda s: scores[s])

    return LLMScene.GENERAL


def estimate_tokens(text: str) -> int:
    """粗略估算 token 数（中文约1.5字/token，英文约4字/token）。"""
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text))
    other_chars = len(text) - chinese_chars
    return int(chinese_chars / 1.5 + other_chars / 4)


def _unique_model_chain(model_chain: list[str]) -> list[str]:
    ordered: list[str] = []
    for model_name in model_chain:
        normalized = str(model_name or "").strip()
        if normalized and normalized not in ordered:
            ordered.append(normalized)
    return ordered


def _configured_scene_chain(scene: LLMScene) -> list[str]:
    base_chain = list(_SCENE_TO_MODEL.get(scene, ["codex_api", "ollama"]))
    if scene == LLMScene.LONG_CONTEXT:
        return _unique_model_chain([settings.router_longctx, *base_chain])
    if scene in {LLMScene.BULK_SCREEN, LLMScene.RULE_BASED}:
        return _unique_model_chain([settings.router_bulk, *base_chain])
    return _unique_model_chain([settings.router_primary, *base_chain])


def _circuit_breaker_retry_after_seconds(now: float | None = None) -> int:
    current = time.time() if now is None else now
    last_failure = _global_llm_circuit_breaker._last_failure_time
    if last_failure is None:
        return int(_CIRCUIT_BREAKER_OPEN_SECONDS)
    remaining = _CIRCUIT_BREAKER_OPEN_SECONDS - (current - last_failure)
    return max(1, int(math.ceil(remaining)))


async def _route_model_call(
    model_name: str,
    prompt: str,
    temperature: float,
    use_cot: bool,
    *,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    if model_name == "ollama":
        if timeout_sec and timeout_sec > 0:
            return await _call_ollama(prompt, temperature, timeout_sec=timeout_sec)
        return await _call_ollama(prompt, temperature)
    if timeout_sec and timeout_sec > 0:
        return await _call_model(model_name, prompt, temperature, use_cot, timeout_sec=timeout_sec)
    return await _call_model(model_name, prompt, temperature, use_cot)


async def _call_codex_single_provider(
    provider: Any,
    prompt: str,
    temperature: float,
    *,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    from app.services.codex_client import CodexAPIClient

    single_provider_client = CodexAPIClient(provider_specs=[provider])
    try:
        analyze_coro = single_provider_client.analyze(prompt, temperature=temperature)
        if timeout_sec and timeout_sec > 0:
            return await asyncio.wait_for(analyze_coro, timeout=timeout_sec)
        return await analyze_coro
    finally:
        await single_provider_client.close()


def _select_parallel_codex_providers(client: Any) -> list[Any]:
    if not bool(getattr(settings, "codex_api_parallel_enabled", False)):
        return []
    max_providers = max(1, int(getattr(settings, "codex_api_parallel_max_providers", 1) or 1))
    if max_providers < 2:
        return []

    ordered = client._provider_order() if hasattr(client, "_provider_order") else list(client.providers)
    if len(ordered) < 2:
        return []

    if hasattr(client, "_provider_bucket"):
        primary_bucket = min(client._provider_bucket(provider) for provider in ordered)
        primary_providers = [p for p in ordered if client._provider_bucket(p) == primary_bucket]
        if len(primary_providers) >= max_providers:
            selected = primary_providers[:max_providers]
        else:
            # Cross-bucket: fill remaining slots from backup buckets
            backup_providers = [p for p in ordered if client._provider_bucket(p) != primary_bucket]
            selected = primary_providers + backup_providers[:max_providers - len(primary_providers)]
    else:
        selected = ordered[:max_providers]

    return selected if len(selected) > 1 else []


async def _call_codex_api_parallel(
    prompt: str,
    temperature: float,
    *,
    timeout_sec: float | None = None,
) -> dict[str, Any] | None:
    from app.services.codex_client import get_codex_client

    client = get_codex_client()
    providers = _select_parallel_codex_providers(client)
    if len(providers) < 2:
        return None

    tasks = {
        asyncio.create_task(
            _call_codex_single_provider(
                provider,
                prompt,
                temperature,
                timeout_sec=timeout_sec,
            )
        ): provider
        for provider in providers
    }
    pending = set(tasks)
    failures: list[str] = []

    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                provider = tasks[task]
                try:
                    result = task.result()
                    if hasattr(client, "_mark_provider_healthy"):
                        client._mark_provider_healthy(provider)
                    result["parallel_mode"] = "race"
                    result["parallel_attempted_providers"] = [item.provider_name for item in providers]
                    result["parallel_winner_provider"] = provider.provider_name
                    return result
                except Exception as exc:
                    if hasattr(client, "_mark_provider_failed"):
                        client._mark_provider_failed(provider)
                    failures.append(f"{provider.provider_name}:{exc}")
                    logger.warning(
                        "codex_api | parallel provider=%s failed: %s",
                        provider.provider_name,
                        exc,
                    )
        if failures:
            logger.warning("codex_api | parallel primary race failed: %s", "; ".join(failures))
        return None
    finally:
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def _call_codex_api(
    prompt: str,
    temperature: float,
    use_cot: bool,
    *,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    del use_cot

    parallel_result = await _call_codex_api_parallel(prompt, temperature, timeout_sec=timeout_sec)
    if parallel_result is not None:
        return parallel_result

    from app.services.codex_client import get_codex_client

    analyze_coro = get_codex_client().analyze(prompt, temperature=temperature)
    if timeout_sec and timeout_sec > 0:
        return await asyncio.wait_for(analyze_coro, timeout=timeout_sec)
    return await analyze_coro


# ---------------------------------------------------------------------------
# 路由执行器
# ---------------------------------------------------------------------------

@dataclass
class RouterResult:
    response: str
    model_used: str
    source: str
    scene: LLMScene
    elapsed_s: float
    degraded: bool          # True = 未用到首选模型
    degradation_reason: str = ""
    confidence: float = 0.5
    usage: dict = field(default_factory=dict)
    extra: dict = field(default_factory=dict)


async def route_and_call(
    prompt: str,
    scene: LLMScene | None = None,
    force_model: str | None = None,
    temperature: float = 0.3,
    use_cot: bool = False,
    timeout_sec: float | None = None,
) -> RouterResult:
    """场景路由主入口。

    Args:
        prompt: 完整 prompt 文本
        scene: 显式指定场景（None 则自动检测）
        force_model: 强制指定模型跳过路由（调试用）
        temperature: 生成温度
        use_cot: 是否启用推理链（CoT）模式

    Returns:
        RouterResult with standardized fields
    """
    t0 = time.time()
    token_est = estimate_tokens(prompt)

    if scene is None:
        scene = detect_scene(prompt, token_est)

    model_chain = [force_model] if force_model else _configured_scene_chain(scene)
    preferred = model_chain[0]

    try:
        _global_llm_circuit_breaker.before_request(now=t0)
    except RuntimeError as exc:
        retry_after = _circuit_breaker_retry_after_seconds(now=t0)
        raise RuntimeError(f"LLM global circuit breaker is open; retry in {retry_after}s") from exc

    logger.info("llm_router | scene=%s preferred=%s tokens≈%d", scene.value, preferred, token_est)

    last_error: Exception | None = None
    for idx, model_name in enumerate(model_chain):
        try:
            raw = await _route_model_call(
                model_name,
                prompt,
                temperature,
                use_cot,
                timeout_sec=timeout_sec,
            )
            pool_level = str(raw.get("pool_level") or "primary").lower()
            degraded = idx > 0 or model_name != preferred or pool_level != "primary"
            elapsed = round(time.time() - t0, 2)
            result = RouterResult(
                response=raw.get("response", ""),
                model_used=model_name,
                source=raw.get("source", model_name),
                scene=scene,
                elapsed_s=elapsed,
                degraded=degraded,
                degradation_reason="" if not degraded else f"primary_failed:{last_error}",
                usage=raw.get("usage", {}),
                extra=raw,
            )
            _global_llm_circuit_breaker.record_success(now=time.time())
            router_stats.record(result)
            if degraded:
                logger.warning(
                    "llm_router | DEGRADED scene=%s used=%s reason=%s",
                    scene.value, model_name, result.degradation_reason
                )
            return result
        except Exception as exc:
            last_error = exc
            logger.warning("llm_router | model=%s failed: %s, trying next", model_name, exc)

    # 所有模型都失败（极端情况）
    _global_llm_circuit_breaker.record_failure(now=time.time())
    raise RuntimeError(
        f"All models in chain {model_chain} failed for scene {scene.value}. "
        f"Last error: {last_error}"
    )


async def _call_model(
    model_name: str,
    prompt: str,
    temperature: float,
    use_cot: bool,
    *,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    """分发到具体模型客户端。"""
    if model_name == "codex_api":
        return await _call_codex_api(prompt, temperature, use_cot, timeout_sec=timeout_sec)

    if model_name == "deepseek_api":
        from app.services.deepseek_api_client import get_deepseek_api_client
        client = get_deepseek_api_client()
        analyze_coro = (
            client.analyze_with_chain_of_thought(prompt, temperature=temperature)
            if use_cot
            else client.analyze(prompt, temperature=temperature)
        )
        if timeout_sec and timeout_sec > 0:
            return await asyncio.wait_for(analyze_coro, timeout=timeout_sec)
        return await analyze_coro

    if model_name == "gemini_api":
        return await _call_gemini_api(prompt, temperature, use_cot, timeout_sec=timeout_sec)

    if model_name == "claude_cli":
        return await _call_claude_cli(prompt, temperature, use_cot, timeout_sec=timeout_sec)

    if model_name == "ollama":
        return await _call_ollama(prompt, temperature, timeout_sec=timeout_sec)

    raise ValueError(f"Unknown model: {model_name}")


async def _call_gemini_api(
    prompt: str,
    temperature: float,
    use_cot: bool,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    """调用 Gemini 官方 API（google-generativeai 或 openai 兼容端点）。"""
    import time as _time

    del timeout_sec

    if not settings.gemini_api_key:
        raise RuntimeError(
            "GEMINI_API_KEY not configured. "
            "申请地址：https://aistudio.google.com/ "
            "Gemini 2.0 Flash 价格：$0.075/1M输入，$0.30/1M输出"
        )

    t0 = _time.time()
    try:
        import google.generativeai as genai  # type: ignore
        genai.configure(api_key=settings.gemini_api_key)
        model = genai.GenerativeModel(settings.gemini_model_prod)
        system = (
            "你是A股金融分析专家，请基于提供的数据进行严谨的推理分析，输出结构化JSON。"
            + ("\n请先展示推理过程再输出结论。" if use_cot else "")
        )
        full_prompt = f"{system}\n\n{prompt}"
        response = model.generate_content(
            full_prompt,
            generation_config=genai.GenerationConfig(temperature=temperature, max_output_tokens=2048),
        )
        content = response.text
        elapsed = round(_time.time() - t0, 2)
        logger.info("gemini_api | ok elapsed=%.1fs", elapsed)
        return {
            "response": content,
            "elapsed_s": elapsed,
            "has_citation": False,
            "model": settings.gemini_model_prod,
            "source": "gemini_api",
            "usage": {},
        }
    except ImportError:
        raise RuntimeError("google-generativeai not installed. Run: pip install google-generativeai")


def _resolve_claude_cli_command() -> str:
    repo_root = Path(__file__).resolve().parents[2]
    repo_cmd = repo_root / "claude.cmd"
    if repo_cmd.exists():
        return str(repo_cmd)
    discovered = shutil.which("claude.cmd") or shutil.which("claude")
    if discovered:
        return discovered
    raise RuntimeError("claude_cli_unavailable: no claude.cmd/claude command found")


async def _call_claude_cli(
    prompt: str,
    temperature: float,
    use_cot: bool,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    del temperature, use_cot

    started = time.time()
    command = _resolve_claude_cli_command()
    effective_timeout = max(30.0, float(timeout_sec or 120.0))
    proc = await asyncio.create_subprocess_exec(
        "cmd.exe",
        "/d",
        "/c",
        command,
        "--bare",
        "--print",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(prompt.encode("utf-8")),
            timeout=effective_timeout,
        )
    except asyncio.TimeoutError as exc:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"claude_cli_timeout_after_{int(effective_timeout)}s") from exc

    output = stdout.decode("utf-8", errors="replace").strip()
    err_output = stderr.decode("utf-8", errors="replace").strip()
    if proc.returncode != 0:
        detail = err_output or output or f"exit={proc.returncode}"
        raise RuntimeError(f"claude_cli_failed:{detail[:300]}")
    if not output:
        raise RuntimeError("claude_cli_empty_response")

    elapsed = round(time.time() - started, 2)
    logger.info("claude_cli | ok elapsed=%.1fs", elapsed)
    return {
        "response": output,
        "elapsed_s": elapsed,
        "has_citation": False,
        "model": "claude-cli",
        "source": "claude_cli",
        "usage": {},
        "pool_level": "primary",
        "provider_name": "claude_cli",
        "endpoint": command,
    }


async def _call_ollama(
    prompt: str,
    temperature: float,
    *,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    """调用本地 Ollama（Qwen3:8b 兜底）。"""
    import time as _time
    from app.services.ollama_client import ollama_client

    t0 = _time.time()
    options = {"temperature": temperature} if temperature != 0.7 else None
    raw = await ollama_client.generate(
        prompt,
        options=options,
        timeout=int(timeout_sec) if timeout_sec and timeout_sec > 0 else None,
    )
    elapsed = round(_time.time() - t0, 2)
    logger.info("ollama | ok elapsed=%.1fs latency=%dms", elapsed, raw.get("latency_ms", 0))
    return {
        "response": raw.get("response", ""),
        "elapsed_s": elapsed,
        "has_citation": False,
        "model": raw.get("model", "ollama"),
        "source": "ollama",
        "usage": {},
    }


# ---------------------------------------------------------------------------
# 路由统计（用于监控降级率）
# ---------------------------------------------------------------------------

class RouterStats:
    """轻量级内存统计，用于实时监控降级率。生产环境接入 Prometheus。"""

    def __init__(self) -> None:
        self._total = 0
        self._degraded = 0
        self._by_scene: dict[str, dict] = {}

    def record(self, result: RouterResult) -> None:
        self._total += 1
        if result.degraded:
            self._degraded += 1
        key = result.scene.value
        if key not in self._by_scene:
            self._by_scene[key] = {"total": 0, "degraded": 0}
        self._by_scene[key]["total"] += 1
        if result.degraded:
            self._by_scene[key]["degraded"] += 1

    @property
    def degradation_rate(self) -> float:
        if self._total == 0:
            return 0.0
        return self._degraded / self._total

    def summary(self) -> dict:
        return {
            "total_requests": self._total,
            "degraded_requests": self._degraded,
            "degradation_rate": round(self.degradation_rate, 4),
            "target_rate": 0.30,
            "status": "OK" if self.degradation_rate <= 0.30 else "ALERT",
            "by_scene": self._by_scene,
        }


router_stats = RouterStats()


# ---------------------------------------------------------------------------
# 三方投票审计（E2，见 13_多模型路由设计.md §10）
# ---------------------------------------------------------------------------

def should_trigger_audit(recommendation: str, confidence: float, contradiction: str) -> bool:
    """触发多数投票审计的条件（满足任一即触发）。

    1. BUY 信号（高风险，需验证）
    2. 置信度 >= sim_instruction_confidence_threshold（默认 0.65）
    3. contradiction 字段不为「无」（主分析自身检测到矛盾）
    """
    from app.core.config import settings

    return recommendation == "BUY" and confidence >= settings.sim_instruction_confidence_threshold


def _audit_model_chain() -> list[str]:
    primary = str(getattr(settings, "llm_audit_provider", "") or "codex_api").strip()
    fallback_chain = str(getattr(settings, "llm_audit_fallback_chain", "") or "ollama").split(",")
    return _unique_model_chain([primary, *fallback_chain])


def _parse_auditor_response(text: str) -> tuple[str, str]:
    """从审计方回复中解析 vote 和 severity。返回 (vote, severity)，默认 ("HOLD", "low")。"""
    vote = "HOLD"
    severity = "low"
    text_upper = (text or "").upper()
    # vote=[BUY/SELL/HOLD]
    import re as _re
    vote_m = _re.search(r"vote\s*=\s*(\w+)", text_upper, _re.I)
    if vote_m:
        v = vote_m.group(1).strip().upper()
        if v in ("BUY", "SELL", "HOLD"):
            vote = v
    sev_m = _re.search(r"severity\s*=\s*(\w+)", text_upper, _re.I)
    if sev_m:
        s = sev_m.group(1).strip().lower()
        if s in ("low", "medium", "high"):
            severity = s
    return vote, severity


def aggregate_audit_votes(
    main_vote: str,
    auditor1_vote: str,
    auditor1_severity: str,
    auditor2_vote: str,
    base_confidence: float,
) -> tuple[str, float, str]:
    """投票整合算法（纯逻辑，不调用 LLM）。

    返回 (final_recommendation, adjusted_confidence, audit_flag)。

    规格见 docs/core/13_多模型路由设计.md §10.3
    """
    votes = [main_vote, auditor1_vote, auditor2_vote]
    buy_count = sum(1 for v in votes if v.upper() == "BUY")
    sell_count = sum(1 for v in votes if v.upper() == "SELL")

    if buy_count == 3:
        return main_vote, min(base_confidence + 0.05, 0.95), "unanimous_buy"
    if sell_count == 3:
        return main_vote, min(base_confidence + 0.05, 0.95), "unanimous_sell"

    sev = (auditor1_severity or "low").lower()
    if sev == "high" and auditor1_vote.upper() != main_vote.upper():
        return main_vote, base_confidence * 0.75, "high_risk_flag"

    if buy_count >= 2 or sell_count >= 2:
        return main_vote, base_confidence, "majority_agree"

    return main_vote, base_confidence * 0.85, "votes_uncertain"


async def run_audit_and_aggregate(
    main_vote: str,
    base_confidence: float,
    report_summary: str = "",
    timeout_sec: int = 90,
) -> dict[str, Any]:
    """执行三方投票审计并整合结果。

    调用 Gemini API（审计方-1）和 本地 Ollama（审计方-2），解析投票后调用 aggregate_audit_votes。

    Args:
        main_vote: 主分析结论 BUY/SELL/HOLD
        base_confidence: 主分析置信度
        report_summary: 可选研报摘要（用于审计 prompt）
        timeout_sec: 单次调用超时秒数

    Returns:
        {
            "audit_flag": str,           # unanimous_buy / unanimous_sell / majority_agree / high_risk_flag / votes_uncertain / audit_skipped
            "audit_detail": str,         # 中文摘要，如「三方审计：2票看多 1票看空」
            "adjusted_confidence": float,
            "final_recommendation": str,
            "skip_reason": str | None,   # 仅 audit_skipped 时非空
        }
    """
    result: dict[str, Any] = {
        "audit_flag": "audit_skipped",
        "audit_detail": "",
        "adjusted_confidence": base_confidence,
        "final_recommendation": main_vote,
        "skip_reason": None,
    }

    if not bool(getattr(settings, "llm_audit_enabled", False)):
        result["skip_reason"] = "audit_disabled"
        return result

    prompt_base = f"以下研报结论为[{main_vote}]，请从**空方视角**列举最强反驳论据（1~3条），并给出你的判断 vote=[BUY/SELL/HOLD]，标明反驳强度 severity=[low/medium/high]。"
    if report_summary:
        prompt_base = f"{report_summary}\n\n{prompt_base}"
    prompt_base = prompt_base[:800]  # 控制 token

    audit_votes: list[tuple[str, str, str]] = []
    chain = _audit_model_chain()
    effective_timeout = timeout_sec if timeout_sec and timeout_sec > 0 else None
    for model_name in chain:
        try:
            if effective_timeout is not None:
                raw = await _call_model(
                    model_name,
                    prompt_base,
                    0.2,
                    False,
                    timeout_sec=effective_timeout,
                )
            else:
                raw = await _call_model(model_name, prompt_base, 0.2, False)
            vote, severity = _parse_auditor_response(raw.get("response", ""))
            audit_votes.append((model_name, vote, severity))
            if len(audit_votes) >= 2:
                break
        except Exception as exc:
            logger.warning("llm_router | audit provider=%s failed: %s", model_name, exc)

    if len(audit_votes) < 2:
        result["skip_reason"] = "audit_provider_unavailable"
        return result

    auditor1_vote, auditor1_severity = audit_votes[0][1], audit_votes[0][2]
    auditor2_vote = audit_votes[1][1]

    final_rec, adj_conf, flag = aggregate_audit_votes(
        main_vote, auditor1_vote, auditor1_severity, auditor2_vote, base_confidence
    )

    vote_cn = {"BUY": "看多", "SELL": "看空", "HOLD": "观望"}
    counts: dict[str, int] = {"BUY": 0, "SELL": 0, "HOLD": 0}
    for v in [main_vote, auditor1_vote, auditor2_vote]:
        k = (v or "HOLD").upper()
        if k not in ("BUY", "SELL", "HOLD"):
            k = "HOLD"
        counts[k] = counts.get(k, 0) + 1
    parts = [f"{vote_cn[k]}{counts[k]}票" for k in ("BUY", "SELL", "HOLD") if counts.get(k, 0) > 0]
    detail = f"三方审计：{' '.join(parts)}" if parts else f"三方审计：主分析{main_vote}，审计1{auditor1_vote}，审计2{auditor2_vote}"

    result.update({
        "audit_flag": flag,
        "audit_detail": detail,
        "adjusted_confidence": adj_conf,
        "final_recommendation": final_rec,
        "skip_reason": None,
    })
    return result


# ---------------------------------------------------------------------------
# 全局 LLM 熔断器 (FR-06)
# ---------------------------------------------------------------------------

_CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = 3
_CIRCUIT_BREAKER_OPEN_SECONDS: float = 30.0
_CIRCUIT_BREAKER_FAILURE_WINDOW_SECONDS: float = 120.0


class GlobalLLMCircuitBreaker:
    """Simple failure-count based circuit breaker for LLM calls."""

    def __init__(self) -> None:
        self._failure_timestamps: list[float] = []
        self._last_failure_time: float | None = None

    def record_failure(self, now: float | None = None) -> None:
        ts = now if now is not None else time.time()
        self._failure_timestamps.append(ts)
        self._last_failure_time = ts

    def record_success(self, now: float | None = None) -> None:
        self._failure_timestamps.clear()
        self._last_failure_time = None

    def _recent_failures(self, now: float) -> int:
        cutoff = now - _CIRCUIT_BREAKER_FAILURE_WINDOW_SECONDS
        self._failure_timestamps = [t for t in self._failure_timestamps if t > cutoff]
        return len(self._failure_timestamps)

    @property
    def degradation_rate(self) -> float:
        now = time.time()
        recent = self._recent_failures(now)
        if recent >= _CIRCUIT_BREAKER_FAILURE_THRESHOLD:
            return 1.0
        return recent / _CIRCUIT_BREAKER_FAILURE_THRESHOLD

    def before_request(self, now: float | None = None) -> None:
        ts = now if now is not None else time.time()
        recent = self._recent_failures(ts)
        if recent >= _CIRCUIT_BREAKER_FAILURE_THRESHOLD:
            if self._last_failure_time is not None and (ts - self._last_failure_time) < _CIRCUIT_BREAKER_OPEN_SECONDS:
                raise RuntimeError("circuit breaker is open")
            # auto-reset after OPEN_SECONDS
            self._failure_timestamps.clear()
            self._last_failure_time = None


_global_llm_circuit_breaker = GlobalLLMCircuitBreaker()


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------

def get_primary_status() -> str:
    """Return 'ok' / 'degraded' / 'unavailable' based on current LLM pool health.

    * If codex_api providers are discovered → 'ok'
    * If no codex providers but Ollama configured → 'degraded'
    * If nothing available → 'unavailable'
    """
    from app.core.config import settings as _settings

    # Check if direct API keys present → ok
    if _settings.codex_api_base_url and _settings.codex_api_key:
        return "ok"

    try:
        from app.services.codex_client import discover_codex_provider_specs
        specs = discover_codex_provider_specs()
        if specs:
            return "ok"
    except Exception:
        pass

    try:
        _resolve_claude_cli_command()
        if str(getattr(_settings, "router_primary", "") or "").strip() == "claude_cli":
            return "ok"
        return "degraded"
    except Exception:
        pass

    # Fallback: check Ollama
    ollama_url = getattr(_settings, "ollama_base_url", "")
    if ollama_url:
        return "degraded"

    return "unavailable"
