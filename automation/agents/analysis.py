"""AnalysisAgent — multi-AI parallel analysis with auto-triage.

For each ProblemSpec received from DiscoveryAgent:
1. Fan-out to 2-3 AI providers in parallel (like sourcemap's worker launch)
2. Collect root-cause, fix-strategy, confidence from each
3. Majority vote / weighted consensus
4. Auto-triage: auto_fix / needs_review / defer
5. Persist analysis to knowledge store for future reuse
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base_agent import AgentConfig, BaseAgent
from .mailbox import Mailbox
from .protocol import (
    AgentRole,
    AnalysisResult,
    ProblemStatus,
    ProblemSpec,
    Severity,
    TriageDecision,
)

logger = logging.getLogger(__name__)

# AI analysis prompt template
_ANALYSIS_PROMPT = """\
你是一个自动代码修复系统的分析模块。请分析以下系统问题并给出修复建议。

## 问题信息
- 问题ID: {problem_id}
- 来源探针: {source_probe}
- 严重级别: {severity}
- 问题描述: {description}
- 建议方法: {suggested_approach}
- 相关文件: {affected_files}

## 请用以下JSON格式回复（只返回JSON，不要其他内容）:
```json
{{
  "root_cause": "简要分析根因（一句话）",
  "fix_strategy": "fix_code|fix_config|fix_test|fix_docs|fix_then_rebuild",
  "confidence": 0.0到1.0之间的数字,
  "affected_files": ["需要修改的文件路径"],
  "fix_description": "修复方案简述"
}}
```
"""


class AnalysisAgent(BaseAgent):
    """Analyses problems using multi-AI consensus and auto-triage."""

    _PROVIDER_MAP: Dict[str, str] = {
        "primary": "chatgpt",
        "secondary": "deepseek",
        "codex": "codex_cli",
    }

    def __init__(
        self,
        mailbox: Mailbox,
        config: Optional[AgentConfig] = None,
    ):
        super().__init__(role=AgentRole.ANALYSIS, mailbox=mailbox, config=config)
        self._knowledge_path = (
            self.config.repo_root / "runtime" / "agents" / "knowledge"
        )
        # Per-instance provider health tracking (skip providers with >= 3 consecutive failures)
        self._provider_failures: Dict[str, int] = {}

    async def handle_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        problems_raw = payload.get("problems", [])
        problems = [
            ProblemSpec.from_dict(p) if isinstance(p, dict) else p
            for p in problems_raw
        ]

        # Parallel analysis (fan-out per problem)
        results = await asyncio.gather(
            *(self._analyze_one(p) for p in problems),
            return_exceptions=True,
        )

        analyses: List[AnalysisResult] = []
        for r in results:
            if isinstance(r, AnalysisResult):
                analyses.append(r)
            elif isinstance(r, Exception):
                logger.warning("Analysis error: %s", r)

        # Persist to knowledge store
        await self._persist_knowledge(analyses)

        logger.info(
            "[%s] Analysis complete: %d/%d problems analysed, %d auto_fix",
            self.agent_id,
            len(analyses),
            len(problems),
            sum(1 for a in analyses if a.triage == TriageDecision.AUTO_FIX.value),
        )

        return {
            "findings": [a.to_dict() for a in analyses],
            "total": len(analyses),
            "auto_fix": sum(1 for a in analyses if a.triage == TriageDecision.AUTO_FIX.value),
            "needs_review": sum(1 for a in analyses if a.triage == TriageDecision.NEEDS_REVIEW.value),
            "deferred": sum(1 for a in analyses if a.triage == TriageDecision.DEFER.value),
        }

    async def _analyze_one(self, problem: ProblemSpec) -> AnalysisResult:
        """Analyse a single problem, optionally using cached knowledge."""

        if problem.current_status == ProblemStatus.BLOCKED.value:
            return AnalysisResult(
                problem_id=problem.problem_id,
                root_cause=problem.blocked_reason or "blocked_by_external_dependency",
                fix_strategy=problem.suggested_approach or "external_dependency",
                risk_level=problem.severity,
                confidence=0.0,
                triage=TriageDecision.DEFER.value,
                source="blocked",
                task_family=problem.task_family,
                lane_id=problem.lane_id,
                current_status=problem.current_status,
                blocker_type=problem.blocker_type,
                blocked_reason=problem.blocked_reason,
                write_scope=list(problem.write_scope or []),
                provider_votes=[],
            )

        # Check knowledge cache first
        cached = self._check_knowledge(problem.problem_id)
        if cached is not None:
            logger.debug("Using cached analysis for %s", problem.problem_id)
            return cached

        # Multi-provider analysis (fan-out)
        # v3: Dynamic provider selection based on health
        available_providers = [
            p for p in ["primary", "secondary", "codex"]
            if self._provider_failures.get(self._PROVIDER_MAP.get(p, p), 0) < 3
        ]
        if not available_providers:
            # All providers down — reset and try all
            logger.warning("[%s] All providers failed — resetting health counters", self.agent_id)
            self._provider_failures.clear()
            available_providers = ["primary", "secondary", "codex"]

        provider_results = await asyncio.gather(
            *(self._provider_analyze(problem, label) for label in available_providers),
            return_exceptions=True,
        )

        votes: List[Dict[str, Any]] = []
        for r in provider_results:
            if isinstance(r, dict):
                votes.append(r)

        # Consensus
        if not votes:
            return AnalysisResult(
                problem_id=problem.problem_id,
                root_cause="analysis_failed",
                fix_strategy="manual_verify",
                confidence=0.0,
                triage=TriageDecision.DEFER.value,
                source="failed",
                task_family=problem.task_family,
                lane_id=problem.lane_id,
                current_status=problem.current_status,
                blocker_type=problem.blocker_type,
                blocked_reason=problem.blocked_reason,
                write_scope=list(problem.write_scope or []),
            )

        return self._consensus(problem, votes)

    async def _provider_analyze(
        self, problem: ProblemSpec, provider_label: str
    ) -> Dict[str, Any]:
        """Single-provider analysis via Web AI unified API.

        Calls ``/api/v1/webai/analyze`` for real AI analysis.
        Falls back to heuristics when the AI service is unavailable.
        """
        webai_url = self.config.service_urls.get(
            "webai", "http://127.0.0.1:8000"
        )

        # Map provider_label to actual provider name
        provider_name = {
            "primary": "chatgpt",
            "secondary": "deepseek",
        }.get(provider_label, "chatgpt")

        # v3: Codex CLI provider branch
        if provider_label == "codex":
            return await self._codex_analyze(problem)

        prompt = _ANALYSIS_PROMPT.format(
            problem_id=problem.problem_id,
            source_probe=problem.source_probe,
            severity=problem.severity,
            description=problem.description,
            suggested_approach=problem.suggested_approach or "fix_code",
            affected_files=", ".join(problem.affected_files or []) or "未知",
        )

        try:
            import httpx
            _timeout = httpx.Timeout(180, connect=5)
            async with httpx.AsyncClient(timeout=_timeout) as client:
                resp = await client.post(
                    f"{webai_url}/api/v1/webai/analyze",
                    json={
                        "provider": provider_name,
                        "prompt": prompt,
                        "timeout_s": 120,
                    },
                )
                if resp.status_code == 200:
                    payload = resp.json()
                    data = payload.get("data", {})
                    ai_text = data.get("response", "")
                    parsed = self._parse_ai_response(ai_text)
                    if parsed:
                        logger.info(
                            "[%s] AI analysis from %s: confidence=%.2f strategy=%s",
                            self.agent_id, provider_name,
                            parsed.get("confidence", 0), parsed.get("fix_strategy", ""),
                        )
                        return {
                            "provider": provider_label,
                            "root_cause": parsed.get("root_cause", "unknown"),
                            "fix_strategy": parsed.get("fix_strategy", "fix_code"),
                            "confidence": parsed.get("confidence", 0.5),
                            "fix_description": parsed.get("fix_description", ""),
                            "affected_files": parsed.get("affected_files", []),
                            "source": "ai",
                        }
                elif resp.status_code == 503:
                    logger.info("[%s] Provider %s session lost, falling back", self.agent_id, provider_name)
                else:
                    logger.warning("[%s] AI provider %s returned %d", self.agent_id, provider_name, resp.status_code)
        except ImportError:
            logger.debug("[%s] httpx not available, using heuristics", self.agent_id)
        except Exception as exc:
            logger.info("[%s] AI provider %s unavailable (%s), using heuristics", self.agent_id, provider_name, exc)
            # v3: Track provider failure
            self._provider_failures[provider_name] = self._provider_failures.get(provider_name, 0) + 1
            if self._provider_failures.get(provider_name, 0) >= 3:
                logger.warning(
                    "[%s] Provider %s hit failure threshold (%d) — will be skipped",
                    self.agent_id, provider_name, self._provider_failures[provider_name],
                )

        # Fallback: rule-based heuristic analysis
        confidence = self._heuristic_confidence(problem)
        strategy = self._heuristic_strategy(problem)

        return {
            "provider": provider_label,
            "root_cause": f"heuristic:{problem.family or 'unknown'}",
            "fix_strategy": strategy,
            "risk_level": problem.severity,
            "confidence": confidence,
            "source": "heuristic",
        }

    async def _codex_analyze(self, problem: ProblemSpec) -> Dict[str, Any]:
        """Analyse a problem using the local Codex CLI."""
        try:
            from . import codex_bridge

            if not await codex_bridge.codex_available():
                raise RuntimeError("Codex CLI not available")

            prompt = _ANALYSIS_PROMPT.format(
                problem_id=problem.problem_id,
                source_probe=problem.source_probe,
                severity=problem.severity,
                description=problem.description,
                suggested_approach=problem.suggested_approach or "fix_code",
                affected_files=", ".join(problem.affected_files or []) or "未知",
            )

            result = await codex_bridge.codex_exec(
                prompt, self.config.repo_root, timeout_s=180,
            )
            if result:
                parsed = self._parse_ai_response(result)
                if parsed:
                    # Reset failure counter on success
                    self._provider_failures.pop("codex_cli", None)
                    logger.info(
                        "[%s] Codex CLI analysis: confidence=%.2f strategy=%s",
                        self.agent_id, parsed.get("confidence", 0),
                        parsed.get("fix_strategy", ""),
                    )
                    return {
                        "provider": "codex",
                        "root_cause": parsed.get("root_cause", "unknown"),
                        "fix_strategy": parsed.get("fix_strategy", "fix_code"),
                        "confidence": parsed.get("confidence", 0.5),
                        "fix_description": parsed.get("fix_description", ""),
                        "affected_files": parsed.get("affected_files", []),
                        "source": "codex_cli",
                    }
        except Exception as exc:
            logger.info("[%s] Codex CLI analysis failed: %s", self.agent_id, exc)
            self._provider_failures["codex_cli"] = (
                self._provider_failures.get("codex_cli", 0) + 1
            )

        # Fall back to heuristics
        return {
            "provider": "codex",
            "root_cause": f"heuristic:{problem.family or 'unknown'}",
            "fix_strategy": self._heuristic_strategy(problem),
            "confidence": self._heuristic_confidence(problem),
            "source": "heuristic",
        }

    @staticmethod
    def _parse_ai_response(text: str) -> Optional[Dict[str, Any]]:
        """Extract JSON from AI response text."""
        if not text:
            return None
        # Try to find JSON block in markdown code fence
        match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if match:
            text = match.group(1)
        else:
            # Try to find raw JSON object
            match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
            if match:
                text = match.group(0)
        try:
            data = json.loads(text)
            # Validate expected fields
            if isinstance(data, dict) and "root_cause" in data:
                confidence = data.get("confidence", 0.5)
                if isinstance(confidence, (int, float)):
                    data["confidence"] = max(0.0, min(1.0, float(confidence)))
                else:
                    data["confidence"] = 0.5
                return data
        except (json.JSONDecodeError, ValueError):
            pass
        return None

    def _consensus(
        self, problem: ProblemSpec, votes: List[Dict[str, Any]]
    ) -> AnalysisResult:
        """Weighted consensus from multiple provider votes."""
        avg_confidence = sum(v.get("confidence", 0) for v in votes) / max(len(votes), 1)

        # Pick the most common strategy
        strategies = [v.get("fix_strategy", "") for v in votes]
        strategy = max(set(strategies), key=strategies.count) if strategies else "fix_code"

        # Pick root cause from highest-confidence vote
        best_vote = max(votes, key=lambda v: v.get("confidence", 0))
        root_cause = best_vote.get("root_cause", "unknown")

        # Triage decision
        if avg_confidence >= 0.7:
            triage = TriageDecision.AUTO_FIX.value
        elif avg_confidence >= 0.4:
            triage = TriageDecision.NEEDS_REVIEW.value
        else:
            triage = TriageDecision.DEFER.value

        return AnalysisResult(
            problem_id=problem.problem_id,
            root_cause=root_cause,
            fix_strategy=strategy,
            risk_level=problem.severity,
            confidence=round(avg_confidence, 3),
            triage=triage,
            source=best_vote.get("source", "heuristic"),
            fix_description=best_vote.get("fix_description", ""),
            task_family=problem.task_family,
            lane_id=problem.lane_id,
            current_status=problem.current_status,
            blocker_type=problem.blocker_type,
            blocked_reason=problem.blocked_reason,
            write_scope=list(problem.write_scope or []),
            provider_votes=votes,
        )

    # ------------------------------------------------------------------
    # Heuristics (baseline, replaced by AI in production)
    # ------------------------------------------------------------------

    @staticmethod
    def _heuristic_confidence(problem: ProblemSpec) -> float:
        """Rule-based confidence estimate."""
        # Test failures and audit findings are high confidence
        probe_scores = {
            "test_failure": 0.85,
            "audit": 0.75,
            "blind_spot": 0.65,
            "catalog_drift": 0.60,
            "runtime_health": 0.90,
            "code_change": 0.40,
        }
        base = probe_scores.get(problem.source_probe, 0.50)

        # Severity boost
        severity_boost = {"P0": 0.10, "P1": 0.05, "P2": 0.0, "P3": -0.05}
        base += severity_boost.get(problem.severity, 0)

        return min(max(base, 0.0), 1.0)

    @staticmethod
    def _heuristic_strategy(problem: ProblemSpec) -> str:
        """Rule-based strategy selection."""
        approach = problem.suggested_approach
        if approach and approach != "fix_code":
            return approach
        if problem.source_probe == "test_failure":
            return "fix_code"
        if problem.source_probe == "blind_spot":
            return "fix_code"
        if problem.source_probe == "runtime_health":
            return "fix_then_rebuild"
        return "fix_code"

    # ------------------------------------------------------------------
    # Knowledge persistence
    # ------------------------------------------------------------------

    _KNOWLEDGE_TTL_SECONDS = 86_400  # 24 h — cached analyses older than this are stale

    def _check_knowledge(self, problem_id: str) -> Optional[AnalysisResult]:
        """Check if we've seen this problem before (with 24 h TTL)."""
        history_path = self._knowledge_path / "analysis_history.jsonl"
        if not history_path.exists():
            return None
        now = datetime.now(timezone.utc)
        try:
            for line in history_path.read_text(encoding="utf-8").strip().split("\n"):
                if not line:
                    continue
                entry = json.loads(line)
                if entry.get("problem_id") == problem_id:
                    ts_raw = entry.get("_ts")
                    if ts_raw:
                        try:
                            ts = datetime.fromisoformat(ts_raw)
                            if (now - ts).total_seconds() > self._KNOWLEDGE_TTL_SECONDS:
                                continue  # stale — skip
                        except (ValueError, TypeError):
                            continue  # unparseable timestamp — treat as stale
                    else:
                        continue  # no timestamp — treat as stale
                    return AnalysisResult.from_dict(entry)
        except Exception:
            pass
        return None

    async def _persist_knowledge(self, analyses: List[AnalysisResult]) -> None:
        """Append analysis results to the knowledge store."""
        if not analyses:
            return
        self._knowledge_path.mkdir(parents=True, exist_ok=True)
        history_path = self._knowledge_path / "analysis_history.jsonl"
        try:
            with open(history_path, "a", encoding="utf-8") as f:
                for a in analyses:
                    entry = a.to_dict()
                    entry["_ts"] = datetime.now(timezone.utc).isoformat()
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            logger.exception("Knowledge persist error")
