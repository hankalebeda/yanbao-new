"""FixAgent — parallel code repair with auto-rollback.

Follows the claude-code-sourcemap Worker pattern:
* Each fix runs in an isolated context (self-contained prompt)
* Multiple fixes execute in parallel via asyncio.gather
* Failed fixes are automatically retried or escalated

Uses a 4-tier AI provider chain for code generation:
  0. NewAPI Gateway  — direct LLM call via relay tokens
  1. Web AI API      — local unified API
  2. Codex HTTP      — worker pool
  3. Codex CLI       — local executable
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
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
    PatchSet,
    Severity,
)

logger = logging.getLogger(__name__)

# Strategy success stats file
STRATEGY_STATS_FILENAME = "fix_strategy_stats.json"
MAX_RETRIES_PER_PROBLEM = 3

# Safety guardrails
_ALLOWED_PATH_PREFIXES = ("app/", "tests/")
_MAX_PATCH_LINES = 200
_MAX_PATCH_DELETE_RATIO = 0.5  # reject if >50% of source deleted

# Code-fix prompt template — system message for structured repair
_FIX_SYSTEM_PROMPT = """\
你是一个生产级自动代码修复系统（Escort Team Fix Agent）。
你的职责是精确修复 Python 项目中的真实问题，生成可直接应用的代码补丁。

## 硬性约束
1. 只修改 app/ 和 tests/ 目录下的文件
2. old_text 必须与源文件中实际内容**逐字符完全匹配**（包括缩进、空格、换行）
3. 单次修复不超过 200 行变更
4. 不添加无关的 docstring、注释、类型注解或重构
5. 不引入 eval()、exec()、os.system()、pickle.loads() 等不安全调用
6. 如无法确定修改内容，返回空 patches 列表——绝不伪造修复
7. 每个补丁必须附带简短的 explanation 说明修改原因
"""

_FIX_USER_PROMPT = """\
## 问题信息
- 问题ID: {problem_id}
- 根因: {root_cause}
- 修复策略: {fix_strategy}
- 置信度: {confidence}
- 修复说明: {fix_description}

## 相关文件内容
{file_contents}

## 请用以下 JSON 格式回复（只返回 JSON，不要 markdown 代码块外的内容）:
```json
{{
  "patches": [
    {{
      "path": "需要修改的文件路径（相对于项目根目录）",
      "old_text": "需要替换的原始文本（多行精确匹配）",
      "new_text": "替换后的新文本（多行）",
      "explanation": "修改原因简述"
    }}
  ]
}}
```
"""


class FixAgent(BaseAgent):
    """Generates code fixes for analysed problems.

    Receives ``AnalysisResult[]`` from the Coordinator and produces
    ``PatchSet[]``.  Each fix runs with full context isolation
    (self-contained problem description + affected files).
    """

    def __init__(
        self,
        mailbox: Mailbox,
        config: Optional[AgentConfig] = None,
    ):
        super().__init__(role=AgentRole.FIX, mailbox=mailbox, config=config)
        self._knowledge_path = (
            self.config.repo_root / "runtime" / "agents" / "knowledge"
        )
        self._strategy_stats = self._load_strategy_stats()
        self._retry_counts: Dict[str, int] = {}

    async def handle_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        action = payload.get("action", "fix")

        if action == "rollback":
            return await self._handle_rollback(payload)

        analyses_raw = payload.get("analyses", [])
        analyses = [
            AnalysisResult.from_dict(a) if isinstance(a, dict) else a
            for a in analyses_raw
        ]

        # Parallel fix generation (fan-out per analysis)
        results = await asyncio.gather(
            *(self._fix_one(a) for a in analyses),
            return_exceptions=True,
        )

        patches: List[PatchSet] = []
        failed: List[str] = []
        for r in results:
            if isinstance(r, PatchSet):
                if r.patches:  # only include if actual patches were generated
                    patches.append(r)
            elif isinstance(r, Exception):
                logger.warning("Fix error: %s", r)
                failed.append(str(r))

        # Update strategy stats
        self._update_strategy_stats(patches)

        logger.info(
            "[%s] Fix complete: %d patches generated, %d failed",
            self.agent_id, len(patches), len(failed),
        )

        return {
            "findings": [p.to_dict() for p in patches],
            "fix_count": len(patches),
            "failed_count": len(failed),
        }

    @contextlib.asynccontextmanager
    async def _claim_problem(self, problem_id: str):
        """FileLock-based task claim: prevent duplicate concurrent fixes.

        Based on the LiteLLM claude-code-sourcemap claimTask() pattern.
        If another worker already holds the lock for this problem_id,
        we yield False and the caller should skip the fix.
        Raises nothing — lock acquisition failure is treated as a skip.
        """
        try:
            from filelock import FileLock, Timeout as FileLockTimeout
        except ImportError:
            # filelock unavailable — degrade gracefully (no concurrent protection)
            yield True
            return

        lock_dir = self.config.repo_root / "runtime" / "agents" / "claims"
        lock_dir.mkdir(parents=True, exist_ok=True)
        # Use first 16 chars of problem_id to keep filenames short and safe.
        safe_id = re.sub(r"[^A-Za-z0-9_-]", "_", problem_id)[:16]
        lock_path = lock_dir / f"{safe_id}.lock"
        lock = FileLock(str(lock_path), timeout=2.6)
        try:
            lock.acquire()
        except FileLockTimeout:
            logger.info(
                "[%s] Problem %s already claimed by another worker, skipping",
                self.agent_id, problem_id,
            )
            yield False
            return
        try:
            yield True
        finally:
            lock.release()

    async def _fix_one(self, analysis: AnalysisResult) -> PatchSet:
        """Generate a fix for a single analysed problem.

        In production, this calls Codex or another AI code generator
        via the existing provider pool.  The baseline implementation
        produces a stub patch for integration testing.
        """
        problem_id = analysis.problem_id
        strategy = self._select_strategy(analysis)

        start = time.monotonic()

        async with self._claim_problem(problem_id) as claimed:
            if not claimed:
                # Another concurrent worker already owns this fix.
                return PatchSet(
                    problem_id=problem_id,
                    patches=[],
                    fix_strategy_used=strategy,
                    task_family=analysis.task_family,
                    lane_id=analysis.lane_id,
                    write_scope=list(analysis.write_scope),
                    duration_seconds=0.0,
                )

            # --- Actual fix generation (pluggable) ---
            # In production: call Codex worker with self-contained prompt
            # containing problem description + affected file contents.
            # For now: generate a structured stub.
            patch = await self._generate_patch(analysis, strategy)
            patch = self._filter_patches_by_scope(analysis, patch)
            patch = self._filter_patches_by_existence(patch)
            patch = self._filter_patches_safety(patch)
            elapsed = time.monotonic() - start

            return PatchSet(
                problem_id=problem_id,
                patches=patch,
                fix_strategy_used=strategy,
                task_family=analysis.task_family,
                lane_id=analysis.lane_id,
                write_scope=list(analysis.write_scope),
                duration_seconds=round(elapsed, 2),
            )

    def _select_strategy(self, analysis: AnalysisResult) -> str:
        """Choose the best fix strategy based on historical success rates."""
        preferred = analysis.fix_strategy
        if preferred in self._strategy_stats:
            stats = self._strategy_stats[preferred]
            sr = stats.get("success", 0) / max(stats.get("total", 1), 1)
            if sr >= 0.3:
                return preferred

        # Fallback to highest success-rate strategy
        if self._strategy_stats:
            best = max(
                self._strategy_stats.items(),
                key=lambda kv: kv[1].get("success", 0) / max(kv[1].get("total", 1), 1),
            )
            if best[1].get("success", 0) > 0:
                return best[0]

        return preferred or "fix_code"

    async def _generate_patch(
        self, analysis: AnalysisResult, strategy: str
    ) -> List[Dict[str, str]]:
        """Generate patch content via AI code generation.

        4-tier provider chain:
          0. NewAPI Gateway (direct LLM via relay tokens)
          1. Web AI unified API
          2. Codex HTTP worker pool
          3. Codex CLI local executable

        Falls back to escalation when all providers fail.
        """
        # Build self-contained context with actual file contents
        file_contents = self._read_affected_files(analysis)
        context = {
            "problem_id": analysis.problem_id,
            "root_cause": analysis.root_cause,
            "fix_strategy": strategy,
            "confidence": analysis.confidence,
            "fix_description": getattr(analysis, "fix_description", ""),
        }

        user_prompt = _FIX_USER_PROMPT.format(
            problem_id=analysis.problem_id,
            root_cause=analysis.root_cause,
            fix_strategy=strategy,
            confidence=analysis.confidence,
            fix_description=context.get("fix_description", ""),
            file_contents=file_contents or "（无法读取相关文件）",
        )

        # Fallback chain: NewAPI → WebAI → Codex HTTP → Codex CLI → escalate
        fallback_log: List[str] = []

        # Tier 0: NewAPI Gateway (production relay)
        new_api_url = self.config.service_urls.get("new_api", "")
        new_api_token = self.config.service_tokens.get("new_api", "")
        if new_api_url:
            patches = await self._call_newapi_fix(
                new_api_url, new_api_token,
                _FIX_SYSTEM_PROMPT, user_prompt,
            )
            if patches:
                return patches
            fallback_log.append("newapi")

        # Tier 1: WebAI REST API
        webai_url = self.config.service_urls.get(
            "webai", "http://127.0.0.1:8000"
        )
        # Build legacy single-prompt for WebAI compatibility
        legacy_prompt = _FIX_SYSTEM_PROMPT + "\n\n" + user_prompt
        patches = await self._call_ai_fix(webai_url, legacy_prompt)
        if patches:
            return patches
        fallback_log.append("webai")

        # Tier 2: Codex worker pool fallback (if configured)
        codex_url = self.config.service_urls.get("codex", "")
        if codex_url:
            patches = await self._call_codex(codex_url, context)
            if patches:
                return patches
            fallback_log.append("codex_http")

        # Tier 3: Codex CLI fallback (local executable)
        patches = await self._call_codex_cli(legacy_prompt, analysis)
        if patches:
            return patches
        fallback_log.append("codex_cli")

        logger.warning(
            "[%s] All AI providers failed for %s — tried: %s",
            self.agent_id, analysis.problem_id, " → ".join(fallback_log),
        )

        # v2: NO stub patches — escalate instead of faking success
        problem_id = analysis.problem_id
        retry_count = self._retry_counts.get(problem_id, 0)
        if retry_count < MAX_RETRIES_PER_PROBLEM:
            self._retry_counts[problem_id] = retry_count + 1
            logger.warning(
                "[%s] AI unavailable for %s (attempt %d/%d) — marking needs_retry",
                self.agent_id, problem_id, retry_count + 1, MAX_RETRIES_PER_PROBLEM,
            )
        else:
            # Exceeded max retries — defer permanently for this session
            logger.warning(
                "[%s] Problem %s exceeded max retries (%d) — deferring",
                self.agent_id, problem_id, MAX_RETRIES_PER_PROBLEM,
            )
            await self._escalate_deferred(problem_id)

        # Return empty — Coordinator will see zero patches and record failure
        return []

    async def _escalate_deferred(self, problem_id: str) -> None:
        """Send escalation to Coordinator for a permanently deferred problem."""
        from .protocol import AgentMessage
        await self.mailbox.send(
            AgentMessage(
                source=self.agent_id,
                target="coordinator",
                msg_type="escalation",
                payload={
                    "level": "deferred",
                    "problem_id": problem_id,
                    "reason": f"Exceeded {MAX_RETRIES_PER_PROBLEM} fix attempts, AI unavailable",
                },
            )
        )

    async def _call_codex_cli(
        self, prompt: str, analysis: AnalysisResult
    ) -> List[Dict[str, str]]:
        """Call Codex CLI as final fallback for code fix generation."""
        try:
            from . import codex_bridge

            if not await codex_bridge.codex_available():
                logger.info("[%s] Codex CLI not available on PATH", self.agent_id)
                return []

            # v5: Configurable timeout (env CODEX_FIX_TIMEOUT, default 300s)
            timeout_s = int(os.environ.get("CODEX_FIX_TIMEOUT", "300"))
            result = await codex_bridge.codex_exec(
                prompt, self.config.repo_root, timeout_s=timeout_s,
            )
            if result:
                patches = self._parse_ai_patches(result)
                # v5: Validate patch quality — reject empty or comment-only patches
                patches = [p for p in patches if self._patch_is_meaningful(p)]
                if patches:
                    logger.info(
                        "[%s] Codex CLI generated %d patches for %s",
                        self.agent_id, len(patches), analysis.problem_id,
                    )
                    return patches
                else:
                    logger.warning(
                        "[%s] Codex CLI output parsed but all patches rejected as trivial",
                        self.agent_id,
                    )
        except Exception as exc:
            logger.info("[%s] Codex CLI fix unavailable: %s", self.agent_id, exc)
        return []

    @staticmethod
    def _patch_is_meaningful(patch: Dict[str, str]) -> bool:
        """Reject empty, whitespace-only, or comment-only patches."""
        content = (patch.get("patch_text") or patch.get("new_text") or patch.get("content") or "").strip()
        if not content:
            return False
        # Reject patches that are purely comments
        lines = [l.strip() for l in content.splitlines() if l.strip()]
        non_comment = [l for l in lines if not l.startswith("#") and not l.startswith("//")]
        return len(non_comment) > 0

    def _read_affected_files(self, analysis: AnalysisResult) -> str:
        """Read contents of files affected by the problem for context."""
        affected = getattr(analysis, "affected_files", None)
        if not affected and hasattr(analysis, "provider_votes"):
            # Try to extract from provider votes
            for v in (analysis.provider_votes or []):
                if isinstance(v, dict) and "affected_files" in v:
                    affected = v["affected_files"]
                    break

        if not affected:
            return ""

        parts = []
        for fpath in affected[:5]:  # Limit to 5 files
            full = self.config.repo_root / fpath
            if full.exists() and full.is_file():
                try:
                    content = full.read_text(encoding="utf-8", errors="replace")
                    # Truncate very large files
                    if len(content) > 5000:
                        content = content[:5000] + "\n... (truncated)"
                    parts.append(f"### {fpath}\n```python\n{content}\n```")
                except Exception:
                    parts.append(f"### {fpath}\n（读取失败）")
        return "\n\n".join(parts) if parts else ""

    async def _call_ai_fix(
        self, webai_url: str, prompt: str
    ) -> List[Dict[str, str]]:
        """Call Web AI unified API for code fix generation."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=240) as client:
                resp = await client.post(
                    f"{webai_url}/api/v1/webai/analyze",
                    json={
                        "provider": "chatgpt",
                        "prompt": prompt,
                        "timeout_s": 180,
                        "max_tokens": 8192,
                    },
                )
                if resp.status_code == 200:
                    payload = resp.json()
                    ai_text = payload.get("data", {}).get("response", "")
                    patches = self._parse_ai_patches(ai_text)
                    if patches:
                        logger.info("[%s] AI generated %d patches", self.agent_id, len(patches))
                        return patches
                    logger.info(
                        "[%s] AI response could not be parsed to patches (len=%d, preview=%.200s)",
                        self.agent_id, len(ai_text), ai_text[:200] if ai_text else "(empty)",
                    )
                else:
                    logger.warning("[%s] AI fix returned %d", self.agent_id, resp.status_code)
        except ImportError:
            logger.debug("[%s] httpx not available for AI fix", self.agent_id)
        except Exception as exc:
            logger.info("[%s] AI fix unavailable (%s)", self.agent_id, exc)
        return []

    async def _call_newapi_fix(
        self,
        base_url: str,
        token: str,
        system_prompt: str,
        user_prompt: str,
    ) -> List[Dict[str, str]]:
        """Call NewAPI Gateway for code fix generation (Tier-0 provider).

        Uses OpenAI-compatible /v1/chat/completions endpoint with
        the relay tokens validated in LiteLLM/ubuntu.txt.
        """
        if not base_url:
            return []
        model = os.environ.get("FIX_AGENT_MODEL", "gpt-5.3-codex")
        fallback_model = os.environ.get("FIX_AGENT_FALLBACK_MODEL", "gpt-5.4")
        timeout_s = int(os.environ.get("FIX_AGENT_NEWAPI_TIMEOUT", "240"))

        for attempt_model in (model, fallback_model):
            try:
                import httpx
                headers = {"Content-Type": "application/json"}
                if token:
                    headers["Authorization"] = f"Bearer {token}"

                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(connect=15.0, read=float(timeout_s), write=30.0, pool=10.0),
                    trust_env=False,
                ) as client:
                    resp = await client.post(
                        f"{base_url.rstrip('/')}/v1/chat/completions",
                        json={
                            "model": attempt_model,
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": user_prompt},
                            ],
                            "max_tokens": 8192,
                            "temperature": 0.2,
                        },
                        headers=headers,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        choices = data.get("choices", [])
                        if choices:
                            ai_text = choices[0].get("message", {}).get("content", "")
                            patches = self._parse_ai_patches(ai_text)
                            # Validate: reject empty/comment-only patches
                            patches = [p for p in patches if self._patch_is_meaningful(p)]
                            if patches:
                                logger.info(
                                    "[%s] NewAPI (%s) generated %d patches",
                                    self.agent_id, attempt_model, len(patches),
                                )
                                return patches
                            logger.info(
                                "[%s] NewAPI (%s) response parsed but patches empty/trivial (len=%d)",
                                self.agent_id, attempt_model, len(ai_text),
                            )
                    elif resp.status_code == 429:
                        logger.warning("[%s] NewAPI rate-limited on %s, trying fallback", self.agent_id, attempt_model)
                        await asyncio.sleep(2)
                        continue
                    else:
                        logger.warning("[%s] NewAPI (%s) returned %d", self.agent_id, attempt_model, resp.status_code)
            except ImportError:
                logger.debug("[%s] httpx not available for NewAPI", self.agent_id)
                return []
            except Exception as exc:
                logger.info("[%s] NewAPI (%s) call failed: %s", self.agent_id, attempt_model, exc)
        return []

    @staticmethod
    def _extract_json_objects(text: str) -> List[str]:
        """Extract top-level JSON objects using balanced-brace counting."""
        candidates: List[str] = []
        i = 0
        while i < len(text):
            if text[i] == '{':
                depth = 0
                start = i
                in_string = False
                escape_next = False
                for j in range(i, len(text)):
                    ch = text[j]
                    if escape_next:
                        escape_next = False
                        continue
                    if ch == '\\':
                        if in_string:
                            escape_next = True
                        continue
                    if ch == '"' and not escape_next:
                        in_string = not in_string
                        continue
                    if in_string:
                        continue
                    if ch == '{':
                        depth += 1
                    elif ch == '}':
                        depth -= 1
                        if depth == 0:
                            candidates.append(text[start:j + 1])
                            i = j
                            break
                else:
                    break  # unbalanced
            i += 1
        return candidates

    @classmethod
    def _parse_ai_patches(cls, text: str) -> List[Dict[str, str]]:
        """Parse AI-generated patches from response text."""
        if not text:
            return []

        # Strip markdown code fences to expose raw JSON
        stripped = re.sub(r"```(?:json)?\s*", "", text)
        stripped = stripped.replace("```", "")

        # Extract all JSON objects and find one with "patches" key
        for candidate in cls._extract_json_objects(stripped):
            try:
                data = json.loads(candidate)
                if isinstance(data, dict) and "patches" in data:
                    raw_patches = data["patches"]
                    result = []
                    for p in raw_patches:
                        if isinstance(p, dict) and "path" in p:
                            result.append({
                                "path": p["path"],
                                "patch_text": p.get("new_text", ""),
                                "old_text": p.get("old_text", ""),
                                "before_sha": "",
                                "explanation": p.get("explanation", ""),
                            })
                    if result:
                        return result
            except (json.JSONDecodeError, ValueError):
                continue

        # Fallback: try parsing entire stripped text as JSON
        try:
            data = json.loads(stripped.strip())
            if isinstance(data, dict) and "patches" in data:
                result = []
                for p in data["patches"]:
                    if isinstance(p, dict) and "path" in p:
                        result.append({
                            "path": p["path"],
                            "patch_text": p.get("new_text", ""),
                            "old_text": p.get("old_text", ""),
                            "before_sha": "",
                            "explanation": p.get("explanation", ""),
                        })
                if result:
                    return result
        except (json.JSONDecodeError, ValueError):
            pass

        return []

    async def _call_codex(
        self, codex_url: str, context: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Call external Codex worker pool for code generation."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=180) as client:
                resp = await client.post(
                    f"{codex_url}/v1/fix",
                    json=context,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("patches", [])
                logger.warning("[%s] Codex returned %d", self.agent_id, resp.status_code)
        except ImportError:
            pass
        except Exception as exc:
            logger.info("[%s] Codex call failed: %s", self.agent_id, exc)
        return []

    def _filter_patches_by_scope(
        self,
        analysis: AnalysisResult,
        patches: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        allowed_scope = list(analysis.write_scope or [])
        if not allowed_scope:
            return patches

        filtered: List[Dict[str, str]] = []
        for patch in patches:
            path = patch.get("path", "")
            if self._path_in_scope(path, allowed_scope):
                filtered.append(patch)
                continue
            logger.warning(
                "[%s] Rejecting out-of-scope patch for %s: %s not in %s",
                self.agent_id,
                analysis.problem_id,
                path,
                allowed_scope,
            )
        return filtered

    def _path_in_scope(self, path: str, allowed_scope: List[str]) -> bool:
        return any(self._pattern_matches_path(pattern, path) for pattern in allowed_scope)

    @staticmethod
    def _pattern_matches_path(pattern: str, path: str) -> bool:
        if pattern == path:
            return True
        prefix = pattern.split("**", 1)[0].rstrip("*").rstrip("/")
        if not prefix:
            return True
        return path.startswith(prefix)

    def _filter_patches_by_existence(
        self, patches: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """Reject patches that reference non-existent files with old_text."""
        filtered: List[Dict[str, str]] = []
        for patch in patches:
            path = patch.get("path", "")
            old_text = patch.get("old_text", "")
            if not path:
                continue
            target = self.config.repo_root / path
            if old_text and not target.exists():
                logger.warning(
                    "[%s] Rejecting patch for non-existent file: %s",
                    self.agent_id, path,
                )
                continue
            filtered.append(patch)
        return filtered

    def _filter_patches_safety(
        self, patches: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """Apply safety guardrails: path whitelist, size limit, deletion ratio."""
        filtered: List[Dict[str, str]] = []
        for patch in patches:
            path = patch.get("path", "")

            # Guardrail 1: path whitelist
            if not any(path.startswith(prefix) for prefix in _ALLOWED_PATH_PREFIXES):
                logger.warning(
                    "[%s] Rejecting patch outside allowed paths: %s",
                    self.agent_id, path,
                )
                continue

            new_text = patch.get("patch_text") or patch.get("new_text") or ""
            old_text = patch.get("old_text", "")

            # Guardrail 2: patch size limit
            change_lines = max(
                len(new_text.splitlines()),
                len(old_text.splitlines()) if old_text else 0,
            )
            if change_lines > _MAX_PATCH_LINES:
                logger.warning(
                    "[%s] Rejecting oversized patch (%d lines > %d): %s",
                    self.agent_id, change_lines, _MAX_PATCH_LINES, path,
                )
                continue

            # Guardrail 3: deletion ratio — reject if removing >50% of source
            if old_text and path:
                target = self.config.repo_root / path
                if target.exists():
                    try:
                        source_lines = len(target.read_text(encoding="utf-8", errors="replace").splitlines())
                        if source_lines > 0:
                            deleted_lines = len(old_text.splitlines())
                            added_lines = len(new_text.splitlines())
                            net_deleted = deleted_lines - added_lines
                            if net_deleted > 0 and (net_deleted / source_lines) > _MAX_PATCH_DELETE_RATIO:
                                logger.warning(
                                    "[%s] Rejecting destructive patch (deletes %.0f%% of %s)",
                                    self.agent_id, (net_deleted / source_lines) * 100, path,
                                )
                                continue
                    except Exception:
                        pass

            # Guardrail 4: SHA256 verify target file hasn't been modified concurrently
            if old_text and path:
                target = self.config.repo_root / path
                if target.exists():
                    current_content = target.read_text(encoding="utf-8", errors="replace")
                    if old_text not in current_content:
                        logger.warning(
                            "[%s] Rejecting patch — old_text not found in current file: %s",
                            self.agent_id, path,
                        )
                        continue

            filtered.append(patch)
        return filtered

    async def _handle_rollback(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle rollback request from Coordinator after failed verification."""
        patches_raw = payload.get("patches", [])
        rolled_back = []
        deferred = []

        for p in patches_raw:
            problem_id = p.get("problem_id", "")
            logger.info("[%s] Rolling back patches for %s", self.agent_id, problem_id)
            rolled_back.append(problem_id)

            # Track retries
            count = self._retry_counts.get(problem_id, 0) + 1
            self._retry_counts[problem_id] = count
            if count >= MAX_RETRIES_PER_PROBLEM:
                logger.warning(
                    "[%s] Problem %s exceeded max retries (%d) — deferring permanently",
                    self.agent_id, problem_id, MAX_RETRIES_PER_PROBLEM,
                )
                deferred.append(problem_id)
                await self._escalate_deferred(problem_id)

        return {
            "findings": [],
            "rolled_back": rolled_back,
            "deferred": deferred,
            "action": "rollback",
        }

    # ------------------------------------------------------------------
    # Strategy stats
    # ------------------------------------------------------------------

    def _load_strategy_stats(self) -> Dict[str, Dict[str, int]]:
        path = self._knowledge_path / STRATEGY_STATS_FILENAME
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}

    def _save_strategy_stats(self) -> None:
        self._knowledge_path.mkdir(parents=True, exist_ok=True)
        path = self._knowledge_path / STRATEGY_STATS_FILENAME
        tmp_path = path.with_suffix(".tmp")
        try:
            tmp_path.write_text(
                json.dumps(self._strategy_stats, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            os.replace(str(tmp_path), str(path))
        except Exception:
            logger.exception("Strategy stats save error")
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _update_strategy_stats(self, patches: List[PatchSet]) -> None:
        for p in patches:
            s = p.fix_strategy_used
            if s not in self._strategy_stats:
                self._strategy_stats[s] = {"total": 0, "success": 0}
            self._strategy_stats[s]["total"] += 1
            if p.patches:
                self._strategy_stats[s]["success"] += 1
                self._retry_counts.pop(p.problem_id, None)  # reset on success
        self._save_strategy_stats()
