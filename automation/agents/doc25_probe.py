"""Doc25-angle driven discovery probe for the Escort Team.

Uses the 39 analysis angles from ``docs/core/25_*`` (and their 4 priority
topic packages) to systematically scan ``docs/core/22_*`` for still-alive
problems that operational probes might miss.

Two scan modes:

1. **Structural scan** — regex/keyword parsing of doc22 for alive markers
   (🔴, 🟡, "Still Alive", "Residual Risk", etc.).  Always runs.
2. **Codex deep scan** — if the Codex CLI is available, sends a compact
    structural summary plus shortened doc22/doc25 excerpts to Codex for
    AI-driven gap analysis.  Optional.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from .protocol import ProblemSpec, ProblemStatus, Severity, HandlingPath

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Priority topic packages — derived from docs/core/25 chapter structure
# ---------------------------------------------------------------------------

_TOPIC_PACKAGES_PATH = Path(__file__).with_name("doc25_priority_topics.json")

_FALLBACK_PRIORITY_TOPICS: List[Dict[str, Any]] = [
    {
        "name": "truth_lineage",
        "label": "真相层与血缘",
        "angles": [1, 5, 6, 9],
        "severity": Severity.P1.value,
        "lane": "gov_registry",
        "task_family": "issue-registry",
        "suggested_approach": HandlingPath.FIX_CODE.value,
        "current_status": ProblemStatus.BLOCKED.value,
        "blocker_type": "resolved_maintenance",
        "blocked_reason": "backfill completed, citations=0, reference mismatches=0; monitor only",
        "keywords": ["真相层", "血缘", "lineage", "truth", "registry", "issue-registry"],
        "write_scope": [
            "app/governance/feature_registry.json",
            "scripts/continuous_repo_audit.py",
        ],
    },
    {
        "name": "bad_batch_runtime",
        "label": "坏批次与运行时恢复",
        "angles": [9, 10, 13, 14, 16, 24],
        "severity": Severity.P1.value,
        "lane": "runtime_blocked",
        "task_family": "runtime-external",
        "suggested_approach": HandlingPath.EXECUTION_AND_MONITORING.value,
        "current_status": ProblemStatus.BLOCKED.value,
        "blocker_type": "external_dependency",
        "blocked_reason": "runtime gate or external dependency remains unresolved",
        "keywords": ["坏批次", "运行时", "恢复", "runtime", "recovery", "runtime anchor", "anchor"],
        "write_scope": [
            "automation/loop_controller/**",
            "automation/writeback_service/**",
        ],
    },
    {
        "name": "fr07_writer_rebuild",
        "label": "FR-07 写入器重建",
        "angles": [7, 10, 12, 13, 15, 16, 38],
        "severity": Severity.P2.value,
        "lane": "gov_mapping",
        "task_family": "feature-governance",
        "suggested_approach": HandlingPath.FIX_THEN_REBUILD.value,
        "current_status": ProblemStatus.BLOCKED.value,
        "blocker_type": "external_dependency",
        "blocked_reason": "mootdx external data source unavailable",
        "keywords": ["fr-07", "writer", "rebuild", "写入器", "重建"],
        "write_scope": [
            "app/governance/build_feature_catalog.py",
            "automation/promote_prep/**",
        ],
    },
    {
        "name": "payment_auth_governance",
        "label": "支付/鉴权/治理",
        "angles": [1, 3, 4, 18, 20, 22, 35],
        "severity": Severity.P2.value,
        "lane": "gov_mapping",
        "task_family": "feature-governance",
        "suggested_approach": HandlingPath.FIX_THEN_REBUILD.value,
        "current_status": ProblemStatus.BLOCKED.value,
        "blocker_type": "external_dependency",
        "blocked_reason": "payment provider not configured, webhook/email not provisioned",
        "keywords": ["支付", "鉴权", "认证", "payment", "auth", "governance", "治理", "契约", "通知", "邮件", "notification", "email", "webhook"],
        "write_scope": [
            "app/auth/**",
            "app/payment/**",
        ],
    },
]


def _default_task_family(lane: str) -> str:
    if lane == "gov_registry":
        return "issue-registry"
    if lane == "gov_mapping":
        return "feature-governance"
    if lane == "runtime_blocked":
        return "runtime-external"
    return "general-fix"


def _default_handling_path(lane: str) -> str:
    if lane == "gov_mapping":
        return HandlingPath.FIX_THEN_REBUILD.value
    if lane == "runtime_blocked":
        return HandlingPath.EXECUTION_AND_MONITORING.value
    return HandlingPath.FIX_CODE.value


def _default_status(lane: str) -> str:
    if lane == "runtime_blocked":
        return ProblemStatus.BLOCKED.value
    return ProblemStatus.ACTIVE.value


def _normalize_topic(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None

    name = str(raw.get("name") or "").strip()
    label = str(raw.get("label") or "").strip()
    if not name or not label:
        return None

    lane = str(raw.get("lane") or "gov_registry").strip() or "gov_registry"
    severity = str(raw.get("severity") or Severity.P2.value).strip() or Severity.P2.value
    angles: List[int] = []
    for item in raw.get("angles", []):
        try:
            angles.append(int(item))
        except (TypeError, ValueError):
            continue

    keywords = [str(item).strip().lower() for item in raw.get("keywords", []) if str(item).strip()]
    for fragment in re.split(r"[\s/与]+", label.lower()):
        fragment = fragment.strip()
        if fragment:
            keywords.append(fragment)
    for fragment in name.lower().split("_"):
        fragment = fragment.strip()
        if fragment:
            keywords.append(fragment)

    return {
        "name": name,
        "label": label,
        "angles": angles,
        "severity": severity,
        "lane": lane,
        "task_family": str(raw.get("task_family") or _default_task_family(lane)).strip(),
        "suggested_approach": str(raw.get("suggested_approach") or _default_handling_path(lane)).strip(),
        "current_status": str(raw.get("current_status") or _default_status(lane)).strip(),
        "blocker_type": str(raw.get("blocker_type") or "").strip(),
        "blocked_reason": str(raw.get("blocked_reason") or "").strip(),
        "keywords": sorted(set(keywords)),
        "write_scope": [
            str(item).strip() for item in raw.get("write_scope", []) if str(item).strip()
        ],
    }


def _load_priority_topics(config_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    path = config_path or _TOPIC_PACKAGES_PATH
    fallback = [topic for topic in (_normalize_topic(item) for item in _FALLBACK_PRIORITY_TOPICS) if topic]

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback

    raw_topics = payload.get("topics") if isinstance(payload, dict) else payload
    if not isinstance(raw_topics, list):
        return fallback

    topics = [topic for topic in (_normalize_topic(item) for item in raw_topics) if topic]
    return topics or fallback


PRIORITY_TOPICS: List[Dict[str, Any]] = _load_priority_topics()

# Keywords that indicate an alive / unresolved problem in doc22
_ALIVE_PATTERNS = [
    re.compile(r"🔴", re.UNICODE),
    re.compile(r"🟡", re.UNICODE),
    re.compile(r"Still\s+Alive", re.IGNORECASE),
    re.compile(r"仍存活", re.UNICODE),
    re.compile(r"Residual\s+Risk", re.IGNORECASE),
    re.compile(r"残余风险", re.UNICODE),
    re.compile(r"External\s+Block", re.IGNORECASE),
    re.compile(r"外部阻塞", re.UNICODE),
]

# Section-2 heading pattern (problem entries)
_SECTION2_HEADING = re.compile(
    r"^#{2,4}\s+(?:P[012]|ISSUE)[\s\-:：]*(.*)", re.MULTILINE
)


# ---------------------------------------------------------------------------
# Probe (imported by discovery.py — registered into ALL_PROBES)
# ---------------------------------------------------------------------------

# Import Probe base from discovery to avoid circular import issues at
# module level — we use a lazy import in the class body instead.
try:
    from .discovery import Probe
except ImportError:  # pragma: no cover — for standalone testing
    from abc import ABC, abstractmethod

    class Probe(ABC):  # type: ignore[no-redef]
        name: str = "base"

        def __init__(self, repo_root: Path):
            self.repo_root = repo_root

        @abstractmethod
        async def scan(self) -> List[ProblemSpec]: ...


class Doc25AngleProbe(Probe):
    """Discover doc22 problems using doc25 analysis angles."""

    name = "doc25_angle"

    def __init__(self, repo_root: Path, topic_config_path: Optional[Path] = None):
        super().__init__(repo_root)
        self._doc22_path: Optional[Path] = None
        self._doc25_path: Optional[Path] = None
        self._topics = _load_priority_topics(topic_config_path)
        self._topics_by_name = {topic["name"]: topic for topic in self._topics}
        self._resolve_docs()

    # ------------------------------------------------------------------
    # Doc resolution
    # ------------------------------------------------------------------

    def _resolve_docs(self) -> None:
        core = self.repo_root / "docs" / "core"
        if not core.is_dir():
            return
        for p in sorted(core.iterdir()):
            name = p.name.lower()
            if not self._doc22_path and "22_" in name and name.endswith(".md"):
                self._doc22_path = p
            if not self._doc25_path and "25_" in name and name.endswith(".md"):
                self._doc25_path = p

    # ------------------------------------------------------------------
    # Main scan
    # ------------------------------------------------------------------

    async def scan(self) -> List[ProblemSpec]:
        if not self._doc22_path or not self._doc22_path.exists():
            logger.info("[doc25_probe] doc22 not found — skipping")
            return []

        doc22_text = self._read_doc(self._doc22_path)
        if not doc22_text:
            return []

        # Mode A: structural scan (always runs)
        problems = self._structural_scan(doc22_text)

        # Mode B: Codex deep scan (if CLI available)
        try:
            from . import codex_bridge

            is_available = await asyncio.wait_for(
                codex_bridge.codex_available(), timeout=10,
            )
            if is_available:
                doc25_text = self._read_doc(self._doc25_path) if self._doc25_path else ""
                ai_problems = await self._codex_scan(doc22_text, doc25_text, problems)
                problems.extend(ai_problems)
        except asyncio.TimeoutError:
            logger.info("[doc25_probe] Codex availability check timed out")
        except Exception as exc:
            logger.info("[doc25_probe] Codex scan skipped: %s", exc)

        return self._deduplicate(problems)

    # ------------------------------------------------------------------
    # Structural scan
    # ------------------------------------------------------------------

    def _structural_scan(self, doc22_text: str) -> List[ProblemSpec]:
        """Parse doc22 for alive problem markers and map to topic packages."""
        problems: List[ProblemSpec] = []

        # Split into lines for context-aware scanning
        lines = doc22_text.splitlines()

        current_heading = ""
        in_code_block = False
        in_closed_section = False
        for i, line in enumerate(lines):
            # Track code blocks to skip them
            if line.strip().startswith("```"):
                in_code_block = not in_code_block
                continue
            if in_code_block:
                continue

            # Track closed/resolved sections
            heading_m = _SECTION2_HEADING.match(line)
            if heading_m:
                current_heading = heading_m.group(1).strip()
                # Reset closed-section flag on new heading
                in_closed_section = False

            lower_line = line.lower()
            if any(kw in lower_line for kw in ("已关闭", "closed", "resolved", "✅")):
                in_closed_section = True
            if in_closed_section:
                continue

            # Check for alive patterns
            for pat in _ALIVE_PATTERNS:
                if pat.search(line):
                    # Found an alive marker — create problem
                    ctx_start = max(0, i - 2)
                    ctx_end = min(len(lines), i + 3)
                    context = "\n".join(lines[ctx_start:ctx_end])

                    topic = self._match_topic(current_heading, context)
                    problem_id = self._make_id(current_heading, line, i)
                    # v5: auto-attach top-3 recommended doc25 angles from matched topic
                    recommended = topic["angles"][:5] if topic else self._infer_angles(context)

                    # v9: Force external-blocker classification for problems
                    # that contain external dependency markers — prevents them
                    # from entering the analysis/fix lane and consuming cycles.
                    is_external_blocked = self._is_external_blocker(context, current_heading)
                    if is_external_blocked:
                        effective_status = ProblemStatus.BLOCKED.value
                        effective_approach = HandlingPath.EXTERNAL_DEPENDENCY.value
                        effective_blocker_type = "external_dependency"
                        effective_blocked_reason = (
                            topic["blocked_reason"] if topic and topic.get("blocked_reason")
                            else "external dependency or manual action required"
                        )
                    else:
                        # v10: Non-external-blocker issues are ACTIVE and
                        # fixable — topic defaults may be BLOCKED for the
                        # external-blocker path, so override to ACTIVE here.
                        effective_status = ProblemStatus.ACTIVE.value
                        effective_approach = (
                            topic["suggested_approach"] if topic else HandlingPath.FIX_CODE.value
                        )
                        effective_blocker_type = ""
                        effective_blocked_reason = ""

                    problems.append(ProblemSpec(
                        problem_id=problem_id,
                        source_probe=self.name,
                        severity=topic["severity"] if topic else Severity.P2.value,
                        family=topic["name"] if topic else "doc22_alive",
                        task_family=topic["task_family"] if topic else "",
                        lane_id=topic["lane"] if topic else "gov_registry",
                        title=current_heading or f"doc22 L{i + 1}",
                        description=context,
                        affected_files=[],
                        affected_frs=[],
                        suggested_approach=effective_approach,
                        current_status=effective_status,
                        blocker_type=effective_blocker_type,
                        blocked_reason=effective_blocked_reason,
                        recommended_angles=recommended,
                        write_scope=topic["write_scope"] if topic else [],
                    ))
                    break  # one problem per line

        return problems

    # ------------------------------------------------------------------
    # Codex deep scan
    # ------------------------------------------------------------------

    async def _codex_scan(
        self,
        doc22_text: str,
        doc25_text: str,
        structural_problems: List[ProblemSpec],
    ) -> List[ProblemSpec]:
        """Use Codex CLI for AI-driven gap analysis."""
        from . import codex_bridge

        prompt = self._build_codex_prompt(doc22_text, doc25_text, structural_problems)
        provider = codex_bridge.detect_provider(self.repo_root)
        result = await codex_bridge.codex_exec(
            prompt,
            self.repo_root,
            timeout_s=120,
            provider=provider,
        )
        if not result:
            return []
        return self._parse_codex_result(result)

    def _build_codex_prompt(
        self,
        doc22_text: str,
        doc25_text: str,
        structural_problems: List[ProblemSpec],
    ) -> str:
        # Keep the Codex payload compact: structural scan is the primary discovery
        # source, while Codex acts as a supplement for missed/under-classified issues.
        doc22_excerpt = doc22_text[:3000] if len(doc22_text) > 3000 else doc22_text
        doc25_excerpt = doc25_text[:1500] if len(doc25_text) > 1500 else doc25_text
        structural_summary = self._format_structural_summary(structural_problems)

        topic_list = "\n".join(
            (
                f"- {t['name']}: 角度 {t['angles']} — {t['label']} "
                f"| lane={t['lane']} | task_family={t['task_family']}"
            )
            for t in self._topics
        )

        return f"""\
以 doc25 的分析角度系统审查 doc22，发现仍存活的问题。

## doc22 内容（摘要）
{doc22_excerpt}

## doc25 分析角度（摘要）
{doc25_excerpt}

## 结构化预扫描结果（优先参考）
{structural_summary}

## 优先专题包
{topic_list}

请先基于“结构化预扫描结果”判断是否还有遗漏或错分；若结构化结果已充分覆盖，则返回 []。
仅补充真正新增的问题，或对已有问题做更合理的专题归属/严重度判断；不要重复复述预扫描已经明确的问题。
最多返回 6 项。

请用以下JSON格式回复（只返回JSON数组，不要其他内容）:
```json
[
  {{
    "problem_id": "doc25_<topic>_<编号>",
    "severity": "P1 或 P2",
    "family": "topic_name",
    "title": "问题标题",
    "description": "问题描述",
    "recommended_angles": [角度编号列表],
    "lane_id": "gov_registry 或 gov_mapping 或 runtime_blocked"
  }}
]
```
"""

    @staticmethod
    def _format_structural_summary(problems: List[ProblemSpec], *, max_items: int = 8) -> str:
        if not problems:
            return "- 无结构化预扫描结果"

        lines = [f"- total={len(problems)}"]
        for problem in problems[:max_items]:
            description = re.sub(r"\s+", " ", problem.description or "").strip()
            if len(description) > 160:
                description = description[:157] + "..."
            lines.append(
                "- "
                f"{problem.problem_id} | severity={problem.severity} | family={problem.family} "
                f"| lane={problem.lane_id} | status={problem.current_status} | title={problem.title} "
                f"| desc={description}"
            )
        if len(problems) > max_items:
            lines.append(f"- ... remaining={len(problems) - max_items}")
        return "\n".join(lines)

    def _parse_codex_result(self, text: str) -> List[ProblemSpec]:
        """Parse Codex JSON output into ProblemSpec list."""
        # Try to extract JSON array from response
        match = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", text, re.DOTALL)
        if match:
            json_text = match.group(1)
        else:
            match = re.search(r"\[.*\]", text, re.DOTALL)
            if match:
                json_text = match.group(0)
            else:
                return []

        try:
            items = json.loads(json_text)
        except (json.JSONDecodeError, ValueError):
            logger.warning("[doc25_probe] Failed to parse Codex JSON")
            return []

        if not isinstance(items, list):
            return []

        problems: List[ProblemSpec] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            topic = self._topics_by_name.get(str(item.get("family") or "").strip())
            problems.append(ProblemSpec(
                problem_id=item.get("problem_id", f"doc25_ai_{len(problems)}"),
                source_probe=self.name,
                severity=item.get("severity", Severity.P2.value),
                family=item.get("family", "doc25_ai"),
                task_family=item.get("task_family", topic["task_family"] if topic else ""),
                title=item.get("title", ""),
                description=item.get("description", ""),
                affected_files=[],
                affected_frs=[],
                suggested_approach=item.get(
                    "suggested_approach",
                    topic["suggested_approach"] if topic else HandlingPath.FIX_CODE.value,
                ),
                current_status=item.get(
                    "current_status",
                    topic["current_status"] if topic else ProblemStatus.ACTIVE.value,
                ),
                blocker_type=item.get("blocker_type", topic["blocker_type"] if topic else ""),
                blocked_reason=item.get("blocked_reason", topic["blocked_reason"] if topic else ""),
                recommended_angles=item.get("recommended_angles", []),
                lane_id=item.get("lane_id", topic["lane"] if topic else "gov_registry"),
                write_scope=topic["write_scope"] if topic else [],
            ))
        return problems

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _read_doc(path: Optional[Path]) -> str:
        if not path or not path.exists():
            return ""
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return ""

    def _match_topic(self, heading: str, context: str) -> Optional[Dict[str, Any]]:
        """Match heading/context to the best priority topic package."""
        combined = (heading + " " + context).lower()
        best: Optional[Dict[str, Any]] = None
        best_score = 0
        for topic in self._topics:
            score = 0
            if topic["label"].lower() in combined:
                score += 4
            for keyword in topic.get("keywords", []):
                if keyword and keyword in combined:
                    score += 2
            for frag in topic["name"].split("_"):
                if frag in combined:
                    score += 1
            if score > best_score:
                best_score = score
                best = topic
        return best if best_score > 0 else None

    @staticmethod
    def _is_external_blocker(context: str, heading: str) -> bool:
        """Detect whether a problem is blocked by an external dependency.

        v9: Checks for known patterns indicating the problem cannot be
        resolved by code changes alone (e.g. mootdx data source outage,
        git status cleanup, external API configuration).
        """
        combined = (heading + " " + context).lower()
        _EXTERNAL_MARKERS = (
            "外部阻塞",
            "external block",
            "外部数据源",
            "mootdx",
            "非代码缺陷",
            "人工核验",
            "人工处理",
            "人工配置",
            "provider-not-configured",
            "git status",
            "repo 纳管",
            "legacy/archive",
            "外部接入",
            "仍未实现",
            "未配置",
            "诚实 skipped",
            "honest skipped",
            "邮件传输",
            "email not configured",
        )
        return any(marker in combined for marker in _EXTERNAL_MARKERS)

    @staticmethod
    def _make_id(heading: str, line: str, lineno: int) -> str:
        """Deterministic problem ID from heading + line content."""
        raw = f"doc25_{heading}_{lineno}_{line[:50]}"
        h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
        return f"doc25_{h}"

    @staticmethod
    def _infer_angles(context: str) -> List[int]:
        """Heuristic angle inference when no topic package matched."""
        lower = context.lower()
        angles: List[int] = []
        # doc25 角度关键词映射（精选高频子集）
        hints = [
            ([1], ["事实", "truth", "真实性", "伪造", "fake"]),
            ([3], ["血缘", "lineage", "溯源"]),
            ([5], ["contract", "契约", "schema"]),
            ([9], ["anchor", "锚点", "runtime_trade_date"]),
            ([10], ["recovery", "恢复", "blocked"]),
            ([17], ["token", "auth", "认证"]),
            ([25], ["bridge", "桥接"]),
            ([30], ["test", "测试", "blind_spot"]),
            ([34], ["registry", "catalog", "治理"]),
        ]
        for ids, kws in hints:
            if any(kw in lower for kw in kws):
                angles.extend(ids)
        return sorted(set(angles))[:5]

    @staticmethod
    def _deduplicate(problems: List[ProblemSpec]) -> List[ProblemSpec]:
        seen: set[str] = set()
        unique: List[ProblemSpec] = []
        for p in problems:
            if p.problem_id not in seen:
                seen.add(p.problem_id)
                unique.append(p)
        return unique
