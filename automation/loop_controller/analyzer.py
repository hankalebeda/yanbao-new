"""Problem analysis engine — extracts and prioritises problems from audit bundles."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from automation.loop_controller.schemas import (
    ProblemSpec,
    Severity,
)

# ---------------------------------------------------------------------------
# Constants aligned to docs/core/25_系统问题分析角度清单.md 39 angles
# ---------------------------------------------------------------------------

CATEGORY_MAP: dict[str, str] = {
    "truth-lineage": "事实/契约/数据",
    "runtime-anchor": "时间/运行态/恢复",
    "fr07-rebuild": "时间/运行态/恢复",
    "fr06-failure-semantics": "事实/契约/数据",
    "payment-auth-governance": "安全/权限/边界",
    "internal-contracts": "事实/契约/数据",
    "shared-artifacts": "测试/治理/决策",
    "issue-registry": "测试/治理/决策",
    "repo-governance": "测试/治理/决策",
    "external-integration": "安全/权限/边界",
    "display-bridge": "展示/桥接/观测",
    "execution-order": "时间/运行态/恢复",
}

# Mapping: family -> recommended diagnosis angles from 25_清单
# Extended to cover all 39 angles — see docs/core/25_系统问题分析角度清单.md
DIAGNOSIS_ANGLES: dict[str, list[int]] = {
    "truth-lineage": [1, 2, 5, 6, 9],           # +2 状态语义诚实性
    "runtime-anchor": [9, 10, 11, 13, 14, 16, 24],  # +11 降级fail-close
    "fr07-rebuild": [7, 10, 12, 13, 15, 16, 38],
    "fr06-failure-semantics": [1, 5, 6, 30, 33],
    "payment-auth-governance": [1, 3, 4, 17, 18, 20, 22, 23, 35],  # +23 人工核验缺口
    "internal-contracts": [3, 4, 7, 17, 21],     # +21 internal最小暴露
    "shared-artifacts": [30, 31, 32, 33],
    "issue-registry": [30, 31, 34, 35, 36, 37],  # +34 FR/NFR映射 +36 缺口分类 +37 台账状态
    "repo-governance": [30, 31, 32],
    "external-integration": [17, 18, 19, 20],
    "display-bridge": [8, 25, 26, 27, 28, 29],   # +8 跨视图一致性 +29 监控可信度
    "execution-order": [9, 10, 13, 14, 39],       # +39 优先级基于真实根因
}

# default risk level per family (from mesh_runner/runner.py FAMILY_DEFAULTS)
FAMILY_SEVERITY: dict[str, Severity] = {
    "truth-lineage": Severity.P1,
    "runtime-anchor": Severity.P1,
    "fr07-rebuild": Severity.P1,
    "fr06-failure-semantics": Severity.P1,
    "payment-auth-governance": Severity.P1,
    "internal-contracts": Severity.P1,
    "shared-artifacts": Severity.P1,
    "issue-registry": Severity.P1,
    "repo-governance": Severity.P2,
    "external-integration": Severity.P2,
    "display-bridge": Severity.P2,
    "execution-order": Severity.P2,
}

EXTERNAL_BLOCKED_KEYWORDS = frozenset({
    "mootdx",
    "provider-not-configured",
    "external_data_source",
    "外部数据源",
    "data_source_unavailable",
    "freeze_or_isolate",
    "k-line",
    "kline",
})

FIXABLE_HANDLING_PATHS = frozenset({"fix_code", "fix_then_rebuild"})

# recommended_action values that signal external/manual blockers
_BLOCKED_ACTION_KEYWORDS = frozenset({
    "freeze_or_isolate",
    "degrade_and_monitor",
    "manual_verify",
})

SEVERITY_ORDER: dict[Severity, int] = {Severity.P0: 0, Severity.P1: 1, Severity.P2: 2, Severity.P3: 3}

# write_scope defaults per family (aligned to runner.py FAMILY_DEFAULTS)
FAMILY_WRITE_SCOPE: dict[str, list[str]] = {
    "truth-lineage": ["app/services", "app/governance"],
    "runtime-anchor": ["app/services", "app/core"],
    "fr07-rebuild": ["app/services", "app/api"],
    "fr06-failure-semantics": ["app/services", "app/api"],
    "payment-auth-governance": ["app/services", "app/api"],
    "internal-contracts": ["app/core", "app/api"],
    "shared-artifacts": ["app/governance", "scripts"],
    "issue-registry": ["app/governance"],
    "repo-governance": ["scripts"],
    "external-integration": ["app/services"],
    "display-bridge": ["app/web", "app/api"],
    "execution-order": ["app/core", "scripts"],
}


def _fingerprint(data: Any) -> str:
    raw = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _is_external_blocked(finding: dict[str, Any]) -> bool:
    text = json.dumps(finding, ensure_ascii=False).lower()
    return any(kw in text for kw in EXTERNAL_BLOCKED_KEYWORDS)


def _extract_affected_frs(finding: dict[str, Any]) -> list[str]:
    text = json.dumps(finding, ensure_ascii=False)
    return sorted(set(re.findall(r"FR-\d{2}(?:-[a-z])?", text)))


def _extract_affected_files(finding: dict[str, Any]) -> list[str]:
    text = json.dumps(finding, ensure_ascii=False)
    return sorted(set(re.findall(r"(?:app|tests|scripts)/[\w/]+\.py", text)))


def _severity_from_finding(finding: dict[str, Any], family: str) -> Severity:
    """Determine severity — promote to P0 if explicitly tagged."""
    risk = finding.get("risk_level", finding.get("severity", "")).upper()
    if risk in ("P0",):
        return Severity.P0
    return FAMILY_SEVERITY.get(family, Severity.P2)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_bundle(
    bundle: dict[str, Any],
    fixed_problems: list[str],
) -> tuple[list[ProblemSpec], int, int, int]:
    """Analyse an audit bundle and return (problems, new, regressions, skipped).

    *bundle* is the ``bundle.json`` produced by mesh_runner, expected to
    contain a ``findings`` list, each having ``family``, ``finding_id``,
    and arbitrary detail fields.

    *fixed_problems* is the list of previously-fixed problem IDs used for
    deduplication and regression detection.
    """
    findings: list[dict[str, Any]] = bundle.get("findings", [])
    if not findings and "shards" in bundle:
        # alternate structure: bundle may contain shards with nested findings
        for shard in bundle.get("shards", []):
            findings.extend(shard.get("findings", []))

    problems: list[ProblemSpec] = []
    new_count = 0
    regression_count = 0
    skipped_count = 0

    seen_ids: set[str] = set()

    for finding in findings:
        family = finding.get("family") or finding.get("issue_key", "unknown")
        finding_id = finding.get("finding_id") or finding.get("id") or finding.get("issue_key") or _fingerprint(finding)
        problem_id = f"{family}:{finding_id}"

        if problem_id in seen_ids:
            continue
        seen_ids.add(problem_id)

        is_regression = problem_id in fixed_problems
        handling_path = str(finding.get("handling_path") or "").strip().lower()
        is_fixable = not handling_path or handling_path in FIXABLE_HANDLING_PATHS
        is_blocked = _is_external_blocked(finding) or not is_fixable

        # Step 3.2: Also check recommended_action for blocker signals
        if not is_blocked:
            rec_action = str(finding.get("recommended_action") or "").strip().lower()
            if any(kw in rec_action for kw in _BLOCKED_ACTION_KEYWORDS):
                is_blocked = True

        if is_blocked:
            skipped_count += 1
            # still record it but mark as external_blocked
            problems.append(ProblemSpec(
                problem_id=problem_id,
                severity=_severity_from_finding(finding, family),
                category=CATEGORY_MAP.get(family, "未分类"),
                family=family,
                title=finding.get("title", finding.get("summary", problem_id)),
                description=finding.get("description", ""),
                diagnosis_angles=DIAGNOSIS_ANGLES.get(family, []),
                affected_files=_extract_affected_files(finding),
                affected_frs=_extract_affected_frs(finding),
                suggested_fix_approach=finding.get("recommended_action", ""),
                write_scope=FAMILY_WRITE_SCOPE.get(family, []),
                is_regression=is_regression,
                is_external_blocked=True,
            ))
            continue

        if is_regression:
            regression_count += 1
        else:
            new_count += 1

        severity = _severity_from_finding(finding, family)
        if is_regression and severity != Severity.P0:
            # promote regression severity by one level
            order = SEVERITY_ORDER.get(severity, 2)
            promoted = max(0, order - 1)
            severity = {0: Severity.P0, 1: Severity.P1}.get(promoted, severity)

        problems.append(ProblemSpec(
            problem_id=problem_id,
            severity=severity,
            category=CATEGORY_MAP.get(family, "未分类"),
            family=family,
            title=finding.get("title", finding.get("summary", problem_id)),
            description=finding.get("description", ""),
            diagnosis_angles=DIAGNOSIS_ANGLES.get(family, []),
            affected_files=_extract_affected_files(finding),
            affected_frs=_extract_affected_frs(finding),
            suggested_fix_approach=finding.get("recommended_action", ""),
            write_scope=FAMILY_WRITE_SCOPE.get(family, []),
            is_regression=is_regression,
            is_external_blocked=False,
        ))

    # sort: P0 first, then P1, P2, P3; regressions first within same severity
    problems.sort(key=lambda p: (
        SEVERITY_ORDER.get(p.severity, 9),
        not p.is_regression,
        p.problem_id,
    ))

    return problems, new_count, regression_count, skipped_count


def detect_drift(
    current_fingerprints: dict[str, str],
    previous_fingerprints: dict[str, str],
) -> list[str]:
    """Return list of artifact paths whose fingerprint changed."""
    drifted: list[str] = []
    for path, fp in current_fingerprints.items():
        if path in previous_fingerprints and previous_fingerprints[path] != fp:
            drifted.append(path)
    return drifted
