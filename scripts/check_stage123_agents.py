from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DOC_01 = REPO_ROOT / "docs" / "core" / "01_需求基线.md"
DOC_02 = REPO_ROOT / "docs" / "core" / "02_系统架构.md"
DOC_99 = REPO_ROOT / "docs" / "core" / "99_AI驱动系统开发与Skill转化指南.md"
AGENTS_DOC = REPO_ROOT / "AGENTS.md"
PROMPT_DOC = REPO_ROOT / "docs" / "提示词" / "18_全量自动化提示词.md"
ISSUE_REGISTER = REPO_ROOT / "docs" / "_temp" / "stage123_loop" / "issue_register.md"
REVIEW_LOG = REPO_ROOT / "docs" / "_temp" / "stage123_loop" / "review_log.md"
RULES_DIR = REPO_ROOT / ".cursor" / "rules"
SKILLS_DIR = REPO_ROOT / ".cursor" / "skills"

NEW_SKILLS = {
    "SC-STAGE123-optimization-suggester": {
        "section": "#### 5.5.23 `SC-STAGE123-optimization-suggester`",
        "target": ".cursor/skills/SC-STAGE123-optimization-suggester/SKILL.md",
    },
    "SC-STAGE123-audit-executor": {
        "section": "#### 5.5.24 `SC-STAGE123-audit-executor`",
        "target": ".cursor/skills/SC-STAGE123-audit-executor/SKILL.md",
    },
}

EXPECTED_RULES = {
    "auto-multi-ai-analysis.mdc",
    "role-commerce.mdc",
    "role-data-engineer.mdc",
    "role-frontend-ux.mdc",
    "role-report-engineer.mdc",
    "role-test-quality.mdc",
}

FORBIDDEN_GENERIC_OUTPUT_PATTERNS = (
    "Outputs: patch set,",
    "Outputs: consistency report, blocker list, patch set",
    "Outputs: contract tests, diff report, blocker list",
)

PROMPT_HEADINGS = [
    "## Prompt 1：单能力实现主提示词",
    "## Prompt 2：建议 agent",
    "## Prompt 3：审核执行 agent",
    "## Prompt 4：双 agent 控制器",
]

REQUIRED_PROMPT_MARKERS = [
    "【角色】",
    "【任务】",
    "【规格来源】",
    "【边界约束】",
    "【产出文件】",
    "【验收命令】",
]

ALLOWED_CORE_DOCS = {
    "docs/core/01_需求基线.md",
    "docs/core/02_系统架构.md",
    "docs/core/03_详细设计.md",
    "docs/core/04_数据治理与血缘.md",
    "docs/core/05_API与数据契约.md",
    "docs/core/99_AI驱动系统开发与Skill转化指南.md",
}

FORBIDDEN_99_EXECUTION_DRIFT = [
    "docs/old/core/26_问题清单.md",
    "docs/old/core/25_生产就绪差距与执行计划.md",
    "辅助文档（重写期间按需新建或参考 old）",
    "40_自动化主流程、41_差距分析与代码验证 → 参考 docs/old/core/",
    "25_生产就绪差距与执行计划、26_问题清单 → 参考 docs/old/core/",
]

FORBIDDEN_99_PRIORITY_DRIFT = [
    "P1：hotspot 采集 → market_data 行情 → report_engine 研报生成 → sim 模拟持仓",
    "P2：用户系统（auth/membership）→ 调度器（scheduler）→ 股票池（stock_pool）",
]

FORBIDDEN_99_FR_MAPPING_DRIFT = [
    "Cookie 会话管理（FR-02）",
    "定时调度（FR-08）",
    "三级会员权益（FR-06）",
    "回撤熔断（FR-04 子功能）",
    "LLM 多层降级（FR-03 依赖）",
    "FR-03：模型生成研报",
    "| FR-03 研报生成 | Rule |",
    "| FR-05 前端展示 | Rule |",
    "| FR-07 测试与质量 | Rule |",
    "□ FR-03 研报详情页：",
    "□ FR-07 权益差异：",
]

FORBIDDEN_99_HOTSPOT_EXAMPLE_DRIFT = [
    "新增知乎热搜采集",
    "HotspotRaw",
    "实现热搜数据采集（微博/抖音/知乎/东财）",
    "/hotspot/collect",
    "tests/test_api.py",
    "## 行动 21 填写示例（FR-01 知乎热搜，完整版）",
    "新增知乎热搜平台的采集能力",
    "本次扩展为支持 zhihu",
    "fetch_zhihu_hot",
    "elif platform == \"zhihu\"",
]

FORBIDDEN_99_RULE_TRIGGER_DRIFT = [
    "开 tests/ 与 01/04/05/99 等治理工件时自动激活",
]

FORBIDDEN_AGENTS_RULE_TRIGGER_DRIFT = [
    "打开 `tests/**` 或 01/04 文档时应用",
]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def ok(message: str) -> None:
    print(f"OK   {message}")


def fail(message: str, errors: list[str]) -> None:
    print(f"FAIL {message}")
    errors.append(message)


def extract_section(text: str, heading: str) -> str | None:
    start = text.find(heading)
    if start == -1:
        return None
    next_match = re.search(r"^## ", text[start + len(heading) :], flags=re.MULTILINE)
    if not next_match:
        return text[start:]
    return text[start : start + len(heading) + next_match.start()]


def extract_marker_block(section: str, marker: str, next_marker: str) -> str | None:
    pattern = re.escape(marker) + r"\s*(.*?)\s*" + re.escape(next_marker)
    match = re.search(pattern, section, flags=re.DOTALL)
    if not match:
        return None
    return match.group(1)


def parse_open_issue_count(text: str) -> int | None:
    match = re.search(r"open_issue_count:\s*(\d+)", text)
    if not match:
        return None
    return int(match.group(1))


def parse_zero_open_streak(text: str) -> int | None:
    match = re.search(r"zero_open_streak:\s*(\d+)", text)
    if not match:
        return None
    return int(match.group(1))


def parse_closed_issue_count(text: str) -> int | None:
    match = re.search(r"closed_issue_count:\s*(\d+)", text)
    if not match:
        return None
    return int(match.group(1))


def parse_reviewed_issue_count(text: str) -> int | None:
    match = re.search(r"reviewed_issue_count:\s*(\d+)", text)
    if not match:
        return None
    return int(match.group(1))


def count_closed_issue_rows(text: str) -> int:
    match = re.search(
        r"## Closed Issues\s+.*?\n\| --- .*?\n(.*)",
        text,
        flags=re.DOTALL,
    )
    if not match:
        return 0
    body = match.group(1)
    return sum(
        1
        for line in body.splitlines()
        if line.startswith("| ")
        and "| ---" not in line
        and "| issue_id " not in line
    )


def count_review_decisions(text: str) -> int:
    return len(
        re.findall(r"^- .* / .* / decision=", text, flags=re.MULTILINE)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate stage1-3 dual-agent scaffolding.")
    parser.add_argument(
        "--allow-open-issues",
        action="store_true",
        help="Skip the final zero-open-issues enforcement.",
    )
    args = parser.parse_args()

    errors: list[str] = []

    doc_01_text = read_text(DOC_01)
    doc_02_text = read_text(DOC_02)
    doc_99_text = read_text(DOC_99)
    agents_text = read_text(AGENTS_DOC)
    prompt_text = read_text(PROMPT_DOC)
    issue_text = read_text(ISSUE_REGISTER)
    review_text = read_text(REVIEW_LOG)

    for skill_id, meta in NEW_SKILLS.items():
        if f"| `{skill_id}` | Skill |" not in doc_01_text:
            fail(f"{skill_id} missing from 01 §5.3 table", errors)
        else:
            ok(f"{skill_id} frozen in 01 §5.3")

        if meta["section"] not in doc_01_text:
            fail(f"{skill_id} missing structured section in 01 §5.5", errors)
        else:
            ok(f"{skill_id} structured section exists in 01 §5.5")

        if meta["target"] not in doc_01_text:
            fail(f"{skill_id} target path missing in 01 §5.5", errors)
        else:
            ok(f"{skill_id} target path frozen in 01")

        skill_path = REPO_ROOT / meta["target"]
        if not skill_path.exists():
            fail(f"{skill_path} missing", errors)
            continue
        ok(f"{skill_path.relative_to(REPO_ROOT)} exists")

        skill_text = read_text(skill_path)
        if f"# {skill_id}" not in skill_text:
            fail(f"{skill_id} heading missing in {skill_path.name}", errors)
        else:
            ok(f"{skill_id} heading present")

        if "Inputs:" not in skill_text or "Outputs:" not in skill_text:
            fail(f"{skill_id} missing Inputs/Outputs block", errors)
        else:
            ok(f"{skill_id} Inputs/Outputs present")

        if "Equivalent replay:" not in skill_text or "Verification:" not in skill_text:
            fail(f"{skill_id} missing replay or verification instructions", errors)
        else:
            ok(f"{skill_id} replay and verification present")

    if "阶段 1-3 的治理型 agent" not in doc_02_text:
        fail("02 §4.7 governance-agent constraint missing", errors)
    else:
        ok("02 §4.7 governance-agent constraint present")

    if "双 agent 循环" not in doc_99_text or "issue_register.md" not in doc_99_text:
        fail("99 action 18/19 dual-agent workflow missing", errors)
    else:
        ok("99 action 18/19 dual-agent workflow present")

    drift_hits = [token for token in FORBIDDEN_99_EXECUTION_DRIFT if token in doc_99_text]
    if drift_hits:
        fail(
            "99 still routes live execution to docs/old/core artifacts: "
            + ", ".join(drift_hits),
            errors,
        )
    else:
        ok("99 keeps docs/old/core archives out of live execution guidance")

    priority_hits = [token for token in FORBIDDEN_99_PRIORITY_DRIFT if token in doc_99_text]
    if priority_hits:
        fail(
            "99 still contains the stale module-priority block that conflicts with 01 §5.1: "
            + ", ".join(priority_hits),
            errors,
        )
    else:
        ok("99 implementation-order reference aligns with 01 §5.1 priority freeze")

    fr_mapping_hits = [token for token in FORBIDDEN_99_FR_MAPPING_DRIFT if token in doc_99_text]
    if fr_mapping_hits:
        fail(
            "99 still contains stale FR-number examples that conflict with 01: "
            + ", ".join(fr_mapping_hits),
            errors,
        )
    else:
        ok("99 FR-number examples align with the current 01 baseline")

    hotspot_hits = [token for token in FORBIDDEN_99_HOTSPOT_EXAMPLE_DRIFT if token in doc_99_text]
    if hotspot_hits:
        fail(
            "99 still contains stale hotspot examples that conflict with the frozen FR-04 scope: "
            + ", ".join(hotspot_hits),
            errors,
        )
    else:
        ok("99 hotspot examples align with the frozen FR-04 source scope")

    rule_trigger_hits = [token for token in FORBIDDEN_99_RULE_TRIGGER_DRIFT if token in doc_99_text]
    if rule_trigger_hits:
        fail(
            "99 still teaches doc-triggered Rule activation that conflicts with 02 §4.7.1: "
            + ", ".join(rule_trigger_hits),
            errors,
        )
    else:
        ok("99 keeps Rule activation scoped to code/test file domains")

    if re.search(r"^globs:.*docs/core/", doc_99_text, flags=re.MULTILINE):
        fail("99 Rule examples still put docs/core/*.md into globs", errors)
    else:
        ok("99 Rule examples keep docs/core/*.md out of globs")

    agents_hits = [token for token in FORBIDDEN_AGENTS_RULE_TRIGGER_DRIFT if token in agents_text]
    if agents_hits:
        fail(
            "AGENTS.md still suggests doc-triggered role auto-apply that conflicts with 02 §4.7.1: "
            + ", ".join(agents_hits),
            errors,
        )
    else:
        ok("AGENTS.md keeps role auto-apply scoped to code/test file domains")

    rule_files = {path.name for path in RULES_DIR.glob("*.mdc")}
    if rule_files != EXPECTED_RULES:
        fail(
            "role Rule set drifted; expected exactly 5 role rules plus auto-multi-ai-analysis",
            errors,
        )
    else:
        ok("role Rule count remains 5+1")

    for skill_path in sorted(SKILLS_DIR.glob("*/SKILL.md")):
        skill_text = read_text(skill_path)
        if any(pattern in skill_text for pattern in FORBIDDEN_GENERIC_OUTPUT_PATTERNS):
            fail(
                f"{skill_path.relative_to(REPO_ROOT)} still uses a generic Outputs line instead of the frozen 01 §5.5 output anchor",
                errors,
            )
        else:
            ok(f"{skill_path.relative_to(REPO_ROOT)} avoids generic Outputs placeholders")

    for heading in PROMPT_HEADINGS:
        section = extract_section(prompt_text, heading)
        if section is None:
            fail(f"{heading} missing from prompt doc", errors)
            continue
        missing = [marker for marker in REQUIRED_PROMPT_MARKERS if marker not in section]
        if missing:
            fail(f"{heading} missing markers: {', '.join(missing)}", errors)
        else:
            ok(f"{heading} contains all 6 required markers")

        spec_block = extract_marker_block(section, "【规格来源】", "【边界约束】")
        if spec_block is None:
            fail(f"{heading} missing a parseable 【规格来源】 block", errors)
            continue
        if "docs/old/" in spec_block:
            fail(f"{heading} references docs/old/** inside 【规格来源】", errors)
        else:
            ok(f"{heading} keeps docs/old/** out of 【规格来源】")

        core_refs = set(re.findall(r"docs/core/[0-9]{2}_[^`\s)]+\.md", spec_block))
        illegal_refs = sorted(core_refs - ALLOWED_CORE_DOCS)
        if illegal_refs:
            fail(
                f"{heading} contains disallowed core refs inside 【规格来源】: {', '.join(illegal_refs)}",
                errors,
            )
        else:
            ok(f"{heading} limits 【规格来源】 to 01/02/03/04/05/99")

    if not ISSUE_REGISTER.exists():
        fail("issue_register.md missing", errors)
    else:
        ok("issue_register.md exists")

    if not REVIEW_LOG.exists():
        fail("review_log.md missing", errors)
    else:
        ok("review_log.md exists")

    open_count = parse_open_issue_count(issue_text)
    if open_count is None:
        fail("issue_register.md missing open_issue_count", errors)
    else:
        ok(f"issue_register.md declares open_issue_count={open_count}")
        if not args.allow_open_issues and open_count != 0:
            fail("issue_register.md still has open issues", errors)
        elif args.allow_open_issues:
            ok("strict zero-open enforcement skipped by flag")
        else:
            ok("issue_register.md has zero open issues")

    zero_streak = parse_zero_open_streak(issue_text)
    if zero_streak is None:
        fail("issue_register.md missing zero_open_streak", errors)
    else:
        ok(f"issue_register.md declares zero_open_streak={zero_streak}")
        if not args.allow_open_issues and zero_streak < 2:
            fail("issue_register.md has not yet reached two consecutive zero-open rounds", errors)
        elif args.allow_open_issues:
            ok("strict zero-streak enforcement skipped by flag")
        else:
            ok("issue_register.md satisfies the two-round zero-open requirement")

    if re.search(r"\|\s*open\s*\|", issue_text, flags=re.IGNORECASE):
        fail("issue_register.md still contains table rows marked open", errors)
    else:
        ok("issue_register.md contains no explicit open-status rows")

    # --- Action 32 completion scan ---
    placeholder_hits: list[str] = []
    stub_hits: list[str] = []
    for py_path in sorted((REPO_ROOT / "app").rglob("*.py")):
        for i, line in enumerate(py_path.read_text("utf-8").splitlines(), 1):
            if re.search(r"TODO|FIXME|raise NotImplementedError", line):
                placeholder_hits.append(f"{py_path.relative_to(REPO_ROOT)}:{i}")
            if "STUB:" in line:
                stub_hits.append(f"{py_path.relative_to(REPO_ROOT)}:{i}")
    if placeholder_hits:
        fail(
            f"Action 32 completion scan: {len(placeholder_hits)} placeholder(s) in app/: "
            + ", ".join(placeholder_hits[:5]),
            errors,
        )
    else:
        ok("Action 32 completion scan: no TODO/FIXME/NotImplementedError in app/")

    if stub_hits:
        fail(
            f"Action 32 completion scan: {len(stub_hits)} STUB marker(s) still present in app/: "
            + ", ".join(stub_hits[:5]),
            errors,
        )
    else:
        ok("Action 32 completion scan: no STUB markers in app/")

    closed_count = parse_closed_issue_count(issue_text)
    closed_rows = count_closed_issue_rows(issue_text)
    if closed_count is None:
        fail("issue_register.md missing closed_issue_count", errors)
    else:
        ok(f"issue_register.md declares closed_issue_count={closed_count}")
        if closed_count != closed_rows:
            fail(
                f"issue_register.md closed_issue_count={closed_count} but closed table has {closed_rows} row(s)",
                errors,
            )
        else:
            ok("issue_register.md closed_issue_count matches closed table rows")

    if "bootstrap" not in review_text:
        fail("review_log.md missing bootstrap or review entry scaffold", errors)
    else:
        ok("review_log.md scaffold present")

    reviewed_count = parse_reviewed_issue_count(review_text)
    decision_count = count_review_decisions(review_text)
    if reviewed_count is None:
        fail("review_log.md missing reviewed_issue_count", errors)
    else:
        ok(f"review_log.md declares reviewed_issue_count={reviewed_count}")
        if reviewed_count != decision_count:
            fail(
                f"review_log.md reviewed_issue_count={reviewed_count} but review entries contain {decision_count} decision row(s)",
                errors,
            )
        else:
            ok("review_log.md reviewed_issue_count matches decision rows")

    if errors:
        print(f"\n{len(errors)} check(s) failed.")
        return 1

    print("\nAll stage1-3 dual-agent checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
