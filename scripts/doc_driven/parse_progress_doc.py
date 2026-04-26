"""
parse_progress_doc.py — 阶段 1: 构建 claim_registry

默认主链（registry）输入:
  - docs/core/01_需求基线.md
  - docs/core/02_系统架构.md
  - docs/core/03_详细设计.md
  - docs/core/04_数据治理与血缘.md
  - docs/core/05_API与数据契约.md
  - app/governance/feature_registry.json
  - scripts/doc_driven/page_expectations.py

兼容链（legacy_v7）输入:
  - docs/core/22_全量功能进度总表_v7_精审.md
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from .page_expectations import PAGE_EXPECTATIONS

# ── 常量 ──────────────────────────────────────────────
_DEFAULT_V7_PATH = Path("docs/core/22_全量功能进度总表_v7_精审.md")
_REGISTRY_PATH = Path("app/governance/feature_registry.json")
_SSOT_DOC_PATHS = (
    Path("docs/core/01_需求基线.md"),
    Path("docs/core/02_系统架构.md"),
    Path("docs/core/03_详细设计.md"),
    Path("docs/core/04_数据治理与血缘.md"),
    Path("docs/core/05_API与数据契约.md"),
)

# legacy_v7: 匹配顶级 FR 组标题: ## FR-00 真实性红线（3 个功能点）
_RE_FR_GROUP = re.compile(
    r"^##\s+((?:FR[-‐][\w-]+|LEGACY[-‐]\w+|OOS[-‐]\w+[-‐]\w+))\s+(.+?)(?:（(\d+)\s*个功能点）)?$"
)

# legacy_v7: 匹配功能点标题: ### FR00-AUTH-01 已发布研报只读保护
_RE_FEATURE = re.compile(
    r"^###\s+(FR[\w]+-[\w]+(?:-[\w]+)*-\d+)\s+(.+)$"
)

# legacy_v7: 评级提取
_RE_CODE_RATING = re.compile(r"\*\*代码实现\*\*\s*\|\s*(✅|⚠️|🔴)\s*(.+?)\s*\|")
_RE_TEST_RATING = re.compile(r"\*\*(核心测试|测试覆盖)\*\*\s*\|\s*(✅|⚠️|🔴)\s*(.+?)\s*\|")

# legacy_v7: 差距提取
_RE_GAP = re.compile(r"\*\*(?:真实)?差距\*\*\s*\|\s*(.+?)\s*\|")

# legacy_v7: 文件 / 测试引用
_RE_FILE_REF = re.compile(r"`(app/[^`]+\.py)`")
_RE_TEST_REF = re.compile(r"`((?:batch\d+::)?test_[\w]+)`")

_FRONTEND_KEYWORDS = {
    "template",
    "html",
    "页面",
    "前端",
    "模板",
    "dom",
    "css",
    "浏览器",
    "路由",
    "subscribe",
    "profile",
    "index",
    "dashboard",
    "report_view",
    "login",
    "register",
    "admin.html",
}

_RE_PRIORITY = re.compile(r"\*\*优先级\*\*\s*[:：]?\s*(P[012])")

_RE_TABLE_FEATURE = re.compile(
    r"^\|\s*((?:LEGACY|OOS)[-‐][\w-]+-\d+)\s*\|(.+)"
)

_LLM_PROVIDERS = ["Gemini", "ChatGPT", "DeepSeek", "Qwen"]
_LLM_SUBFUNCTIONS = ["分析", "批量", "会话管理", "状态查询"]


def build_claim_registry(
    mode: str = "registry",
    legacy_v7_path: str | Path | None = None,
) -> list[dict]:
    """构建 claim_registry.

    mode:
      - registry: 默认主链（不依赖 22）
      - legacy_v7: 显式兼容链（解析 22）
    """
    normalized_mode = (mode or "registry").strip().lower()
    if normalized_mode == "registry":
        return _build_claim_registry_from_registry()
    if normalized_mode in {"legacy_v7", "legacy", "compat"}:
        return parse_progress_doc(path=legacy_v7_path)
    raise ValueError(f"unsupported claim mode: {mode}")


def _build_claim_registry_from_registry() -> list[dict]:
    _assert_ssot_docs_exist_and_readable()

    if not _REGISTRY_PATH.exists():
        raise FileNotFoundError(f"missing governance registry: {_REGISTRY_PATH}")
    registry_data = json.loads(_REGISTRY_PATH.read_text(encoding="utf-8-sig"))
    features = registry_data.get("features", []) if isinstance(registry_data, dict) else []

    page_feature_ids = {
        feature_id
        for item in PAGE_EXPECTATIONS
        for feature_id in item.fr_ids
    }

    claim_registry: list[dict] = []
    for feature in features:
        feature_id = str(feature.get("feature_id") or "").strip()
        if not feature_id:
            continue
        fr_id = str(feature.get("fr_id") or _infer_group(feature_id)).strip()
        gaps = [str(item).strip() for item in (feature.get("gaps") or []) if str(item).strip()]
        required_test_kinds = [str(item).strip() for item in (feature.get("required_test_kinds") or [])]

        claim_registry.append(
            {
                "fr_id": fr_id,
                "feature_id": feature_id,
                "title": str(feature.get("title") or feature_id).strip(),
                "claimed_gap": "；".join(gaps) if gaps else "无",
                "code_rating": _verdict_to_rating(feature.get("code_verdict")),
                "test_rating": _verdict_to_rating(feature.get("test_verdict")),
                "referenced_files": [],
                "referenced_tests": [],
                "has_frontend": (
                    ("page" in required_test_kinds)
                    or bool(feature.get("runtime_page_path"))
                    or feature_id in page_feature_ids
                ),
                "priority": _priority_from_feature(feature, has_gap=bool(gaps)),
                "claim_source": "feature_registry",
            }
        )

    return claim_registry


def _assert_ssot_docs_exist_and_readable() -> None:
    missing = [str(path) for path in _SSOT_DOC_PATHS if not path.exists()]
    if missing:
        raise FileNotFoundError("missing SSOT docs: " + ", ".join(missing))
    for path in _SSOT_DOC_PATHS:
        # 主链把 01~05 作为输入依赖；只要文档不可读就直接 fail-close。
        path.read_text(encoding="utf-8-sig")


def _priority_from_feature(feature: dict, *, has_gap: bool) -> str:
    visibility = str(feature.get("visibility") or "").strip().lower()
    if visibility in {"deprecated", "out_of_ssot"}:
        return "P2"
    if has_gap:
        return "P1"
    return "P1"


def _verdict_to_rating(verdict: object) -> str:
    text = str(verdict or "").strip()
    if not text:
        return "unknown"
    if any(token in text for token in ("❌", "🔴")):
        return "fail"
    if "⚠️" in text or "⚠" in text:
        return "warn"
    if "✅" in text:
        return "ok"
    return "unknown"


def parse_progress_doc(path: str | Path | None = None) -> list[dict]:
    """legacy_v7 兼容入口: 解析 22_v7 总表为 claim_registry."""
    path = Path(path) if path else _DEFAULT_V7_PATH
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    registry: list[dict] = []
    current_group: str | None = None
    current_feature: dict | None = None

    for i, line in enumerate(lines):
        m_group = _RE_FR_GROUP.match(line)
        if m_group:
            _flush(current_feature, registry)
            current_feature = None
            current_group = _normalize_fr_id(m_group.group(1))
            continue

        m_feat = _RE_FEATURE.match(line)
        if m_feat:
            _flush(current_feature, registry)
            feature_id = m_feat.group(1)
            title = m_feat.group(2).strip()
            current_feature = {
                "fr_id": current_group or _infer_group(feature_id),
                "feature_id": feature_id,
                "title": title,
                "claimed_gap": "无",
                "code_rating": "unknown",
                "test_rating": "unknown",
                "referenced_files": [],
                "referenced_tests": [],
                "has_frontend": False,
                "priority": "P1",
                "raw_section_start": i + 1,
                "claim_source": "legacy_v7",
            }
            continue

        if current_feature is None:
            continue

        m_code = _RE_CODE_RATING.search(line)
        if m_code:
            current_feature["code_rating"] = _rating_to_str(m_code.group(1))

        m_test = _RE_TEST_RATING.search(line)
        if m_test:
            current_feature["test_rating"] = _rating_to_str(m_test.group(2))

        m_gap = _RE_GAP.search(line)
        if m_gap:
            gap_text = m_gap.group(1).strip()
            if gap_text and gap_text not in {"—", "无"}:
                current_feature["claimed_gap"] = gap_text

        for m in _RE_FILE_REF.finditer(line):
            ref = m.group(1)
            if ref not in current_feature["referenced_files"]:
                current_feature["referenced_files"].append(ref)

        for m in _RE_TEST_REF.finditer(line):
            ref = m.group(1)
            if ref not in current_feature["referenced_tests"]:
                current_feature["referenced_tests"].append(ref)

        lower = line.lower()
        if any(kw in lower for kw in _FRONTEND_KEYWORDS):
            current_feature["has_frontend"] = True

        m_pri = _RE_PRIORITY.search(line)
        if m_pri:
            current_feature["priority"] = m_pri.group(1)

    _flush(current_feature, registry)
    _parse_table_features(lines, registry)
    _parse_llm_provider_table(lines, registry)
    return registry


def _flush(feature: dict | None, registry: list[dict]) -> None:
    if feature is not None:
        registry.append(feature)


def _parse_table_features(lines: list[str], registry: list[dict]) -> None:
    existing = {r["feature_id"] for r in registry}
    current_group: str | None = None

    for i, line in enumerate(lines):
        m_group = _RE_FR_GROUP.match(line)
        if m_group:
            current_group = _normalize_fr_id(m_group.group(1))
            continue

        m = _RE_TABLE_FEATURE.match(line)
        if not m:
            continue
        fid = m.group(1)
        if fid in existing:
            continue
        cells = [c.strip() for c in m.group(2).split("|")]
        title = cells[0] if cells else fid
        route_desc = cells[1] if len(cells) > 1 else ""
        code_ok = "✅" in line
        test_ok = "✅" in (cells[3] if len(cells) > 3 else "")

        registry.append(
            {
                "fr_id": current_group or _infer_group(fid),
                "feature_id": fid,
                "title": f"{title} {route_desc}".strip(),
                "claimed_gap": "无",
                "code_rating": "ok" if code_ok else "unknown",
                "test_rating": "ok" if test_ok else "warn",
                "referenced_files": [],
                "referenced_tests": [],
                "has_frontend": False,
                "priority": "P2",
                "raw_section_start": i + 1,
                "claim_source": "legacy_v7",
            }
        )
        existing.add(fid)


def _parse_llm_provider_table(lines: list[str], registry: list[dict]) -> None:
    existing = {r["feature_id"] for r in registry}
    for provider in _LLM_PROVIDERS:
        for sub_idx, subfunc in enumerate(_LLM_SUBFUNCTIONS, start=1):
            fid = f"FR06-LLM-{provider.upper()}-{sub_idx:02d}"
            if fid in existing:
                continue
            registry.append(
                {
                    "fr_id": "FR-06",
                    "feature_id": fid,
                    "title": f"{provider} {subfunc}",
                    "claimed_gap": "无 pytest (仅手动脚本)",
                    "code_rating": "ok",
                    "test_rating": "warn",
                    "referenced_files": [f"ai-api/{provider.lower()}_web/"],
                    "referenced_tests": [],
                    "has_frontend": False,
                    "priority": "P2",
                    "raw_section_start": 0,
                    "claim_source": "legacy_v7",
                }
            )
            existing.add(fid)


def _normalize_fr_id(raw: str) -> str:
    return raw.replace("‐", "-").strip()


def _infer_group(feature_id: str) -> str:
    m = re.match(r"FR(\d+)([Bb])?", feature_id)
    if m:
        num = m.group(1).zfill(2)
        suffix = "-b" if m.group(2) else ""
        return f"FR-{num}{suffix}"
    return "UNKNOWN"


def _rating_to_str(emoji: str) -> str:
    return {"✅": "ok", "⚠️": "warn", "🔴": "fail"}.get(emoji, "unknown")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="构建 claim_registry")
    parser.add_argument(
        "--mode",
        choices=["registry", "legacy_v7"],
        default="registry",
        help="claim 构建模式（默认 registry；legacy_v7 为兼容模式）",
    )
    parser.add_argument(
        "--input",
        default=str(_DEFAULT_V7_PATH),
        help="legacy_v7 模式下的 22 输入路径",
    )
    parser.add_argument("--output", default="output/claim_registry.json")
    args = parser.parse_args()

    if args.mode == "legacy_v7":
        registry = build_claim_registry(mode="legacy_v7", legacy_v7_path=args.input)
    else:
        registry = build_claim_registry(mode="registry")

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[claims] mode={args.mode} count={len(registry)} -> {out}")


if __name__ == "__main__":
    main()
