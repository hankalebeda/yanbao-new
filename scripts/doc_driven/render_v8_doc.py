"""
render_v8_doc.py — 阶段 6: 生成 v8 验真版总表

沿用 claim_registry 的 FR 编号、标题、分组, 避免阅读习惯变化.
每个 FR 统一新增机器维护字段:
  总表声明 / 验真方式 / 实测状态 / 证据来源 / 失败摘要 /
  测试质量判定 / 修复动作

v8 正文由管道全自动重写, 人工不直接编辑 v8.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict


def render_v8_doc(
    claim_registry: list[dict],
    gap_report: dict,           # {summary: ..., details: [...]}
    quality_report: list[dict],
    verification_plan: list[dict] | None = None,
    claim_source: str = "registry",
) -> str:
    """生成 v8 总表 Markdown 正文."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    gap_details = gap_report.get("details", [])
    gap_summary = gap_report.get("summary", {})

    # 构建 feature_id → gap 映射
    gap_map: dict[str, dict] = {}
    for g in gap_details:
        gap_map[g["feature_id"]] = g

    # 构建 FR → 假测试 映射
    fake_by_fr: dict[str, list[dict]] = defaultdict(list)
    for q in quality_report:
        for fr in q.get("mapped_frs", []):
            fake_by_fr[fr].append(q)

    # 构建 feature_id → 验证项 映射
    plan_map: dict[str, list[dict]] = defaultdict(list)
    if verification_plan:
        for item in verification_plan:
            plan_map[item["feature_id"]].append(item)

    # 按 FR 组分组
    groups: dict[str, list[dict]] = {}
    for claim in claim_registry:
        fr_id = claim["fr_id"]
        groups.setdefault(fr_id, []).append(claim)

    # ── 生成文档 ──

    lines: list[str] = []
    _h = lines.append

    _h(f"# 全量功能进度总表 v8 — 验真版")
    _h("")
    _h(f"> **自动生成时间**: {now}")
    if claim_source == "legacy_v7":
        _h("> **输入来源**: legacy/compat 22_v7(声明) + SSOT 01~05(基线) + 实测(验真)")
    else:
        _h("> **输入来源**: SSOT 01~05 + feature_registry.json + page_expectations.py + 实测(验真)")
    _h(f"> **本文档由管道自动生成, 人工不直接编辑**")
    _h("")
    _h("---")
    _h("")

    # ── 系统级摘要 ──
    _h("## 系统级验真摘要")
    _h("")
    total = gap_summary.get("total_features", 0)
    by_v = gap_summary.get("by_verdict", {})
    _h("| 指标 | 值 |")
    _h("|:-----|:---|")
    _h(f"| 总功能点 | {total} |")
    _h(f"| ✅ CONFIRMED (声明与实测一致) | {by_v.get('CONFIRMED', 0)} |")
    _h(f"| 🔴 FALSE_CLEAR (声明无差距但有问题) | {by_v.get('FALSE_CLEAR', 0)} |")
    _h(f"| ⚠️ KNOWN_GAP (已知差距) | {by_v.get('KNOWN_GAP', 0)} |")
    _h(f"| 🟢 UNDERCLAIMED (声明有差距但已修复) | {by_v.get('UNDERCLAIMED', 0)} |")
    _h(f"| ⬜ UNVERIFIED (无法自动验真) | {by_v.get('UNVERIFIED', 0)} |")
    _h(f"| 确认率 | {gap_summary.get('confirmed_rate', 0)}% |")
    _h(f"| 假测试关联 FR 数 | {len(fake_by_fr)} |")

    ci_block = gap_summary.get("ci_should_block", False)
    _h(f"| **CI 阻断** | {'⛔ 是' if ci_block else '✅ 否'} |")
    _h("")

    if gap_summary.get("false_clear_features"):
        _h("### ⛔ FALSE_CLEAR 功能点 (声明无差距但实测有问题)")
        _h("")
        for fc in gap_summary["false_clear_features"]:
            g = gap_map.get(fc, {})
            _h(f"- **{fc}** {g.get('title', '')}: {'; '.join(g.get('actual_failures', [])[:2])}")
        _h("")

    fake_count = sum(1 for q in quality_report if q.get("issue_kind") in ("FAKE", "HOLLOW"))
    weak_count = sum(1 for q in quality_report if q.get("issue_kind") == "WEAK")
    if fake_count or weak_count:
        _h(f"### 🔍 测试质量问题汇总: FAKE/HOLLOW={fake_count}, WEAK={weak_count}")
        _h("")

    _h("---")
    _h("")

    # ── 逐 FR 组输出 ──
    for fr_id, features in groups.items():
        # 组标题
        first = features[0]
        _h(f"## {fr_id} ({len(features)} 个功能点)")
        _h("")

        for claim in features:
            fid = claim["feature_id"]
            title = claim["title"]
            gap = gap_map.get(fid, {})
            verdict = gap.get("verdict", "UNVERIFIED")
            verdict_icon = _verdict_icon(verdict)

            _h(f"### {fid} {title}")
            _h("")
            _h(f"| 维度 | 内容 |")
            _h(f"|:-----|:-----|")
            _h(f"| **总表声明** | 代码: {claim.get('code_rating', '?')}, 测试: {claim.get('test_rating', '?')}, 差距: {claim.get('claimed_gap', '无')} |")

            # 验真方式
            vplan = plan_map.get(fid, [])
            verify_kinds = sorted(set(v["verify_kind"] for v in vplan)) if vplan else ["未规划"]
            _h(f"| **验真方式** | {', '.join(verify_kinds)} |")

            # 实测状态
            _h(f"| **实测状态** | {verdict_icon} {verdict} |")

            # 证据来源
            if gap.get("actual_failures"):
                evidence = "; ".join(gap["actual_failures"][:3])
            elif verdict == "CONFIRMED":
                evidence = "全部验真项通过"
            else:
                evidence = "—"
            _h(f"| **证据来源** | {evidence} |")

            # 失败摘要
            if gap.get("actual_failures"):
                _h(f"| **失败摘要** | {'; '.join(gap['actual_failures'][:5])} |")

            # 测试质量判定
            fr_fakes = fake_by_fr.get(claim["fr_id"], [])
            fid_fakes = [q for q in quality_report if fid in [f"FR-{m}" for m in (q.get("mapped_frs") or [])]]
            all_issues = fr_fakes + fid_fakes
            if all_issues:
                for q in all_issues[:3]:
                    _h(f"| **测试质量** | {q.get('issue_kind', '?')}: `{q.get('test_func', '?')}` ({q.get('pattern', '?')}) |")

            # 修复动作
            remediation = gap.get("remediation", "")
            if remediation:
                _h(f"| **修复动作** | {remediation} |")

            _h("")
            _h("---")
            _h("")

    # ── 附录: 假测试清单 ──
    if quality_report:
        _h("## 附录 A: 假测试/弱测试清单")
        _h("")
        _h("| 文件 | 函数 | 行号 | 类型 | 模式 | 建议 |")
        _h("|:-----|:-----|:-----|:-----|:-----|:-----|")
        for q in sorted(quality_report, key=lambda x: (x.get("issue_kind", ""), x.get("test_file", ""))):
            fname = Path(q.get("test_file", "")).name
            _h(f"| {fname} | `{q.get('test_func', '')}` | {q.get('line_no', '')} "
               f"| {q.get('issue_kind', '')} | {q.get('pattern', '')} | {q.get('suggestion', '')} |")
        _h("")

    return "\n".join(lines)


def _verdict_icon(verdict: str) -> str:
    return {
        "CONFIRMED": "✅",
        "FALSE_CLEAR": "🔴",
        "KNOWN_GAP": "⚠️",
        "UNDERCLAIMED": "🟢",
        "UNVERIFIED": "⬜",
    }.get(verdict, "❓")


# ── CLI 入口 ──────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="生成 v8 验真总表")
    parser.add_argument("--claims", default="output/claim_registry.json")
    parser.add_argument("--gaps", default="output/gap_report.json")
    parser.add_argument("--quality", default="output/test_quality_report.json")
    parser.add_argument("--plan", default="output/verification_plan.json")
    parser.add_argument(
        "--claim-source",
        choices=["registry", "legacy_v7"],
        default="registry",
        help="claim 来源（默认 registry）",
    )
    parser.add_argument("--output", default="docs/core/22_全量功能进度总表_v8_验真版.md")
    args = parser.parse_args()

    claims = json.loads(Path(args.claims).read_text(encoding="utf-8"))
    gap_report = json.loads(Path(args.gaps).read_text(encoding="utf-8"))
    quality = []
    qpath = Path(args.quality)
    if qpath.exists():
        quality = json.loads(qpath.read_text(encoding="utf-8"))
    plan = []
    ppath = Path(args.plan)
    if ppath.exists():
        plan = json.loads(ppath.read_text(encoding="utf-8"))

    md = render_v8_doc(claims, gap_report, quality, plan, claim_source=args.claim_source)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")
    print(f"[render] v8 总表已生成 → {out} ({len(md)} 字符)")


if __name__ == "__main__":
    main()
