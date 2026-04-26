"""
analyze_gaps.py — 阶段 4: 差距分析

对比 claim_registry（主链或兼容链声明）与 raw_results（实测结果）,
产出 gap_report: 每条 FR 的验真结论.

验真结论分类:
  CONFIRMED     — 声明与实测一致, 功能确认成立
  FALSE_CLEAR   — 声明"无差距"但实测发现问题
  KNOWN_GAP     — 声明已知差距, 实测也确认差距存在
  UNDERCLAIMED  — 声明有差距, 但实测发现已修复
  UNVERIFIED    — 无法通过自动化验真 (如需人工确认)
"""
from __future__ import annotations

import json
from pathlib import Path
from dataclasses import dataclass, asdict, field


@dataclass
class GapEntry:
    """单条差距报告."""
    feature_id: str
    fr_id: str
    title: str
    verdict: str          # CONFIRMED / FALSE_CLEAR / KNOWN_GAP / UNDERCLAIMED / UNVERIFIED
    claimed_gap: str      # claim_registry 中声明的差距
    actual_failures: list[str] = field(default_factory=list)  # 实测失败项
    test_quality_issues: list[str] = field(default_factory=list)  # 关联的假测试
    confidence: float = 1.0  # 结论置信度 (0~1)
    remediation: str = ""    # 修复动作


def analyze_gaps(
    claim_registry: list[dict],
    raw_results: list[dict],
    quality_report: list[dict] | None = None,
) -> list[dict]:
    """分析差距, 返回 gap_report (list[dict]).

    Args:
        claim_registry: claim 声明列表（可来自 registry 主链或 legacy_v7）
        raw_results:    验真测试的执行结果列表
                        每条: {feature_id, verify_kind, passed: bool, failure_msg: str}
        quality_report: 测试质量审计结果 (可选)
    """
    quality_report = quality_report or []

    # 构建 feature_id → 实测失败 映射 (支持精确匹配和前缀匹配)
    failures_by_feature: dict[str, list[str]] = {}
    for r in raw_results:
        fid = r.get("feature_id", "")
        if not r.get("passed", True):
            msg = f"[{r.get('verify_kind', '?')}] {r.get('failure_msg', '未知失败')}"
            failures_by_feature.setdefault(fid, []).append(msg)
            # 对短 ID (如 "FR09") 也按前缀分配到所有匹配的 claim
            # 这在后面查找时处理

    # 构建 FR → 假测试 映射
    fake_by_fr: dict[str, list[str]] = {}
    for q in quality_report:
        kind = q.get("issue_kind", "")
        if kind in ("FAKE", "HOLLOW"):
            for fr in q.get("mapped_frs", []):
                desc = f"{q['test_func']} ({kind}: {q['pattern']})"
                fake_by_fr.setdefault(fr, []).append(desc)

    first_claim_by_fr: dict[str, str] = {}
    for claim in claim_registry:
        fr = str(claim.get("fr_id") or "")
        if fr and fr not in first_claim_by_fr:
            first_claim_by_fr[fr] = str(claim.get("feature_id") or "")

    gaps: list[GapEntry] = []

    for claim in claim_registry:
        fid = claim["feature_id"]
        fr_id = claim["fr_id"]
        title = claim["title"]
        claimed_gap = claim.get("claimed_gap", "无")
        code_rating = claim.get("code_rating", "unknown")
        test_rating = claim.get("test_rating", "unknown")

        actual_failures = failures_by_feature.get(fid, [])
        # 也检查 fr_id 级别的失败 (如 "FR09" 应匹配到 FR-09 组的某个功能)
        # 但只当该 FR 组中没有精确匹配时才使用 (避免过度分配)
        fr_num = fr_id.replace("FR-", "FR")  # FR-09 → FR09
        if not actual_failures and fr_num in failures_by_feature:
            # 该 FR 组有通用失败, 但仅标记给组内第一个功能点
            if first_claim_by_fr.get(fr_id) == fid:
                actual_failures = failures_by_feature[fr_num]
        test_issues = fake_by_fr.get(fr_id, [])

        # 判定 verdict
        has_claimed_gap = claimed_gap not in ("无", "—", "", None)
        has_actual_failure = len(actual_failures) > 0
        has_fake_tests = len(test_issues) > 0

        if not has_actual_failure and not has_fake_tests:
            if has_claimed_gap:
                verdict = "UNDERCLAIMED"
                remediation = "claim 声明有差距但实测通过, 考虑更新 claim_registry"
            else:
                verdict = "CONFIRMED"
                remediation = ""
        elif has_actual_failure:
            if has_claimed_gap:
                verdict = "KNOWN_GAP"
                remediation = f"已知差距, 需修复: {'; '.join(actual_failures[:3])}"
            else:
                verdict = "FALSE_CLEAR"
                remediation = f"claim 声明无差距但实测失败: {'; '.join(actual_failures[:3])}"
        elif has_fake_tests:
            # 仅有假测试问题, 无实际功能失败
            verdict = "CONFIRMED" if not has_claimed_gap else "KNOWN_GAP"
            remediation = f"需替换假测试: {'; '.join(test_issues[:3])}"
        else:
            verdict = "UNVERIFIED"
            remediation = "需人工验证"

        confidence = _calc_confidence(actual_failures, test_issues, code_rating, test_rating)

        gaps.append(GapEntry(
            feature_id=fid,
            fr_id=fr_id,
            title=title,
            verdict=verdict,
            claimed_gap=claimed_gap,
            actual_failures=actual_failures,
            test_quality_issues=test_issues,
            confidence=confidence,
            remediation=remediation,
        ))

    return [asdict(g) for g in gaps]


def _calc_confidence(
    failures: list[str],
    fake_tests: list[str],
    code_rating: str,
    test_rating: str,
) -> float:
    """计算结论置信度."""
    c = 1.0
    if not failures and fake_tests:
        c -= 0.2 * min(len(fake_tests), 3)  # 假测试降低置信度
    if code_rating == "warn":
        c -= 0.1
    if test_rating in ("warn", "fail"):
        c -= 0.15
    return max(0.1, round(c, 2))


def summarize_gaps(gap_report: list[dict]) -> dict:
    """汇总差距报告统计."""
    by_verdict: dict[str, int] = {}
    for g in gap_report:
        v = g["verdict"]
        by_verdict[v] = by_verdict.get(v, 0) + 1

    total = len(gap_report)
    false_clear_items = [g for g in gap_report if g["verdict"] == "FALSE_CLEAR"]
    known_gap_items = [g for g in gap_report if g["verdict"] == "KNOWN_GAP"]

    return {
        "total_features": total,
        "by_verdict": by_verdict,
        "confirmed_rate": round(by_verdict.get("CONFIRMED", 0) / max(total, 1) * 100, 1),
        "false_clear_count": len(false_clear_items),
        "false_clear_features": [g["feature_id"] for g in false_clear_items],
        "known_gap_count": len(known_gap_items),
        "known_gap_features": [g["feature_id"] for g in known_gap_items],
        "ci_should_block": len(false_clear_items) > 0 or any(
            g.get("test_quality_issues") for g in gap_report
            if g["verdict"] != "CONFIRMED"
        ),
    }


# ── CLI 入口 ──────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="差距分析")
    parser.add_argument("--claims", default="output/claim_registry.json")
    parser.add_argument("--results", default="output/raw_results.json")
    parser.add_argument("--quality", default="output/test_quality_report.json")
    parser.add_argument("--output", default="output/gap_report.json")
    args = parser.parse_args()

    claims = json.loads(Path(args.claims).read_text(encoding="utf-8"))
    results = json.loads(Path(args.results).read_text(encoding="utf-8"))
    quality = []
    qpath = Path(args.quality)
    if qpath.exists():
        quality = json.loads(qpath.read_text(encoding="utf-8"))

    report = analyze_gaps(claims, results, quality)
    summary = summarize_gaps(report)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    output_data = {"summary": summary, "details": report}
    out.write_text(json.dumps(output_data, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[gap] 差距分析: {summary['total_features']} FR, "
          f"CONFIRMED={summary['by_verdict'].get('CONFIRMED', 0)}, "
          f"FALSE_CLEAR={summary['false_clear_count']}, "
          f"KNOWN_GAP={summary['known_gap_count']}")
    if summary["ci_should_block"]:
        print("[gap] ⛔ CI 应阻断: 存在 FALSE_CLEAR 或假测试关联")


if __name__ == "__main__":
    main()
