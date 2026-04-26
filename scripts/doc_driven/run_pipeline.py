"""
run_pipeline.py — 验真管道主入口

串联 6 个阶段:
  1. 构建 claim_registry.json
     - 默认: registry 主链（SSOT 01~05 + feature_registry + page_expectations）
     - 兼容: legacy_v7（显式指定）
  2. 构建 verification plan → verification_plan.json
  3. 运行参数化验真测试 → raw_results.json
  4. 差距分析 → gap_report.json
  5. 测试质量审计 → test_quality_report.json
  6. 生成 v8 总表 → 22_全量功能进度总表_v8_验真版.md

用法:
  python -m scripts.doc_driven.run_pipeline
  python -m scripts.doc_driven.run_pipeline --skip-tests   # 跳过阶段3
  python -m scripts.doc_driven.run_pipeline --only-audit    # 只跑审计+渲染
  python -m scripts.doc_driven.run_pipeline --claim-source legacy_v7 --legacy-v7-path docs/core/22_全量功能进度总表_v7_精审.md
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from app.governance.build_feature_catalog import (
    _collect_feature_marker_index_for_path,
    infer_fr_id_from_nodeid,
)

# ── 常量 ──────────────────────────────────────────────

OUTPUT_DIR = Path("output")
ROOT = Path(__file__).resolve().parents[2]
STAGE3_TEST_TARGETS = (
    "tests/test_doc_driven_verify.py",
    "tests/test_features_page.py",
    "tests/test_governance_alignment.py",
)
LEGACY_V7_PATH = Path("docs/core/22_全量功能进度总表_v7_精审.md")
V8_PATH = Path("docs/core/22_全量功能进度总表_v8_验真版.md")

CLAIM_REGISTRY = OUTPUT_DIR / "claim_registry.json"
VERIFICATION_PLAN = OUTPUT_DIR / "verification_plan.json"
RAW_RESULTS = OUTPUT_DIR / "raw_results.json"
GAP_REPORT = OUTPUT_DIR / "gap_report.json"
QUALITY_REPORT = OUTPUT_DIR / "test_quality_report.json"


def _build_stage3_feature_marker_index(test_targets: tuple[str, ...] = STAGE3_TEST_TARGETS) -> dict[str, list[str]]:
    marker_index: dict[str, set[str]] = {}
    for rel_path in test_targets:
        path = ROOT / rel_path
        if not path.exists():
            continue
        file_index = _collect_feature_marker_index_for_path(path, root=ROOT)
        for nodeid, feature_ids in file_index.items():
            marker_index.setdefault(nodeid, set()).update(item for item in feature_ids if item)
    return {nodeid: sorted(feature_ids) for nodeid, feature_ids in marker_index.items()}


def _fr_group_key_from_nodeid(nodeid: str) -> str:
    fr_id = infer_fr_id_from_nodeid(nodeid)
    return fr_id.replace("FR-", "FR") if fr_id else ""


def _feature_ids_for_nodeid(nodeid: str, marker_index: dict[str, list[str]]) -> list[str]:
    base_nodeid = nodeid.split("[", 1)[0]
    explicit_feature_ids = marker_index.get(base_nodeid, [])
    if explicit_feature_ids:
        return explicit_feature_ids
    fr_group_key = _fr_group_key_from_nodeid(nodeid)
    if fr_group_key:
        return [fr_group_key]
    feature_id = _extract_feature_from_nodeid(nodeid)
    return [feature_id] if feature_id else []


def _append_raw_result(
    raw_results: list[dict],
    *,
    nodeid: str,
    passed: bool,
    failure_msg: str,
    duration: float = 0,
    marker_index: dict[str, list[str]],
) -> None:
    feature_ids = _feature_ids_for_nodeid(nodeid, marker_index) or [""]
    verify_kind = _extract_kind_from_nodeid(nodeid)
    for feature_id in feature_ids:
        raw_results.append(
            {
                "feature_id": feature_id,
                "verify_kind": verify_kind,
                "passed": passed,
                "failure_msg": failure_msg,
                "nodeid": nodeid,
                "duration": duration,
            }
        )


def _log(stage: int, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] Stage {stage}/6: {msg}")


def run_pipeline(
    skip_tests: bool = False,
    only_audit: bool = False,
    claim_source: str = "registry",
    legacy_v7_path: str | None = None,
):
    """执行完整验真管道."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    claim_source = (claim_source or "registry").strip().lower()
    legacy_v7 = Path(legacy_v7_path) if legacy_v7_path else LEGACY_V7_PATH
    start = time.monotonic()

    # ── 阶段 1: 构建 claim_registry ───────────────
    from .parse_progress_doc import build_claim_registry
    if claim_source == "legacy_v7":
        import warnings
        warnings.warn(
            "legacy_v7 claim source creates a self-referential verification loop "
            "(22_v7 is both the claim source and the verification target). "
            "Use --claim-source=registry for independent verification.",
            UserWarning,
            stacklevel=2,
        )
        _log(1, f"⚠ 构建 claim_registry (legacy_v7 — 自证模式，仅用于兼容): {legacy_v7}")
        claim_registry = build_claim_registry(mode="legacy_v7", legacy_v7_path=legacy_v7)
    else:
        _log(1, "构建 claim_registry (registry主链: SSOT01~05 + feature_registry + page_expectations)")
        claim_registry = build_claim_registry(mode="registry")
    CLAIM_REGISTRY.write_text(
        json.dumps(claim_registry, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _log(1, f"[OK] 生成 {len(claim_registry)} 条 claim -> {CLAIM_REGISTRY}")

    # ── 阶段 2: 构建验真计划 ─────────────────────
    _log(2, "构建 verification plan")
    from .build_verification_plan import build_verification_plan
    plan = build_verification_plan(claim_registry)
    VERIFICATION_PLAN.write_text(
        json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _log(2, f"[OK] 生成 {len(plan)} 条验真项 -> {VERIFICATION_PLAN}")

    # ── 阶段 3: 运行参数化验真测试 ──────────────
    raw_results: list[dict] = []
    if not skip_tests and not only_audit:
        _log(3, "运行参数化验真测试")
        raw_results = _run_verification_tests()
        RAW_RESULTS.write_text(
            json.dumps(raw_results, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        _log(3, f"[OK] 收集 {len(raw_results)} 条实测结果 -> {RAW_RESULTS}")
    else:
        _log(3, "跳过 (--skip-tests 或 --only-audit)")
        if RAW_RESULTS.exists():
            raw_results = json.loads(RAW_RESULTS.read_text(encoding="utf-8"))
            _log(3, f"  使用缓存: {len(raw_results)} 条")

    # ── 阶段 4: 差距分析 ─────────────────────────
    # 先执行阶段5以便阶段4可引用
    _log(5, "测试质量审计")
    from .audit_test_quality import audit_test_quality
    quality_report = audit_test_quality()
    QUALITY_REPORT.write_text(
        json.dumps(quality_report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    fake = sum(1 for q in quality_report if q["issue_kind"] == "FAKE")
    hollow = sum(1 for q in quality_report if q["issue_kind"] == "HOLLOW")
    weak = sum(1 for q in quality_report if q["issue_kind"] == "WEAK")
    _log(5, f"[OK] FAKE={fake}, HOLLOW={hollow}, WEAK={weak} -> {QUALITY_REPORT}")

    _log(4, "差距分析")
    from .analyze_gaps import analyze_gaps, summarize_gaps
    gap_details = analyze_gaps(claim_registry, raw_results, quality_report)
    gap_summary = summarize_gaps(gap_details)
    gap_report = {"summary": gap_summary, "details": gap_details}
    GAP_REPORT.write_text(
        json.dumps(gap_report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _log(4, f"[OK] CONFIRMED={gap_summary.get('by_verdict', {}).get('CONFIRMED', 0)}, "
         f"FALSE_CLEAR={gap_summary.get('false_clear_count', 0)} -> {GAP_REPORT}")

    # ── 阶段 6: 生成 v8 总表 ────────────────────
    _log(6, "生成 v8 验真总表")
    from .render_v8_doc import render_v8_doc
    v8_md = render_v8_doc(
        claim_registry,
        gap_report,
        quality_report,
        plan,
        claim_source=claim_source,
    )
    V8_PATH.write_text(v8_md, encoding="utf-8")
    _log(6, f"[OK] v8 总表 -> {V8_PATH} ({len(v8_md)} 字符)")

    elapsed = time.monotonic() - start
    print(f"\n{'='*60}")
    print(f"管道完成, 耗时 {elapsed:.1f}s")
    print(f"产物:")
    print(f"  {CLAIM_REGISTRY}")
    print(f"  {VERIFICATION_PLAN}")
    print(f"  {RAW_RESULTS}")
    print(f"  {QUALITY_REPORT}")
    print(f"  {GAP_REPORT}")
    print(f"  {V8_PATH}")

    if gap_summary.get("ci_should_block"):
        print(f"\n[BLOCK] CI 阻断: {gap_summary.get('false_clear_count', 0)} FALSE_CLEAR, "
              f"{fake + hollow} FAKE/HOLLOW 测试")
        return 1
    else:
        print(f"\n[PASS] CI 通过: {gap_summary.get('confirmed_rate', 0)}% 确认率")
        return 0


def _run_verification_tests() -> list[dict]:
    """运行 pytest 收集实测结果.

    运行 test_doc_driven_verify.py, 并解析 JSON 报告.
    """
    json_report = OUTPUT_DIR / "pytest_verify_report.json"
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/test_doc_driven_verify.py",
        f"--json-report-file={json_report}",
        "--json-report",
        "-v", "--tb=short",
        "--no-header",
    ]

    # 先尝试用 pytest-json-report; 不可用时回退到基本模式
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300
        )
    except FileNotFoundError:
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/test_doc_driven_verify.py",
            "-v", "--tb=short",
            f"--junitxml={OUTPUT_DIR / 'junit_verify.xml'}",
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300
        )

    # 解析结果
    raw_results: list[dict] = []
    if json_report.exists():
        report_data = json.loads(json_report.read_text(encoding="utf-8"))
        for test in report_data.get("tests", []):
            # 从 nodeid 提取 feature_id: test_doc_driven_verify.py::test_FR00_AUTH_01_contract
            nodeid = test.get("nodeid", "")
            feature_id = _extract_feature_from_nodeid(nodeid)
            verify_kind = _extract_kind_from_nodeid(nodeid)
            raw_results.append({
                "feature_id": feature_id,
                "verify_kind": verify_kind,
                "passed": test.get("outcome") == "passed",
                "failure_msg": test.get("call", {}).get("longrepr", "") if test.get("outcome") != "passed" else "",
                "nodeid": nodeid,
                "duration": test.get("duration", 0),
            })
    else:
        # 从 stdout 解析基本 PASSED/FAILED
        for line in result.stdout.splitlines():
            if " PASSED" in line or " FAILED" in line:
                parts = line.strip().split()
                if parts:
                    nodeid = parts[0]
                    passed = "PASSED" in line
                    feature_id = _extract_feature_from_nodeid(nodeid)
                    verify_kind = _extract_kind_from_nodeid(nodeid)
                    raw_results.append({
                        "feature_id": feature_id,
                        "verify_kind": verify_kind,
                        "passed": passed,
                        "failure_msg": "" if passed else "FAILED (详见 pytest 输出)",
                        "nodeid": nodeid,
                    })

    return raw_results


def _run_verification_tests() -> list[dict]:
    """Run the doc-driven verification surface and collect structured results."""
    json_report = OUTPUT_DIR / "pytest_verify_report.json"
    marker_index = _build_stage3_feature_marker_index()
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        *STAGE3_TEST_TARGETS,
        f"--json-report-file={json_report}",
        "--json-report",
        "-v",
        "--tb=short",
        "--no-header",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except FileNotFoundError:
        cmd = [
            sys.executable,
            "-m",
            "pytest",
            *STAGE3_TEST_TARGETS,
            "-v",
            "--tb=short",
            f"--junitxml={OUTPUT_DIR / 'junit_verify.xml'}",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    raw_results: list[dict] = []
    if json_report.exists():
        report_data = json.loads(json_report.read_text(encoding="utf-8"))
        for test in report_data.get("tests", []):
            nodeid = test.get("nodeid", "")
            _append_raw_result(
                raw_results,
                nodeid=nodeid,
                passed=test.get("outcome") == "passed",
                failure_msg=test.get("call", {}).get("longrepr", "") if test.get("outcome") != "passed" else "",
                duration=test.get("duration", 0),
                marker_index=marker_index,
            )
        return raw_results

    for line in result.stdout.splitlines():
        if " PASSED" not in line and " FAILED" not in line:
            continue
        parts = line.strip().split()
        if not parts:
            continue
        nodeid = parts[0]
        passed = "PASSED" in line
        _append_raw_result(
            raw_results,
            nodeid=nodeid,
            passed=passed,
            failure_msg="" if passed else "FAILED (璇﹁ pytest 杈撳嚭)",
            marker_index=marker_index,
        )

    return raw_results


def _extract_feature_from_nodeid(nodeid: str) -> str:
    """从 pytest nodeid 提取 feature_id.

    例: test_doc_driven_verify.py::test_FR00_AUTH_01_contract → FR00-AUTH-01
    """
    import re
    m = re.search(r"test_(FR\d+)_(\w+?)_(\d+)", nodeid)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""


def _extract_kind_from_nodeid(nodeid: str) -> str:
    """从 pytest nodeid 提取 verify_kind."""
    for kind in ("browser", "dom", "contract", "api", "test_quality"):
        if kind in nodeid.lower():
            return kind
    return "general"


# ── CLI 入口 ──────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="验真管道主入口")
    parser.add_argument("--skip-tests", action="store_true",
                        help="跳过阶段3 (参数化验真测试)")
    parser.add_argument("--only-audit", action="store_true",
                        help="只运行审计+渲染 (跳过阶段3)")
    parser.add_argument(
        "--claim-source",
        choices=["registry", "legacy_v7"],
        default="registry",
        help="claim 来源（默认 registry；legacy_v7 仅兼容模式）",
    )
    parser.add_argument(
        "--legacy-v7-path",
        default=None,
        help="legacy_v7 模式下的 22 输入路径",
    )
    args = parser.parse_args()

    exit_code = run_pipeline(
        skip_tests=args.skip_tests,
        only_audit=args.only_audit,
        claim_source=args.claim_source,
        legacy_v7_path=args.legacy_v7_path,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
