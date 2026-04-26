"""
audit_test_quality.py — 阶段 5: 弱测试/假测试扫描

覆盖所有前端相关测试, 统一识别以下模式:
  - `assert ... or True`          → FAKE
  - `assert callable(...)`        → HOLLOW
  - `inspect.getsource` 型源码自证 → FAKE
  - 过宽 `status_code in (...)`   → WEAK
  - 纯 HTML 子串伪 DOM 检查       → WEAK
  - `assert True`                 → FAKE
  - `pass` 作为唯一测试体         → FAKE

每个问题测试回写到 FR 维度: FAKE / HOLLOW / WEAK, 并建议替换方案.
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from dataclasses import dataclass, asdict

# ── 扫描模式定义 ──────────────────────────────────────

@dataclass
class TestIssue:
    """单条测试质量问题."""
    test_file: str
    test_func: str
    line_no: int
    issue_kind: str   # FAKE / HOLLOW / WEAK
    pattern: str      # 命中的模式名
    evidence: str     # 有问题的代码片段
    suggestion: str   # 建议替换
    mapped_frs: list[str]  # 关联的 FR ID

# 从测试函数名推断 FR
_RE_FR_FROM_TEST = re.compile(r"test_(?:fr|nfr)?(\d+)", re.IGNORECASE)

# 前端相关测试文件 pattern
_TEST_GLOB_PATTERNS = [
    "tests/test_v7_audit_batch*.py",
    "tests/test_fr*.py",
    "tests/test_doc_driven*.py",
]


def _infer_fr_ids(func_name: str) -> list[str]:
    """从测试函数名推断 FR 编号."""
    m = _RE_FR_FROM_TEST.match(func_name)
    if m:
        num = m.group(1).zfill(2)
        return [f"FR-{num}"]
    return []


def _scan_file_ast(filepath: Path) -> list[TestIssue]:
    """用 AST 扫描单个测试文件中的弱测试模式."""
    issues: list[TestIssue] = []
    try:
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(filepath))
    except (SyntaxError, UnicodeDecodeError):
        return issues

    lines = source.splitlines()

    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not node.name.startswith("test_"):
            continue

        func_name = node.name
        fr_ids = _infer_fr_ids(func_name)
        body = node.body

        # Pattern 1: 函数体只有 pass
        if len(body) == 1 and isinstance(body[0], ast.Pass):
            issues.append(TestIssue(
                test_file=str(filepath),
                test_func=func_name,
                line_no=node.lineno,
                issue_kind="FAKE",
                pattern="empty_body_pass",
                evidence=f"def {func_name}(...): pass",
                suggestion="实现真实业务断言: HTTP 调用 + DB 验证",
                mapped_frs=fr_ids,
            ))
            continue

        # 遍历函数体中的断言
        for stmt in ast.walk(node):
            if isinstance(stmt, ast.Assert):
                assert_line = _get_line(lines, stmt.lineno)

                # Pattern 2: assert ... or True
                if isinstance(stmt.test, ast.BoolOp) and isinstance(stmt.test.op, ast.Or):
                    for val in stmt.test.values:
                        if isinstance(val, ast.Constant) and val.value is True:
                            issues.append(TestIssue(
                                test_file=str(filepath),
                                test_func=func_name,
                                line_no=stmt.lineno,
                                issue_kind="FAKE",
                                pattern="or_true",
                                evidence=assert_line,
                                suggestion="移除 `or True`, 让断言可以真正失败",
                                mapped_frs=fr_ids,
                            ))

                # Pattern 3: assert True
                if isinstance(stmt.test, ast.Constant) and stmt.test.value is True:
                    issues.append(TestIssue(
                        test_file=str(filepath),
                        test_func=func_name,
                        line_no=stmt.lineno,
                        issue_kind="FAKE",
                        pattern="assert_true",
                        evidence=assert_line,
                        suggestion="替换为具体业务断言",
                        mapped_frs=fr_ids,
                    ))

                # Pattern 4: assert callable(...)
                if (isinstance(stmt.test, ast.Call) and
                        isinstance(stmt.test.func, ast.Name) and
                        stmt.test.func.id == "callable"):
                    issues.append(TestIssue(
                        test_file=str(filepath),
                        test_func=func_name,
                        line_no=stmt.lineno,
                        issue_kind="HOLLOW",
                        pattern="assert_callable",
                        evidence=assert_line,
                        suggestion="替换为调用该函数并断言返回值/行为",
                        mapped_frs=fr_ids,
                    ))

        # 基于文本的模式扫描 (AST 难以覆盖的)
        func_source = _get_func_source(lines, node)

        # Pattern 5: inspect.getsource 型源码自证
        if "inspect.getsource" in func_source or "getsource(" in func_source:
            issues.append(TestIssue(
                test_file=str(filepath),
                test_func=func_name,
                line_no=node.lineno,
                issue_kind="FAKE",
                pattern="getsource_proof",
                evidence="使用 inspect.getsource 检查源码",
                suggestion="替换为真实 HTTP 调用验证行为",
                mapped_frs=fr_ids,
            ))

        # Pattern 6: 过宽状态码 — status_code in (200, 201, 301, 302, 400, 401, 403, 404, 422, 500)
        wide_status = re.findall(
            r"status_code\s+in\s+\([\d,\s]{20,}\)", func_source
        )
        if wide_status:
            issues.append(TestIssue(
                test_file=str(filepath),
                test_func=func_name,
                line_no=node.lineno,
                issue_kind="WEAK",
                pattern="wide_status_code",
                evidence=wide_status[0][:80],
                suggestion="精确断言预期状态码, 如 `assert resp.status_code == 200`",
                mapped_frs=fr_ids,
            ))

        # Pattern 7: 纯字符串子串 HTML 检查 (非 DOM 解析)
        html_frag_checks = re.findall(
            r'assert\s+["\'][\w\-<>]+["\']\s+in\s+\w+\.(text|content)', func_source
        )
        if html_frag_checks:
            issues.append(TestIssue(
                test_file=str(filepath),
                test_func=func_name,
                line_no=node.lineno,
                issue_kind="WEAK",
                pattern="html_substring_check",
                evidence="使用 `\"string\" in response.text` 检查 HTML",
                suggestion="替换为 BeautifulSoup/lxml DOM 解析 + 选择器校验",
                mapped_frs=fr_ids,
            ))

    return issues


def _get_line(lines: list[str], lineno: int) -> str:
    if 0 < lineno <= len(lines):
        return lines[lineno - 1].strip()
    return ""


def _get_func_source(lines: list[str], node: ast.FunctionDef) -> str:
    start = node.lineno - 1
    end = node.end_lineno if hasattr(node, "end_lineno") and node.end_lineno else start + 50
    return "\n".join(lines[start:end])


def audit_test_quality(
    test_root: Path | None = None,
    extra_patterns: list[str] | None = None,
) -> list[dict]:
    """扫描所有测试文件, 返回 quality_report (list[dict])."""
    root = test_root or Path("tests")
    patterns = extra_patterns or _TEST_GLOB_PATTERNS

    all_files: set[Path] = set()
    for pat in patterns:
        # 解析 glob 路径: pat 可能是 "tests/test_v7*.py"
        base = Path(pat).parent
        glob_pat = Path(pat).name
        for f in base.glob(glob_pat):
            all_files.add(f.resolve())

    # 如果使用绝对 root, 也扫描
    if root.exists():
        for f in root.glob("test_*.py"):
            all_files.add(f.resolve())

    all_issues: list[TestIssue] = []
    for fp in sorted(all_files):
        all_issues.extend(_scan_file_ast(fp))

    return [asdict(i) for i in all_issues]


# ── CLI 入口 ──────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="测试质量审计")
    parser.add_argument("--output", default="output/test_quality_report.json")
    args = parser.parse_args()

    report = audit_test_quality()
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    fake = sum(1 for r in report if r["issue_kind"] == "FAKE")
    hollow = sum(1 for r in report if r["issue_kind"] == "HOLLOW")
    weak = sum(1 for r in report if r["issue_kind"] == "WEAK")
    print(f"[audit] 扫描完成: FAKE={fake}, HOLLOW={hollow}, WEAK={weak}, 共{len(report)}条 → {out}")


if __name__ == "__main__":
    main()
