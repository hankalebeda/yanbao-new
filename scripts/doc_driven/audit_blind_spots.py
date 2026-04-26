from __future__ import annotations

import ast
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]
TESTS_DIR = ROOT / "tests"
TEMPLATES_DIR = ROOT / "app" / "web" / "templates"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import app
from scripts.doc_driven.audit_test_quality import audit_test_quality
from scripts.doc_driven.page_expectations import PAGE_EXPECTATIONS


EXCLUDED_ROUTES = {
    "/favicon.ico",
    "/health",
    "/logout",
}
EXCLUDED_PREFIXES = (
    "/api/",
    "/auth/",
    "/docs",
    "/openapi",
    "/redoc",
    "/static/",
)


@dataclass
class BlindSpotIssue:
    issue_id: str
    severity: str
    kind: str
    title: str
    evidence: list[str]
    recommendation: str


@dataclass
class GuardedAssertion:
    test_file: str
    test_func: str
    line_no: int
    pattern: str
    evidence: str


@dataclass
class SeedDefaultIssue:
    file: str
    function: str
    argument: str
    default_value: str
    line_no: int


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _test_files() -> list[Path]:
    return sorted(TESTS_DIR.glob("test_*.py"))


def _safe_unparse(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:
        return ""


def _iter_non_nested_ast_nodes(statements: list[ast.stmt]):
    stack: list[ast.AST] = list(reversed(statements))
    while stack:
        node = stack.pop()
        yield node
        for child in reversed(list(ast.iter_child_nodes(node))):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                continue
            stack.append(child)


def _body_has_assert(statements: list[ast.stmt]) -> bool:
    for item in _iter_non_nested_ast_nodes(statements):
        if isinstance(item, ast.Assert):
            return True
        if isinstance(item, ast.Expr) and isinstance(item.value, ast.Call):
            func_text = _safe_unparse(item.value.func)
            if func_text.endswith("pytest.fail") or func_text == "pytest.fail":
                return True
    return False


def _body_has_pass(statements: list[ast.stmt]) -> bool:
    return any(isinstance(item, ast.Pass) for item in _iter_non_nested_ast_nodes(statements))


def _condition_looks_guarded(node: ast.AST) -> bool:
    text = _safe_unparse(node)
    risky_tokens = (
        "status_code",
        "items",
        "data",
        "payload",
        "body",
        "response",
        "resp",
        "total",
        "count",
        "token",
        "tokens",
        "chain",
        "len(",
        "hasattr(",
        "refresh_token",
        "access_token",
        ".get(",
    )
    return any(token in text for token in risky_tokens)


def _scan_guarded_assertions() -> list[GuardedAssertion]:
    findings: list[GuardedAssertion] = []
    for path in _test_files():
        try:
            source = _read_text(path)
            tree = ast.parse(source, filename=str(path))
        except (SyntaxError, UnicodeDecodeError):
            continue

        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not node.name.startswith("test_"):
                continue

            for stmt in _iter_non_nested_ast_nodes(node.body):
                if isinstance(stmt, ast.Pass):
                    findings.append(
                        GuardedAssertion(
                            test_file=str(path.relative_to(ROOT)),
                            test_func=node.name,
                            line_no=stmt.lineno,
                            pattern="pass_in_test",
                            evidence="pass",
                        )
                    )
                elif isinstance(stmt, ast.If):
                    if not _condition_looks_guarded(stmt.test):
                        continue
                    if not (_body_has_assert(stmt.body) or _body_has_pass(stmt.body)):
                        continue
                    if _body_has_assert(stmt.orelse):
                        continue
                    findings.append(
                        GuardedAssertion(
                            test_file=str(path.relative_to(ROOT)),
                            test_func=node.name,
                            line_no=stmt.lineno,
                            pattern="guarded_assertion_branch",
                            evidence=_safe_unparse(stmt.test)[:220],
                        )
                    )
                elif isinstance(stmt, ast.Assert):
                    text = _safe_unparse(stmt.test)
                    if (
                        isinstance(stmt.test, ast.Compare)
                        and any(isinstance(op, ast.NotEq) for op in stmt.test.ops)
                        and "status_code" in text
                    ):
                        findings.append(
                            GuardedAssertion(
                                test_file=str(path.relative_to(ROOT)),
                                test_func=node.name,
                                line_no=stmt.lineno,
                                pattern="soft_negative_status_assert",
                                evidence=text[:220],
                            )
                        )
                        continue
                    if not isinstance(stmt.test, ast.BoolOp) or not isinstance(stmt.test.op, ast.Or):
                        continue
                    if "status_code" not in text:
                        continue
                    findings.append(
                        GuardedAssertion(
                            test_file=str(path.relative_to(ROOT)),
                            test_func=node.name,
                            line_no=stmt.lineno,
                            pattern="soft_or_assert",
                            evidence=text[:220],
                        )
                    )

    deduped: list[GuardedAssertion] = []
    seen: set[tuple[str, str, int, str]] = set()
    for item in findings:
        key = (item.test_file, item.test_func, item.line_no, item.pattern)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _scan_time_coupled_seed_defaults() -> list[SeedDefaultIssue]:
    path = ROOT / "tests" / "helpers_ssot.py"
    if not path.exists():
        return []
    try:
        tree = ast.parse(_read_text(path), filename=str(path))
    except (SyntaxError, UnicodeDecodeError):
        return []

    issues: list[SeedDefaultIssue] = []
    date_literal_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        defaults = list(node.args.defaults or [])
        if not defaults:
            positional_pairs: list[tuple[str, ast.AST]] = []
        else:
            positional_args = list(node.args.args)
            default_arg_names = [arg.arg for arg in positional_args[-len(defaults):]]
            positional_pairs = list(zip(default_arg_names, defaults))

        keyword_pairs = [
            (arg.arg, default_node)
            for arg, default_node in zip(node.args.kwonlyargs, node.args.kw_defaults or [])
            if default_node is not None
        ]

        for arg_name, default_node in positional_pairs + keyword_pairs:
            if not isinstance(default_node, ast.Constant) or not isinstance(default_node.value, str):
                continue
            value = default_node.value
            if not date_literal_re.match(value):
                continue
            issues.append(
                SeedDefaultIssue(
                    file=str(path.relative_to(ROOT)),
                    function=node.name,
                    argument=arg_name,
                    default_value=value,
                    line_no=default_node.lineno,
                )
            )
    return issues


def _enumerate_html_routes() -> list[str]:
    routes: list[str] = []
    for route in app.routes:
        path = getattr(route, "path", "")
        methods = getattr(route, "methods", set()) or set()
        if not path or "GET" not in methods:
            continue
        if path in EXCLUDED_ROUTES:
            continue
        if any(path.startswith(prefix) for prefix in EXCLUDED_PREFIXES):
            continue
        routes.append(path)
    return sorted(set(routes))


def _route_static_tokens(route: str) -> list[str]:
    return [token for token in re.split(r"\{[^}]+\}", route) if token]


def _text_mentions_route(text: str, route: str) -> bool:
    tokens = _route_static_tokens(route)
    if not tokens:
        return route in text
    offset = 0
    for token in tokens:
        position = text.find(token, offset)
        if position < 0:
            return False
        offset = position + len(token)
    return True


def _route_coverage_stats() -> dict[str, Any]:
    routes = _enumerate_html_routes()
    by_route: dict[str, list[Any]] = {}
    for expectation in PAGE_EXPECTATIONS:
        by_route.setdefault(expectation.route, []).append(expectation)

    dom_text = _read_text(TESTS_DIR / "test_doc_driven_verify.py") if (TESTS_DIR / "test_doc_driven_verify.py").exists() else ""
    browser_text = _read_text(TESTS_DIR / "test_gate_browser_playwright.py") if (TESTS_DIR / "test_gate_browser_playwright.py").exists() else ""

    missing_expectations: list[str] = []
    pages_without_dom: list[str] = []
    pages_without_browser: list[str] = []
    pages_without_selectors: list[str] = []
    pages_without_expected_api: list[str] = []
    selectors_missing_in_template: list[str] = []
    retired_compat_pages: list[str] = []
    non_html_expectations: list[str] = []

    for route in routes:
        if route not in by_route:
            missing_expectations.append(route)

    for expectation in PAGE_EXPECTATIONS:
        if getattr(expectation, "retention_mode", "active") == "retired_compat":
            retired_compat_pages.append(expectation.page_id)
        if getattr(expectation, "contract_kind", "html_page") != "html_page":
            non_html_expectations.append(expectation.page_id)
        template_path = TEMPLATES_DIR / expectation.template
        template_text = _read_text(template_path) if template_path.exists() else ""
        mentions_dom = (
            expectation.page_id in dom_text
            or expectation.template in dom_text
            or _text_mentions_route(dom_text, expectation.route)
        )
        mentions_browser = (
            expectation.page_id in browser_text
            or expectation.template in browser_text
            or _text_mentions_route(browser_text, expectation.route)
        )

        if not expectation.must_have_selectors:
            pages_without_selectors.append(expectation.page_id)
        for selector in expectation.must_have_selectors:
            token = selector.strip()
            if not token:
                continue
            if token in {"html", "body"}:
                continue
            probe = token
            if token.startswith("#"):
                probe = f'id="{token[1:]}"'
            elif token.startswith("."):
                probe = token[1:]
            elif re.match(r"^\w+\[\w+=['\"].+['\"]\]$", token):
                probe = token.split("=", 1)[-1].strip("]").strip("'").strip('"')
            elif "name='" in token:
                probe = token.split("name='")[-1].split("'")[0]
            elif "type='" in token:
                probe = token.split("type='")[-1].split("'")[0]
            if probe and probe not in template_text:
                selectors_missing_in_template.append(f"{expectation.page_id}:{selector}")
        if getattr(expectation, "expect_dom_reference", True) and not mentions_dom:
            pages_without_dom.append(expectation.page_id)
        if getattr(expectation, "expect_browser_reference", True) and not mentions_browser:
            pages_without_browser.append(expectation.page_id)
        if not expectation.expected_api_calls and expectation.page_id not in {
            "login",
            "register",
            "forgot_password",
            "reset_password",
            "terms",
            "privacy",
            "403",
            "404",
            "500",
        }:
            pages_without_expected_api.append(expectation.page_id)

    return {
        "html_routes": routes,
        "missing_expectations": sorted(missing_expectations),
        "pages_without_dom": sorted(set(pages_without_dom)),
        "pages_without_browser": sorted(set(pages_without_browser)),
        "pages_without_selectors": sorted(set(pages_without_selectors)),
        "pages_without_expected_api": sorted(set(pages_without_expected_api)),
        "selectors_missing_in_template": sorted(set(selectors_missing_in_template)),
        "retired_compat_pages": sorted(set(retired_compat_pages)),
        "non_html_expectations": sorted(set(non_html_expectations)),
    }


def _feature_page_mapping_stats() -> dict[str, Any]:
    registry = json.loads((ROOT / "app" / "governance" / "feature_registry.json").read_text(encoding="utf-8"))["features"]
    registry_page_features = {
        item["feature_id"]
        for item in registry
        if (
            ("page" in (item.get("required_test_kinds") or []) and item.get("runtime_page_path"))
            or item["feature_id"].startswith("LEGACY-REPORT-")
            or (item["feature_id"].startswith("OOS-MOCK-PAY-") and item.get("runtime_page_path"))
        )
    }
    expectation_feature_ids = {
        feature_id
        for expectation in PAGE_EXPECTATIONS
        for feature_id in expectation.fr_ids
    }
    return {
        "registry_page_features": sorted(registry_page_features),
        "expectation_feature_ids": sorted(expectation_feature_ids),
        "only_in_registry": sorted(registry_page_features - expectation_feature_ids),
        "only_in_expectations": sorted(expectation_feature_ids - registry_page_features),
    }


def _build_issues(
    *,
    quality_report: list[dict[str, Any]],
    guarded_assertions: list[GuardedAssertion],
    seed_default_issues: list[SeedDefaultIssue],
    route_stats: dict[str, Any],
    feature_stats: dict[str, Any],
) -> list[BlindSpotIssue]:
    issues: list[BlindSpotIssue] = []

    fake_count = sum(1 for item in quality_report if item["issue_kind"] == "FAKE")
    hollow_count = sum(1 for item in quality_report if item["issue_kind"] == "HOLLOW")
    weak_count = sum(1 for item in quality_report if item["issue_kind"] == "WEAK")
    if fake_count or hollow_count or weak_count:
        sample = quality_report[:8]
        evidence = [
            f"FAKE={fake_count}, HOLLOW={hollow_count}, WEAK={weak_count}",
        ]
        evidence.extend(
            f"{item['test_file']}::{item['test_func']} -> {item['pattern']}"
            for item in sample
        )
        issues.append(
            BlindSpotIssue(
                issue_id="BLIND-001",
                severity="P1",
                kind="test_method",
                title="The current test suite still contains fake, hollow, and weak checks",
                evidence=evidence,
                recommendation="Make test-quality audit a first gate and refuse to trust green results until FAKE/HOLLOW patterns are removed.",
            )
        )

    if guarded_assertions:
        evidence = [
            f"guarded_assertions={len(guarded_assertions)}",
        ]
        evidence.extend(
            f"{item.test_file}::{item.test_func}:{item.line_no} -> {item.pattern} [{item.evidence}]"
            for item in guarded_assertions[:10]
        )
        issues.append(
            BlindSpotIssue(
                issue_id="BLIND-002",
                severity="P1",
                kind="test_method",
                title="Multiple tests still guard assertions behind success-only branches",
                evidence=evidence,
                recommendation="Replace conditional assertions with hard failure branches so unexpected status/data emptiness cannot pass silently.",
            )
        )

    if seed_default_issues:
        evidence = [f"time_coupled_seed_defaults={len(seed_default_issues)}"]
        evidence.extend(
            f"{item.file}::{item.function}:{item.line_no} -> {item.argument}={item.default_value}"
            for item in seed_default_issues[:10]
        )
        issues.append(
            BlindSpotIssue(
                issue_id="BLIND-007",
                severity="P1",
                kind="test_method",
                title="Some shared test helpers hardcode historical trade dates and will drift as the runtime window moves",
                evidence=evidence,
                recommendation="Use dynamic runtime anchors in shared seeds, or require each test to set trade_date explicitly when visibility windows matter.",
            )
        )

    if route_stats["missing_expectations"]:
        evidence = [
            f"html_routes={len(route_stats['html_routes'])}",
            f"missing_expectations={len(route_stats['missing_expectations'])}",
            "routes: " + ", ".join(route_stats["missing_expectations"][:12]),
        ]
        issues.append(
            BlindSpotIssue(
                issue_id="BLIND-003",
                severity="P2",
                kind="coverage_gap",
                title="Several HTML routes exist without any formal page expectation entry",
                evidence=evidence,
                recommendation="Drive page coverage from the real route inventory, not from a hand-maintained subset.",
            )
        )

    if route_stats["pages_without_browser"] or route_stats["pages_without_dom"]:
        evidence = []
        if route_stats["pages_without_dom"]:
            evidence.append("pages_without_dom: " + ", ".join(route_stats["pages_without_dom"][:12]))
        if route_stats["pages_without_browser"]:
            evidence.append("pages_without_browser: " + ", ".join(route_stats["pages_without_browser"][:12]))
        issues.append(
            BlindSpotIssue(
                issue_id="BLIND-004",
                severity="P2",
                kind="coverage_gap",
                title="A page can still ship without the full contract + DOM + browser evidence set",
                evidence=evidence,
                recommendation="Require user-facing pages to have route coverage in both doc-driven DOM tests and Playwright browser tests.",
            )
        )

    if route_stats["pages_without_selectors"] or route_stats["pages_without_expected_api"]:
        evidence = []
        if route_stats["pages_without_selectors"]:
            evidence.append("pages_without_selectors: " + ", ".join(route_stats["pages_without_selectors"][:12]))
        if route_stats["pages_without_expected_api"]:
            evidence.append("pages_without_expected_api: " + ", ".join(route_stats["pages_without_expected_api"][:12]))
        issues.append(
            BlindSpotIssue(
                issue_id="BLIND-005",
                severity="P2",
                kind="coverage_gap",
                title="Some page expectations are too weak to catch real rendering or wiring regressions",
                evidence=evidence,
                recommendation="Every dynamic page expectation should pin DOM anchors and expected API calls instead of only asserting a 200 response.",
            )
        )

    if route_stats["selectors_missing_in_template"]:
        issues.append(
            BlindSpotIssue(
                issue_id="BLIND-008",
                severity="P1",
                kind="selector_drift",
                title="Some declared page selectors no longer exist in the bound HTML templates",
                evidence=route_stats["selectors_missing_in_template"][:12],
                recommendation="Keep page_expectations aligned with the real template structure; otherwise DOM coverage metrics become misleading.",
            )
        )

    if feature_stats["only_in_registry"] or feature_stats["only_in_expectations"]:
        evidence = []
        if feature_stats["only_in_registry"]:
            evidence.append("only_in_registry: " + ", ".join(feature_stats["only_in_registry"][:12]))
        if feature_stats["only_in_expectations"]:
            evidence.append("only_in_expectations: " + ", ".join(feature_stats["only_in_expectations"][:12]))
        issues.append(
            BlindSpotIssue(
                issue_id="BLIND-006",
                severity="P2",
                kind="coverage_gap",
                title="Feature registry and page expectations are not aligned one-to-one",
                evidence=evidence,
                recommendation="Use feature_registry.json as the source of truth and fail the audit whenever page expectations drift from it.",
            )
        )

    return issues


def build_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    issues = report.get("issues") or []
    route_stats = report.get("route_stats") or {}
    lines: list[str] = []
    lines.append("# Blind Spot Audit")
    lines.append("")
    lines.append(f"- Generated at: `{report.get('generated_at', 'unknown')}`")
    lines.append(f"- Root: `{ROOT}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Fake/Hollow/Weak test issues: `{summary['fake_count']}/{summary['hollow_count']}/{summary['weak_count']}`")
    lines.append(f"- Guarded assertions: `{summary['guarded_assertions']}`")
    lines.append(f"- Time-coupled seed defaults: `{summary['time_coupled_seed_defaults']}`")
    lines.append(f"- HTML routes without page expectations: `{summary['missing_expectations']}`")
    lines.append(f"- Pages without DOM test references: `{summary['pages_without_dom']}`")
    lines.append(f"- Pages without browser test references: `{summary['pages_without_browser']}`")
    lines.append(f"- Registry/page mapping drift: `{summary['mapping_drift']}`")
    lines.append(f"- Selectors missing in templates: `{summary['selector_drift']}`")
    lines.append(
        f"- Retired compatibility expectations tracked: `{len(route_stats.get('retired_compat_pages', []))}`"
    )
    lines.append("")
    lines.append("## Issues")
    lines.append("")
    if not issues:
        lines.append("- No blind-spot issues detected.")
    else:
        for issue in issues:
            lines.append(f"### {issue['issue_id']}")
            lines.append("")
            lines.append(f"- Severity: `{issue['severity']}`")
            lines.append(f"- Kind: `{issue['kind']}`")
            lines.append(f"- Title: {issue['title']}")
            lines.append("- Evidence:")
            for item in issue["evidence"]:
                lines.append(f"  - {item}")
            lines.append(f"- Recommendation: {issue['recommendation']}")
            lines.append("")
    return "\n".join(lines) + "\n"


def audit_blind_spots() -> dict[str, Any]:
    quality_report = audit_test_quality()
    guarded_assertions = _scan_guarded_assertions()
    seed_default_issues = _scan_time_coupled_seed_defaults()
    route_stats = _route_coverage_stats()
    feature_stats = _feature_page_mapping_stats()
    issues = _build_issues(
        quality_report=quality_report,
        guarded_assertions=guarded_assertions,
        seed_default_issues=seed_default_issues,
        route_stats=route_stats,
        feature_stats=feature_stats,
    )

    summary = {
        "fake_count": sum(1 for item in quality_report if item["issue_kind"] == "FAKE"),
        "hollow_count": sum(1 for item in quality_report if item["issue_kind"] == "HOLLOW"),
        "weak_count": sum(1 for item in quality_report if item["issue_kind"] == "WEAK"),
        "guarded_assertions": len(guarded_assertions),
        "time_coupled_seed_defaults": len(seed_default_issues),
        "missing_expectations": len(route_stats["missing_expectations"]),
        "pages_without_dom": len(route_stats["pages_without_dom"]),
        "pages_without_browser": len(route_stats["pages_without_browser"]),
        "mapping_drift": len(feature_stats["only_in_registry"]) + len(feature_stats["only_in_expectations"]),
        "selector_drift": len(route_stats["selectors_missing_in_template"]),
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "issues": [asdict(item) for item in issues],
        "guarded_assertions": [asdict(item) for item in guarded_assertions],
        "seed_default_issues": [asdict(item) for item in seed_default_issues],
        "route_stats": route_stats,
        "feature_stats": feature_stats,
        "quality_report": quality_report,
    }


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    try:
        tmp_path.write_text(content, encoding="utf-8")
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _write_report_artifacts(report: dict[str, Any], *, output_json: Path, output_md: Path) -> None:
    _atomic_write_text(output_json, json.dumps(report, ensure_ascii=False, indent=2))
    _atomic_write_text(output_md, build_markdown(report))


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Audit blind spots in the verification method.")
    parser.add_argument("--output-json", default="output/blind_spot_audit.json")
    parser.add_argument("--output-md", default="output/blind_spot_audit.md")
    args = parser.parse_args()

    report = audit_blind_spots()

    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    _write_report_artifacts(report, output_json=output_json, output_md=output_md)

    summary = report["summary"]
    print(
        "[blind_spots] "
        f"FAKE={summary['fake_count']} "
        f"HOLLOW={summary['hollow_count']} "
        f"WEAK={summary['weak_count']} "
        f"GUARDED={summary['guarded_assertions']} "
        f"TIME_SEEDS={summary['time_coupled_seed_defaults']} "
        f"MISSING_EXPECTATIONS={summary['missing_expectations']} "
        f"DOM_REFERENCE_GAPS={summary['pages_without_dom']} "
        f"BROWSER_REFERENCE_GAPS={summary['pages_without_browser']} "
        f"DRIFT={summary['mapping_drift']} "
        f"SELECTOR_DRIFT={summary['selector_drift']}"
    )
    print(f"[blind_spots] json -> {output_json}")
    print(f"[blind_spots] md   -> {output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
