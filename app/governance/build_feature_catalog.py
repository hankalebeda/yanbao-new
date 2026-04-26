"""
build_feature_catalog.py — 功能治理生成脚本

读取 feature_registry.json（唯一注册源），扫描 FastAPI 路由 / HTML 模板 / pytest 节点，
生成三份产物：
  1. catalog_snapshot.json — 运行时快照，供 /features 页面消费
  2. docs/_temp/22_governance_feature_catalog.generated.md — 人读版治理快照
  3. mismatch_report.json — 对齐缺口报告

用法：
  python -m app.governance.build_feature_catalog
"""

import argparse
import ast
import builtins
import importlib
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from glob import glob
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
GOV_DIR = ROOT / "app" / "governance"
REGISTRY_PATH = GOV_DIR / "feature_registry.json"
SNAPSHOT_PATH = GOV_DIR / "catalog_snapshot.json"
MISMATCH_PATH = GOV_DIR / "mismatch_report.json"
TEMPLATE_DIR = ROOT / "app" / "web" / "templates"
DEFAULT_PROGRESS_DOC_REL_PATH = Path("docs/_temp/22_governance_feature_catalog.generated.md")
DOCUMENT_TOTAL_PROGRESS_DOC_REL_PATH = Path("docs/core/22_全量功能进度总表_v7_精审.md")
JUNIT_CANDIDATES = (
    ROOT / "output" / "junit.xml",
    ROOT / "output" / "pytest_results.xml",
)
PRIMARY_JUNIT_PATH = JUNIT_CANDIDATES[0]
JUNIT_FRESH_SECONDS = 24 * 3600

_FR_FILE_PATTERN = re.compile(r"(?:^|[\\/])test_fr(?P<num>\d{2})(?P<suffix>b)?(?:[_./\\]|$)", re.IGNORECASE)
_FR_TEST_NAME_PATTERN = re.compile(r"::test_fr(?P<num>\d{2})(?P<suffix>b)?(?:_|$)", re.IGNORECASE)
_MAIN_ROUTE_DECORATOR_PATTERN = re.compile(
    r"@app\.(?P<method>get|post|patch|put|delete)\(\s*[\"'](?P<path>[^\"']+)[\"']",
    re.IGNORECASE,
)
_ROUTER_DECORATOR_PATTERN = re.compile(
    r"@(?P<router>router|features_router|governance_router)\.(?P<method>get|post|patch|put|delete)\(\s*[\"'](?P<path>[^\"']+)[\"']",
    re.IGNORECASE,
)
_ROUTER_PREFIX_PATTERN = re.compile(
    r"(?P<router>router|features_router|governance_router)\s*=\s*APIRouter\((?P<body>.*?)\)",
    re.IGNORECASE | re.DOTALL,
)
_ROUTER_PREFIX_VALUE_PATTERN = re.compile(r"prefix\s*=\s*[\"'](?P<prefix>[^\"']+)[\"']", re.IGNORECASE)
_ROUTER_IMPORT_SPECS = (
    ("app.api.routes_admin", ("router",)),
    ("app.api.routes_auth", ("router",)),
    ("app.api.routes_billing", ("router",)),
    ("app.api.routes_business", ("router",)),
    ("app.api.routes_governance", ("features_router", "governance_router")),
    ("app.api.routes_internal", ("router",)),
    ("app.api.routes_sim", ("router",)),
    ("gemini_web", ("router",)),
    ("chatgpt_web", ("router",)),
    ("deepseek_web", ("router",)),
    ("qwen_web", ("router",)),
    ("webai", ("router",)),
)


def resolve_progress_doc_path() -> Path:
    override = os.getenv("FEATURE_CATALOG_DOC_PATH", "").strip()
    if override:
        override_path = Path(override)
        return override_path if override_path.is_absolute() else (ROOT / override_path)
    return ROOT / DEFAULT_PROGRESS_DOC_REL_PATH


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    try:
        tmp_path.write_text(content, encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _atomic_write_json(path: Path, payload: dict) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))


DOC_PATH = resolve_progress_doc_path()
_DOCUMENT_TOTAL_CACHE: dict[str, int | None] = {}
_PROGRESS_DOC_TOTAL_PATTERN = re.compile(r"\|\s*功能点总量\s*\|\s*`(?P<total>\d+)`")
_FAIL_CLOSE_ROUTE_REASONS = {
    ("POST", "/api/v1/admin/dag/retrigger"): "RETIRED_ROUTE",
    ("POST", "/api/v1/internal/reports/clear"): "RETIRED_ROUTE",
    ("POST", "/api/v1/internal/stats/clear"): "RETIRED_ROUTE",
    ("GET", "/api/v1/membership/subscription/status"): "FAIL_CLOSE_ROUTE",
}
_DOC22_LEGACY_PREFIXES = ("LEGACY-",)
_DOC22_MOCK_PAY_PREFIXES = ("OOS-MOCK-PAY-",)
_DOC22_PROVIDER_POLICY_PREFIXES = (
    "FR06-LLM-WEBAI-",
    "FR06-LLM-GEMINI-",
    "FR06-LLM-CHATGPT-",
    "FR06-LLM-DEEPSEEK-",
    "FR06-LLM-QWEN-",
)
_DOC22_EXTERNAL_BLOCKER_FEATURE_IDS = frozenset(
    {
        "FR09-AUTH-05",
        "FR09-BILLING-01",
        "FR09-BILLING-02",
        "FR12-ADMIN-08",
    }
)
_MISSING = object()


@contextmanager
def _temporary_main_import_symbols():
    report_symbol = _MISSING
    try:
        from app.models import Report as report_symbol
    except Exception:
        report_symbol = _MISSING

    previous_report = getattr(builtins, "Report", _MISSING)
    if report_symbol is not _MISSING:
        setattr(builtins, "Report", report_symbol)
    try:
        yield
    finally:
        if previous_report is _MISSING:
            if hasattr(builtins, "Report"):
                delattr(builtins, "Report")
        else:
            setattr(builtins, "Report", previous_report)


def _is_retired_route(api: dict) -> bool:
    method = str(api.get("method") or "").upper()
    path = str(api.get("path") or "")
    return _FAIL_CLOSE_ROUTE_REASONS.get((method, path)) == "RETIRED_ROUTE"


def _format_fr_id(number: str, suffix: str | None = None) -> str:
    if suffix and suffix.lower() == "b":
        return f"FR-{int(number):02d}-b"
    return f"FR-{int(number):02d}"


def infer_fr_id_from_nodeid(nodeid: str) -> str | None:
    normalized = nodeid.strip().replace("\\", "/")
    file_match = _FR_FILE_PATTERN.search(normalized)
    if file_match:
        return _format_fr_id(file_match.group("num"), file_match.group("suffix"))
    test_name_match = _FR_TEST_NAME_PATTERN.search(normalized)
    if test_name_match:
        return _format_fr_id(test_name_match.group("num"), test_name_match.group("suffix"))
    return None


def fr_id_from_feature_id(feature_id: str | None) -> str | None:
    value = (feature_id or "").strip()
    match = re.match(r"^FR(?P<num>\d{2})(?P<suffix>B)?-", value, re.IGNORECASE)
    if not match:
        return None
    if (match.group("suffix") or "").upper() == "B":
        return f"FR-{match.group('num')}-b"
    return f"FR-{match.group('num')}"


def _strip_param_suffix(nodeid: str) -> str:
    return nodeid.split("[", 1)[0]


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _parse_duration_seconds(value: str | None) -> float:
    try:
        seconds = float(str(value or "0").strip() or "0")
    except ValueError:
        return 0.0
    return max(0.0, seconds)


def _candidate_nodeids_from_junit(classname: str, name: str) -> list[str]:
    keys = [f"{classname}::{name}"]
    parts = classname.split(".")
    if len(parts) < 2:
        return keys

    file_path = "/".join(parts[:2]) + ".py"
    if len(parts) > 2:
        keys.append(f"{file_path}::{parts[2]}::{name}")
    keys.append(f"{file_path}::{name}")
    return keys


def _iter_marker_calls(expr: ast.AST | list[ast.AST]) -> list[ast.AST]:
    if isinstance(expr, list):
        return list(expr)
    if isinstance(expr, (ast.List, ast.Tuple, ast.Set)):
        return list(expr.elts)
    return [expr]


def _extract_feature_ids_from_marker_expr(expr: ast.AST | list[ast.AST] | None) -> list[str]:
    if expr is None:
        return []
    feature_ids: list[str] = []
    for candidate in _iter_marker_calls(expr):
        if not isinstance(candidate, ast.Call):
            continue
        func = candidate.func
        if not isinstance(func, ast.Attribute) or func.attr != "feature":
            continue
        if not candidate.args:
            continue
        arg = candidate.args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            feature_ids.append(arg.value)
    return feature_ids


def _collect_feature_marker_index_for_path(path: Path, *, root: Path = ROOT) -> dict[str, list[str]]:
    text = path.read_text(encoding="utf-8-sig")
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError:
        return {}
    try:
        rel_path = path.relative_to(root).as_posix()
    except ValueError:
        rel_path = path.as_posix()

    module_feature_ids: list[str] = []
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "pytestmark":
                    module_feature_ids.extend(_extract_feature_ids_from_marker_expr(stmt.value))

    marker_index: dict[str, set[str]] = {}

    def _record(nodeid: str, feature_ids: list[str]) -> None:
        cleaned = [item for item in feature_ids if item]
        if not cleaned:
            return
        marker_index.setdefault(nodeid, set()).update(cleaned)

    for stmt in tree.body:
        if isinstance(stmt, ast.FunctionDef) and stmt.name.startswith("test_"):
            feature_ids = module_feature_ids + _extract_feature_ids_from_marker_expr(getattr(stmt, "decorator_list", []))
            _record(f"{rel_path}::{stmt.name}", feature_ids)
        elif isinstance(stmt, ast.ClassDef) and stmt.name.startswith("Test"):
            class_feature_ids = module_feature_ids + _extract_feature_ids_from_marker_expr(getattr(stmt, "decorator_list", []))
            for body_stmt in stmt.body:
                if isinstance(body_stmt, ast.FunctionDef) and body_stmt.name.startswith("test_"):
                    feature_ids = class_feature_ids + _extract_feature_ids_from_marker_expr(getattr(body_stmt, "decorator_list", []))
                    _record(f"{rel_path}::{stmt.name}::{body_stmt.name}", feature_ids)

    return {key: sorted(value) for key, value in marker_index.items()}


def collect_feature_marker_index() -> dict[str, list[str]]:
    tests_dir = ROOT / "tests"
    marker_index: dict[str, set[str]] = {}
    if not tests_dir.exists():
        return {}

    for path in sorted(tests_dir.glob("test_*.py")):
        file_index = _collect_feature_marker_index_for_path(path)
        for nodeid, feature_ids in file_index.items():
            marker_index.setdefault(nodeid, set()).update(feature_ids)
    return {key: sorted(value) for key, value in marker_index.items()}


def _count_unique_test_nodes(test_nodes_by_feature: dict[str, list[str]]) -> int:
    return len(
        {
            str(node)
            for nodes in test_nodes_by_feature.values()
            if isinstance(nodes, list)
            for node in nodes
            if node
        }
    )


def _normalize_test_collection_summary(
    test_nodes_by_feature: dict[str, list[str]],
    meta: dict | None,
) -> dict[str, int]:
    summary = dict(meta or {})
    total_collected = _count_unique_test_nodes(test_nodes_by_feature)
    if total_collected <= 0:
        total_collected = int(summary.get("total_collected") or 0)

    mapped_by_feature_marker = int(summary.get("mapped_by_feature_marker") or 0)
    mapped_by_fr_inference = int(summary.get("mapped_by_fr_inference") or 0)
    unmapped = int(summary.get("unmapped") or 0)
    if mapped_by_feature_marker + mapped_by_fr_inference + unmapped != total_collected:
        unmapped = max(0, total_collected - mapped_by_feature_marker - mapped_by_fr_inference)

    return {
        "total_collected": int(total_collected),
        "mapped_by_feature_marker": mapped_by_feature_marker,
        "mapped_by_fr_inference": mapped_by_fr_inference,
        "unmapped": unmapped,
    }


def summarize_catalog_features(features: list[dict]) -> dict[str, object]:
    status_summary: dict[str, int] = defaultdict(int)
    negative_status_summary = {
        "negative_total": 0,
        "retired_route": 0,
        "fail_close_route": 0,
        "deprecated": 0,
        "out_of_ssot": 0,
    }
    latest_feature_verified_at = None

    for feature in features:
        status = str(feature.get("structural_status") or "UNKNOWN")
        status_summary[status] += 1

        last_verified_at = str(feature.get("last_verified_at") or "").strip()
        if last_verified_at and (
            latest_feature_verified_at is None or last_verified_at > latest_feature_verified_at
        ):
            latest_feature_verified_at = last_verified_at

        visibility = str(feature.get("visibility") or "").strip()
        flags = {str(flag or "").strip() for flag in (feature.get("mismatch_flags") or []) if flag}

        negative_bucket = None
        if "RETIRED_ROUTE" in flags:
            negative_bucket = "retired_route"
        elif "FAIL_CLOSE_ROUTE" in flags:
            negative_bucket = "fail_close_route"
        elif visibility == "deprecated":
            negative_bucket = "deprecated"
        elif visibility == "out_of_ssot" or status == "OUT_OF_SSOT":
            negative_bucket = "out_of_ssot"

        if negative_bucket:
            negative_status_summary[negative_bucket] += 1
            negative_status_summary["negative_total"] += 1

    return {
        "status_summary": dict(status_summary),
        "negative_status_summary": negative_status_summary,
        "latest_feature_verified_at": latest_feature_verified_at,
    }


def split_test_collection_map(test_nodes: dict) -> tuple[dict[str, list[str]], dict]:
    groups: dict[str, list[str]] = {}
    if isinstance(test_nodes, dict):
        for key, value in test_nodes.items():
            if str(key).startswith("__"):
                continue
            if isinstance(value, (list, tuple, set)):
                groups[str(key)] = sorted({str(node) for node in value if node})
    meta = test_nodes.get("__meta__", {}) if isinstance(test_nodes, dict) else {}
    return groups, _normalize_test_collection_summary(groups, meta if isinstance(meta, dict) else None)


def build_feature_test_traceability(feature: dict, test_nodes_by_feature: dict[str, list[str]]) -> dict[str, object]:
    feature_id = str(feature.get("feature_id") or "")
    fr_id = str(feature.get("fr_id") or "")
    exact_feature_nodes = list(test_nodes_by_feature.get(feature_id, []))
    fr_bucket_nodes = list(test_nodes_by_feature.get(fr_id, []))
    fr_inference_nodes = [node for node in fr_bucket_nodes if node not in exact_feature_nodes]

    visibility = str(feature.get("visibility") or "").strip()
    fail_close_reason = _feature_fail_close_reason(feature)
    if visibility in {"out_of_ssot", "deprecated"} or fail_close_reason in {"RETIRED_ROUTE", "FAIL_CLOSE_ROUTE"}:
        mapping_source = "governance_excluded"
        mapping_note = "governance excludes retired/deprecated/out-of-SSOT entries from ready/test claims"
    elif exact_feature_nodes:
        mapping_source = "feature_marker"
        mapping_note = "feature-level pytest markers map tests directly to this feature"
    elif fr_inference_nodes:
        mapping_source = "fr_inference_fallback"
        mapping_note = "no direct feature marker found; fallback uses FR-level test naming only and excludes sibling feature markers"
    else:
        mapping_source = "unmapped"
        mapping_note = "no collected pytest node maps to this feature"

    return {
        "mapping_source": mapping_source,
        "mapping_note": mapping_note,
        "exact_feature_node_count": len(exact_feature_nodes),
        "fr_inference_node_count": len(fr_inference_nodes),
        "exact_feature_nodeids": exact_feature_nodes,
        "fr_inference_nodeids": fr_inference_nodes,
    }


def get_feature_test_nodes(feature: dict, test_nodes_by_feature: dict[str, list[str]]) -> list[str]:
    if feature.get("visibility") in {"out_of_ssot", "deprecated"}:
        return []
    feature_id = str(feature.get("feature_id") or "")
    fr_id = str(feature.get("fr_id") or "")
    exact = test_nodes_by_feature.get(feature_id, [])
    if exact:
        return exact
    return test_nodes_by_feature.get(fr_id, [])


def derive_feature_verification_metadata(
    test_nodeids: list[str],
    junit_results: dict[str, str],
    junit_generated_at: str | None,
    node_verified_at: dict[str, str] | None = None,
) -> tuple[str, str | None]:
    test_status = "UNKNOWN"
    last_verified_at = None
    if test_nodeids and junit_results:
        statuses = [junit_results.get(node) for node in test_nodeids if node in junit_results]
        if statuses:
            test_status = "FAIL" if any(status == "FAIL" for status in statuses) else "PASS"
            verified_timestamps = [
                timestamp
                for node in test_nodeids
                for timestamp in [((node_verified_at or {}).get(node))]
                if timestamp
            ]
            # `last_verified_at` must reflect the latest matched testcase timestamp, not the junit batch timestamp.
            last_verified_at = max(verified_timestamps) if verified_timestamps else None
    return test_status, last_verified_at


def summarize_feature_traceability(features: list[dict]) -> dict[str, int]:
    summary = {
        "feature_marker": 0,
        "fr_inference_fallback": 0,
        "unmapped": 0,
        "governance_excluded": 0,
    }
    for feature in features:
        trace = feature.get("test_traceability") or {}
        mapping_source = str(trace.get("mapping_source") or "").strip()
        if mapping_source in summary:
            summary[mapping_source] += 1
    return summary


def summarize_catalog_denominators(features: list[dict]) -> dict[str, int]:
    negative_summary = summarize_catalog_features(features)["negative_status_summary"]
    negative_total = int(negative_summary.get("negative_total") or 0)
    ready_strict_count = sum(1 for feature in features if feature.get("structural_status") == "READY")
    ready_with_gaps_count = sum(1 for feature in features if feature.get("structural_status") == "READY_WITH_GAPS")
    blocked_count = sum(
        1
        for feature in features
        if str(feature.get("structural_status") or "").startswith("BLOCKED") or feature.get("structural_status") == "MISMATCH"
    )
    total = len(features)
    return {
        "catalog_total": total,
        "eligible_total": max(0, total - negative_total),
        "negative_total": negative_total,
        "ready_strict_count": ready_strict_count,
        "ready_with_gaps_count": ready_with_gaps_count,
        "blocked_count": blocked_count,
    }


def _iter_junit_testcases_with_timestamp(element, inherited_timestamp: str | None = None):
    current_timestamp = element.get("timestamp") or inherited_timestamp
    if element.tag == "testcase":
        yield element, current_timestamp
        return
    for child in list(element):
        yield from _iter_junit_testcases_with_timestamp(child, current_timestamp)


def load_document_total_from_progress_doc(doc_path: Path | None = None) -> int | None:
    path = doc_path if doc_path is not None else (ROOT / DOCUMENT_TOTAL_PROGRESS_DOC_REL_PATH)
    key = str(path)
    if key in _DOCUMENT_TOTAL_CACHE:
        return _DOCUMENT_TOTAL_CACHE[key]
    if not path.exists():
        _DOCUMENT_TOTAL_CACHE[key] = None
        return None
    content = path.read_text(encoding="utf-8-sig")
    match = _PROGRESS_DOC_TOTAL_PATTERN.search(content)
    value = int(match.group("total")) if match else None
    _DOCUMENT_TOTAL_CACHE[key] = value
    return value


def _fail_close_reason_from_api(api: dict) -> str | None:
    method = (api.get("method") or "").upper()
    path = api.get("path")
    return _FAIL_CLOSE_ROUTE_REASONS.get((method, path))


def _feature_governance_flags(feature: dict) -> list[str]:
    raw_flags = feature.get("governance_flags") or []
    if not isinstance(raw_flags, list):
        return []
    flags: list[str] = []
    for flag in raw_flags:
        text = str(flag or "").strip()
        if text:
            flags.append(text)
    return flags


def _feature_fail_close_reason(feature: dict) -> str | None:
    for flag in _feature_governance_flags(feature):
        if flag in {"RETIRED_ROUTE", "FAIL_CLOSE_ROUTE"}:
            return flag
    return _fail_close_reason_from_api(feature.get("primary_api", {}))


def _feature_id_text(feature: dict) -> str:
    return str(feature.get("feature_id") or "").strip()


def _feature_id_matches_prefixes(feature: dict, prefixes: tuple[str, ...]) -> bool:
    feature_id = _feature_id_text(feature).upper()
    return any(feature_id.startswith(prefix) for prefix in prefixes)


def _is_doc22_legacy_feature(feature: dict) -> bool:
    return _feature_id_matches_prefixes(feature, _DOC22_LEGACY_PREFIXES)


def _is_doc22_mock_pay_feature(feature: dict) -> bool:
    return _feature_id_matches_prefixes(feature, _DOC22_MOCK_PAY_PREFIXES)


def _is_doc22_provider_policy_feature(feature: dict) -> bool:
    return _feature_id_matches_prefixes(feature, _DOC22_PROVIDER_POLICY_PREFIXES)


def _is_doc22_external_blocker_feature(feature: dict) -> bool:
    return _feature_id_text(feature).upper() in _DOC22_EXTERNAL_BLOCKER_FEATURE_IDS


def _is_governance_terminal_feature(feature: dict) -> bool:
    return _feature_fail_close_reason(feature) in {"RETIRED_ROUTE", "FAIL_CLOSE_ROUTE"}


def summarize_catalog_audit_scope(features: list[dict]) -> dict[str, object]:
    bucket_feature_ids = {
        "legacy_compat": [],
        "mock_pay_oos": [],
        "provider_policy_oos": [],
        "external_blocker": [],
        "governance_terminal": [],
    }

    for feature in features:
        feature_id = _feature_id_text(feature)
        if not feature_id:
            continue
        if _is_doc22_legacy_feature(feature):
            bucket_feature_ids["legacy_compat"].append(feature_id)
        if _is_doc22_mock_pay_feature(feature):
            bucket_feature_ids["mock_pay_oos"].append(feature_id)
        if _is_doc22_provider_policy_feature(feature):
            bucket_feature_ids["provider_policy_oos"].append(feature_id)
        if _is_doc22_external_blocker_feature(feature):
            bucket_feature_ids["external_blocker"].append(feature_id)
        if _is_governance_terminal_feature(feature):
            bucket_feature_ids["governance_terminal"].append(feature_id)

    governance_negative_total = sum(
        1
        for feature in features
        if str(feature.get("visibility") or "").strip() in {"out_of_ssot", "deprecated"}
        or _is_governance_terminal_feature(feature)
        or str(feature.get("structural_status") or "").strip() == "OUT_OF_SSOT"
    )
    legacy_compat_total = len(bucket_feature_ids["legacy_compat"])
    mock_pay_oos_total = len(bucket_feature_ids["mock_pay_oos"])
    provider_policy_oos_total = len(bucket_feature_ids["provider_policy_oos"])
    external_blocker_total = len(bucket_feature_ids["external_blocker"])
    doc22_excluded_total = (
        legacy_compat_total + mock_pay_oos_total + provider_policy_oos_total + external_blocker_total
    )
    total = len(features)
    return {
        "catalog_total": total,
        "governance_eligible_total": max(0, total - governance_negative_total),
        "governance_negative_total": governance_negative_total,
        "doc22_code_presence_total": max(0, total - legacy_compat_total - mock_pay_oos_total),
        "doc22_active_total": max(0, total - doc22_excluded_total),
        "doc22_excluded_total": doc22_excluded_total,
        "legacy_compat_total": legacy_compat_total,
        "mock_pay_oos_total": mock_pay_oos_total,
        "provider_policy_oos_total": provider_policy_oos_total,
        "external_blocker_total": external_blocker_total,
        "governance_terminal_total": len(bucket_feature_ids["governance_terminal"]),
        "bucket_feature_ids": bucket_feature_ids,
    }


def load_registry() -> dict:
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _collect_routes_from_route_container(route_container) -> set[str]:
    routes = set()
    for route in getattr(route_container, "routes", []):
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None)
        if path and methods:
            for method in methods:
                routes.add(f"{method.upper()} {path}")
    return routes


def _scan_router_modules() -> set[str]:
    routes = set()
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "ai-api"))
    for module_name, router_attrs in _ROUTER_IMPORT_SPECS:
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            print(f"[WARN] 鏃犳硶鍔犺浇 router 妯″潡 {module_name}: {exc}", file=sys.stderr)
            continue
        for attr in router_attrs:
            route_container = getattr(module, attr, None)
            if route_container is not None:
                routes.update(_collect_routes_from_route_container(route_container))
    return routes


def _scan_main_decorator_routes() -> set[str]:
    main_path = ROOT / "app" / "main.py"
    if not main_path.exists():
        return set()
    text = main_path.read_text(encoding="utf-8-sig")
    return {
        f"{match.group('method').upper()} {match.group('path')}"
        for match in _MAIN_ROUTE_DECORATOR_PATTERN.finditer(text)
    }


def _scan_route_decorators_from_files() -> set[str]:
    routes = set()
    candidates = list((ROOT / "app" / "api").glob("routes_*.py"))
    candidates.extend((ROOT / "ai-api").glob("*_web.py"))
    for path in candidates:
        try:
            text = path.read_text(encoding="utf-8-sig")
        except Exception as exc:
            print(f"[WARN] 鏃犳硶璇诲彇 route 婧愭枃浠?{path}: {exc}", file=sys.stderr)
            continue
        router_prefixes: dict[str, str] = {}
        for prefix_match in _ROUTER_PREFIX_PATTERN.finditer(text):
            prefix_value_match = _ROUTER_PREFIX_VALUE_PATTERN.search(prefix_match.group("body"))
            router_prefixes[prefix_match.group("router")] = (
                prefix_value_match.group("prefix") if prefix_value_match else ""
            )

        for match in _ROUTER_DECORATOR_PATTERN.finditer(text):
            router_name = match.group("router")
            prefix = router_prefixes.get(router_name, "")
            route_path = match.group("path")
            if prefix and route_path.startswith("/"):
                route_path = f"{prefix.rstrip('/')}{route_path}"
            routes.add(f"{match.group('method').upper()} {route_path}")
    return routes


def scan_fastapi_routes() -> set[str]:
    """扫描 FastAPI app 的所有已注册路由，返回 'METHOD /path' 集合。"""
    routes = set()
    try:
        sys.path.insert(0, str(ROOT))
        os.environ.setdefault("MOCK_LLM", "true")
        os.environ.setdefault("ENABLE_SCHEDULER", "false")
        os.environ.setdefault("STRICT_REAL_DATA", "false")
        with _temporary_main_import_symbols():
            from app.main import app
        for route in app.routes:
            path = getattr(route, "path", None)
            methods = getattr(route, "methods", None)
            if path and methods:
                for m in methods:
                    routes.add(f"{m.upper()} {path}")
    except Exception as e:
        print(f"[WARN] 无法扫描 FastAPI 路由: {e}", file=sys.stderr)
    return routes

_scan_fastapi_routes_from_main_app = scan_fastapi_routes


def _scan_fastapi_routes_resilient() -> set[str]:
    routes = _scan_fastapi_routes_from_main_app()
    routes.update(_scan_router_modules())
    routes.update(_scan_main_decorator_routes())
    routes.update(_scan_route_decorators_from_files())
    return routes


scan_fastapi_routes = _scan_fastapi_routes_resilient


def scan_html_templates() -> set[str]:
    """扫描 HTML 模板文件名。"""
    templates = set()
    if TEMPLATE_DIR.exists():
        for p in TEMPLATE_DIR.glob("*.html"):
            templates.add(p.stem)
    return templates


def collect_pytest_nodes() -> dict[str, object]:
    """通过 pytest --collect-only 获取测试节点，优先按 feature marker 分组。"""
    nodes_by_feature: dict[str, set[str]] = defaultdict(set)
    all_nodes: list[str] = []
    explicit_feature_index = collect_feature_marker_index()
    mapped_by_feature_marker = 0
    mapped_by_fr_inference = 0

    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "tests", "--collect-only", "-q", "--no-header"],
            capture_output=True, text=True, cwd=str(ROOT), timeout=60
        )
        for line in result.stdout.splitlines():
            line = line.strip()
            if "::" in line and not line.startswith("="):
                all_nodes.append(line)
    except Exception as e:
        print(f"[WARN] pytest collect 失败: {e}", file=sys.stderr)

    for node in all_nodes:
        base_nodeid = _strip_param_suffix(node)
        explicit_feature_ids = explicit_feature_index.get(base_nodeid, [])
        if explicit_feature_ids:
            mapped_by_feature_marker += 1
            for feature_id in explicit_feature_ids:
                nodes_by_feature[feature_id].add(node)
            continue

        fr_id = infer_fr_id_from_nodeid(node)
        if fr_id:
            mapped_by_fr_inference += 1
            nodes_by_feature[fr_id].add(node)
        else:
            nodes_by_feature["_other"].add(node)

    rendered = {key: sorted(value) for key, value in nodes_by_feature.items()}
    rendered["__meta__"] = {
        "total_collected": len(all_nodes),
        "mapped_by_feature_marker": mapped_by_feature_marker,
        "mapped_by_fr_inference": mapped_by_fr_inference,
        "unmapped": len(rendered.get("_other", [])),
    }
    return rendered


def count_collected_test_nodes(
    test_nodes_by_feature: dict[str, list[str]],
    test_collection_summary: dict | None = None,
) -> int:
    summary_total = int(((test_collection_summary or {}).get("total_collected")) or 0)
    if summary_total > 0:
        return summary_total
    return _count_unique_test_nodes(test_nodes_by_feature)


def load_junit_results(junit_path: Path | None = None) -> dict[str, str]:
    """加载 JUnit XML 结果，返回 {node_id: 'PASS'|'FAIL'}。
    
    支持两种 key 格式:
    - pytest 风格: tests/test_fr01_pool_refresh.py::test_fr01_pool_no_st
    - JUnit classname 风格: tests.test_fr01_pool_refresh::test_fr01_pool_no_st
    """
    results = {}
    if junit_path and junit_path.exists():
        try:
            import xml.etree.ElementTree as ET
            tree = ET.parse(junit_path)
            for tc in tree.iter("testcase"):
                classname = tc.get("classname", "")
                name = tc.get("name", "")
                status = "FAIL" if (tc.find("failure") is not None or tc.find("error") is not None) else "PASS"
                for key in _candidate_nodeids_from_junit(classname, name):
                    results[key] = status
        except Exception:
            pass
    return results


def load_junit_node_verified_at(junit_path: Path | None = None) -> dict[str, str]:
    verified_at: dict[str, str] = {}
    if junit_path and junit_path.exists():
        try:
            import xml.etree.ElementTree as ET

            tree = ET.parse(junit_path)
            root = tree.getroot()
            suite_elapsed_seconds: dict[str, float] = defaultdict(float)
            for tc, suite_timestamp in _iter_junit_testcases_with_timestamp(root):
                classname = tc.get("classname", "")
                name = tc.get("name", "")
                if not classname or not name:
                    continue

                duration_seconds = _parse_duration_seconds(tc.get("time"))
                testcase_timestamp = tc.get("timestamp")
                if testcase_timestamp:
                    testcase_start = _parse_iso_datetime(testcase_timestamp)
                    if testcase_start is None:
                        continue
                    testcase_verified_at = testcase_start
                else:
                    suite_start = _parse_iso_datetime(suite_timestamp)
                    if suite_start is None or not suite_timestamp:
                        continue
                    suite_elapsed_seconds[suite_timestamp] += duration_seconds
                    testcase_verified_at = suite_start + timedelta(seconds=suite_elapsed_seconds[suite_timestamp])

                rendered_timestamp = testcase_verified_at.isoformat()
                for key in _candidate_nodeids_from_junit(classname, name):
                    verified_at[key] = rendered_timestamp
        except Exception:
            return {}
    return verified_at


def count_junit_testcases(junit_path: Path | None = None) -> int:
    if not junit_path or not junit_path.exists():
        return 0
    try:
        import xml.etree.ElementTree as ET

        tree = ET.parse(junit_path)
        return sum(1 for _ in tree.iter("testcase"))
    except Exception:
        return 0


def _path_display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT)).replace("\\", "/")
    except ValueError:
        return str(path)


def resolve_junit_source_path(junit_source: str | None) -> Path | None:
    source = str(junit_source or "").strip()
    if not source:
        return None
    path = Path(source)
    if path.is_absolute():
        return path
    return ROOT / path


def _should_refresh_junit_bundle(
    bundle: tuple[dict[str, str], str | None, str | None, str, int | None, str | None],
) -> bool:
    _, junit_source, _, junit_freshness, _, junit_stale_reason = bundle
    return junit_source is None or junit_freshness != "fresh" or junit_stale_reason is not None


def refresh_primary_junit_artifact() -> tuple[int | None, str | None]:
    PRIMARY_JUNIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "pytest",
        "tests",
        "-q",
        f"--junitxml={PRIMARY_JUNIT_PATH.as_posix()}",
    ]
    print(f"[INFO] refreshing JUnit artifact: {' '.join(command)}")
    try:
        result = subprocess.run(command, cwd=str(ROOT))
    except Exception as exc:
        print(f"[WARN] failed to refresh JUnit artifact: {exc}")
        return None, str(exc)

    if result.returncode != 0:
        print(
            f"[WARN] full pytest exited with code {result.returncode}; "
            "retaining produced JUnit artifact for truthful governance output"
        )

    if not PRIMARY_JUNIT_PATH.exists():
        message = f"pytest_did_not_write:{_path_display(PRIMARY_JUNIT_PATH)}"
        print(f"[WARN] {message}")
        return result.returncode, message

    return result.returncode, None


def load_latest_junit_results_bundle(
    *,
    expected_total: int | None = None,
) -> tuple[dict[str, str], str | None, str | None, str, int | None, str | None]:
    """Select the newest existing junit candidate that is fresh enough to trust."""
    now = datetime.now(timezone.utc)
    best: tuple[datetime, Path, dict[str, str]] | None = None
    stale_reason = None
    newest_existing: tuple[datetime, Path, int] | None = None

    for path in JUNIT_CANDIDATES:
        if not path.exists():
            continue
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        testcase_count = count_junit_testcases(path)
        if newest_existing is None or mtime > newest_existing[0]:
            newest_existing = (mtime, path, testcase_count)
        if expected_total is not None and testcase_count and testcase_count != expected_total:
            if newest_existing is not None and newest_existing[1] == path:
                stale_reason = f"testcase_count_mismatch:{testcase_count}!={expected_total}"
            continue
        parsed = load_junit_results(path)
        if not parsed:
            continue
        if best is None or mtime > best[0]:
            best = (mtime, path, parsed)

    if best is None:
        freshness = "stale" if stale_reason else "missing"
        return {}, None, None, freshness, None, stale_reason

    mtime, path, parsed = best
    age_seconds = max(0, int((now - mtime).total_seconds()))
    freshness = "fresh" if age_seconds <= JUNIT_FRESH_SECONDS else "stale"

    if newest_existing is not None:
        newest_mtime, newest_path, newest_count = newest_existing
        if (
            expected_total is not None
            and newest_count
            and newest_count != expected_total
            and newest_path != path
        ):
            freshness = "stale"
            stale_reason = f"testcase_count_mismatch:{newest_count}!={expected_total}"
        elif newest_path == path:
            stale_reason = None

    return parsed, _path_display(path), mtime.isoformat(), freshness, age_seconds, stale_reason


def load_junit_node_verified_at_index(
    *,
    expected_total: int | None = None,
) -> dict[str, str]:
    latest_by_node: dict[str, str] = {}
    latest_by_node_dt: dict[str, datetime] = {}

    for path in JUNIT_CANDIDATES:
        if not path.exists():
            continue
        testcase_count = count_junit_testcases(path)
        if expected_total is not None and testcase_count and testcase_count != expected_total:
            continue
        node_verified_at = load_junit_node_verified_at(path)
        if not node_verified_at:
            continue
        for nodeid, timestamp in node_verified_at.items():
            try:
                parsed_ts = datetime.fromisoformat(timestamp)
            except Exception:
                continue
            current = latest_by_node_dt.get(nodeid)
            if current is None or parsed_ts > current:
                latest_by_node_dt[nodeid] = parsed_ts
                latest_by_node[nodeid] = timestamp

    return latest_by_node


def _merge_stale_reasons(*reasons: str | None) -> str | None:
    merged: list[str] = []
    for reason in reasons:
        text = str(reason or "").strip()
        if text and text not in merged:
            merged.append(text)
    if not merged:
        return None
    return ";".join(merged)


# --- HTML 页面路径 → 模板名映射 ---
PAGE_TEMPLATE_MAP = {
    "/": "index",
    "/reports": "reports_list",
    "/login": "login",
    "/register": "register",
    "/subscribe": "subscribe",
    "/forgot-password": "forgot_password",
    "/reset-password": "reset_password",
    "/profile": "profile",
    "/admin": "admin",
    "/dashboard": "dashboard",
    "/portfolio/sim-dashboard": "sim_dashboard",
    "/features": "features",
    "/terms": "terms",
    "/privacy": "privacy",
}


def _api_route_exists(api: dict | None, code_routes: set[str]) -> bool:
    api = api or {}
    method = str(api.get("method") or "").upper()
    path = str(api.get("path") or "")
    if not method or not path:
        return False
    route_key = f"{method} {path}"
    if "{" in path:
        prefix = path.split("{", 1)[0]
        return any(method in route and prefix in route for route in code_routes)
    return route_key in code_routes


def _template_name_for_page_path(page_path: str | None) -> str | None:
    if not page_path:
        return None
    clean_path = str(page_path).split("{", 1)[0].rstrip("/") or "/"
    return PAGE_TEMPLATE_MAP.get(clean_path)


def _feature_route_exists(feature: dict, code_routes: set[str], *, structural_status: str | None = None) -> bool:
    visibility = str(feature.get("visibility") or "").lower()
    if visibility in {"out_of_ssot", "deprecated"}:
        return False
    if _feature_fail_close_reason(feature) in {"RETIRED_ROUTE", "FAIL_CLOSE_ROUTE"}:
        return False
    if structural_status == "BLOCKED_API":
        return False
    return _api_route_exists(feature.get("primary_api"), code_routes)


def _feature_page_exists(
    feature: dict,
    html_templates: set[str],
    *,
    structural_status: str | None = None,
) -> bool:
    visibility = str(feature.get("visibility") or "").lower()
    if visibility in {"out_of_ssot", "deprecated"}:
        return False
    if _feature_fail_close_reason(feature) in {"RETIRED_ROUTE", "FAIL_CLOSE_ROUTE"}:
        return False
    page_path = str(feature.get("runtime_page_path") or "").strip()
    if not page_path:
        return False
    if structural_status == "BLOCKED_PAGE":
        return False
    template_name = _template_name_for_page_path(page_path)
    if template_name is None:
        return True
    return template_name in html_templates


def determine_status(feature: dict, code_routes: set[str], html_templates: set[str],
                     test_nodes_by_feature: dict, junit_results: dict) -> tuple[str, list[str]]:
    """推导单个 feature 的 structural_status 与 mismatch_flags。"""
    flags = []
    vis = feature.get("visibility", "public")

    if vis in ("out_of_ssot", "deprecated"):
        return "OUT_OF_SSOT", ["out_of_ssot"]

    fail_close_reason = _feature_fail_close_reason(feature)
    if fail_close_reason:
        if fail_close_reason in {"RETIRED_ROUTE", "FAIL_CLOSE_ROUTE"}:
            return "OUT_OF_SSOT", [fail_close_reason]
        return "BLOCKED_API", [fail_close_reason]

    # 1) 检查 API 路由是否存在
    api = feature.get("primary_api", {})
    if api.get("method") and api.get("path"):
        route_key = f"{api['method'].upper()} {api['path']}"
        # 简化：直接检查路径模板是否在注册路由中
        route_exists = any(
            api["method"].upper() in r and api["path"].split("{")[0] in r
            for r in code_routes
        ) if "{" in api["path"] else (route_key in code_routes)

        if not route_exists:
            flags.append("BLOCKED_API")

    # 2) 检查 HTML 页面是否存在
    page_path = feature.get("runtime_page_path")
    required_kinds = feature.get("required_test_kinds", [])
    if page_path and "page" in required_kinds:
        # 规范化页面路径（去除参数部分）
        clean_path = page_path.split("{")[0].rstrip("/") or "/"
        template_name = PAGE_TEMPLATE_MAP.get(clean_path)
        if template_name and template_name not in html_templates:
            flags.append("BLOCKED_PAGE")

    # 3) 检查是否有测试
    feature_tests = get_feature_test_nodes(feature, test_nodes_by_feature)
    if required_kinds and not feature_tests:
        flags.append("BLOCKED_TEST")

    # 4) 推导最终状态
    if "BLOCKED_API" in flags:
        return "BLOCKED_API", flags
    if "BLOCKED_PAGE" in flags:
        return "BLOCKED_PAGE", flags
    if "BLOCKED_TEST" in flags:
        return "BLOCKED_TEST", flags
    if flags:
        return "MISMATCH", flags
    if feature.get("gaps"):
        return "READY_WITH_GAPS", []
    return "READY", []


def determine_status(feature: dict, code_routes: set[str], html_templates: set[str],
                     test_nodes_by_feature: dict, junit_results: dict) -> tuple[str, list[str]]:
    """Override the legacy status helper with fact-based route/page checks."""
    del junit_results
    flags = []
    vis = feature.get("visibility", "public")

    if vis in ("out_of_ssot", "deprecated"):
        return "OUT_OF_SSOT", ["out_of_ssot"]

    fail_close_reason = _feature_fail_close_reason(feature)
    if fail_close_reason:
        if fail_close_reason in {"RETIRED_ROUTE", "FAIL_CLOSE_ROUTE"}:
            return "OUT_OF_SSOT", [fail_close_reason]
        return "BLOCKED_API", [fail_close_reason]

    api = feature.get("primary_api", {})
    if api.get("method") and api.get("path") and not _api_route_exists(api, code_routes):
        flags.append("BLOCKED_API")

    page_path = feature.get("runtime_page_path")
    required_kinds = feature.get("required_test_kinds", [])
    if page_path and "page" in required_kinds:
        template_name = _template_name_for_page_path(page_path)
        if template_name and template_name not in html_templates:
            flags.append("BLOCKED_PAGE")

    feature_tests = get_feature_test_nodes(feature, test_nodes_by_feature)
    if required_kinds and not feature_tests:
        flags.append("BLOCKED_TEST")

    if "BLOCKED_API" in flags:
        return "BLOCKED_API", flags
    if "BLOCKED_PAGE" in flags:
        return "BLOCKED_PAGE", flags
    if "BLOCKED_TEST" in flags:
        return "BLOCKED_TEST", flags
    if flags:
        return "MISMATCH", flags
    if feature.get("gaps"):
        return "READY_WITH_GAPS", []
    return "READY", []


def build_catalog_snapshot_payload(
    *,
    registry: dict,
    code_routes: set[str],
    html_templates: set[str],
    test_nodes_raw: dict[str, object],
    generated_at: str | None = None,
    junit_bundle: tuple[dict[str, str], str | None, str | None, str, int | None, str | None] | None = None,
    node_verified_at: dict[str, str] | None = None,
) -> tuple[dict, list[dict]]:
    features = registry.get("features", [])
    test_nodes_by_feature, test_collection_summary = split_test_collection_map(test_nodes_raw)
    total_tests = count_collected_test_nodes(test_nodes_by_feature, test_collection_summary)

    if junit_bundle is None:
        junit_bundle = load_latest_junit_results_bundle(expected_total=total_tests or None)

    (
        junit_results,
        junit_source,
        junit_generated_at,
        junit_freshness,
        junit_age_seconds,
        junit_stale_reason,
    ) = junit_bundle
    if node_verified_at is None:
        node_verified_at = load_junit_node_verified_at(resolve_junit_source_path(junit_source))
    mapped_by_feature_marker = int((test_collection_summary or {}).get("mapped_by_feature_marker") or 0)
    mapped_by_fr_inference = int((test_collection_summary or {}).get("mapped_by_fr_inference") or 0)
    if total_tests > 0 and mapped_by_feature_marker == 0 and junit_freshness == "fresh":
        junit_freshness = "stale"
        junit_stale_reason = _merge_stale_reasons(junit_stale_reason, "feature_marker_coverage_zero")
    elif total_tests > 0 and mapped_by_feature_marker == 0:
        junit_stale_reason = _merge_stale_reasons(junit_stale_reason, "feature_marker_coverage_zero")

    catalog: list[dict] = []
    mismatch_report: list[dict] = []

    for feat in features:
        status, flags = determine_status(feat, code_routes, html_templates, test_nodes_by_feature, junit_results)
        test_traceability = build_feature_test_traceability(feat, test_nodes_by_feature)

        feature_test_nodes = (
            []
            if status == "OUT_OF_SSOT" or "FAIL_CLOSE_ROUTE" in flags
            else get_feature_test_nodes(feat, test_nodes_by_feature)
        )
        test_status, last_verified_at = derive_feature_verification_metadata(
            feature_test_nodes,
            junit_results,
            junit_generated_at,
            node_verified_at=node_verified_at,
        )

        # Detect potentially stale gaps: code+test both pass but gaps remain
        stale_gaps: list[str] = []
        if feat.get("gaps"):
            cv = (feat.get("code_verdict") or "")
            tv = (feat.get("test_verdict") or "")
            if cv.startswith("✅") and tv.startswith("✅"):
                stale_gaps = list(feat["gaps"])

        catalog.append(
            {
                **feat,
                "structural_status": status,
                "mismatch_flags": flags,
                "route_exists": _feature_route_exists(feat, code_routes, structural_status=status),
                "page_exists": _feature_page_exists(feat, html_templates, structural_status=status),
                "test_nodeids": feature_test_nodes,
                "last_test_status": test_status,
                "last_verified_at": last_verified_at,
                "test_traceability": test_traceability,
                "stale_gaps": stale_gaps,
            }
        )

        if flags:
            mismatch_report.append(
                {
                    "feature_id": feat["feature_id"],
                    "title": feat["title"],
                    "fr_id": feat["fr_id"],
                    "flags": flags,
                    "status": status,
                }
            )

    catalog_summary = summarize_catalog_features(catalog)
    denominator_summary = summarize_catalog_denominators(catalog)
    audit_scope_summary = summarize_catalog_audit_scope(catalog)
    feature_traceability_summary = summarize_feature_traceability(catalog)
    snapshot = {
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "registry_generated_at": registry.get("generated_at"),
        "total": len(catalog),
        "status_summary": catalog_summary["status_summary"],
        "negative_status_summary": catalog_summary["negative_status_summary"],
        "denominator_summary": denominator_summary,
        "audit_scope_summary": audit_scope_summary,
        "feature_traceability_summary": feature_traceability_summary,
        "latest_feature_verified_at": catalog_summary["latest_feature_verified_at"],
        "test_result_source": junit_source,
        "test_result_generated_at": junit_generated_at,
        "test_result_freshness": junit_freshness,
        "test_result_age_seconds": junit_age_seconds,
        "test_result_stale_reason": junit_stale_reason,
        "test_collection_summary": test_collection_summary,
        "features": catalog,
    }
    return snapshot, mismatch_report


def build_catalog(*, junit_refresh_mode: str = "never", write_progress_doc: bool = True):
    """Build governance snapshot artifacts from registry, routes, templates, tests, and JUnit."""
    if junit_refresh_mode not in {"never", "if_needed", "always"}:
        raise ValueError(f"unsupported junit_refresh_mode: {junit_refresh_mode}")

    now = datetime.now(timezone.utc).isoformat()
    registry = load_registry()
    features = registry.get("features", [])

    print(f"[INFO] 注册表加载 {len(features)} 条功能点")

    # 扫描
    code_routes = scan_fastapi_routes()
    print(f"[INFO] FastAPI 路由 {len(code_routes)} 条")

    html_templates = scan_html_templates()
    print(f"[INFO] HTML 模板 {len(html_templates)} 个")

    test_nodes_raw = collect_pytest_nodes()
    test_nodes_by_feature, test_collection_summary = split_test_collection_map(test_nodes_raw)
    total_tests = count_collected_test_nodes(test_nodes_by_feature, test_collection_summary)
    print(f"[INFO] pytest 节点 {total_tests} 个（{len(test_nodes_by_feature)} 个显式映射分组）")

    junit_bundle = load_latest_junit_results_bundle(expected_total=total_tests or None)
    refresh_needed = junit_refresh_mode == "always" or (
        junit_refresh_mode == "if_needed" and _should_refresh_junit_bundle(junit_bundle)
    )
    if not refresh_needed and _should_refresh_junit_bundle(junit_bundle):
        _, existing_source, _, existing_freshness, _, existing_stale_reason = junit_bundle
        skip_reason = existing_stale_reason or (
            "missing_junit_artifact" if existing_source is None else f"freshness={existing_freshness}"
        )
        print(
            "[INFO] skipping JUnit refresh during catalog build "
            f"({skip_reason}); run with --refresh-junit after an explicit pytest pass when needed"
        )
    if refresh_needed:
        _, existing_source, _, existing_freshness, _, existing_stale_reason = junit_bundle
        refresh_reason = existing_stale_reason or (
            "missing_junit_artifact" if existing_source is None else f"freshness={existing_freshness}"
        )
        print(f"[INFO] refreshing JUnit before snapshot build ({refresh_reason})")
        refresh_primary_junit_artifact()
        junit_bundle = load_latest_junit_results_bundle(expected_total=total_tests or None)

    (
        junit_results,
        junit_source,
        junit_generated_at,
        junit_freshness,
        junit_age_seconds,
        junit_stale_reason,
    ) = junit_bundle
    node_verified_at = load_junit_node_verified_at(resolve_junit_source_path(junit_source))
    if junit_results and junit_source:
        print(f"[INFO] 加载 JUnit 结果 {Path(junit_source).name}（freshness={junit_freshness}）")
    elif junit_stale_reason:
        print(f"[WARN] 忽略陈旧 JUnit 产物: {junit_stale_reason}")

    if not junit_results:
        print("[INFO] no valid JUnit artifact found")

    snapshot, mismatch_report = build_catalog_snapshot_payload(
        registry=registry,
        code_routes=code_routes,
        html_templates=html_templates,
        test_nodes_raw=test_nodes_raw,
        generated_at=now,
        junit_bundle=junit_bundle,
        node_verified_at=node_verified_at,
    )
    catalog = snapshot["features"]
    status_counts = snapshot["status_summary"]

    _atomic_write_json(SNAPSHOT_PATH, snapshot)
    print(f"[OK] catalog_snapshot.json ({len(catalog)} 条)")

    # --- 写 mismatch_report.json ---
    _atomic_write_json(MISMATCH_PATH, {"generated_at": now, "mismatches": mismatch_report})
    print(f"[OK] mismatch_report.json ({len(mismatch_report)} 条缺口)")

    if write_progress_doc:
        # --- 写功能进度总表文档 ---
        generate_22_doc(catalog, status_counts, now)
        try:
            doc_display = DOC_PATH.relative_to(ROOT)
        except ValueError:
            doc_display = DOC_PATH
        print(f"[OK] {doc_display}")


def generate_22_doc(catalog: list[dict], status_counts: dict, generated_at: str):
    """生成 22 人读版总表。"""
    # 统计有页面的功能点
    page_total = sum(1 for f in catalog if f.get("runtime_page_path"))
    page_ok = sum(1 for f in catalog if f.get("runtime_page_path") and f.get("page_exists", True))
    page_blocked = page_total - page_ok
    test_ok = sum(1 for f in catalog if f.get("last_test_status") == "PASS")

    # 审计统计
    total_gaps = sum(len(f.get("gaps", [])) for f in catalog)
    code_red = sum(1 for f in catalog if "🔴" in f.get("code_verdict", "") or "❌" in f.get("code_verdict", ""))
    test_red = sum(1 for f in catalog if "🔴" in f.get("test_verdict", ""))
    audited = sum(1 for f in catalog if f.get("code_verdict"))
    code_ok = sum(1 for f in catalog if f.get("code_verdict", "").startswith("✅"))
    code_partial = sum(1 for f in catalog if "⚠️" in f.get("code_verdict", ""))
    test_fail = sum(1 for f in catalog if f.get("last_test_status") == "FAIL")
    test_unknown = sum(1 for f in catalog if f.get("last_test_status", "UNKNOWN") == "UNKNOWN")

    lines = [
        "# 全量功能进度总表 v6.0",
        "",
        "> 本文件为自动生成产物，禁止手工编辑。",
        "> 修改状态请编辑 `app/governance/feature_registry.json` 或修复对应代码/测试/文档。",
        "> 生成脚本：`python -m app.governance.build_feature_catalog`",
        "",
        "---",
        "",
        "## 摘要",
        "",
        f"- **总功能点**: {len(catalog)}",
    ]

    for s in ["READY", "READY_WITH_GAPS", "MISMATCH", "BLOCKED_TEST", "BLOCKED_PAGE", "BLOCKED_API", "OUT_OF_SSOT"]:
        lines.append(f"- **{s}**: {status_counts.get(s, 0)}")

    lines.extend([
        f"- **前端页面**: {page_ok}/{page_total} 已实现" + (f"（{page_blocked} 个缺失）" if page_blocked else ""),
        f"- **测试通过**: {test_ok} PASS / {test_fail} FAIL / {test_unknown} UNKNOWN",
        f"- **深度审计**: {audited} 已审计 → ✅{code_ok} ⚠️{code_partial} 🔴{code_red} 代码问题 / 🔴{test_red} 测试缺失",
        f"- **具体差距项**: {total_gaps}",
        f"- **最近生成**: {generated_at}",
        "",
        "---",
        "",
        "## FR 索引",
        "",
        "| FR | 名称 | 功能点数 | READY | BLOCKED | 有页面 | 审计差距 | 进度 |",
        "|:---|:-----|:------:|:-----:|:-------:|:-----:|:-------:|:----:|",
    ])

    # 分组统计
    fr_groups: dict[str, list[dict]] = defaultdict(list)
    fr_names = {
        "FR-00": "真实性红线", "FR-01": "股票池筛选", "FR-02": "定时调度(DAG)",
        "FR-03": "Cookie与会话管理", "FR-04": "多源数据采集", "FR-05": "市场状态机",
        "FR-06": "研报生成", "FR-07": "预测结算与回灌", "FR-08": "模拟实盘追踪",
        "FR-09": "商业化与权益", "FR-09-b": "系统清理与归档", "FR-10": "完整站点与看板",
        "FR-11": "用户反馈", "FR-12": "管理员后台", "FR-13": "业务事件推送",
    }

    for feat in catalog:
        fr_id = feat.get("fr_id", "UNKNOWN")
        fr_groups[fr_id].append(feat)

    for fr_id in sorted(fr_groups.keys(), key=lambda x: (x.replace("FR-", "").replace("-b", "z"), x)):
        group = fr_groups[fr_id]
        ready = sum(1 for f in group if f["structural_status"] == "READY")
        blocked = sum(1 for f in group if f["structural_status"].startswith("BLOCKED"))
        has_page = sum(1 for f in group if f.get("runtime_page_path"))
        total = len(group)
        pct = f"{ready}/{total}" if total else "0/0"
        name = fr_names.get(fr_id, fr_id)
        page_str = f"{has_page}" if has_page else "-"
        gap_count = sum(len(f.get("gaps", [])) for f in group)
        gap_str = f"🔴{gap_count}" if gap_count else "✅0"
        lines.append(f"| {fr_id} | {name} | {total} | {ready} | {blocked} | {page_str} | {gap_str} | {pct} |")

    lines.extend(["", "---", ""])

    # 逐 FR 详细功能点
    for fr_id in sorted(fr_groups.keys(), key=lambda x: (x.replace("FR-", "").replace("-b", "z"), x)):
        name = fr_names.get(fr_id, fr_id)
        lines.append(f"## {fr_id} {name}")
        lines.append("")

        for feat in fr_groups[fr_id]:
            fid = feat["feature_id"]
            title = feat["title"]
            status = feat["structural_status"]
            status_emoji = {"READY": "🟢", "MISMATCH": "🟡", "OUT_OF_SSOT": "⚪"}.get(status, "🔴")
            lines.append(f"### {fid} {title}")
            lines.append("")
            lines.append("| 属性 | 值 |")
            lines.append("|:-----|:---|")
            lines.append(f"| 用途 | {title} |")

            api = feat.get("primary_api", {})
            if api.get("method"):
                lines.append(f"| 主 API | `{api['method']} {api['path']}` |")

            params = feat.get("request_params", [])
            if params:
                param_lines = []
                for p in params:
                    req_mark = '*' if p.get('required') else ''
                    default_part = f", 默认={p['default']}" if p.get('default') is not None else ""
                    desc_part = f" — {p['description']}" if p.get('description') else ""
                    param_lines.append(f"`{p['name']}`({p['type']}{req_mark}{default_part}){desc_part}")
                lines.append(f"| 请求参数 | {'<br>'.join(param_lines)} |")

            example = feat.get("default_example", {})
            if example.get("curl"):
                lines.append(f"| 默认示例 | `{example['curl']}` |")

            resp = feat.get("key_response_fields", [])
            if resp:
                resp_lines = []
                for r in resp:
                    desc_part = f" — {r['description']}" if r.get('description') else ""
                    resp_lines.append(f"`{r['name']}`({r['type']}){desc_part}")
                lines.append(f"| 关键返回 | {'<br>'.join(resp_lines)} |")

            page = feat.get("runtime_page_path")
            if page:
                page_ok = "✅" if feat.get("page_exists", True) else "❌ 模板缺失"
                lines.append(f"| 运行入口 | {page} {page_ok} |")

            tests = feat.get("test_nodeids", [])
            if tests:
                lines.append(f"| 测试节点 | {'; '.join(tests[:3])} |")

            test_st = feat.get("last_test_status", "UNKNOWN")
            test_emoji = {"PASS": "✅", "FAIL": "❌", "UNKNOWN": "❓"}.get(test_st, "❓")
            lines.append(f"| 当前状态 | {status_emoji} {status} |")
            lines.append(f"| 最近测试 | {test_emoji} {test_st} |")

            # ── 三维审计对比 ──
            spec_req = feat.get("spec_requirement")
            code_v = feat.get("code_verdict")
            test_v = feat.get("test_verdict")
            gaps_list = feat.get("gaps", [])

            if spec_req:
                lines.append(f"| **方案要求** | {spec_req} |")
            if code_v:
                lines.append(f"| **代码实现** | {code_v} |")
            if test_v:
                lines.append(f"| **测试覆盖** | {test_v} |")
            if gaps_list:
                gap_items = "<br>".join(f"• {g}" for g in gaps_list)
                lines.append(f"| **⚠ 差距** | {gap_items} |")

            flags = feat.get("mismatch_flags", [])
            if flags:
                lines.append(f"| 缺口描述 | {', '.join(flags)} |")

            refs = feat.get("ssot_refs", [])
            if refs:
                lines.append(f"| SSOT 锚点 | {', '.join(refs)} |")

            lines.append(f"| 可见性 | {feat.get('visibility', 'public')} |")
            lines.append(f"| 负责角色 | {feat.get('owner_scope', '-')} |")
            lines.append("")

    # --- 下一步行动汇总 ---
    blocked_items = [f for f in catalog if f["structural_status"].startswith("BLOCKED")]
    oos_items = [f for f in catalog if f["structural_status"] == "OUT_OF_SSOT"]
    mismatch_items = [f for f in catalog if f["structural_status"] == "MISMATCH"]
    fail_items = [f for f in catalog if f.get("last_test_status") == "FAIL"]
    unknown_test = [f for f in catalog if f.get("last_test_status", "UNKNOWN") == "UNKNOWN"
                    and f["structural_status"] in {"READY", "READY_WITH_GAPS"}]
    code_critical = [f for f in catalog if "❌" in f.get("code_verdict", "")]
    code_red = [f for f in catalog if "🔴" in f.get("code_verdict", "")
                and "❌" not in f.get("code_verdict", "")]
    test_red = [f for f in catalog if "🔴" in f.get("test_verdict", "")]
    has_gaps = [f for f in catalog if f.get("gaps")]

    lines.extend(["---", "", "## 下一步行动", ""])

    if not blocked_items and not mismatch_items and not fail_items:
        lines.append("> ✅ 所有 SSOT 内功能点均为 READY，无阻塞项。")
        lines.append("")
    else:
        lines.append("### 🔴 阻塞项（必须立即处理）")
        lines.append("")
        if blocked_items:
            lines.append("| 功能点 | 状态 | 缺口 | 建议行动 |")
            lines.append("|:-------|:-----|:-----|:---------|")
            for f in blocked_items:
                flags = ", ".join(f.get("mismatch_flags", []))
                action = []
                if "BLOCKED_API" in flags:
                    api = f.get("primary_api", {})
                    action.append(f"注册路由 `{api.get('method','')} {api.get('path','')}`")
                if "BLOCKED_PAGE" in flags:
                    action.append(f"创建模板 `{f.get('runtime_page_path','')}`")
                if "BLOCKED_TEST" in flags:
                    action.append(f"编写测试 `tests/test_{f['fr_id'].lower().replace('-','')}_*.py`")
                lines.append(f"| {f['feature_id']} {f['title']} | {f['structural_status']} | {flags} | {'; '.join(action)} |")
            lines.append("")

        if mismatch_items:
            lines.append("### 🟡 不一致项")
            lines.append("")
            for f in mismatch_items:
                lines.append(f"- **{f['feature_id']}** {f['title']}: {', '.join(f.get('mismatch_flags', []))}")
            lines.append("")

        if fail_items:
            lines.append("### ❌ 测试失败项")
            lines.append("")
            for f in fail_items:
                tests = f.get("test_nodeids", [])
                lines.append(f"- **{f['feature_id']}** {f['title']}: 需修复 `{tests[0] if tests else '?'}`")
            lines.append("")

    # ── 审计发现的真实差距 ──
    if code_critical:
        lines.append(f"### ❌ 代码完全未实现（{len(code_critical)} 个 CRITICAL）")
        lines.append("")
        for f in code_critical:
            cv = f.get("code_verdict", "")
            lines.append(f"- **{f['feature_id']}** {f['title']}: {cv}")
            for g in f.get("gaps", []):
                if "CRITICAL" in g:
                    lines.append(f"  - 🚨 {g}")
        lines.append("")

    if code_red:
        lines.append(f"### 🔴 代码实现有问题（{len(code_red)} 个）")
        lines.append("")
        for f in code_red:
            cv = f.get("code_verdict", "")
            lines.append(f"- **{f['feature_id']}** {f['title']}: {cv}")
        lines.append("")

    if test_red:
        lines.append(f"### 🔴 测试严重缺失（{len(test_red)} 个 — 核心逻辑零测试覆盖）")
        lines.append("")
        for f in test_red:
            tv = f.get("test_verdict", "")
            lines.append(f"- **{f['feature_id']}** {f['title']}: {tv}")
        lines.append("")

    if has_gaps:
        all_gaps = []
        for f in has_gaps:
            for g in f.get("gaps", []):
                all_gaps.append((f["feature_id"], f["title"], g))
        lines.append(f"### 📋 全量差距清单（{len(all_gaps)} 项）")
        lines.append("")
        lines.append("| # | 功能点 | 差距描述 |")
        lines.append("|:-:|:-------|:---------|")
        for i, (fid, title, gap) in enumerate(all_gaps, 1):
            lines.append(f"| {i} | {fid} | {gap} |")
        lines.append("")

    if unknown_test:
        lines.append(f"### ⚠ 测试状态未知（{len(unknown_test)} 个 READY 功能点未验证）")
        lines.append("")
        lines.append("> 运行 `pytest tests/ --junitxml=output/junit.xml` 后重新生成本表以获取真实测试状态。")
        lines.append("")

    if oos_items:
        lines.append(f"### ⚪ SSOT 范围外（{len(oos_items)} 个，仅供参考）")
        lines.append("")
        for f in oos_items:
            vis = f.get("visibility", "")
            note = "废弃路由，待清理" if vis == "deprecated" else "测试辅助，不纳入 SSOT"
            lines.append(f"- **{f['feature_id']}** {f['title']} → {note}")
        lines.append("")

    lines.extend(["---", "",
        "## 如何验证本表真实性", "",
        "本表由 `build_feature_catalog.py` 自动扫描以下 4 个数据源生成，**非人工填写**：", "",
        "1. **`feature_registry.json`** — 119 条功能点注册（唯一手工维护源）",
        "2. **FastAPI 路由扫描** — 检查 `app.main.app` 所有已注册路由，若接口未注册标记 `BLOCKED_API`",
        "3. **HTML 模板扫描** — 检查 `app/web/templates/*.html`，若页面缺失标记 `BLOCKED_PAGE`",
        "4. **pytest 节点收集** — 优先读取 `@pytest.mark.feature(...)`，缺失时才回退到 `test_frXX` 文件/函数名推断；未映射节点会单独计数",
        "",
        "> 💡 要验证不被 AI 欺骗：直接运行 `python -m app.governance.build_feature_catalog`，",
        "> 对比生成结果与本文件是否一致。任何手工篡改都会被覆盖。",
    ])

    # 写文件
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DOC_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build governance catalog snapshot and related artifacts.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--refresh-junit",
        action="store_true",
        help="Force-refresh output/junit.xml from the full pytest suite before building the snapshot.",
    )
    group.add_argument(
        "--skip-junit-refresh",
        action="store_true",
        help="Compatibility flag: keep the current JUnit artifact. Catalog build skips JUnit refresh by default.",
    )
    parser.add_argument(
        "--skip-progress-doc",
        action="store_true",
        help="Skip writing docs/_temp progress output and only refresh governance JSON/XML artifacts.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    if args.refresh_junit:
        refresh_mode = "always"
    elif args.skip_junit_refresh:
        refresh_mode = "never"
    else:
        refresh_mode = "never"
    build_catalog(
        junit_refresh_mode=refresh_mode,
        write_progress_doc=not args.skip_progress_doc,
    )
