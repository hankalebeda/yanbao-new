from __future__ import annotations

import ast
import re
from pathlib import Path

from app.core.error_codes import ERROR_CODE_TO_HTTP, ERROR_CODE_WHITELIST, normalize_error_code
from app.core.request_context import reset_request_id, set_request_id

ROOT = Path(__file__).resolve().parents[1]
CODE_FILES = [ROOT / "app" / "main.py", *sorted((ROOT / "app" / "api").glob("routes_*.py"))]
EXPECTED_ERROR_CODES = {
    "CIRCUIT_BREAKER_OPEN",
    "COLD_START_ERROR",
    "CONCURRENT_CONFLICT",
    "DATA_SOURCE_UNAVAILABLE",
    "DEPENDENCY_NOT_READY",
    "EMAIL_NOT_VERIFIED",
    "FORBIDDEN",
    "IDEMPOTENCY_CONFLICT",
    "INTERNAL_ERROR",
    "INVALID_OAUTH_STATE",
    "INVALID_PAYLOAD",
    "INVALID_PROVIDER",
    "LLM_ALL_FAILED",
    "NOT_FOUND",
    "NOT_IMPLEMENTED",
    "NOT_IN_CORE_POOL",
    "OAUTH_PROVIDER_UNAVAILABLE",
    "PAYMENT_PROVIDER_NOT_CONFIGURED",
    "PAYMENT_SIGNATURE_INVALID",
    "RATE_LIMITED",
    "REPORT_ALREADY_REFERENCED_BY_SIM",
    "REPORT_HISTORY_WINDOW_EXCEEDED",
    "REPORT_NOT_AVAILABLE",
    "RESET_TOKEN_EXPIRED",
    "ROUTE_RETIRED",
    "STALE_TASK_EXPIRED",
    "TIER_ALREADY_ACTIVE",
    "TASK_NOT_FOUND",
    "TIER_NOT_AVAILABLE",
    "UNAUTHORIZED",
    "UPSTREAM_TIMEOUT",
    "VALIDATION_FAILED",
    "REPORT_DATA_INCOMPLETE",
}
CODE_LIKE_DETAIL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_:-]*$")


def _iter_http_exception_details(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    details: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name) or node.func.id != "HTTPException":
            continue
        for keyword in node.keywords:
            if keyword.arg != "detail":
                continue
            if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                details.append(keyword.value.value)
    return details


def _iter_explicit_error_code_literals(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    error_codes: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg != "error_code":
                    continue
                if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                    error_codes.append(keyword.value.value)
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if not isinstance(key, ast.Constant) or key.value != "error_code":
                continue
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                error_codes.append(value.value)
    return error_codes


def test_error_code_whitelist_matches_ssot_projection():
    assert set(ERROR_CODE_TO_HTTP) == EXPECTED_ERROR_CODES
    assert "OK" in ERROR_CODE_WHITELIST
    assert "INTERNAL_ERROR" in ERROR_CODE_WHITELIST


def test_normalize_error_code_rejects_unknown_and_suffix_forms():
    assert normalize_error_code("MADE_UP_ERROR") == "INTERNAL_ERROR"
    assert normalize_error_code("invalid_stock_code") == "INVALID_STOCK_CODE"
    assert normalize_error_code("report_not_found") == "INTERNAL_ERROR"
    assert normalize_error_code({"error_code": "OAUTH_PROVIDER_UNAVAILABLE: provider not ready"}) == "INTERNAL_ERROR"
    assert normalize_error_code({"detail": {"error_code": "REPORT_NOT_AVAILABLE"}}) == "INTERNAL_ERROR"
    assert normalize_error_code("OAUTH_PROVIDER_UNAVAILABLE: provider not ready") == "INTERNAL_ERROR"
    assert normalize_error_code("REPORT_NOT_AVAILABLE") == "REPORT_NOT_AVAILABLE"


def test_envelope_rejects_suffix_form_explicit_error_codes():
    from app.core.response import envelope

    token = set_request_id("req-error-code-gate")
    try:
        payload = envelope(
            code=1,
            error_code="OAUTH_PROVIDER_UNAVAILABLE: provider not ready",
            error="OAUTH_PROVIDER_UNAVAILABLE",
        )
    finally:
        reset_request_id(token)

    assert payload["error_code"] == "OAUTH_PROVIDER_UNAVAILABLE"


def test_error_code_whitelist_matches_runtime_emitters():
    violations: list[str] = []
    for path in CODE_FILES:
        for detail in _iter_http_exception_details(path):
            if CODE_LIKE_DETAIL_RE.fullmatch(detail) and detail not in ERROR_CODE_WHITELIST:
                violations.append(f"{path.relative_to(ROOT)} detail={detail}")
        for error_code in _iter_explicit_error_code_literals(path):
            if CODE_LIKE_DETAIL_RE.fullmatch(error_code) and error_code not in ERROR_CODE_WHITELIST:
                violations.append(f"{path.relative_to(ROOT)} error_code={error_code}")
    assert not violations, "runtime emitters must use exact whitelisted error codes:\n" + "\n".join(sorted(violations))


def test_http_exception_code_like_details_are_exact_whitelisted_codes():
    violations: list[str] = []
    for path in CODE_FILES:
        for detail in _iter_http_exception_details(path):
            if not CODE_LIKE_DETAIL_RE.fullmatch(detail):
                continue
            if detail not in ERROR_CODE_WHITELIST:
                violations.append(f"{path.relative_to(ROOT)}: {detail}")
    assert not violations, "code-like HTTPException.detail values must be exact whitelisted error codes:\n" + "\n".join(violations)


def test_explicit_error_code_literals_are_exact_whitelisted_codes():
    violations: list[str] = []
    for path in CODE_FILES:
        for error_code in _iter_explicit_error_code_literals(path):
            if not CODE_LIKE_DETAIL_RE.fullmatch(error_code):
                continue
            if error_code not in ERROR_CODE_WHITELIST:
                violations.append(f"{path.relative_to(ROOT)}: {error_code}")
    assert not violations, "explicit error_code literals must be exact whitelisted error codes:\n" + "\n".join(violations)
