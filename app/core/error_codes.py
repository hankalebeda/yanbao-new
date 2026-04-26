OK = 0
VALIDATION_ERROR = 3001
AUTH_ERROR = 4001
NOT_FOUND = 4004
INTERNAL_ERROR = 9000

# ---------------------------------------------------------------------------
# Canonical error-code → HTTP status mapping (33 codes)
# ---------------------------------------------------------------------------

ERROR_CODE_TO_HTTP: dict[str, int] = {
    "CIRCUIT_BREAKER_OPEN": 503,
    "COLD_START_ERROR": 503,
    "CONCURRENT_CONFLICT": 409,
    "DATA_SOURCE_UNAVAILABLE": 503,
    "DEPENDENCY_NOT_READY": 503,
    "EMAIL_NOT_VERIFIED": 403,
    "FORBIDDEN": 403,
    "IDEMPOTENCY_CONFLICT": 409,
    "INTERNAL_ERROR": 500,
    "INVALID_OAUTH_STATE": 400,
    "INVALID_PAYLOAD": 400,
    "INVALID_PROVIDER": 400,
    "LLM_ALL_FAILED": 503,
    "NOT_FOUND": 404,
    "NOT_IMPLEMENTED": 501,
    "NOT_IN_CORE_POOL": 422,
    "OAUTH_PROVIDER_UNAVAILABLE": 503,
    "PAYMENT_PROVIDER_NOT_CONFIGURED": 503,
    "PAYMENT_SIGNATURE_INVALID": 400,
    "RATE_LIMITED": 429,
    "REPORT_ALREADY_REFERENCED_BY_SIM": 409,
    "REPORT_DATA_INCOMPLETE": 422,
    "REPORT_HISTORY_WINDOW_EXCEEDED": 422,
    "REPORT_NOT_AVAILABLE": 404,
    "RESET_TOKEN_EXPIRED": 400,
    "ROUTE_RETIRED": 410,
    "STALE_TASK_EXPIRED": 410,
    "TIER_ALREADY_ACTIVE": 409,
    "TASK_NOT_FOUND": 404,
    "TIER_NOT_AVAILABLE": 422,
    "UNAUTHORIZED": 401,
    "UPSTREAM_TIMEOUT": 504,
    "VALIDATION_FAILED": 422,
}

ERROR_CODE_WHITELIST: set[str] = {"OK"} | set(ERROR_CODE_TO_HTTP.keys()) | {
    # Legacy codes still used by web-facing routes or membership checks
    "COOKIE_SESSION_NOT_FOUND", "INVALID_STOCK_CODE", "invalid_stock_code",
    "MEMBERSHIP_UNCONFIRMED", "membership_required",
}


def http_status_for(error_code: str) -> int:
    """Return the HTTP status for a canonical error code (default 500)."""
    return ERROR_CODE_TO_HTTP.get(error_code, 500)


HTTP_STATUS_TO_ERROR_CODE: dict[int, str] = {
    400: "INVALID_PAYLOAD",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    409: "CONCURRENT_CONFLICT",
    410: "ROUTE_RETIRED",
    422: "VALIDATION_FAILED",
    429: "RATE_LIMITED",
    500: "INTERNAL_ERROR",
    501: "NOT_IMPLEMENTED",
    503: "DATA_SOURCE_UNAVAILABLE",
    504: "UPSTREAM_TIMEOUT",
}


def normalize_error_code(detail, *, status_code: int | None = None) -> str:
    """Normalize arbitrary detail into a canonical error code string.

    If the detail string itself is a canonical code, return it directly.
    Otherwise fall back to a status-code-based default.
    """
    if detail is None:
        if status_code is not None:
            return HTTP_STATUS_TO_ERROR_CODE.get(status_code, "INTERNAL_ERROR")
        return "INTERNAL_ERROR"
    if isinstance(detail, dict):
        detail = detail.get("error_code") or detail.get("detail") or detail.get("code") or ""
    raw = str(detail).strip()
    upper = raw.upper().replace(" ", "_").replace("-", "_")
    if upper in ERROR_CODE_WHITELIST:
        return upper
    if status_code is not None:
        return HTTP_STATUS_TO_ERROR_CODE.get(status_code, "INTERNAL_ERROR")
    return "INTERNAL_ERROR"


def map_http_detail_to_error_code(detail) -> int:
    text = str(detail or "").lower()
    if "not_found" in text:
        return NOT_FOUND
    if "invalid" in text or "validation" in text:
        return VALIDATION_ERROR
    if "auth" in text or "unauthorized" in text:
        return AUTH_ERROR
    return VALIDATION_ERROR
