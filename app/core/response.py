from typing import Any
from uuid import uuid4

from app.core.request_context import get_request_id


def envelope(code: int = 0, message: str = "ok", data: Any = None, error: Any = None,
             error_code: str | None = None, error_message: str | None = None,
             degraded: bool | None = None,
             degraded_reason: str | None = None):
    rid = get_request_id() or str(uuid4())
    resp = {
        "success": code == 0,
        "message": message,
        "data": data,
        "request_id": rid,
        "error": error,
    }
    if error_code is not None:
        # Strip suffix form "CODE: extra message" to just "CODE"
        if isinstance(error_code, str) and ":" in error_code:
            error_code = error_code.split(":")[0].strip()
        resp["error_code"] = error_code
    if error is not None or error_message is not None:
        resp["error_message"] = error_message or error_code or str(error or "")
    if degraded is not None:
        resp["degraded"] = degraded
    if degraded_reason is not None:
        resp["degraded_reason"] = degraded_reason
    return resp
