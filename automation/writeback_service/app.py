from __future__ import annotations

import base64
import contextlib
import difflib
import hashlib
import json
import os
import re
import time
import urllib.request
import urllib.error
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterator
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

DEFAULT_DENY_22 = "docs/core/22_\u5168\u91cf\u529f\u80fd\u8fdb\u5ea6\u603b\u8868_v7_\u7cbe\u5ba1.md"
DEFAULT_SHARED_ARTIFACT_DENY_PATHS = (
    "output/junit.xml",
    "app/governance/catalog_snapshot.json",
    "output/blind_spot_audit.json",
    "github/automation/continuous_audit/latest_run.json",
)
INFRA_CONTROL_PLANE_STATE = "automation/control_plane/current_state.json"
DEFAULT_WRITEBACK_A_DENY_PREFIXES = ("runtime/",)
DEFAULT_WRITEBACK_A_DENY_PATHS = (DEFAULT_DENY_22, *DEFAULT_SHARED_ARTIFACT_DENY_PATHS)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_rel_path(value: str) -> str:
    return value.replace("\\", "/").lstrip("./")


@dataclass(frozen=True)
class ServiceConfig:
    repo_root: Path
    audit_dir: Path
    allow_prefixes: tuple[str, ...]
    deny_prefixes: tuple[str, ...]
    deny_paths: tuple[str, ...]
    triage_dir: Path | None = None
    auth_token: str = ""
    require_triage: bool = False
    require_fencing: bool = False
    lock_timeout_seconds: float = 8.0
    allow_shared_artifacts: bool = False
    app_base_url: str = ""
    internal_token: str = ""

    @staticmethod
    def _parse_csv(value: str, default: tuple[str, ...]) -> tuple[str, ...]:
        parts = [item.strip() for item in value.split(",") if item.strip()]
        return tuple(parts) if parts else default

    @classmethod
    def from_env(cls) -> "ServiceConfig":
        repo_root = Path(os.getenv("WRITEBACK_REPO_ROOT") or Path(__file__).resolve().parents[2]).resolve()
        audit_dir = Path(
            os.getenv("WRITEBACK_AUDIT_DIR") or (repo_root / "automation" / "writeback_service" / ".audit")
        ).resolve()
        triage_dir = Path(
            os.getenv("WRITEBACK_TRIAGE_DIR") or (repo_root / "runtime" / "issue_mesh" / "promote_prep" / "triage")
        ).resolve()
        allow_prefixes = cls._parse_csv(
            os.getenv("WRITEBACK_ALLOW_PREFIXES", ""),
            ("LiteLLM/", "docs/_temp/"),
        )
        deny_prefixes = cls._parse_csv(
            os.getenv("WRITEBACK_DENY_PREFIXES", ""),
            DEFAULT_WRITEBACK_A_DENY_PREFIXES,
        )
        deny_paths = cls._parse_csv(
            os.getenv("WRITEBACK_DENY_PATHS", ""),
            DEFAULT_WRITEBACK_A_DENY_PATHS,
        )
        require_triage = str(os.getenv("WRITEBACK_REQUIRE_TRIAGE", "true")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        require_fencing = str(os.getenv("WRITEBACK_REQUIRE_FENCING", "false")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        timeout = float(os.getenv("WRITEBACK_LOCK_TIMEOUT_SECONDS", "8"))
        return cls(
            repo_root=repo_root,
            audit_dir=audit_dir,
            allow_prefixes=allow_prefixes,
            deny_prefixes=deny_prefixes,
            deny_paths=tuple(_normalize_rel_path(item) for item in deny_paths),
            triage_dir=triage_dir,
            auth_token=str(os.getenv("WRITEBACK_AUTH_TOKEN", "")).strip(),
            require_triage=require_triage,
            require_fencing=require_fencing,
            lock_timeout_seconds=timeout,
            allow_shared_artifacts=str(os.getenv("WRITEBACK_ALLOW_SHARED_ARTIFACTS", "false")).strip().lower() in {"1", "true", "yes", "on"},
            app_base_url=str(os.getenv("APP_BASE_URL", "")).strip(),
            internal_token=str(os.getenv("INTERNAL_TOKEN", "")).strip(),
        )


class Actor(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str | None = None
    id: str | None = None


class ReadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    target_path: str


class PreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    target_path: str
    base_sha256: str
    patch_text: str


class CommitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    target_path: str
    base_sha256: str
    patch_text: str
    idempotency_key: str
    actor: Actor | None = None
    request_id: str | None = None
    run_id: str | None = None
    triage_record_id: str | None = None
    lease_id: str | None = None
    fencing_token: int | None = None


class RollbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    commit_id: str
    idempotency_key: str
    actor: Actor | None = None
    request_id: str | None = None
    run_id: str | None = None
    lease_id: str | None = None
    fencing_token: int | None = None


class BatchItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    target_path: str
    base_sha256: str
    patch_text: str


class BatchPreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: list[BatchItem]


class BatchCommitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: list[BatchItem]
    idempotency_key: str
    actor: Actor | None = None
    request_id: str | None = None
    run_id: str | None = None
    triage_record_ids: list[str] = Field(default_factory=list)
    lease_id: str | None = None
    fencing_token: int | None = None


class BatchRollbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    commit_ids: list[str]
    idempotency_key: str
    actor: Actor | None = None
    request_id: str | None = None
    run_id: str | None = None
    lease_id: str | None = None
    fencing_token: int | None = None


class LeaseClaimRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    round_id: str
    target_paths: list[str]
    lease_seconds: int = Field(default=600, ge=30, le=3600)


class LeaseReleaseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    lease_id: str
    reason: str = "complete"


def _audit_commit_path(cfg: ServiceConfig, commit_id: str) -> Path:
    return cfg.audit_dir / "commits" / f"{commit_id}.json"


def _idempotency_path(cfg: ServiceConfig, idempotency_key: str) -> Path:
    return cfg.audit_dir / "idempotency" / f"{_sha256_text(idempotency_key)}.json"


def _preview_path(cfg: ServiceConfig, relative_path: str, base_sha256: str, patch_hash: str) -> Path:
    payload = json.dumps(
        {
            "relative_path": _normalize_rel_path(relative_path),
            "base_sha256": base_sha256,
            "patch_hash": patch_hash,
        },
        ensure_ascii=True,
        sort_keys=True,
    )
    return cfg.audit_dir / "preview" / f"{_sha256_text(payload)}.json"


def _triage_dir(cfg: ServiceConfig) -> Path:
    return (cfg.triage_dir or (cfg.repo_root / "runtime" / "issue_mesh" / "promote_prep" / "triage")).resolve()


def _triage_record_path(cfg: ServiceConfig, triage_record_id: str) -> Path:
    token = str(triage_record_id or "").strip()
    if not token or not re.fullmatch(r"[A-Za-z0-9_.-]+__[A-Za-z0-9_.-]+", token):
        raise HTTPException(status_code=409, detail="TRIAGE_REQUIRED")
    return _triage_dir(cfg) / f"{token}.json"


def _lock_path(cfg: ServiceConfig, lock_name: str) -> Path:
    return cfg.audit_dir / ".locks" / f"{_sha256_text(lock_name)}.lock"


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8", newline="") as temp_file:
            temp_file.write(content)
            temp_file.flush()
            os.fsync(temp_file.fileno())
            temp_path = Path(temp_file.name)
        os.replace(temp_path, path)
    finally:
        if temp_path is not None and temp_path.exists():
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=True, sort_keys=True, indent=2))


# ---------------------------------------------------------------------------
# Journal-first helpers (P0 write transaction hardening)
# ---------------------------------------------------------------------------

def _journal_dir(cfg: ServiceConfig) -> Path:
    return cfg.audit_dir / "journals"


def _write_commit_journal(cfg: ServiceConfig, intent: dict[str, Any]) -> Path:
    """Write a journal entry BEFORE mutating the target file.

    On recovery, un-cleaned journals indicate interrupted commits that
    need replay or rollback.
    """
    jdir = _journal_dir(cfg)
    jdir.mkdir(parents=True, exist_ok=True)
    journal_id = intent.get("idempotency_key") or str(uuid4())
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", journal_id)[:120]
    path = jdir / f"{safe_id}.json"
    _atomic_write_text(path, json.dumps(intent, ensure_ascii=True, sort_keys=True, indent=2))
    return path


def _remove_journal(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        path.unlink()


def _write_batch_progress(cfg: ServiceConfig, batch_key: str, committed: list[dict[str, Any]]) -> Path:
    """Append compensation log for batch commit progress.

    If the batch (or its rollback) fails, this file records which files
    were already written so a recovery process can act.
    """
    jdir = _journal_dir(cfg)
    jdir.mkdir(parents=True, exist_ok=True)
    safe_key = re.sub(r"[^a-zA-Z0-9_-]", "_", batch_key)[:120]
    path = jdir / f"batch_{safe_key}_progress.json"
    _atomic_write_text(path, json.dumps({
        "batch_key": batch_key,
        "committed_files": committed,
        "updated_at": _utc_now_iso(),
    }, ensure_ascii=True, sort_keys=True, indent=2))
    return path


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return data
    return None


def _load_commit_or_404(cfg: ServiceConfig, commit_id: str) -> dict[str, Any]:
    record = _load_json(_audit_commit_path(cfg, commit_id))
    if not record:
        raise HTTPException(status_code=404, detail="COMMIT_NOT_FOUND")
    return record


def _append_audit_event(cfg: ServiceConfig, event: dict[str, Any]) -> None:
    audit_path = cfg.audit_dir / "audit_log.jsonl"
    lock_name = f"audit-log:{audit_path.as_posix()}"
    with _file_lock(cfg, lock_name):
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with audit_path.open("a", encoding="utf-8", newline="\n") as file_obj:
            file_obj.write(json.dumps(event, ensure_ascii=True, sort_keys=True))
            file_obj.write("\n")


def _query_runtime_gates(cfg: ServiceConfig) -> dict[str, Any] | None:
    """Query the authoritative runtime gates API.

    Returns the parsed ``data`` dict on success, or ``None`` on any failure
    (network, timeout, auth, parse).  Callers must fall back to file-based
    operator config when this returns ``None``.
    """
    base_url = cfg.app_base_url.rstrip("/")
    if not base_url:
        return None
    url = f"{base_url}/api/v1/internal/runtime/gates"
    req = urllib.request.Request(url, method="GET")
    if cfg.internal_token:
        req.add_header("X-Internal-Token", cfg.internal_token)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        if isinstance(body, dict):
            return body.get("data") if isinstance(body.get("data"), dict) else body
    except Exception:
        pass
    return None


def _control_plane_state_path(cfg: ServiceConfig) -> Path:
    return (cfg.repo_root / INFRA_CONTROL_PLANE_STATE).resolve()


def _read_promote_target_mode(cfg: ServiceConfig) -> tuple[str, str | None]:
    """Determine promote_target_mode.

    Step 1: Try the authoritative runtime gates API for runtime truth.
    Step 2: Fall back to the operator config file for promote_target_mode.
    The file is *only* an operator config source — it does not carry runtime authority.

    Default is "infra" — missing or invalid control-plane state must
    fail closed so writeback-B cannot silently reopen doc22 writes.
    """
    # --- runtime gate (authoritative) ---
    gates = _query_runtime_gates(cfg)
    if gates is not None:
        # The API is the source of truth for promote decisions.
        # We still need the *operator config* file for which mode to use.
        pass  # gates fetched; continue to file for mode selection

    # --- operator config (file) ---
    state_path = _control_plane_state_path(cfg)
    if not state_path.exists():
        return "infra", "CONTROL_PLANE_STATE_MISSING_DEFAULT_INFRA"
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return "infra", "CONTROL_PLANE_STATE_INVALID_DEFAULT_INFRA"
    if not isinstance(payload, dict):
        return "infra", "CONTROL_PLANE_STATE_INVALID_DEFAULT_INFRA"
    mode = str(payload.get("promote_target_mode") or "").strip().lower()
    if mode in {"infra", "doc22"}:
        return mode, None
    return "infra", "CONTROL_PLANE_STATE_INVALID_DEFAULT_INFRA"


def _raise_target_denied(
    cfg: ServiceConfig,
    *,
    detail: str,
    reason: str,
    target_path: str,
    relative_path: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    event: dict[str, Any] = {
        "timestamp": _utc_now_iso(),
        "event": "target_denied",
        "target_path": str(target_path),
        "detail": detail,
        "reason": reason,
    }
    if relative_path:
        event["relative_path"] = relative_path
    if extra:
        event.update(extra)
    _append_audit_event(cfg, event)
    raise HTTPException(status_code=403, detail=detail)


@contextlib.contextmanager
def _file_lock(cfg: ServiceConfig, lock_name: str) -> Iterator[None]:
    lock_path = _lock_path(cfg, lock_name)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    fd: int | None = None
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            os.write(fd, f"{os.getpid()}:{_utc_now_iso()}".encode("utf-8"))
            break
        except FileExistsError:
            if (time.monotonic() - started) >= cfg.lock_timeout_seconds:
                raise HTTPException(status_code=423, detail="RESOURCE_LOCKED")
            time.sleep(0.05)
    try:
        yield
    finally:
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)
        with contextlib.suppress(FileNotFoundError):
            lock_path.unlink()


def _resolve_allowed_target(cfg: ServiceConfig, target_path: str) -> tuple[Path, str]:
    raw = Path(target_path)
    resolved = raw.resolve() if raw.is_absolute() else (cfg.repo_root / raw).resolve()
    try:
        relative = resolved.relative_to(cfg.repo_root)
    except ValueError as exc:
        _raise_target_denied(
            cfg,
            detail="TARGET_PATH_OUTSIDE_REPO",
            reason="TARGET_PATH_OUTSIDE_REPO",
            target_path=str(target_path),
        )
        raise AssertionError("unreachable") from exc

    rel_posix = _normalize_rel_path(relative.as_posix())
    is_shared_artifact = rel_posix in DEFAULT_SHARED_ARTIFACT_DENY_PATHS
    if is_shared_artifact:
        if not cfg.allow_shared_artifacts:
            _raise_target_denied(
                cfg,
                detail="TARGET_PATH_FORBIDDEN",
                reason="SHARED_ARTIFACT_WRITEBACK_FORBIDDEN",
                target_path=str(target_path),
                relative_path=rel_posix,
            )
        else:
            return resolved, rel_posix

    if rel_posix == DEFAULT_DENY_22:
        promote_target_mode, control_plane_reason = _read_promote_target_mode(cfg)
        if promote_target_mode != "doc22":
            _raise_target_denied(
                cfg,
                detail="DOC22_WRITEBACK_BLOCKED_BY_INFRA_MODE",
                reason="DOC22_WRITEBACK_BLOCKED_BY_INFRA_MODE",
                target_path=str(target_path),
                relative_path=rel_posix,
                extra={
                    "promote_target_mode": promote_target_mode,
                    "control_plane_state_path": str(_control_plane_state_path(cfg)),
                    "control_plane_state_reason": control_plane_reason,
                },
            )
        else:
            # Dynamic doc22 mode: short-circuit frozen deny/allow prefixes
            # so that stale startup-time policy cannot block doc22 writes.
            return resolved, rel_posix

    deny_paths = {_normalize_rel_path(item) for item in cfg.deny_paths}
    if rel_posix in deny_paths:
        _raise_target_denied(
            cfg,
            detail="TARGET_PATH_FORBIDDEN",
            reason="CONFIGURED_DENY_PATH",
            target_path=str(target_path),
            relative_path=rel_posix,
        )

    for deny_prefix in cfg.deny_prefixes:
        prefix = _normalize_rel_path(deny_prefix).rstrip("/")
        if rel_posix == prefix or rel_posix.startswith(f"{prefix}/"):
            _raise_target_denied(
                cfg,
                detail="TARGET_PATH_FORBIDDEN",
                reason="CONFIGURED_DENY_PREFIX",
                target_path=str(target_path),
                relative_path=rel_posix,
                extra={"matched_prefix": prefix},
            )

    allowed = False
    for allow_prefix in cfg.allow_prefixes:
        prefix = _normalize_rel_path(allow_prefix).rstrip("/")
        if rel_posix == prefix or rel_posix.startswith(f"{prefix}/"):
            allowed = True
            break
    if not allowed:
        _raise_target_denied(
            cfg,
            detail="TARGET_PATH_NOT_ALLOWLISTED",
            reason="TARGET_PATH_NOT_ALLOWLISTED",
            target_path=str(target_path),
            relative_path=rel_posix,
        )

    return resolved, rel_posix


def _read_file_content(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _compute_diff_summary(current_text: str, patched_text: str) -> dict[str, Any]:
    diff_lines = list(
        difflib.unified_diff(
            current_text.splitlines(),
            patched_text.splitlines(),
            fromfile="current",
            tofile="patched",
            lineterm="",
        )
    )
    added = sum(1 for line in diff_lines if line.startswith("+") and not line.startswith("+++"))
    removed = sum(1 for line in diff_lines if line.startswith("-") and not line.startswith("---"))
    preview_limit = 200
    return {
        "changed": current_text != patched_text,
        "lines_added": added,
        "lines_removed": removed,
        "preview": diff_lines[:preview_limit],
        "preview_truncated": len(diff_lines) > preview_limit,
    }


def _fingerprint(operation: str, payload: dict[str, Any]) -> str:
    canonical = json.dumps({"operation": operation, "payload": payload}, ensure_ascii=True, sort_keys=True)
    return _sha256_text(canonical)


def _check_idempotency(
    cfg: ServiceConfig,
    *,
    operation: str,
    idempotency_key: str,
    fingerprint: str,
    logical_match: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, Path]:
    path = _idempotency_path(cfg, idempotency_key)
    existing = _load_json(path)
    if not existing:
        return None, path
    if existing.get("operation") != operation:
        raise HTTPException(status_code=409, detail="IDEMPOTENCY_CONFLICT")
    commit_id = str(existing.get("commit_id") or "")
    if not commit_id:
        raise HTTPException(status_code=409, detail="IDEMPOTENCY_CONFLICT")
    record = _load_commit_or_404(cfg, commit_id)
    if existing.get("fingerprint") == fingerprint:
        return record, path
    if logical_match is not None and _matches_logical_idempotency(record, operation=operation, logical_match=logical_match):
        return record, path
    raise HTTPException(status_code=409, detail="IDEMPOTENCY_CONFLICT")


def _matches_logical_idempotency(
    record: dict[str, Any],
    *,
    operation: str,
    logical_match: dict[str, Any],
) -> bool:
    if operation == "commit":
        return (
            record.get("operation") == "commit"
            and str(record.get("relative_path") or "") == str(logical_match.get("target_path") or "")
            and str(record.get("base_sha256") or "") == str(logical_match.get("base_sha256") or "")
            and str(record.get("patch_hash") or "") == str(logical_match.get("patch_hash") or "")
            and str(record.get("run_id") or "") == str(logical_match.get("run_id") or "")
        )
    if operation == "rollback":
        return (
            record.get("operation") == "rollback"
            and str(record.get("rollback_of") or "") == str(logical_match.get("commit_id") or "")
            and str(record.get("run_id") or "") == str(logical_match.get("run_id") or "")
        )
    return False


def _save_idempotency(
    *,
    path: Path,
    idempotency_key: str,
    operation: str,
    fingerprint: str,
    commit_id: str,
) -> None:
    payload = {
        "idempotency_key": idempotency_key,
        "operation": operation,
        "fingerprint": fingerprint,
        "commit_id": commit_id,
        "saved_at": _utc_now_iso(),
    }
    _atomic_write_json(path, payload)


def _public_commit(record: dict[str, Any]) -> dict[str, Any]:
    result = dict(record)
    result.pop("previous_content_b64", None)
    return result


def _require_auth(cfg: ServiceConfig, request: Request) -> None:
    if not cfg.auth_token:
        return
    if (request.headers.get("Authorization") or "").strip() != f"Bearer {cfg.auth_token}":
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")


def _require_allowing_triage(
    cfg: ServiceConfig,
    *,
    triage_record_id: str | None,
    relative_path: str,
    base_sha256: str,
    patch_hash: str,
) -> dict[str, Any]:
    if not cfg.require_triage:
        return {}
    if not str(triage_record_id or "").strip():
        raise HTTPException(status_code=409, detail="TRIAGE_REQUIRED")
    record = _load_json(_triage_record_path(cfg, str(triage_record_id)))
    if not record:
        raise HTTPException(status_code=409, detail="TRIAGE_REQUIRED")
    if str(record.get("decision") or "").lower() != "allow" or not bool(record.get("auto_commit")):
        raise HTTPException(status_code=409, detail="TRIAGE_NOT_ALLOWED")

    triage_target = _normalize_rel_path(
        str(record.get("relative_target_path") or record.get("target_path") or "")
    )
    if triage_target and triage_target != relative_path:
        raise HTTPException(status_code=409, detail="TRIAGE_TARGET_MISMATCH")
    triage_base_sha = str(record.get("base_sha256") or "").strip()
    if triage_base_sha and triage_base_sha != base_sha256:
        raise HTTPException(status_code=409, detail="TRIAGE_BASE_SHA_MISMATCH")
    triage_patch_hash = str(record.get("patch_hash") or "").strip()
    if triage_patch_hash and triage_patch_hash != patch_hash:
        raise HTTPException(status_code=409, detail="TRIAGE_PATCH_MISMATCH")
    return record


def _validate_fencing(
    cfg: ServiceConfig,
    *,
    lease_id: str | None,
    fencing_token: int | None,
    target_paths: list[str],
) -> None:
    """Validate lease/fencing and optionally fail closed when service requires it."""
    if lease_id is None and fencing_token is None:
        if cfg.require_fencing:
            raise HTTPException(status_code=409, detail="FENCING_REQUIRED")
        return
    if lease_id is None or fencing_token is None:
        raise HTTPException(
            status_code=409,
            detail="FENCING_INCOMPLETE: both lease_id and fencing_token required",
        )
    from app.services.writeback_coordination import (
        LeaseRejectedError,
        WritebackCoordination,
    )

    state_path = cfg.repo_root / "runtime" / "writeback_coordination" / "state.json"
    coord = WritebackCoordination(state_path)
    try:
        coord.assert_submit_allowed(lease_id, fencing_token, target_paths)
    except LeaseRejectedError as exc:
        raise HTTPException(status_code=409, detail=f"FENCING_REJECTED:{exc.reason}") from exc


def create_app(config: ServiceConfig | None = None) -> FastAPI:
    cfg = config or ServiceConfig.from_env()
    cfg.audit_dir.mkdir(parents=True, exist_ok=True)
    (cfg.audit_dir / "commits").mkdir(parents=True, exist_ok=True)
    (cfg.audit_dir / "idempotency").mkdir(parents=True, exist_ok=True)
    (cfg.audit_dir / "preview").mkdir(parents=True, exist_ok=True)
    (cfg.audit_dir / ".locks").mkdir(parents=True, exist_ok=True)
    _triage_dir(cfg).mkdir(parents=True, exist_ok=True)

    app = FastAPI(title="Writeback Service", version="1.0.0")
    app.state.writeback_config = cfg

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "auth_enabled": bool(cfg.auth_token),
            "require_triage": cfg.require_triage,
            "require_fencing": cfg.require_fencing,
        }

    @app.post("/v1/read")
    def read_target(payload: ReadRequest, request: Request) -> dict[str, Any]:
        _require_auth(cfg, request)
        target, rel_path = _resolve_allowed_target(cfg, payload.target_path)
        if not target.exists():
            raise HTTPException(status_code=404, detail="TARGET_NOT_FOUND")
        content = _read_file_content(target)
        return {
            "target_path": str(target),
            "relative_path": rel_path,
            "content": content,
            "sha256": _sha256_text(content),
        }

    @app.post("/v1/preview")
    def preview_patch(payload: PreviewRequest, request: Request) -> dict[str, Any]:
        _require_auth(cfg, request)
        target, rel_path = _resolve_allowed_target(cfg, payload.target_path)
        current = _read_file_content(target)
        current_sha = _sha256_text(current)
        patched = payload.patch_text
        patched_sha = _sha256_text(patched)
        patch_hash = _sha256_text(payload.patch_text)
        _atomic_write_json(
            _preview_path(cfg, rel_path, payload.base_sha256, patch_hash),
            {
                "relative_path": rel_path,
                "base_sha256": payload.base_sha256,
                "current_sha256": current_sha,
                "patched_sha256": patched_sha,
                "patch_hash": patch_hash,
                "previewed_at": _utc_now_iso(),
            },
        )
        return {
            "target_path": str(target),
            "relative_path": rel_path,
            "base_sha256": payload.base_sha256,
            "current_sha256": current_sha,
            "patched_sha256": patched_sha,
            "conflict": payload.base_sha256 != current_sha,
            "patch_hash": patch_hash,
            "diff_summary": _compute_diff_summary(current, patched),
        }

    @app.post("/v1/commit")
    def commit_patch(payload: CommitRequest, request: Request) -> dict[str, Any]:
        _require_auth(cfg, request)
        target, rel_path = _resolve_allowed_target(cfg, payload.target_path)
        actor_payload = payload.actor.model_dump() if payload.actor else {}
        patch_hash = _sha256_text(payload.patch_text)
        fp = _fingerprint(
            "commit",
            {
                "target_path": rel_path,
                "base_sha256": payload.base_sha256,
                "patch_hash": patch_hash,
                "run_id": payload.run_id,
            },
        )
        idempotency_lock_name = f"idem:commit:{payload.idempotency_key}"
        with _file_lock(cfg, idempotency_lock_name):
            existing, id_path = _check_idempotency(
                cfg,
                operation="commit",
                idempotency_key=payload.idempotency_key,
                fingerprint=fp,
                logical_match={
                    "target_path": rel_path,
                    "base_sha256": payload.base_sha256,
                    "patch_hash": patch_hash,
                    "run_id": payload.run_id,
                },
            )
            if existing is not None:
                return {
                    "commit_id": existing["commit_id"],
                    "new_sha256": existing["new_sha256"],
                    "status": "committed",
                    "idempotent_replay": True,
                }

            preview_record = _load_json(_preview_path(cfg, rel_path, payload.base_sha256, patch_hash))
            if not preview_record:
                raise HTTPException(status_code=409, detail="PREVIEW_REQUIRED")
            triage_record = _require_allowing_triage(
                cfg,
                triage_record_id=payload.triage_record_id,
                relative_path=rel_path,
                base_sha256=payload.base_sha256,
                patch_hash=patch_hash,
            )

            _validate_fencing(
                cfg,
                lease_id=payload.lease_id,
                fencing_token=payload.fencing_token,
                target_paths=[rel_path],
            )

            path_lock_name = f"path:{rel_path}"
            with _file_lock(cfg, path_lock_name):
                previous_content = _read_file_content(target)
                previous_sha = _sha256_text(previous_content)
                if rel_path == DEFAULT_DENY_22 and payload.run_id and payload.run_id in previous_content:
                    # Record skip as a pseudo-commit so idempotency cache
                    # blocks subsequent attempts with the same key from
                    # turning into a real commit.
                    skip_id = f"skip-runid-{_sha256_text(payload.run_id + rel_path)[:16]}"
                    skip_record = {
                        "commit_id": skip_id,
                        "operation": "commit",
                        "target_path": str(target),
                        "relative_path": rel_path,
                        "base_sha256": previous_sha,
                        "new_sha256": previous_sha,
                        "patch_hash": patch_hash,
                        "idempotency_key": payload.idempotency_key,
                        "request_id": payload.request_id,
                        "run_id": payload.run_id,
                        "triage_record_id": payload.triage_record_id,
                        "actor": actor_payload,
                        "created_at": _utc_now_iso(),
                        "rolled_back_at": None,
                        "rollback_commit_id": None,
                        "rollback_of": None,
                        "previous_content_b64": "",
                        "skip_reason": "RUN_ID_ALREADY_PRESENT",
                    }
                    _atomic_write_json(_audit_commit_path(cfg, skip_id), skip_record)
                    _save_idempotency(
                        path=id_path,
                        idempotency_key=payload.idempotency_key,
                        operation="commit",
                        fingerprint=fp,
                        commit_id=skip_id,
                    )
                    return {
                        "commit_id": skip_id,
                        "new_sha256": previous_sha,
                        "status": "skipped",
                        "skip_reason": "RUN_ID_ALREADY_PRESENT",
                        "idempotent_replay": False,
                    }
                if previous_sha != payload.base_sha256:
                    raise HTTPException(status_code=409, detail="BASE_SHA_MISMATCH")

                # --- Journal-first: write intent before mutating target ---
                journal_path = _write_commit_journal(cfg, {
                    "idempotency_key": payload.idempotency_key,
                    "target_path": rel_path,
                    "base_sha256": previous_sha,
                    "patch_hash": patch_hash,
                    "run_id": payload.run_id,
                    "previous_content_b64": base64.b64encode(previous_content.encode("utf-8")).decode("ascii"),
                    "created_at": _utc_now_iso(),
                })

                target.parent.mkdir(parents=True, exist_ok=True)
                _atomic_write_text(target, payload.patch_text)
                new_sha = _sha256_text(payload.patch_text)

                commit_id = str(uuid4())
                record = {
                    "commit_id": commit_id,
                    "operation": "commit",
                    "target_path": str(target),
                    "relative_path": rel_path,
                    "base_sha256": previous_sha,
                    "new_sha256": new_sha,
                    "patch_hash": patch_hash,
                    "idempotency_key": payload.idempotency_key,
                    "request_id": payload.request_id,
                    "run_id": payload.run_id,
                    "triage_record_id": payload.triage_record_id,
                    "triage_decision_source": triage_record.get("decision_source"),
                    "actor": actor_payload,
                    "created_at": _utc_now_iso(),
                    "rolled_back_at": None,
                    "rollback_commit_id": None,
                    "rollback_of": None,
                    "previous_content_b64": base64.b64encode(previous_content.encode("utf-8")).decode("ascii"),
                }
                _atomic_write_json(_audit_commit_path(cfg, commit_id), record)
                _save_idempotency(
                    path=id_path,
                    idempotency_key=payload.idempotency_key,
                    operation="commit",
                    fingerprint=fp,
                    commit_id=commit_id,
                )
                _append_audit_event(
                    cfg,
                    {
                        "timestamp": _utc_now_iso(),
                        "event": "commit",
                        "commit_id": commit_id,
                        "relative_path": rel_path,
                        "base_sha256": previous_sha,
                        "new_sha256": new_sha,
                        "triage_record_id": payload.triage_record_id,
                    },
                )
                # --- Journal cleanup: commit fully persisted ---
                _remove_journal(journal_path)

                return {
                    "commit_id": commit_id,
                    "new_sha256": new_sha,
                    "status": "committed",
                    "idempotent_replay": False,
                }

    @app.get("/v1/commits/{commit_id}")
    def get_commit(commit_id: str, request: Request) -> dict[str, Any]:
        _require_auth(cfg, request)
        return _public_commit(_load_commit_or_404(cfg, commit_id))

    @app.post("/v1/rollback")
    def rollback_commit(payload: RollbackRequest, request: Request) -> dict[str, Any]:
        _require_auth(cfg, request)
        actor_payload = payload.actor.model_dump() if payload.actor else {}
        fp = _fingerprint(
            "rollback",
            {
                "commit_id": payload.commit_id,
                "run_id": payload.run_id,
            },
        )
        idempotency_lock_name = f"idem:rollback:{payload.idempotency_key}"
        with _file_lock(cfg, idempotency_lock_name):
            existing, id_path = _check_idempotency(
                cfg,
                operation="rollback",
                idempotency_key=payload.idempotency_key,
                fingerprint=fp,
                logical_match={
                    "commit_id": payload.commit_id,
                    "run_id": payload.run_id,
                },
            )
            if existing is not None:
                return {
                    "commit_id": existing["commit_id"],
                    "new_sha256": existing["new_sha256"],
                    "status": "rolled_back",
                    "idempotent_replay": True,
                }

            original = _load_commit_or_404(cfg, payload.commit_id)
            if original.get("operation") != "commit" or original.get("rollback_of"):
                raise HTTPException(status_code=409, detail="ROLLBACK_TARGET_INVALID")
            if original.get("rolled_back_at"):
                raise HTTPException(status_code=409, detail="ALREADY_ROLLED_BACK")

            target = Path(str(original["target_path"])).resolve()
            rel_path = _normalize_rel_path(str(original["relative_path"]))
            # Re-validate the target policy on rollback.
            _resolve_allowed_target(cfg, str(target))

            # Validate fencing — rollback must also hold a valid lease
            _validate_fencing(
                cfg,
                lease_id=payload.lease_id,
                fencing_token=payload.fencing_token,
                target_paths=[rel_path],
            )

            path_lock_name = f"path:{rel_path}"
            with _file_lock(cfg, path_lock_name):
                current_content = _read_file_content(target)
                current_sha = _sha256_text(current_content)
                expected_sha = str(original["new_sha256"])
                if current_sha != expected_sha:
                    raise HTTPException(status_code=409, detail="ROLLBACK_TARGET_CHANGED")

                previous_content_b64 = str(original.get("previous_content_b64") or "")
                try:
                    restored_content = base64.b64decode(previous_content_b64.encode("ascii")).decode("utf-8")
                except Exception as exc:
                    raise HTTPException(status_code=409, detail="ROLLBACK_PAYLOAD_INVALID") from exc

                _atomic_write_text(target, restored_content)
                rollback_sha = _sha256_text(restored_content)
                rollback_id = str(uuid4())
                rollback_record = {
                    "commit_id": rollback_id,
                    "operation": "rollback",
                    "target_path": str(target),
                    "relative_path": rel_path,
                    "base_sha256": expected_sha,
                    "new_sha256": rollback_sha,
                    "patch_hash": _sha256_text(restored_content),
                    "idempotency_key": payload.idempotency_key,
                    "request_id": payload.request_id,
                    "run_id": payload.run_id,
                    "triage_record_id": original.get("triage_record_id"),
                    "actor": actor_payload,
                    "created_at": _utc_now_iso(),
                    "rolled_back_at": None,
                    "rollback_commit_id": None,
                    "rollback_of": payload.commit_id,
                    "previous_content_b64": base64.b64encode(current_content.encode("utf-8")).decode("ascii"),
                }
                _atomic_write_json(_audit_commit_path(cfg, rollback_id), rollback_record)

                original["rolled_back_at"] = _utc_now_iso()
                original["rollback_commit_id"] = rollback_id
                _atomic_write_json(_audit_commit_path(cfg, payload.commit_id), original)

                _save_idempotency(
                    path=id_path,
                    idempotency_key=payload.idempotency_key,
                    operation="rollback",
                    fingerprint=fp,
                    commit_id=rollback_id,
                )
                _append_audit_event(
                    cfg,
                    {
                        "timestamp": _utc_now_iso(),
                        "event": "rollback",
                        "rollback_commit_id": rollback_id,
                        "rollback_of": payload.commit_id,
                        "relative_path": rel_path,
                        "base_sha256": expected_sha,
                        "new_sha256": rollback_sha,
                    },
                )
                return {
                    "commit_id": rollback_id,
                    "new_sha256": rollback_sha,
                    "status": "rolled_back",
                    "idempotent_replay": False,
                }

    # ------------------------------------------------------------------
    # Batch endpoints — ordered locking, all-or-nothing semantics
    # ------------------------------------------------------------------

    @app.post("/v1/batch-preview")
    def batch_preview(payload: BatchPreviewRequest, request: Request) -> dict[str, Any]:
        _require_auth(cfg, request)
        results: list[dict[str, Any]] = []
        for item in payload.items:
            target, rel_path = _resolve_allowed_target(cfg, item.target_path)
            current = _read_file_content(target)
            current_sha = _sha256_text(current)
            patched = item.patch_text
            patched_sha = _sha256_text(patched)
            patch_hash = _sha256_text(item.patch_text)
            _atomic_write_json(
                _preview_path(cfg, rel_path, item.base_sha256, patch_hash),
                {
                    "relative_path": rel_path,
                    "base_sha256": item.base_sha256,
                    "current_sha256": current_sha,
                    "patched_sha256": patched_sha,
                    "patch_hash": patch_hash,
                    "previewed_at": _utc_now_iso(),
                },
            )
            results.append({
                "target_path": str(target),
                "relative_path": rel_path,
                "base_sha256": item.base_sha256,
                "current_sha256": current_sha,
                "patched_sha256": patched_sha,
                "conflict": item.base_sha256 != current_sha,
                "patch_hash": patch_hash,
                "diff_summary": _compute_diff_summary(current, patched),
            })
        return {"results": results}

    @app.post("/v1/batch-commit")
    def batch_commit(payload: BatchCommitRequest, request: Request) -> dict[str, Any]:
        """All-or-nothing batch commit with ordered locking and fencing."""
        _require_auth(cfg, request)
        actor_payload = payload.actor.model_dump() if payload.actor else {}

        # Resolve all targets upfront — fail fast on policy violation
        resolved: list[tuple[Path, str, BatchItem, str]] = []
        for item in payload.items:
            target, rel_path = _resolve_allowed_target(cfg, item.target_path)
            patch_hash = _sha256_text(item.patch_text)
            resolved.append((target, rel_path, item, patch_hash))

        # Early fencing check (fail-fast before locks); re-validated under lock below
        all_rel_paths = [r[1] for r in resolved]
        _validate_fencing(
            cfg,
            lease_id=payload.lease_id,
            fencing_token=payload.fencing_token,
            target_paths=all_rel_paths,
        )

        # Batch idempotency check
        batch_fp = _fingerprint(
            "batch-commit",
            {
                "items": [
                    {"target_path": r[1], "base_sha256": r[2].base_sha256, "patch_hash": r[3]}
                    for r in resolved
                ],
                "run_id": payload.run_id,
            },
        )
        batch_idem_lock = f"idem:batch-commit:{payload.idempotency_key}"
        with _file_lock(cfg, batch_idem_lock):
            existing_batch, batch_id_path = _check_idempotency(
                cfg,
                operation="batch-commit",
                idempotency_key=payload.idempotency_key,
                fingerprint=batch_fp,
            )
            if existing_batch is not None:
                return {
                    "status": "committed",
                    "idempotent_replay": True,
                    "commits": existing_batch.get("commits", []),
                }

            # Acquire locks in sorted order to prevent deadlocks
            sorted_resolved = sorted(resolved, key=lambda r: r[1])
            lock_names = [f"path:{r[1]}" for r in sorted_resolved]
            commit_results: list[dict[str, Any]] = []
            rollback_stack: list[tuple[Path, str, str]] = []  # (target, rel_path, previous_content)

            # Lock acquisition context — nested ordered locks
            lock_stack: list[contextlib.AbstractContextManager[None]] = []
            try:
                for lock_name in lock_names:
                    cm = _file_lock(cfg, lock_name)
                    cm.__enter__()
                    lock_stack.append(cm)

                # Re-validate fencing under lock to close the race window
                _validate_fencing(
                    cfg,
                    lease_id=payload.lease_id,
                    fencing_token=payload.fencing_token,
                    target_paths=all_rel_paths,
                )

                # Phase 1: validate all items (previews, triage, sha checks)
                for target, rel_path, item, patch_hash in sorted_resolved:
                    preview_record = _load_json(_preview_path(cfg, rel_path, item.base_sha256, patch_hash))
                    if not preview_record:
                        raise HTTPException(status_code=409, detail=f"PREVIEW_REQUIRED:{rel_path}")

                    # Find matching triage_record_id — supports both exact file
                    # match and directory-scope triage (e.g. target_path="app/"
                    # covers all files under app/).
                    triage_record_id: str | None = None
                    for trid in (payload.triage_record_ids or []):
                        try:
                            trec = _load_json(_triage_record_path(cfg, trid))
                            if not trec:
                                continue
                            triage_target = _normalize_rel_path(
                                str(trec.get("relative_target_path") or trec.get("target_path") or "")
                            )
                            # Exact file match
                            if triage_target == rel_path:
                                triage_record_id = trid
                                break
                            # Directory-scope match: triage covers a prefix
                            triage_prefix = triage_target.rstrip("/")
                            if triage_prefix and rel_path.startswith(f"{triage_prefix}/"):
                                triage_record_id = trid
                                break
                        except HTTPException:
                            continue

                    _require_allowing_triage(
                        cfg,
                        triage_record_id=triage_record_id,
                        relative_path=rel_path,
                        base_sha256=item.base_sha256,
                        patch_hash=patch_hash,
                    )

                    previous_content = _read_file_content(target)
                    previous_sha = _sha256_text(previous_content)
                    if previous_sha != item.base_sha256:
                        raise HTTPException(status_code=409, detail=f"BASE_SHA_MISMATCH:{rel_path}")

                # Phase 2: write all files (with compensation log)
                batch_progress_path: Path | None = None
                for target, rel_path, item, patch_hash in sorted_resolved:
                    previous_content = _read_file_content(target)
                    rollback_stack.append((target, rel_path, previous_content))

                    target.parent.mkdir(parents=True, exist_ok=True)
                    _atomic_write_text(target, item.patch_text)
                    new_sha = _sha256_text(item.patch_text)

                    commit_id = str(uuid4())
                    item_key = f"{payload.idempotency_key}:{rel_path}"
                    record = {
                        "commit_id": commit_id,
                        "operation": "commit",
                        "target_path": str(target),
                        "relative_path": rel_path,
                        "base_sha256": _sha256_text(previous_content),
                        "new_sha256": new_sha,
                        "patch_hash": patch_hash,
                        "idempotency_key": item_key,
                        "request_id": payload.request_id,
                        "run_id": payload.run_id,
                        "triage_record_id": None,
                        "actor": actor_payload,
                        "created_at": _utc_now_iso(),
                        "rolled_back_at": None,
                        "rollback_commit_id": None,
                        "rollback_of": None,
                        "previous_content_b64": base64.b64encode(
                            previous_content.encode("utf-8")
                        ).decode("ascii"),
                        "batch_idempotency_key": payload.idempotency_key,
                    }
                    _atomic_write_json(_audit_commit_path(cfg, commit_id), record)
                    _append_audit_event(cfg, {
                        "timestamp": _utc_now_iso(),
                        "event": "batch-commit",
                        "commit_id": commit_id,
                        "relative_path": rel_path,
                        "base_sha256": record["base_sha256"],
                        "new_sha256": new_sha,
                        "batch_key": payload.idempotency_key,
                    })
                    commit_results.append({
                        "commit_id": commit_id,
                        "relative_path": rel_path,
                        "new_sha256": new_sha,
                    })
                    # Update compensation log after each successful file write
                    batch_progress_path = _write_batch_progress(
                        cfg, payload.idempotency_key, commit_results,
                    )

            except Exception:
                # Rollback all written files on any failure.
                # Compensation log (batch_progress_path) is intentionally
                # preserved so recovery can identify partially-written state.
                for rb_target, _, rb_content in reversed(rollback_stack):
                    with contextlib.suppress(Exception):
                        _atomic_write_text(rb_target, rb_content)
                raise
            finally:
                # Release locks in reverse order
                for cm in reversed(lock_stack):
                    with contextlib.suppress(Exception):
                        cm.__exit__(None, None, None)

            # Save batch idempotency
            batch_record = {
                "commit_id": payload.idempotency_key,
                "operation": "batch-commit",
                "commits": commit_results,
                "created_at": _utc_now_iso(),
            }
            _atomic_write_json(
                _audit_commit_path(cfg, f"batch-{_sha256_text(payload.idempotency_key)[:16]}"),
                batch_record,
            )
            _save_idempotency(
                path=batch_id_path,
                idempotency_key=payload.idempotency_key,
                operation="batch-commit",
                fingerprint=batch_fp,
                commit_id=f"batch-{_sha256_text(payload.idempotency_key)[:16]}",
            )

            # Batch fully committed — clean up compensation log
            if batch_progress_path is not None:
                _remove_journal(batch_progress_path)

            return {
                "status": "committed",
                "idempotent_replay": False,
                "commits": commit_results,
            }

    @app.post("/v1/batch-rollback")
    def batch_rollback(payload: BatchRollbackRequest, request: Request) -> dict[str, Any]:
        """Roll back multiple commits in reverse order."""
        _require_auth(cfg, request)
        actor_payload = payload.actor.model_dump() if payload.actor else {}

        # Validate fencing for all rollback target paths
        rollback_rel_paths: list[str] = []
        for cid in payload.commit_ids:
            try:
                orig = _load_commit_or_404(cfg, cid)
                rollback_rel_paths.append(_normalize_rel_path(str(orig["relative_path"])))
            except HTTPException:
                pass  # Will be handled per-item below
        if rollback_rel_paths:
            _validate_fencing(
                cfg,
                lease_id=payload.lease_id,
                fencing_token=payload.fencing_token,
                target_paths=rollback_rel_paths,
            )

        results: list[dict[str, Any]] = []

        for commit_id in reversed(payload.commit_ids):
            try:
                original = _load_commit_or_404(cfg, commit_id)
                if original.get("operation") != "commit" or original.get("rollback_of"):
                    results.append({"commit_id": commit_id, "status": "skipped", "reason": "INVALID_TARGET"})
                    continue
                if original.get("rolled_back_at"):
                    results.append({"commit_id": commit_id, "status": "skipped", "reason": "ALREADY_ROLLED_BACK"})
                    continue

                target = Path(str(original["target_path"])).resolve()
                rel_path = _normalize_rel_path(str(original["relative_path"]))
                _resolve_allowed_target(cfg, str(target))

                path_lock_name = f"path:{rel_path}"
                with _file_lock(cfg, path_lock_name):
                    current_content = _read_file_content(target)
                    current_sha = _sha256_text(current_content)
                    expected_sha = str(original["new_sha256"])
                    if current_sha != expected_sha:
                        results.append({
                            "commit_id": commit_id,
                            "status": "failed",
                            "reason": "TARGET_CHANGED",
                        })
                        continue

                    previous_content_b64 = str(original.get("previous_content_b64") or "")
                    restored_content = base64.b64decode(
                        previous_content_b64.encode("ascii")
                    ).decode("utf-8")

                    _atomic_write_text(target, restored_content)
                    rollback_sha = _sha256_text(restored_content)
                    rollback_id = str(uuid4())
                    rollback_record = {
                        "commit_id": rollback_id,
                        "operation": "rollback",
                        "target_path": str(target),
                        "relative_path": rel_path,
                        "base_sha256": expected_sha,
                        "new_sha256": rollback_sha,
                        "patch_hash": _sha256_text(restored_content),
                        "idempotency_key": f"{payload.idempotency_key}:{commit_id}",
                        "request_id": payload.request_id,
                        "run_id": payload.run_id,
                        "actor": actor_payload,
                        "created_at": _utc_now_iso(),
                        "rolled_back_at": None,
                        "rollback_commit_id": None,
                        "rollback_of": commit_id,
                        "previous_content_b64": base64.b64encode(
                            current_content.encode("utf-8")
                        ).decode("ascii"),
                    }
                    _atomic_write_json(_audit_commit_path(cfg, rollback_id), rollback_record)
                    original["rolled_back_at"] = _utc_now_iso()
                    original["rollback_commit_id"] = rollback_id
                    _atomic_write_json(_audit_commit_path(cfg, commit_id), original)
                    _append_audit_event(cfg, {
                        "timestamp": _utc_now_iso(),
                        "event": "batch-rollback",
                        "rollback_commit_id": rollback_id,
                        "rollback_of": commit_id,
                        "relative_path": rel_path,
                        "batch_key": payload.idempotency_key,
                    })
                    results.append({
                        "commit_id": rollback_id,
                        "rollback_of": commit_id,
                        "status": "rolled_back",
                    })

            except HTTPException:
                raise
            except Exception as exc:
                results.append({
                    "commit_id": commit_id,
                    "status": "failed",
                    "reason": str(exc)[:200],
                })

        return {"status": "rolled_back", "results": results}

    # -- lease management (for Kestra / external callers) --------------------

    @app.post("/v1/lease/claim")
    def lease_claim(payload: LeaseClaimRequest, request: Request) -> dict[str, Any]:
        _require_auth(cfg, request)
        from app.services.writeback_coordination import WritebackCoordination

        state_path = cfg.repo_root / "runtime" / "writeback_coordination" / "state.json"
        coord = WritebackCoordination(state_path)
        lease = coord.claim(
            payload.round_id,
            payload.target_paths,
            lease_seconds=payload.lease_seconds,
        )
        return {
            "lease_id": lease.lease_id,
            "fencing_token": lease.fencing_token,
            "lease_until": lease.lease_until,
        }

    @app.post("/v1/lease/release")
    def lease_release(payload: LeaseReleaseRequest, request: Request) -> dict[str, Any]:
        _require_auth(cfg, request)
        from app.services.writeback_coordination import WritebackCoordination

        state_path = cfg.repo_root / "runtime" / "writeback_coordination" / "state.json"
        coord = WritebackCoordination(state_path)
        coord.release(
            lease={"lease_id": payload.lease_id},
            reason=payload.reason,
        )
        return {"status": "released", "lease_id": payload.lease_id}

    return app


app = create_app()
