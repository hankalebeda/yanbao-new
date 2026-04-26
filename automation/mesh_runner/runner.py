from __future__ import annotations

import json
import os
import re
import subprocess
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

from automation.mesh_runner.manifest_builder import (
    DEFAULT_BENCHMARK_LABEL,
    DEFAULT_READONLY_MAX_WORKERS,
    PROTOCOL_ROOT,
    build_readonly_manifest,
    repo_root,
)
from app.core.config import settings

RunExecutor = Callable[[Path, Any], Any]

ALLOWED_ISSUE_STATUS = {"still_alive", "stale", "narrow_required"}
ALLOWED_HANDLING_PATH = {
    "fix_code",
    "fix_then_rebuild",
    "manual_verify",
    "external_integration",
    "freeze_or_isolate",
    "execution_and_monitoring",
}
ALLOWED_RISK_LEVEL = {"P0", "P1", "P2", "P3"}
TERMINAL_RUN_STATUSES = {"completed", "finished", "failed"}
TERMINAL_SHARD_STATUSES = {"COMPLETED", "FAILED", "TIMEOUT", "SKIPPED"}
STALE_RUN_RECLAIM_ERROR = "STALE_RUN_RECLAIMED_AFTER_SERVICE_RESTART"
READONLY_PROVIDER_SHARDS = ("ro-a", "ro-b", "ro-c", "ro-d")
READONLY_PROVIDER_LANES = {
    "ro-a": "codex-ro-a",
    "ro-b": "codex-ro-b",
    "ro-c": "codex-ro-c",
    "ro-d": "codex-ro-d",
}

FAMILY_DEFAULTS: dict[str, dict[str, Any]] = {
    "truth-lineage": {
        "risk_level": "P1",
        "issue_status": "still_alive",
        "handling_path": "fix_code",
        "timeout_seconds": 900,
        "recommended_action": "Patch data lineage or evidence linking code, then rerun readonly mesh for verification.",
    },
    "runtime-anchor": {
        "risk_level": "P1",
        "issue_status": "still_alive",
        "handling_path": "fix_then_rebuild",
        "timeout_seconds": 900,
        "recommended_action": "Fix runtime-anchor drift and rebuild impacted snapshots before promote evaluation.",
    },
    "fr07-rebuild": {
        "risk_level": "P1",
        "issue_status": "still_alive",
        "handling_path": "fix_then_rebuild",
        "timeout_seconds": 900,
        "recommended_action": "Fix FR-07 baseline/rebuild defects, then run rebuild and verify runtime anchors.",
    },
    "fr06-failure-semantics": {
        "risk_level": "P1",
        "issue_status": "still_alive",
        "handling_path": "fix_code",
        "timeout_seconds": 900,
        "recommended_action": "Align FR-06 failure semantics and timeout contracts before any promote activity.",
    },
    "payment-auth-governance": {
        "risk_level": "P1",
        "issue_status": "narrow_required",
        "handling_path": "manual_verify",
        "recommended_action": "Reconcile payment, OAuth, and membership truth before closing the governance residual.",
    },
    "internal-contracts": {
        "risk_level": "P1",
        "issue_status": "narrow_required",
        "handling_path": "fix_code",
        "timeout_seconds": 900,
        "recommended_action": "Fix internal orchestration contract drift and validate the affected control-plane endpoints.",
    },
    "shared-artifacts": {
        "risk_level": "P1",
        "issue_status": "narrow_required",
        "handling_path": "execution_and_monitoring",
        "recommended_action": "Keep promote blocked until runtime gates and shared artifact evidence are aligned.",
    },
    "issue-registry": {
        "risk_level": "P1",
        "issue_status": "narrow_required",
        "handling_path": "execution_and_monitoring",
        "recommended_action": "Close ISSUE-REGISTRY explanation gaps before claiming governance convergence.",
    },
    "repo-governance": {
        "risk_level": "P2",
        "issue_status": "narrow_required",
        "handling_path": "execution_and_monitoring",
        "recommended_action": "Reduce legacy and archive governance drift without masking runtime root causes.",
    },
    "external-integration": {
        "risk_level": "P2",
        "issue_status": "narrow_required",
        "handling_path": "external_integration",
        "recommended_action": "Track and narrow the remaining external integration residue with explicit evidence.",
    },
    "display-bridge": {
        "risk_level": "P2",
        "issue_status": "narrow_required",
        "handling_path": "manual_verify",
        "recommended_action": "Align dashboard and admin projections with the current truth-layer contracts.",
    },
    "execution-order": {
        "risk_level": "P2",
        "issue_status": "narrow_required",
        "handling_path": "execution_and_monitoring",
        "recommended_action": "Reorder execution by root cause priority and continue monitoring until recovery closes.",
    },
}

TASK_ROLE_BY_ID: dict[str, str] = {
    "truth-lineage": "数据工程师",
    "runtime-anchor": "测试与质量",
    "fr07-rebuild": "数据工程师",
    "fr06-failure-semantics": "研报生成工程师",
    "payment-auth-governance": "商业与鉴权",
    "internal-contracts": "测试与质量",
    "shared-artifacts": "测试与质量",
    "issue-registry": "测试与质量",
    "repo-governance": "测试与质量",
    "external-integration": "商业与鉴权",
    "display-bridge": "前端与体验",
    "execution-order": "测试与质量",
}


def _catalog_records() -> list[dict[str, Any]]:
    path = PROTOCOL_ROOT / "protocol" / "family_catalog.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return [item for item in payload if isinstance(item, dict)]


def _catalog_record_by_family() -> dict[str, dict[str, Any]]:
    return {
        str(item.get("family_id") or "").strip(): item
        for item in _catalog_records()
        if str(item.get("family_id") or "").strip()
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            temp_path = Path(handle.name)
        os.replace(temp_path, path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))


def _read_last_message(task: dict[str, Any]) -> str:
    attempts = task.get("attempts") or []
    for attempt in reversed(attempts):
        candidate = Path(str(attempt.get("last_message_path") or ""))
        if candidate.exists():
            return candidate.read_text(encoding="utf-8", errors="replace").strip()
    return ""


def _git_status_snapshot(root: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), "status", "--short"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.returncode != 0:
        return f"git status failed: {completed.stderr.strip()}"
    return completed.stdout


def _provider_shard(provider_name: str) -> str | None:
    normalized = str(provider_name or "").strip().lower()
    if normalized.endswith("-stable"):
        return "stable"
    for shard in READONLY_PROVIDER_SHARDS:
        if normalized.endswith(f"-{shard}") or normalized == shard:
            return shard
    return None


def _provider_lane(provider_name: str) -> str | None:
    shard = _provider_shard(provider_name)
    if shard == "stable":
        return "codex-stable"
    if shard in READONLY_PROVIDER_LANES:
        return READONLY_PROVIDER_LANES[shard]
    return None


def _configured_model_for_provider(repo_root: Path, provider_name: str) -> str | None:
    clean = str(provider_name or "").strip()
    if not clean:
        return None
    config_path = repo_root / "ai-api" / "codex" / clean / "config.toml"
    if not config_path.exists():
        return None
    try:
        payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    model = str(payload.get("model") or "").strip()
    return model or None


def _route_metadata(repo_root: Path, provider_allowlist: list[str], selected_provider: str | None) -> dict[str, Any]:
    routed = [str(item).strip() for item in (provider_allowlist or []) if str(item).strip()]
    selected = str(selected_provider or "").strip()
    route_provider = selected or (routed[0] if routed else "")
    lane = _provider_lane(route_provider)
    shard = _provider_shard(route_provider)
    fallback_hop = routed.index(selected) if selected and selected in routed else None
    resolved_model = _configured_model_for_provider(repo_root, selected) if selected else None
    return {
        "lane": lane,
        "shard": shard,
        "provider_allowlist": routed,
        "fallback_hop": fallback_hop,
        "resolved_model": resolved_model,
    }


def _summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        f"# Issue Mesh Summary: {summary.get('run_id')}",
        "",
        f"- success: `{summary.get('success')}`",
        f"- task_count: `{summary.get('task_count')}`",
        "",
        "## Tasks",
    ]
    for task in summary.get("tasks") or []:
        lines.append(f"### {task.get('task_id')}")
        lines.append(f"- goal: {task.get('goal')}")
        lines.append(f"- success: `{task.get('success')}`")
        lines.append(f"- selected_provider: `{task.get('selected_provider')}`")
        if task.get("lane"):
            lines.append(f"- lane: `{task.get('lane')}`")
        if task.get("shard"):
            lines.append(f"- shard: `{task.get('shard')}`")
        if task.get("provider_allowlist"):
            lines.append(f"- provider_allowlist: `{task.get('provider_allowlist')}`")
        if task.get("fallback_hop") is not None:
            lines.append(f"- fallback_hop: `{task.get('fallback_hop')}`")
        if task.get("resolved_model"):
            lines.append(f"- resolved_model: `{task.get('resolved_model')}`")
        last_message = _read_last_message(task)
        if last_message:
            lines.append("")
            lines.append("```text")
            lines.append(last_message)
            lines.append("```")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _safe_json_loads(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return None


def _parse_kv_lines(text: str) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower().replace(" ", "_")
        value = value.strip().strip("` ")
        if key in {
            "issue_key",
            "title",
            "risk_level",
            "issue_status",
            "handling_path",
            "recommended_action",
            "evidence_refs",
            "ssot_refs",
        }:
            payload[key] = value
    return payload


def _extract_json_payloads(text: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []

    whole = _safe_json_loads(text)
    if isinstance(whole, dict):
        payloads.append(whole)
    elif isinstance(whole, list):
        payloads.extend(item for item in whole if isinstance(item, dict))

    for match in re.finditer(r"```json\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL):
        parsed = _safe_json_loads(match.group(1).strip())
        if isinstance(parsed, dict):
            payloads.append(parsed)
        elif isinstance(parsed, list):
            payloads.extend(item for item in parsed if isinstance(item, dict))

    return payloads


def _first_finding_dict(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("findings", "results", "issues"):
        candidates = payload.get(key)
        if isinstance(candidates, list):
            for item in candidates:
                if isinstance(item, dict):
                    return item
    return payload


def _normalize_ref_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return []
        parsed = _safe_json_loads(trimmed)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
        return [chunk.strip() for chunk in trimmed.split(",") if chunk.strip()]
    return []


def _normalize_risk_level(value: Any, default: str) -> str:
    candidate = str(value or "").upper().strip()
    return candidate if candidate in ALLOWED_RISK_LEVEL else default


def _normalize_issue_status(value: Any, default: str) -> str:
    candidate = str(value or "").lower().strip()
    return candidate if candidate in ALLOWED_ISSUE_STATUS else default


def _normalize_handling_path(value: Any, default: str) -> str:
    candidate = str(value or "").lower().strip()
    return candidate if candidate in ALLOWED_HANDLING_PATH else default


def _family_group_from_task_id(task_id: str) -> tuple[str, str]:
    if "--" not in task_id:
        return task_id, ""
    family_id, group_id = task_id.split("--", 1)
    return family_id.strip(), group_id.strip()


def _catalog_ssot_refs() -> dict[tuple[str, str], list[str]]:
    mapping: dict[tuple[str, str], list[str]] = {}
    for family in _catalog_records():
        family_id = str(family.get("family_id") or "").strip()
        ssot_refs = [str(item).strip() for item in (family.get("ssot_refs") or []) if str(item).strip()]
        if family_id:
            mapping[(family_id, "")] = ssot_refs
    return mapping


def _dedupe_refs(items: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _extract_structured_finding(last_message: str) -> dict[str, Any]:
    for payload in _extract_json_payloads(last_message):
        finding = _first_finding_dict(payload)
        if any(
            key in finding
            for key in (
                "issue_key",
                "issue_id",
                "risk_level",
                "issue_status",
                "handling_path",
                "recommended_action",
                "suggested_action",
                "contract_gap",
                "ssot_refs",
            )
        ):
            return finding

    kv_payload = _parse_kv_lines(last_message)
    return kv_payload if kv_payload else {}


def _defaults_for_family(family_id: str) -> dict[str, Any]:
    return dict(
        FAMILY_DEFAULTS.get(
            family_id,
            {
                "risk_level": "P2",
                "issue_status": "narrow_required",
                "handling_path": "execution_and_monitoring",
                "recommended_action": "Review readonly audit output and decide next action based on current gates.",
            },
        )
    )


def _findings_bundle(summary: dict[str, Any]) -> dict[str, Any]:
    group_ssot_index = _catalog_ssot_refs()
    findings: list[dict[str, Any]] = []

    for task in summary.get("tasks") or []:
        task_id = str(task.get("task_id") or "")
        family_id, group_id = _family_group_from_task_id(task_id)
        defaults = _defaults_for_family(family_id)
        attempts = [item for item in (task.get("attempts") or []) if isinstance(item, dict)]

        attempt_evidence_refs = [
            str(attempt.get("last_message_path") or "").strip()
            for attempt in attempts
            if str(attempt.get("last_message_path") or "").strip()
        ]
        attempt_evidence_refs.extend(
            str(attempt.get("stdout_path") or "").strip()
            for attempt in attempts
            if str(attempt.get("stdout_path") or "").strip()
        )
        attempt_evidence_refs.extend(
            str(attempt.get("stderr_path") or "").strip()
            for attempt in attempts
            if str(attempt.get("stderr_path") or "").strip()
        )
        last_message = _read_last_message(task)
        structured = _extract_structured_finding(last_message)
        has_success_attempt = any(bool(attempt.get("ok")) for attempt in attempts) or bool(task.get("success"))
        has_structured_finding = bool(structured)
        has_message_text = bool(last_message.strip())

        structured_evidence_refs = _normalize_ref_list(structured.get("evidence_refs") or structured.get("runtime_evidence"))
        ssot_refs = _normalize_ref_list(structured.get("ssot_refs"))
        if not ssot_refs:
            ssot_refs = list(group_ssot_index.get((family_id, group_id), []))

        issue_key = str(structured.get("issue_key") or structured.get("issue_id") or task_id).strip() or task_id
        title = str(structured.get("title") or structured.get("contract_gap") or task.get("goal") or task_id).strip() or task_id
        non_actionable_output = (not has_success_attempt) or (not has_structured_finding) or (not has_message_text)
        if non_actionable_output:
            issue_status = "narrow_required"
            handling_path = "execution_and_monitoring"
            recommended_action = (
                "Task output is non-actionable (provider failure, empty message, or missing structured finding). "
                "Fix execution/runtime connectivity first, then rerun issue-mesh before code-fix attempts."
            )
        else:
            issue_status = _normalize_issue_status(structured.get("issue_status"), defaults["issue_status"])
            handling_path = _normalize_handling_path(structured.get("handling_path"), defaults["handling_path"])
            recommended_action = str(structured.get("recommended_action") or structured.get("suggested_action") or defaults["recommended_action"]).strip()

        finding = {
            "issue_key": issue_key,
            "title": title,
            "risk_level": _normalize_risk_level(structured.get("risk_level"), defaults["risk_level"]),
            "issue_status": issue_status,
            "handling_path": handling_path,
            "recommended_action": recommended_action,
            "evidence_refs": _dedupe_refs(structured_evidence_refs + attempt_evidence_refs),
            "ssot_refs": _dedupe_refs(ssot_refs),
            "source_task_id": task_id,
            "source_run_id": str(summary.get("run_id") or ""),
        }
        findings.append(finding)

    return {
        "run_id": str(summary.get("run_id") or ""),
        "generated_at": _now_iso(),
        "finding_count": len(findings),
        "findings": findings,
    }


def _normalize_summary(summary: Any, *, run_id: str, manifest_path: Path, output_dir: Path) -> dict[str, Any]:
    payload = asdict(summary) if hasattr(summary, "__dataclass_fields__") else dict(summary)
    payload.setdefault("run_id", run_id)
    payload.setdefault("manifest_path", str(manifest_path))
    payload.setdefault("output_dir", str(output_dir))
    payload.setdefault("task_count", len(payload.get("tasks") or []))
    payload.setdefault("success", True)
    payload.setdefault("started_at", _now_iso())
    payload.setdefault("finished_at", _now_iso())
    return payload


def _annotate_summary_routes(summary: dict[str, Any], specs: list[dict[str, Any]], repo_root: Path) -> None:
    spec_by_task_id = {str(spec.get("task_id") or ""): spec for spec in specs}
    for task in summary.get("tasks") or []:
        if not isinstance(task, dict):
            continue
        spec = spec_by_task_id.get(str(task.get("task_id") or ""))
        if spec is None:
            continue
        provider_allowlist = [str(item).strip() for item in (spec.get("provider_allowlist") or []) if str(item).strip()]
        if not provider_allowlist and not _provider_shard(str(task.get("selected_provider") or "")):
            continue
        metadata = _route_metadata(
            repo_root,
            provider_allowlist,
            str(task.get("selected_provider") or "").strip() or None,
        )
        task["provider_allowlist"] = metadata["provider_allowlist"]
        if metadata["lane"]:
            task["lane"] = metadata["lane"]
        if metadata["shard"]:
            task["shard"] = metadata["shard"]
        task["fallback_hop"] = metadata["fallback_hop"]
        task["resolved_model"] = metadata["resolved_model"]


RUN_ID_PATTERN = re.compile(r"^issue-mesh-(\d{8})-(\d{3})$")


def _next_run_id(runtime_root: Path) -> str:
    stamp = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d")
    highest = 0
    candidate_roots = [runtime_root]
    legacy_root = runtime_root / "runs"
    if legacy_root.exists():
        candidate_roots.append(legacy_root)
    for root in candidate_roots:
        if not root.exists():
            continue
        for path in root.iterdir():
            if not path.is_dir():
                continue
            match = RUN_ID_PATTERN.fullmatch(path.name)
            if match and match.group(1) == stamp:
                highest = max(highest, int(match.group(2)))
    return f"issue-mesh-{stamp}-{highest + 1:03d}"


def _format_run_id(run_id: str | None, runtime_root: Path) -> str:
    if run_id:
        candidate = str(run_id).strip()
        if not RUN_ID_PATTERN.fullmatch(candidate):
            raise ValueError("INVALID_RUN_ID")
        return candidate
    return _next_run_id(runtime_root)


def _task_result_status(task: dict[str, Any]) -> str:
    if bool(task.get("success")):
        return "COMPLETED"
    attempts = task.get("attempts") or []
    statuses = {str(item.get("status") or "").lower() for item in attempts if isinstance(item, dict)}
    if "timeout" in statuses:
        return "TIMEOUT"
    if not attempts:
        return "SKIPPED"
    return "FAILED"


def _task_shadow_markdown(task: dict[str, Any]) -> str:
    last_message = _read_last_message(task)
    lines = [
        f"# Issue Mesh Shard: {task.get('task_id')}",
        "",
        f"- goal: {task.get('goal')}",
        f"- success: `{task.get('success')}`",
        f"- selected_provider: `{task.get('selected_provider')}`",
    ]
    if task.get("lane"):
        lines.append(f"- lane: `{task.get('lane')}`")
    if task.get("shard"):
        lines.append(f"- shard: `{task.get('shard')}`")
    if task.get("provider_allowlist"):
        lines.append(f"- provider_allowlist: `{task.get('provider_allowlist')}`")
    if task.get("fallback_hop") is not None:
        lines.append(f"- fallback_hop: `{task.get('fallback_hop')}`")
    if task.get("resolved_model"):
        lines.append(f"- resolved_model: `{task.get('resolved_model')}`")
    if last_message:
        lines.extend(["", "```text", last_message, "```"])
    return "\n".join(lines).rstrip() + "\n"


@dataclass(frozen=True)
class MeshRunnerConfig:
    repo_root: Path
    runtime_root: Path
    auth_token: str
    canonical_provider: str
    readonly_max_workers: int
    readonly_lane: str = ""
    stable_lane: str = ""
    readonly_provider_allowlist: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "MeshRunnerConfig":
        root = Path(os.environ.get("ISSUE_MESH_REPO_ROOT") or repo_root()).resolve()
        ro_allowlist_raw = str(os.environ.get("CODEX_READONLY_PROVIDER_ALLOWLIST") or "").strip()
        ro_allowlist = [p.strip() for p in ro_allowlist_raw.split(",") if p.strip()] if ro_allowlist_raw else []
        return cls(
            repo_root=root,
            runtime_root=(root / "runtime" / "issue_mesh").resolve(),
            auth_token=str(os.environ.get("MESH_RUNNER_AUTH_TOKEN") or "").strip(),
            canonical_provider=str(
                os.environ.get("CODEX_CANONICAL_PROVIDER") or settings.codex_canonical_provider or "newapi-192.168.232.141-3000"
            ).strip(),
            readonly_max_workers=max(
                1,
                int(os.environ.get("ISSUE_MESH_READONLY_MAX_WORKERS") or DEFAULT_READONLY_MAX_WORKERS),
            ),
            readonly_lane=str(os.environ.get("CODEX_READONLY_LANE") or "").strip(),
            stable_lane=str(os.environ.get("CODEX_STABLE_LANE") or "").strip(),
            readonly_provider_allowlist=ro_allowlist,
        )


class MeshRunnerService:
    def __init__(self, config: MeshRunnerConfig, execute_manifest_fn: RunExecutor | None = None) -> None:
        self.config = config
        self._execute_manifest_fn = execute_manifest_fn or self._default_execute_manifest
        self._lock = threading.Lock()
        self._completion_events: dict[str, threading.Event] = {}
        self.config.runtime_root.mkdir(parents=True, exist_ok=True)
        self._resume_incomplete_runs()

    @staticmethod
    def _default_execute_manifest(root: Path, manifest: Any) -> Any:
        import sys
        _repo = str(Path(__file__).resolve().parents[2])
        if _repo not in sys.path:
            sys.path.insert(0, _repo)
        from codex.mesh import execute_manifest
        return execute_manifest(root, manifest)

    def _run_root(self, run_id: str) -> Path:
        return self.config.runtime_root / run_id

    def _status_path(self, run_id: str) -> Path:
        return self._run_root(run_id) / "status.json"

    def _manifest_path(self, run_id: str) -> Path:
        return self._run_root(run_id) / "manifest.json"

    def _summary_path(self, run_id: str) -> Path:
        return self._run_root(run_id) / "summary.json"

    def _bundle_path(self, run_id: str) -> Path:
        return self._run_root(run_id) / "bundle.json"

    def _legacy_bundle_path(self, run_id: str) -> Path:
        return self._run_root(run_id) / "findings_bundle.json"

    def _summary_md_path(self, run_id: str) -> Path:
        return self._run_root(run_id) / "summary.md"

    def _shard_root(self, run_id: str, shard_id: str) -> Path:
        return self._run_root(run_id) / shard_id

    def _task_spec_path(self, run_id: str, shard_id: str) -> Path:
        return self._shard_root(run_id, shard_id) / "task_spec.json"

    def _result_path(self, run_id: str, shard_id: str) -> Path:
        return self._shard_root(run_id, shard_id) / "result.json"

    def _shadow_root(self, run_id: str) -> Path:
        return self.config.repo_root / "docs" / "_temp" / "issue_mesh_shadow" / run_id

    def _shadow_path(self, run_id: str, shard_id: str) -> Path:
        return self._shadow_root(run_id) / f"{shard_id}.md"

    @staticmethod
    def _shard_id(index: int) -> str:
        return f"shard_{index:02d}"

    def _materialize_task_specs(self, run_id: str, manifest: Any) -> list[dict[str, Any]]:
        tasks = list(getattr(manifest, "tasks", []) or [])
        catalog_by_family = _catalog_record_by_family()
        specs: list[dict[str, Any]] = []
        for index, task in enumerate(tasks, start=1):
            task_id = str(getattr(task, "task_id", ""))
            family_id = task_id
            shard_id = str(catalog_by_family.get(family_id, {}).get("shard_id") or self._shard_id(index))
            provider_allowlist = [str(item).strip() for item in (getattr(task, "provider_allowlist", []) or []) if str(item).strip()]
            primary_provider = provider_allowlist[0] if provider_allowlist else ""
            lane = _provider_lane(primary_provider)
            route_shard = _provider_shard(primary_provider)
            role = TASK_ROLE_BY_ID.get(task_id, "测试与质量")
            prompt_template_id = str(
                catalog_by_family.get(family_id, {}).get("prompt_template_id")
                or f"family_{index:02d}_{family_id.replace('-', '_')}_v1"
            )
            spec = {
                "run_id": run_id,
                "shard_id": shard_id,
                "family_id": family_id or task_id,
                "role": role,
                "read_scope": list(getattr(task, "read_scope", []) or []),
                "write_scope": [],
                "prompt_template_id": prompt_template_id,
                "timeout_seconds": int(getattr(task, "timeout_seconds", 600) or 600),
                "output_path": self._result_path(run_id, shard_id).relative_to(self.config.repo_root).as_posix(),
                "task_id": task_id,
                "goal": str(getattr(task, "goal", "")),
            }
            if provider_allowlist:
                spec["provider_allowlist"] = list(provider_allowlist)
            if lane:
                spec["lane"] = lane
            if route_shard:
                spec["route_shard"] = route_shard
            specs.append(spec)
            task_spec_payload = {
                "run_id": run_id,
                "shard_id": shard_id,
                "family_id": spec["family_id"],
                "role": spec["role"],
                "read_scope": spec["read_scope"],
                "write_scope": spec["write_scope"],
                "prompt_template_id": spec["prompt_template_id"],
                "timeout_seconds": spec["timeout_seconds"],
                "output_path": spec["output_path"],
            }
            if provider_allowlist:
                task_spec_payload["provider_allowlist"] = list(provider_allowlist)
            if lane:
                task_spec_payload["lane"] = lane
            if route_shard:
                task_spec_payload["route_shard"] = route_shard
            _atomic_write_json(
                self._task_spec_path(run_id, shard_id),
                task_spec_payload,
            )
            _atomic_write_json(
                self._result_path(run_id, shard_id),
                {
                    "run_id": run_id,
                    "shard_id": shard_id,
                    "status": "PENDING",
                    "findings": [],
                    "shadow_path": self._shadow_path(run_id, shard_id).relative_to(self.config.repo_root).as_posix(),
                    "error": None,
                    "started_at": None,
                    "finished_at": None,
                },
            )
        return specs

    def _mark_results_running(self, run_id: str, specs: list[dict[str, Any]]) -> None:
        now = _now_iso()
        for spec in specs:
            payload = {
                "run_id": run_id,
                "shard_id": spec["shard_id"],
                "status": "RUNNING",
                "findings": [],
                "shadow_path": self._shadow_path(run_id, str(spec["shard_id"])).relative_to(self.config.repo_root).as_posix(),
                "error": None,
                "started_at": now,
                "finished_at": None,
            }
            if spec.get("provider_allowlist"):
                payload["provider_allowlist"] = list(spec["provider_allowlist"])
            if spec.get("lane"):
                payload["lane"] = spec["lane"]
            if spec.get("route_shard"):
                payload["shard"] = spec["route_shard"]
            _atomic_write_json(
                self._result_path(run_id, str(spec["shard_id"])),
                payload,
            )

    def _write_task_results(self, run_id: str, summary: dict[str, Any], specs: list[dict[str, Any]]) -> None:
        spec_by_task_id = {str(spec["task_id"]): spec for spec in specs}
        shadow_root = self._shadow_root(run_id)
        shadow_root.mkdir(parents=True, exist_ok=True)
        finished_at = str(summary.get("finished_at") or _now_iso())
        task_entries = {str(task.get("task_id") or ""): task for task in (summary.get("tasks") or [])}
        for task_id, spec in spec_by_task_id.items():
            task = task_entries.get(task_id)
            shadow_path = self._shadow_path(run_id, str(spec["shard_id"]))
            if task is None:
                missing_payload = {
                    "run_id": run_id,
                    "shard_id": spec["shard_id"],
                    "status": "FAILED",
                    "findings": [],
                    "shadow_path": shadow_path.relative_to(self.config.repo_root).as_posix(),
                    "error": "TASK_RESULT_MISSING",
                    "started_at": None,
                    "finished_at": finished_at,
                }
                if spec.get("provider_allowlist"):
                    missing_payload["provider_allowlist"] = list(spec["provider_allowlist"])
                if spec.get("lane"):
                    missing_payload["lane"] = spec["lane"]
                if spec.get("route_shard"):
                    missing_payload["shard"] = spec["route_shard"]
                _atomic_write_json(
                    self._result_path(run_id, str(spec["shard_id"])),
                    missing_payload,
                )
                continue
            _atomic_write_text(shadow_path, _task_shadow_markdown(task))
            structured = _extract_structured_finding(_read_last_message(task))
            result_payload = {
                "run_id": run_id,
                "shard_id": spec["shard_id"],
                "status": _task_result_status(task),
                "findings": [structured] if structured else [],
                "shadow_path": shadow_path.relative_to(self.config.repo_root).as_posix(),
                "error": task.get("error"),
                "started_at": task.get("started_at"),
                "finished_at": task.get("finished_at") or finished_at,
            }
            for key in ("selected_provider", "provider_allowlist", "lane", "shard", "fallback_hop", "resolved_model"):
                if key in task:
                    result_payload[key] = task.get(key)
            _atomic_write_json(
                self._result_path(run_id, str(spec["shard_id"])),
                result_payload,
            )

    def _write_status(self, run_id: str, payload: dict[str, Any]) -> None:
        _atomic_write_json(self._status_path(run_id), payload)

    def _fail_incomplete_run(self, status_path: Path, payload: dict[str, Any], run_id: str, reclaimed_at: str) -> None:
        payload.update(
            {
                "run_id": run_id,
                "status": "failed",
                "finished_at": reclaimed_at,
                "summary_path": None,
                "bundle_path": None,
                "summary_markdown": None,
            }
        )
        if not str(payload.get("error") or "").strip():
            payload["error"] = STALE_RUN_RECLAIM_ERROR
        self._write_status(run_id, payload)

        for result_path in sorted(status_path.parent.glob("shard_*/result.json")):
            try:
                result_payload = json.loads(result_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            shard_status = str(result_payload.get("status") or "").strip().upper()
            if shard_status in TERMINAL_SHARD_STATUSES and result_payload.get("finished_at"):
                continue

            result_payload.update(
                {
                    "status": "FAILED",
                    "started_at": result_payload.get("started_at") or payload.get("started_at"),
                    "finished_at": reclaimed_at,
                }
            )
            if not str(result_payload.get("error") or "").strip():
                result_payload["error"] = STALE_RUN_RECLAIM_ERROR
            _atomic_write_json(result_path, result_payload)

    def _resume_incomplete_runs(self) -> None:
        reclaimed_at = _now_iso()
        for status_path in sorted(self.config.runtime_root.glob("*/status.json")):
            try:
                payload = json.loads(status_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            run_status = str(payload.get("status") or "").strip().lower()
            if run_status in TERMINAL_RUN_STATUSES:
                continue

            run_id = str(payload.get("run_id") or status_path.parent.name).strip() or status_path.parent.name
            manifest_path = Path(str(payload.get("manifest_path") or self._manifest_path(run_id)))
            try:
                from codex.mesh import load_manifest

                manifest = load_manifest(manifest_path)
                task_specs = self._materialize_task_specs(run_id, manifest)
            except Exception:
                self._fail_incomplete_run(status_path, payload, run_id, reclaimed_at)
                continue

            payload.update(
                {
                    "run_id": run_id,
                    "status": "queued",
                    "started_at": None,
                    "finished_at": None,
                    "summary_path": None,
                    "bundle_path": None,
                    "summary_markdown": None,
                    "error": None,
                    "task_specs": [
                        self._task_spec_path(run_id, str(spec["shard_id"])).relative_to(self.config.repo_root).as_posix()
                        for spec in task_specs
                    ],
                }
            )
            self._write_status(run_id, payload)
            self._completion_events[run_id] = threading.Event()
            thread = threading.Thread(target=self._execute_run, args=(run_id, manifest, task_specs), daemon=True)
            thread.start()

    def start_run(
        self,
        *,
        run_id: str | None,
        run_label: str | None,
        benchmark_label: str | None,
        max_workers: int | None,
        provider_allowlist: list[str] | None,
        audit_scope: str,
        shard_strategy: str,
        control_state_snapshot: str | None,
        audit_context: dict[str, Any] | None,
        wait_for_completion: bool,
        wait_timeout_seconds: int,
    ) -> dict[str, Any]:
        with self._lock:
            run_id = _format_run_id(run_id, self.config.runtime_root)
            run_root = self._run_root(run_id)
            if run_root.exists():
                raise ValueError("RUN_ID_ALREADY_EXISTS")
            run_root.mkdir(parents=True, exist_ok=False)
        audit_context_path = run_root / "audit_context.json"
        control_state_path = run_root / "control_state.json"
        git_status_path = run_root / "git_status.txt"
        explicit_allowlist = [p for p in (provider_allowlist or []) if str(p).strip()]
        env_allowlist = [p for p in (self.config.readonly_provider_allowlist or []) if str(p).strip()]
        if explicit_allowlist:
            provider_allowlist = list(explicit_allowlist)
        elif env_allowlist:
            provider_allowlist = list(env_allowlist)
        else:
            # When lane isolation is enabled (deploy scripts set CODEX_READONLY_LANE),
            # never silently fall back to a single canonical provider. This avoids
            # "looks concurrent but actually single failure domain" behavior.
            if self.config.readonly_lane:
                raise ValueError("READONLY_PROVIDER_ALLOWLIST_REQUIRED")
            provider_allowlist = [self.config.canonical_provider]
        normalized_audit_context = dict(audit_context or {})
        normalized_audit_context.setdefault("audit_scope", audit_scope)
        normalized_audit_context.setdefault("shard_strategy", shard_strategy)
        if control_state_snapshot:
            normalized_audit_context.setdefault("control_state_snapshot", control_state_snapshot)
        _atomic_write_json(audit_context_path, normalized_audit_context)
        _atomic_write_text(git_status_path, _git_status_snapshot(self.config.repo_root))
        _atomic_write_json(
            control_state_path,
            {
                "control_state": "Recovery-Rearm",
                "readonly_wave_max_workers": self.config.readonly_max_workers,
                "runtime_mutating_slots": 2,
            },
        )
        effective_workers = max_workers or self.config.readonly_max_workers
        workers_cap = int(os.environ.get("ISSUE_MESH_MAX_WORKERS_CAP") or 0)
        if workers_cap > 0:
            effective_workers = min(effective_workers, workers_cap)
        manifest = build_readonly_manifest(
            provider_allowlist=provider_allowlist,
            max_workers=effective_workers,
            benchmark_label=benchmark_label or DEFAULT_BENCHMARK_LABEL,
            audit_context=normalized_audit_context,
            extra_read_scope=[
                audit_context_path.relative_to(self.config.repo_root).as_posix(),
                control_state_path.relative_to(self.config.repo_root).as_posix(),
                git_status_path.relative_to(self.config.repo_root).as_posix(),
            ],
            audit_scope=audit_scope,
            shard_strategy=shard_strategy,
        )
        task_specs = self._materialize_task_specs(run_id, manifest)
        _atomic_write_json(self._manifest_path(run_id), asdict(manifest))
        self._write_status(
            run_id,
            {
                "run_id": run_id,
                "run_label": run_label or run_id,
                "status": "queued",
                "manifest_path": str(self._manifest_path(run_id)),
                "summary_path": None,
                "bundle_path": None,
                "output_dir": str(run_root),
                "audit_context_path": str(audit_context_path),
                "created_at": _now_iso(),
                "started_at": None,
                "finished_at": None,
                "summary_markdown": None,
                "task_specs": [
                    self._task_spec_path(run_id, str(spec["shard_id"])).relative_to(self.config.repo_root).as_posix()
                    for spec in task_specs
                ],
            },
        )
        self._completion_events[run_id] = threading.Event()
        thread = threading.Thread(target=self._execute_run, args=(run_id, manifest, task_specs), daemon=True)
        thread.start()
        if wait_for_completion:
            return self.get_run(
                run_id,
                wait_for_completion=True,
                wait_timeout_seconds=wait_timeout_seconds or 35 * 60,
            )
        return self.get_run(run_id)

    def _execute_run(self, run_id: str, manifest: Any, task_specs: list[dict[str, Any]]) -> None:
        status = self.get_run(run_id)
        status["status"] = "running"
        status["started_at"] = _now_iso()
        self._write_status(run_id, status)
        self._mark_results_running(run_id, task_specs)
        try:
            summary = self._execute_manifest_fn(self.config.repo_root, manifest)
            normalized = _normalize_summary(
                summary,
                run_id=run_id,
                manifest_path=self._manifest_path(run_id),
                output_dir=self._run_root(run_id),
            )
            _annotate_summary_routes(normalized, task_specs, self.config.repo_root)
            bundle = _findings_bundle(normalized)
            summary_markdown = _summary_markdown(normalized)
            _atomic_write_json(self._summary_path(run_id), normalized)
            _atomic_write_json(self._bundle_path(run_id), bundle)
            _atomic_write_json(self._legacy_bundle_path(run_id), bundle)
            _atomic_write_text(self._summary_md_path(run_id), summary_markdown)
            self._write_task_results(run_id, normalized, task_specs)
            status.update(
                {
                    "status": "completed" if normalized.get("success") else "failed",
                    "summary_path": str(self._summary_path(run_id)),
                    "bundle_path": str(self._bundle_path(run_id)),
                    "finished_at": _now_iso(),
                    "summary_markdown": summary_markdown,
                }
            )
        except Exception as exc:
            finished_at = _now_iso()
            for spec in task_specs:
                failed_payload = {
                    "run_id": run_id,
                    "shard_id": spec["shard_id"],
                    "status": "FAILED",
                    "findings": [],
                    "shadow_path": self._shadow_path(run_id, str(spec["shard_id"])).relative_to(self.config.repo_root).as_posix(),
                    "error": str(exc),
                    "started_at": status.get("started_at"),
                    "finished_at": finished_at,
                }
                if spec.get("provider_allowlist"):
                    failed_payload["provider_allowlist"] = list(spec["provider_allowlist"])
                if spec.get("lane"):
                    failed_payload["lane"] = spec["lane"]
                if spec.get("route_shard"):
                    failed_payload["shard"] = spec["route_shard"]
                _atomic_write_json(
                    self._result_path(run_id, str(spec["shard_id"])),
                    failed_payload,
                )
            status.update(
                {
                    "status": "failed",
                    "finished_at": finished_at,
                    "error": str(exc),
                }
            )
        self._write_status(run_id, status)
        completion_event = self._completion_events.get(run_id)
        if completion_event is not None:
            completion_event.set()

    def get_run(
        self,
        run_id: str,
        *,
        wait_for_completion: bool = False,
        wait_timeout_seconds: int = 0,
    ) -> dict[str, Any]:
        if wait_for_completion:
            completion_event = self._completion_events.get(run_id)
            if completion_event is not None:
                completion_event.wait(timeout=max(0, int(wait_timeout_seconds)))
        path = self._status_path(run_id)
        if not path.exists():
            raise KeyError(run_id)
        return json.loads(path.read_text(encoding="utf-8"))

    def get_bundle(self, run_id: str) -> dict[str, Any]:
        path = self._bundle_path(run_id)
        if not path.exists():
            path = self._legacy_bundle_path(run_id)
        if not path.exists():
            raise FileNotFoundError(run_id)
        return json.loads(path.read_text(encoding="utf-8"))
