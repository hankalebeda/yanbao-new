from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts import codex_mesh

PROTOCOL_ROOT = _REPO_ROOT / "LiteLLM" / "issue_mesh"
PROMPTS_ROOT = PROTOCOL_ROOT / "prompts"
FAMILY_CATALOG_PATH = PROTOCOL_ROOT / "protocol" / "family_catalog.json"
DEFAULT_READONLY_MAX_WORKERS = 12
DEFAULT_BENCHMARK_LABEL = "issue-mesh-readonly"
DEFAULT_SHARD_TIMEOUT_SECONDS = 900
RUNTIME_ISSUE_MESH_ROOT = _REPO_ROOT / "runtime" / "issue_mesh"
READONLY_PROVIDER_SHARDS = ("ro-a", "ro-b", "ro-c", "ro-d")
READONLY_PROVIDER_LANES = {
    "ro-a": "codex-ro-a",
    "ro-b": "codex-ro-b",
    "ro-c": "codex-ro-c",
    "ro-d": "codex-ro-d",
}
REQUIRED_FAMILY_IDS = [
    "truth-lineage",
    "runtime-anchor",
    "fr07-rebuild",
    "fr06-failure-semantics",
    "payment-auth-governance",
    "internal-contracts",
    "shared-artifacts",
    "issue-registry",
    "repo-governance",
    "external-integration",
    "display-bridge",
    "execution-order",
]


def repo_root() -> Path:
    return _REPO_ROOT


def _resolve_doc(pattern: str) -> str:
    matches = sorted((_REPO_ROOT / "docs" / "core").glob(pattern))
    if not matches:
        raise FileNotFoundError(f"Unable to locate docs/core/{pattern}")
    return matches[0].relative_to(_REPO_ROOT).as_posix()


def _read_prompt(name: str) -> str:
    return (PROMPTS_ROOT / name).read_text(encoding="utf-8")


def _load_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected list payload in {path}")
    return [item for item in payload if isinstance(item, dict)]


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _load_family_catalog() -> list[dict[str, Any]]:
    return _load_json(FAMILY_CATALOG_PATH)


def _dedupe_scope(items: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _provider_shard_suffix(provider_name: str) -> str | None:
    normalized = str(provider_name or "").strip().lower()
    if normalized.endswith("-stable"):
        return "stable"
    for shard in READONLY_PROVIDER_SHARDS:
        if normalized.endswith(f"-{shard}") or normalized == shard:
            return shard
    return None


def _task_shard_index(shard_id: str) -> int:
    match = re.search(r"(\d+)$", str(shard_id or "").strip())
    if not match:
        return 0
    return max(0, int(match.group(1)) - 1)


def _route_provider_allowlist(shard_id: str, provider_allowlist: list[str]) -> list[str]:
    cleaned = _dedupe_scope(list(provider_allowlist or []))
    if len(cleaned) <= 1:
        return cleaned

    providers_by_shard: dict[str, str] = {}
    passthrough: list[str] = []
    for provider_name in cleaned:
        shard = _provider_shard_suffix(provider_name)
        if shard in READONLY_PROVIDER_SHARDS:
            providers_by_shard.setdefault(shard, provider_name)
            continue
        if shard == "stable":
            continue
        passthrough.append(provider_name)

    if not providers_by_shard:
        return cleaned

    ordered_suffixes = list(READONLY_PROVIDER_SHARDS)
    start = _task_shard_index(shard_id) % len(ordered_suffixes)
    ring = ordered_suffixes[start:] + ordered_suffixes[:start]
    routed: list[str] = []
    for suffix in ring:
        provider_name = providers_by_shard.get(suffix)
        if not provider_name:
            continue
        routed.append(provider_name)
        if len(routed) == 2:
            break

    return routed or passthrough or cleaned


def _common_scope(extra_read_scope: list[str] | None = None) -> list[str]:
    return _dedupe_scope(
        [
            "LiteLLM/issue_mesh/README.md",
            "LiteLLM/issue_mesh/protocol/control_state.md",
            "LiteLLM/issue_mesh/protocol/family_catalog.json",
            "LiteLLM/issue_mesh/protocol/workspace_modes.md",
            "app",
            "tests",
            "automation",
            "runtime/issue_mesh",
            "docs/_temp/issue_mesh_shadow",
            *(extra_read_scope or []),
        ]
    )


def _doc_path(pattern: str) -> Path:
    return (_REPO_ROOT / _resolve_doc(pattern)).resolve()


def _resolve_docs(patterns: list[str]) -> list[str]:
    return [_resolve_doc(pattern) for pattern in patterns]


def _extract_current_layer_text(doc22_text: str) -> str:
    current_baseline: list[str] = []
    current_board: list[str] = []
    current_writeback: list[str] = []
    bucket: list[str] | None = None
    for line in doc22_text.splitlines():
        if 'id="current-baseline"' in line:
            bucket = current_baseline
            continue
        if line.startswith("## 2."):
            bucket = current_board
        elif 'id="current-writeback-detail"' in line:
            bucket = current_writeback
            continue
        elif line.startswith("## 3.") and bucket is current_board:
            bucket = None
        if bucket is not None:
            bucket.append(line)
    return "\n".join(current_baseline + current_board + current_writeback)


def _runtime_issue_mesh_state(max_runs: int = 12, max_intents: int = 20) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "runtime_root": "runtime/issue_mesh",
        "runs": [],
        "promote_prep_intents": [],
    }

    run_status_paths: list[Path] = []
    for root in (RUNTIME_ISSUE_MESH_ROOT, RUNTIME_ISSUE_MESH_ROOT / "runs"):
        if not root.exists():
            continue
        run_status_paths.extend(
            [
                path
                for path in root.glob("*/status.json")
                if path.parent.name != "promote_prep" and path.parent.parent != (RUNTIME_ISSUE_MESH_ROOT / "promote_prep")
            ]
        )
    if run_status_paths:
        run_status_paths = sorted(
            run_status_paths,
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in run_status_paths[:max_runs]:
            payload = _read_json_dict(path)
            if not payload:
                continue
            snapshot["runs"].append(
                {
                    "run_id": payload.get("run_id"),
                    "status": payload.get("status"),
                    "created_at": payload.get("created_at"),
                    "started_at": payload.get("started_at"),
                    "finished_at": payload.get("finished_at"),
                }
            )

    intents_root = RUNTIME_ISSUE_MESH_ROOT / "promote_prep" / "intents"
    if intents_root.exists():
        intent_paths = sorted(
            intents_root.glob("*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in intent_paths[:max_intents]:
            payload = _read_json_dict(path)
            if not payload:
                continue
            snapshot["promote_prep_intents"].append(
                {
                    "intent_id": payload.get("intent_id"),
                    "run_id": payload.get("run_id"),
                    "status": payload.get("status"),
                    "logical_target": payload.get("logical_target"),
                    "created_at": payload.get("created_at"),
                    "updated_at": payload.get("updated_at"),
                }
            )

    return snapshot


def _audit_context_header(audit_context: dict[str, Any] | None) -> str:
    if not audit_context:
        return ""
    # If controller already embedded full runtime context, use it as-is.
    # If required keys are missing, enrich from local filesystem and mark
    # the source so downstream consumers know the provenance.
    _REQUIRED_KEYS = ("runtime_gates", "shared_artifacts", "runtime_anchors", "docs", "automation")
    missing = [k for k in _REQUIRED_KEYS if k not in audit_context]
    if missing:
        audit_context.setdefault("enrichment_source", "manifest_builder_fallback")
        audit_context.setdefault("enrichment_missing_keys", missing)
    payload = json.dumps(audit_context, ensure_ascii=False, indent=2)
    return (
        "## Runtime Audit Context\n"
        "The following JSON snapshot is the current runtime-only audit context.\n"
        "Use it together with the docs in read_scope. Do not treat docs/_temp/problem as runtime truth.\n\n"
        f"```json\n{payload}\n```\n\n"
    )


def _runtime_snapshot_header(runtime_snapshot: dict[str, Any]) -> str:
    payload = json.dumps(runtime_snapshot, ensure_ascii=False, indent=2)
    return (
        "## Runtime Issue Mesh State\n"
        "The following JSON snapshot captures current runtime state under runtime/issue_mesh.\n"
        "Treat it as readonly runtime evidence for dynamic shard planning.\n\n"
        f"```json\n{payload}\n```\n\n"
    )


def _required_family_catalog(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    family_by_id = {
        str(item.get("family_id") or "").strip(): item
        for item in catalog
        if str(item.get("family_id") or "").strip()
    }
    missing = [family_id for family_id in REQUIRED_FAMILY_IDS if family_id not in family_by_id]
    if missing:
        raise ValueError(f"Missing required issue families in family_catalog.json: {missing}")
    return [family_by_id[family_id] for family_id in REQUIRED_FAMILY_IDS]


def _build_dynamic_tasks(
    *,
    provider_allowlist: list[str],
    benchmark_label: str,
    audit_context: dict[str, Any] | None,
    extra_read_scope: list[str],
    audit_scope: str,
    shard_strategy: str,
) -> list[codex_mesh.MeshTaskManifest]:
    runtime_snapshot = _runtime_issue_mesh_state()
    catalog = _required_family_catalog(_load_family_catalog())

    prefix = _audit_context_header(audit_context) + _runtime_snapshot_header(runtime_snapshot)
    shared_scope = _common_scope(extra_read_scope)
    shared_scope.extend(
        [
            "runtime/issue_mesh/promote_prep/intents",
        ]
    )

    tasks: list[codex_mesh.MeshTaskManifest] = []
    for family in catalog:
        family_id = str(family["family_id"])
        goal = str(family["goal"])
        prompt_file = str(family["prompt_file"])
        shard_id = str(family.get("shard_id") or family_id)
        routed_allowlist = _route_provider_allowlist(shard_id, provider_allowlist)
        primary_provider = routed_allowlist[0] if routed_allowlist else ""
        fallback_provider = routed_allowlist[1] if len(routed_allowlist) > 1 else ""
        route_shard = _provider_shard_suffix(primary_provider) or ""
        route_lane = READONLY_PROVIDER_LANES.get(route_shard, "codex-readonly")
        role = str(family.get("role") or "测试与质量")
        prompt_template_id = str(family.get("prompt_template_id") or family_id.replace("-", "_"))
        output_fields = [str(item) for item in (family.get("output_fields") or []) if str(item).strip()]
        ssot_refs = [str(item) for item in (family.get("ssot_refs") or []) if str(item).strip()]
        read_scope = _dedupe_scope(
            _resolve_docs([str(item) for item in (family.get("read_scope_docs") or []) if str(item).strip()])
            + [str(item) for item in (family.get("read_scope_static") or []) if str(item).strip()]
            + list(shared_scope)
        )
        prompt = (
            prefix
            + _read_prompt(prompt_file)
            + "\n\n"
            + "## Worker Protocol\n"
            + "- Read only.\n"
            + "- Output JSON only.\n"
            + "- Do not edit runtime/issue_mesh/<run_id>/<shard_id>/result.json directly.\n"
            + "- Return the final JSON only via stdout / last_message; mesh_runner persists shard result.json.\n"
            + "- Never write app/**, tests/**, automation/**, or docs/core/**.\n"
            + "- For fix_code/fix_then_rebuild, include concrete code evidence_refs under app/** or tests/** when possible.\n\n"
            + "## Task Shard\n"
            + f"- shard_id: `{shard_id}`\n"
            + f"- family_id: `{family_id}`\n"
            + f"- role: `{role}`\n"
            + f"- prompt_template_id: `{prompt_template_id}`\n"
            + f"- audit_scope: `{audit_scope}`\n"
            + f"- shard_strategy: `{shard_strategy}`\n"
            + f"- route_lane: `{route_lane}`\n"
            + f"- route_shard: `{route_shard or 'unassigned'}`\n"
            + f"- route_provider_allowlist: `{routed_allowlist}`\n"
            + f"- route_fallback_provider: `{fallback_provider or 'none'}`\n"
            + f"- ssot_refs: `{ssot_refs}`\n"
            + f"- output_fields: `{output_fields}`\n"
        )
        tasks.append(
            codex_mesh.MeshTaskManifest(
                task_id=family_id,
                goal=goal,
                prompt=prompt,
                task_kind="analysis",
                read_scope=read_scope,
                write_scope=[],
                provider_allowlist=list(routed_allowlist),
                provider_denylist=[],
                timeout_seconds=DEFAULT_SHARD_TIMEOUT_SECONDS,
                benchmark_label=benchmark_label,
                output_mode="json",
                working_root=str(_REPO_ROOT),
                allow_native_subagents=True,
                inner_agent_max_threads=1,
            )
        )
    return tasks


def build_readonly_manifest(
    *,
    provider_allowlist: list[str],
    max_workers: int = DEFAULT_READONLY_MAX_WORKERS,
    benchmark_label: str | None = None,
    audit_context: dict[str, Any] | None = None,
    extra_read_scope: list[str] | None = None,
    audit_scope: str = "current-layer",
    shard_strategy: str = "family-view-ssot",
) -> codex_mesh.MeshRunManifest:
    # Concurrency unification: prefer effective_workers from audit_context
    # (set by loop_controller via doc05 concurrency model) over the static default.
    resolved_workers = max_workers
    if audit_context and "effective_workers" in audit_context:
        try:
            ctx_workers = int(audit_context["effective_workers"])
            if ctx_workers > 0:
                resolved_workers = ctx_workers
        except (TypeError, ValueError):
            pass  # fallback to max_workers
    tasks = _build_dynamic_tasks(
        provider_allowlist=list(provider_allowlist),
        benchmark_label=benchmark_label or DEFAULT_BENCHMARK_LABEL,
        audit_context=audit_context,
        extra_read_scope=list(extra_read_scope or []),
        audit_scope=audit_scope,
        shard_strategy=shard_strategy,
    )
    return codex_mesh.MeshRunManifest(
        tasks=tasks,
        execution_mode="mesh",
        max_workers=max(1, int(resolved_workers)),
        benchmark_label=benchmark_label or DEFAULT_BENCHMARK_LABEL,
        ephemeral=False,
        provider_allowlist=list(provider_allowlist),
        provider_denylist=[],
    )
