from __future__ import annotations

import asyncio
import json
import os
import queue
import re
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import zip_longest
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from uuid import uuid4

import httpx

try:
    import redis as redis_lib
except ModuleNotFoundError:  # pragma: no cover
    redis_lib = None  # type: ignore[assignment]

CURRENT_PROGRESS_DOC = "docs/core/22_\u5168\u91cf\u529f\u80fd\u8fdb\u5ea6\u603b\u8868_v7_\u7cbe\u5ba1.md"
LEGACY_PROGRESS_DOC = CURRENT_PROGRESS_DOC
INFRA_CONTROL_PLANE_STATE = "automation/control_plane/current_state.json"
INFRA_CONTROL_PLANE_STATUS = "automation/control_plane/current_status.md"
CANONICAL_BUNDLE_NAME = "bundle.json"
LEGACY_BUNDLE_NAME = "findings_bundle.json"
EXPECTED_SHARD_COUNT = 12
FINAL_SHARD_STATUSES = {"COMPLETED", "FAILED", "TIMEOUT", "SKIPPED"}
TERMINAL_INTENT_STATUSES = {"written", "rejected", "superseded"}
FINDING_PRIORITY = {
    "runtime-anchor": 0,
    "fr06-failure-semantics": 1,
    "fr07-rebuild": 2,
    "issue-registry": 3,
    "external-integration": 4,
    "payment-auth-governance": 4,
    "repo-governance": 5,
}
ANCHOR_21 = "### 2.1 \u5f53\u524d\u4ecd\u5b58\u6d3b\u95ee\u9898"
ANCHOR_23 = "### 2.3 \u5f53\u524d\u6b8b\u4f59\u771f\u5b9e\u9879"
ANCHOR_45 = "### 4.5 \u540e\u7eed\u4f18\u5148\u7ea7"
# Layer-scoped anchor markers for run_id presence checks
_STATUS_NOTE_SECTION_START = 'id="current-writeback-detail"'
_STATUS_NOTE_SECTION_END = "## 4."
_CURRENT_LAYER_SECTION_MARKERS = (ANCHOR_21, ANCHOR_23, ANCHOR_45)
TRIAGE_DECISIONS = {"allow", "shadow_only", "freeze"}
DEFAULT_TRIAGE_MODEL = "gpt-5.4"
RISK_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
HIGH_RISK_WRITEBACK_PREFIXES = (
    "app/",
    "tests/",
    "automation/",
    "scripts/",
    "LiteLLM/",
    "docs/_temp/",
)
DEFAULT_HIGH_RISK_ALLOW_CONFIDENCE = 0.75
CODE_FIX_ALLOWED_PREFIXES = (
    "app/",
    "tests/",
)
DEFAULT_NEW_CODE_FIX_SUFFIXES = {
    ".py",
    ".ps1",
    ".json",
    ".yml",
    ".yaml",
    ".toml",
    ".ini",
}
UNGROUNDED_NEW_FILE_SUFFIXES = {
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".mjs",
    ".cjs",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_text(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _safe_promote_target_mode(runtime_gates: dict[str, Any], audit_context: dict[str, Any]) -> str:
    automation = audit_context.get("automation") if isinstance(audit_context, dict) else {}
    if not isinstance(automation, dict):
        automation = {}
    for candidate in (
        automation.get("promote_target_mode"),
        audit_context.get("promote_target_mode") if isinstance(audit_context, dict) else None,
        runtime_gates.get("promote_target_mode") if isinstance(runtime_gates, dict) else None,
    ):
        value = str(candidate or "").strip().lower()
        if value in {"infra", "doc22"}:
            return value
    return "doc22"


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


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _clean_text(value: Any) -> str:
    text = str(value or "").replace("\n", " ").strip()
    return text or "-"


def _normalize_repo_rel_path(value: str) -> str:
    return str(value or "").replace("\\", "/").lstrip("./")


def _looks_like_diff_patch(value: str) -> bool:
    text = str(value or "").lstrip()
    if not text:
        return False
    if text.startswith("*** Begin Patch"):
        return True
    lines = text.splitlines()
    head = lines[:12]
    if any(
        line.startswith(("*** Update File:", "*** Add File:", "*** Delete File:"))
        for line in head
    ):
        return True
    if any(line.startswith("@@") for line in head):
        return True
    if len(head) >= 2 and head[0].startswith("--- ") and head[1].startswith("+++ "):
        return True
    return False


def _merge_synthesis_explanation(primary: str, fallback: str) -> str:
    preferred = str(primary or "").strip()
    backup = str(fallback or "").strip()
    if preferred and backup and preferred != backup:
        return f"{backup}: {preferred}"
    return preferred or backup


def _extract_affected_files_from_finding(finding: dict[str, Any]) -> list[str]:
    """Extract file paths mentioned in a finding's affected_files or text."""
    explicit = finding.get("affected_files") or []
    if explicit and isinstance(explicit, list):
        return [str(p) for p in explicit if isinstance(p, str)]
    # Fallback: regex scan the finding for repo-relative paths
    text = json.dumps(finding, ensure_ascii=False)
    return sorted(set(re.findall(r"(?:app|tests|automation|scripts)/[\w/.-]+\.py", text)))


def _infer_target_path_fallback(
    *,
    explanation: str,
    file_contexts: list[dict[str, str]],
    candidate_targets: list[str],
) -> str:
    """Try to infer a target_path when the LLM returned one empty.

    Strategy (in priority order):
    1. Regex-extract a repo-relative .py path from the explanation text.
    2. If exactly one candidate_target exists, use it.
    3. If exactly one file_context path is under CODE_FIX_ALLOWED_PREFIXES, use it.
    """
    # 1) regex from explanation
    text = str(explanation or "")
    matches = re.findall(r"(?:app|tests|automation|scripts)/[\w/.-]+\.py", text)
    if len(matches) == 1:
        return _normalize_repo_rel_path(matches[0])

    # 2) single candidate_target
    py_candidates = [c for c in (candidate_targets or []) if c.endswith(".py")]
    if len(py_candidates) == 1:
        return py_candidates[0]

    # 3) single file_context under allowed prefixes
    ctx_paths = [
        _normalize_repo_rel_path(str(item.get("path") or ""))
        for item in (file_contexts or [])
        if any(
            _path_matches_prefix(_normalize_repo_rel_path(str(item.get("path") or "")), pfx)
            for pfx in CODE_FIX_ALLOWED_PREFIXES
        )
        and _normalize_repo_rel_path(str(item.get("path") or "")).endswith(".py")
    ]
    if len(ctx_paths) == 1:
        return ctx_paths[0]

    return ""


def _resolve_repo_evidence_ref(repo_root: Path, ref: str) -> tuple[str, Path] | None:
    """Resolve an evidence reference to a repo-relative file path.

    Supports:
    - repo-relative refs (e.g. docs/core/04_xxx.md)
    - absolute refs under repo_root (Windows or POSIX)
    - refs with trailing hints like "path/to/file (line hint)"
    """
    raw_ref = str(ref or "").strip()
    if not raw_ref:
        return None

    candidates: list[str] = [raw_ref]
    for splitter in (" (", " [", " @"):
        if splitter in raw_ref:
            candidates.append(raw_ref.split(splitter, 1)[0].strip())

    seen: set[str] = set()
    for candidate in candidates:
        cleaned = candidate.strip().strip('"').strip("'")
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)

        path_obj = Path(cleaned)
        if path_obj.is_absolute():
            resolved = path_obj.resolve(strict=False)
        else:
            normalized = _normalize_repo_rel_path(cleaned)
            if not normalized:
                continue
            resolved = (repo_root / normalized).resolve(strict=False)

        try:
            rel = resolved.relative_to(repo_root).as_posix()
        except ValueError:
            continue

        if resolved.exists() and resolved.is_file():
            return rel, resolved

    return None


def _path_matches_prefix(path: str, prefix: str) -> bool:
    normalized_path = _normalize_repo_rel_path(path)
    normalized_prefix = _normalize_repo_rel_path(prefix).rstrip("/")
    return normalized_path == normalized_prefix or normalized_path.startswith(f"{normalized_prefix}/")


def _target_path_risk_level(target_path: str) -> str:
    normalized = _normalize_repo_rel_path(target_path)
    for prefix in HIGH_RISK_WRITEBACK_PREFIXES:
        if _path_matches_prefix(normalized, prefix):
            return "high"
    return "standard"


def _safe_triage_token(value: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9_.-]+", "-", str(value or "")).strip("-")
    return token or "triage"


def _triage_record_id(owner: str, layer: str) -> str:
    return f"{_safe_triage_token(owner)}__{_safe_triage_token(layer)}"


def _family_key(finding: dict[str, Any]) -> str:
    for key in ("family_id", "source_task_id", "issue_key"):
        candidate = str(finding.get(key) or "").strip()
        if candidate:
            return candidate.split("--", 1)[0].strip()
    return ""


def _sort_findings(bundle: dict[str, Any]) -> dict[str, Any]:
    findings = [dict(item) for item in (bundle.get("findings") or []) if isinstance(item, dict)]
    findings.sort(key=lambda item: (FINDING_PRIORITY.get(_family_key(item), 99), _family_key(item), str(item.get("issue_key") or "")))
    normalized = dict(bundle)
    normalized["findings"] = findings
    normalized["finding_count"] = int(normalized.get("finding_count") or len(findings))
    return normalized


def _render_candidate_writeback(bundle: dict[str, Any]) -> str:
    lines = [f"# Candidate Writeback: {bundle.get('run_id')}", "", f"- generated_at: `{bundle.get('generated_at')}`", f"- finding_count: `{bundle.get('finding_count')}`", "", "## Findings"]
    for finding in bundle.get("findings") or []:
        lines.extend([f"### {finding.get('title') or finding.get('issue_key')}", f"- issue_key: `{finding.get('issue_key')}`", f"- risk_level: `{finding.get('risk_level')}`", f"- issue_status: `{finding.get('issue_status')}`", f"- handling_path: `{finding.get('handling_path')}`", f"- recommended_action: {finding.get('recommended_action')}", ""])
    return "\n".join(lines).rstrip() + "\n"


def _render_candidate_blocks(bundle: dict[str, Any], logical_target: str) -> dict[str, Any]:
    return {"run_id": bundle.get("run_id"), "generated_at": bundle.get("generated_at"), "logical_target": logical_target, "finding_count": bundle.get("finding_count"), "blocks": {"summary": {"title": "Readonly Issue Mesh Summary", "finding_keys": [item.get("issue_key") for item in (bundle.get("findings") or [])]}, "findings": bundle.get("findings") or []}}


def _evidence_hash(finding: dict[str, Any]) -> str:
    payload = {"issue_key": finding.get("issue_key"), "evidence_refs": finding.get("evidence_refs") or [], "ssot_refs": finding.get("ssot_refs") or []}
    return _sha256_text(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _intent_dedupe_key(bundle: dict[str, Any], logical_target: str) -> str:
    parts = [f"{str(item.get('issue_key') or '').strip()}:{str(item.get('evidence_hash') or _evidence_hash(item)).strip()}:{logical_target}" for item in (bundle.get("findings") or [])]
    return _sha256_text("\n".join(sorted(parts or [f"__empty__::__empty__:{logical_target}"])))


def _anchor_insert_before_heading(source_text: str, insert_text: str, heading_prefix: str) -> str:
    marker = f"\n{heading_prefix}"
    idx = source_text.find(marker)
    if idx == -1:
        return source_text.rstrip() + "\n\n" + insert_text.rstrip() + "\n"
    return source_text[:idx].rstrip() + "\n\n" + insert_text.rstrip() + "\n" + source_text[idx:]


def _replace_markdown_section(source_text: str, heading_line: str, replacement_text: str) -> str:
    marker = f"{heading_line}\n"
    start = source_text.find(marker)
    if start == -1:
        raise ValueError(f"anchor_not_found:{heading_line}")
    after = source_text[start + len(marker) :]
    end_candidates = [idx for idx in (after.find("\n### "), after.find("\n## ")) if idx != -1]
    end = min(end_candidates) + 1 if end_candidates else len(after)
    return source_text[:start] + replacement_text.rstrip() + "\n" + after[end:]


def _run_id_in_layer_scope(run_id: str, full_text: str, layer: str) -> bool:
    """Check if run_id appears only within the target layer's section(s).

    This prevents status-note's run_id from short-circuiting current-layer
    prepare (and vice versa) when both target the same document.
    """
    if not run_id:
        return False
    if layer == "status-note":
        start = full_text.find(_STATUS_NOTE_SECTION_START)
        if start == -1:
            return False
        end = full_text.find(_STATUS_NOTE_SECTION_END, start)
        section = full_text[start:end] if end != -1 else full_text[start:]
        return run_id in section
    if layer == "current-layer":
        for marker in _CURRENT_LAYER_SECTION_MARKERS:
            idx = full_text.find(marker)
            if idx == -1:
                continue
            section_start = idx
            after = full_text[section_start + len(marker):]
            end_candidates = [i for i in (after.find("\n### "), after.find("\n## ")) if i != -1]
            section_end = section_start + len(marker) + (min(end_candidates) if end_candidates else len(after))
            if run_id in full_text[section_start:section_end]:
                return True
        return False
    # Fallback: full-text check for unknown layers
    return run_id in full_text


def _normalize_finding_record(finding: dict[str, Any]) -> dict[str, Any]:
    return {
        "issue_key": _clean_text(finding.get("issue_key")),
        "risk_level": _clean_text(finding.get("risk_level")).upper(),
        "issue_status": _clean_text(finding.get("issue_status")).lower(),
        "handling_path": _clean_text(finding.get("handling_path")).lower(),
        "recommended_action": _clean_text(finding.get("recommended_action")),
        "evidence_hash": _evidence_hash(finding),
    }


def _bundle_semantic_fingerprint(bundle: dict[str, Any], *, layer: str, target_anchor: str) -> str:
    normalized_findings = sorted(
        [_normalize_finding_record(item) for item in (bundle.get("findings") or []) if isinstance(item, dict)],
        key=lambda item: (
            item["issue_key"],
            item["risk_level"],
            item["issue_status"],
            item["handling_path"],
            item["recommended_action"],
            item["evidence_hash"],
        ),
    )
    payload = {
        "layer": layer,
        "target_anchor": target_anchor,
        "findings": normalized_findings,
    }
    return _sha256_text(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _risk_summary(bundle: dict[str, Any]) -> dict[str, Any]:
    counts = {level: 0 for level in ("P0", "P1", "P2", "P3")}
    for item in (bundle.get("findings") or []):
        level = _clean_text(item.get("risk_level")).upper()
        if level in counts:
            counts[level] += 1
    highest = next((level for level in ("P0", "P1", "P2", "P3") if counts[level] > 0), "P3")
    return {
        "counts": counts,
        "highest_risk": highest,
        "finding_count": int(bundle.get("finding_count") or sum(counts.values())),
    }


def _patch_delta_summary(current_text: str, patch_text: str) -> dict[str, int]:
    current_lines = current_text.splitlines()
    patch_lines = patch_text.splitlines()
    changed = sum(1 for left, right in zip_longest(current_lines, patch_lines, fillvalue=None) if left != right)
    return {
        "lines_before": len(current_lines),
        "lines_after": len(patch_lines),
        "line_delta": len(patch_lines) - len(current_lines),
        "changed_lines": changed,
    }


def _extract_json_dict_from_text(raw_text: str) -> dict[str, Any]:
    text = str(raw_text or "").strip()
    if not text:
        return {}
    candidates = [text]
    candidates.extend(match.strip() for match in re.findall(r"```json\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL) if match.strip())
    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _normalize_triage_decision(payload: dict[str, Any], *, default_decision: str, default_reason: str) -> dict[str, Any]:
    decision = str(payload.get("decision") or "").strip().lower()
    if decision not in TRIAGE_DECISIONS:
        decision = default_decision
    confidence = payload.get("confidence")
    try:
        normalized_confidence = float(confidence)
    except Exception:
        normalized_confidence = 0.0
    return {
        "decision": decision,
        "reason": _clean_text(payload.get("reason") or default_reason),
        "confidence": max(0.0, min(1.0, normalized_confidence)),
        "raw": payload,
    }


def _remaining_budget_seconds(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return max(0.0, deadline - time.perf_counter())


def _bounded_stage_timeout(
    configured_timeout: float,
    *,
    deadline: float | None = None,
    minimum_timeout: float = 1.0,
) -> float:
    configured = max(minimum_timeout, float(configured_timeout))
    remaining = _remaining_budget_seconds(deadline)
    if remaining is None:
        return configured
    if remaining <= 0:
        raise TimeoutError("AI_TRIAGE_TOTAL_BUDGET_EXHAUSTED")
    return max(minimum_timeout, min(configured, remaining))


def _run_codex_cli_fallback(
    prompt: str,
    system_prompt: str,
    repo_root: Path,
    *,
    timeout_seconds: float = 300.0,
) -> dict[str, Any]:
    """Use the Codex CLI binary as a last-resort LLM backend."""
    import shutil
    codex_bin = shutil.which("codex")
    if not codex_bin:
        raise RuntimeError("CODEX_CLI_NOT_FOUND")
    combined = f"[System]\n{system_prompt}\n\n[User]\n{prompt}"
    # Write output to temp file via -o flag
    output_path = repo_root / "runtime" / f"codex_cli_output_{uuid4().hex}.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    env = {**os.environ}
    canonical_provider = str(
        env.get("CODEX_CANONICAL_PROVIDER") or "newapi-192.168.232.141-3000-stable"
    ).strip()
    codex_home = repo_root / "ai-api" / "codex" / canonical_provider
    if not codex_home.is_dir():
        raise RuntimeError(f"CODEX_CANONICAL_PROVIDER_HOME_NOT_FOUND:{canonical_provider}")
    env["CODEX_HOME"] = str(codex_home)
    start = time.perf_counter()
    try:
        proc = subprocess.run(
            [codex_bin, "exec", "-m", DEFAULT_TRIAGE_MODEL,
             "--sandbox", "read-only", "--ephemeral",
             "-o", str(output_path), "-"],
            input=combined,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=max(1.0, float(timeout_seconds)),
            cwd=str(repo_root),
            env=env,
        )
        elapsed = round(time.perf_counter() - start, 3)
        output = ""
        if output_path.exists():
            output = output_path.read_text(encoding="utf-8").strip()
        if not output:
            output = (proc.stdout or "").strip()
        if proc.returncode != 0 and not output:
            raise RuntimeError(f"CODEX_CLI_EXIT_{proc.returncode}: {(proc.stderr or '')[:200]}")
        return {
            "response": output,
            "provider_name": "codex-cli",
            "model": DEFAULT_TRIAGE_MODEL,
            "reasoning_effort": "high",
            "pool_level": "codex_cli_fallback",
            "elapsed_s": elapsed,
        }
    finally:
        output_path.unlink(missing_ok=True)


def _normalize_openai_base_url(value: str) -> str:
    clean = str(value or "").strip().rstrip("/")
    if not clean:
        return ""
    return clean if clean.endswith("/v1") else f"{clean}/v1"


def _extract_response_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for item in payload.get("output") or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        parts.append(output_text.strip())
    return "\n".join(parts).strip()


def _resolve_triage_gateway(repo_root: Path) -> dict[str, str]:
    base_url = _normalize_openai_base_url(
        os.environ.get("PROMOTE_PREP_LLM_BASE_URL")
        or os.environ.get("PROMOTE_PREP_NEW_API_BASE_URL")
        or ""
    )
    api_key = str(
        os.environ.get("PROMOTE_PREP_LLM_API_KEY")
        or os.environ.get("PROMOTE_PREP_NEW_API_TOKEN")
        or ""
    ).strip()
    provider_name = str(
        os.environ.get("CODEX_CANONICAL_PROVIDER") or "newapi-192.168.232.141-3000-stable"
    ).strip()
    if base_url and api_key:
        return {
            "base_url": base_url,
            "api_key": api_key,
            "provider_name": provider_name,
        }

    provider_dir = repo_root / "ai-api" / "codex" / provider_name
    key_path = provider_dir / "key.txt"
    if key_path.exists():
        lines = [line.strip() for line in key_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if len(lines) >= 2:
            base_url = _normalize_openai_base_url(lines[0])
            api_key = lines[1]
    if not api_key:
        auth_payload = _read_json(provider_dir / "auth.json") or {}
        api_key = str(auth_payload.get("OPENAI_API_KEY") or auth_payload.get("api_key") or "").strip()
    if not base_url:
        provider_payload = _read_json(provider_dir / "provider.json") or {}
        base_url = _normalize_openai_base_url(str(provider_payload.get("endpoint") or ""))
    if not (base_url and api_key):
        raise ValueError("TRIAGE_GATEWAY_NOT_CONFIGURED")
    return {
        "base_url": base_url,
        "api_key": api_key,
        "provider_name": provider_name,
    }


def _extract_json_dict_from_llm(raw_text: str) -> dict[str, Any]:
    text = str(raw_text or "").strip()
    if not text:
        raise ValueError("TRIAGE_EMPTY_RESPONSE")
    payload = _extract_json_dict_from_text(text)
    if payload:
        return payload
    raise ValueError("TRIAGE_JSON_PARSE_FAILED")


def _policy_triage(*, layer: str, bundle: dict[str, Any], runtime_gates: dict[str, Any], preview_summary: dict[str, Any] | None) -> dict[str, Any]:
    summary = _risk_summary(bundle)
    highest_risk = str(summary["highest_risk"])
    runtime_status = _clean_text(runtime_gates.get("status")).lower()
    promote_allowed = bool((runtime_gates.get("shared_artifact_promote") or {}).get("allowed"))
    preview_conflict = bool((preview_summary or {}).get("conflict"))
    if preview_conflict:
        return {
            "decision": "freeze",
            "reason": "preview conflict detected",
            "confidence": 1.0,
            "highest_risk_level": highest_risk,
        }
    if highest_risk == "P0":
        return {
            "decision": "freeze",
            "reason": "P0 finding requires freeze",
            "confidence": 1.0,
            "highest_risk_level": highest_risk,
        }
    if layer == "current-layer" and (runtime_status != "ready" or not promote_allowed):
        return {
            "decision": "freeze",
            "reason": "current-layer promote is blocked by runtime gates",
            "confidence": 1.0,
            "highest_risk_level": highest_risk,
        }
    if highest_risk == "P3":
        return {
            "decision": "shadow_only",
            "reason": "low-priority findings stay in shadow only",
            "confidence": 0.9,
            "highest_risk_level": highest_risk,
        }
    return {
        "decision": "allow",
        "reason": "policy allows automatic commit",
        "confidence": 0.8,
        "highest_risk_level": highest_risk,
    }


def _restrictiveness_rank(decision: str) -> int:
    order = {"freeze": 0, "shadow_only": 1, "allow": 2}
    return order.get(decision, 0)


def _stable_patch_timestamp(bundle: dict[str, Any]) -> str:
    generated_at = str(bundle.get("generated_at") or "").strip()
    return generated_at or _now_iso()


def _promote_idempotency_key(run_id: str, layer: str, target_anchor: str) -> str:
    return f"issue-mesh:{run_id}:{layer}:{target_anchor}"


def _promote_request_id(run_id: str, layer: str, target_anchor: str) -> str:
    normalized_anchor = re.sub(r"[^a-zA-Z0-9_.-]+", "-", target_anchor).strip("-") or "target"
    return f"req-{run_id}-{layer}-{normalized_anchor}"


def _build_status_note_markdown(*, run_id: str, bundle: dict[str, Any], runtime_gates: dict[str, Any], audit_context: dict[str, Any], semantic_fingerprint: str, patch_timestamp: str, target_anchor: str) -> str:
    lines = [f"#### [{run_id}] issue-mesh status-note @ {patch_timestamp}", "", f"- run_id: `{run_id}`", f"- layer: `status-note`", f"- target_anchor: `{target_anchor}`", f"- patch_timestamp: `{patch_timestamp}`", f"- semantic_fingerprint: `{semantic_fingerprint}`", f"- finding_count: `{bundle.get('finding_count')}`", f"- runtime_gates.status: `{runtime_gates.get('status')}`", f"- shared_artifact_promote.allowed: `{((runtime_gates.get('shared_artifact_promote') or {}).get('allowed'))}`", f"- public_runtime_status: `{audit_context.get('public_runtime_status')}`"]
    for item in (bundle.get("findings") or [])[:6]:
        lines.append(f"- `{item.get('issue_key')}` / `{item.get('risk_level')}` / `{item.get('issue_status')}` / {item.get('recommended_action')}")
    return "\n".join(lines).rstrip() + "\n"


def _render_current_layer_21(*, run_id: str, target_anchor: str, patch_timestamp: str, semantic_fingerprint: str, finding_count: int, runtime_status: str, public_runtime_status: str, highest_risk_level: str) -> str:
    return f"{ANCHOR_21}\n\n| item | status | note |\n|:---|:---|:---|\n| live runtime recovery | `{runtime_status}` | `public_runtime_status={public_runtime_status}; run_id={run_id}` |\n| issue-mesh findings | `updated` | `layer=current-layer; target_anchor={target_anchor}; patch_timestamp={patch_timestamp}; semantic_fingerprint={semantic_fingerprint}; finding_count={finding_count}; highest_risk={highest_risk_level}` |\n"


def _render_current_layer_23(*, run_id: str, target_anchor: str, patch_timestamp: str, semantic_fingerprint: str, finding_count: int, top_issue_keys: list[str], runtime_status: str, promote_allowed: bool, public_runtime_status: str) -> str:
    top_summary = ", ".join(top_issue_keys) if top_issue_keys else "-"
    return f"{ANCHOR_23}\n| item | status | note |\n|:---|:---|:---|\n| issue-mesh current-layer snapshot | `updated` | `run_id={run_id}; layer=current-layer; target_anchor={target_anchor}; patch_timestamp={patch_timestamp}; semantic_fingerprint={semantic_fingerprint}; finding_count={finding_count}; top_issue_keys={top_summary}` |\n| runtime gate status | `{runtime_status}` | `shared_artifact_promote.allowed={promote_allowed}` |\n| public runtime status | `{public_runtime_status}` | `from audit/context` |\n"


def _render_current_layer_45(*, run_id: str, target_anchor: str, patch_timestamp: str, semantic_fingerprint: str, finding_count: int, highest_risk_level: str) -> str:
    return f"{ANCHOR_45}\n\n1. `live runtime recovery`\n2. `FR-06`\n3. `FR-07`\n4. `ISSUE-REGISTRY`\n5. `external integration`\n6. `repo governance`\n\n> run_id=`{run_id}` / layer=`current-layer` / target_anchor=`{target_anchor}` / patch_timestamp=`{patch_timestamp}` / semantic_fingerprint=`{semantic_fingerprint}` / finding_count={finding_count} / highest_risk={highest_risk_level}\n"


@dataclass(frozen=True)
class PromotePrepConfig:
    repo_root: Path
    shadow_root: Path
    runtime_root: Path
    auth_token: str
    redis_url: str
    queue_name: str
    consumer_poll_seconds: float
    lease_seconds: int

    @classmethod
    def from_env(cls) -> "PromotePrepConfig":
        root = Path(os.environ.get("ISSUE_MESH_REPO_ROOT") or Path(__file__).resolve().parents[2]).resolve()
        return cls(root, (root / "docs" / "_temp" / "issue_mesh_shadow").resolve(), (root / "runtime" / "issue_mesh" / "promote_prep").resolve(), str(os.environ.get("PROMOTE_PREP_AUTH_TOKEN") or "").strip(), str(os.environ.get("PROMOTE_PREP_REDIS_URL") or "").strip(), str(os.environ.get("PROMOTE_PREP_QUEUE_NAME") or "issue_mesh_shadow").strip(), float(os.environ.get("PROMOTE_PREP_POLL_SECONDS") or "1"), max(1, int(os.environ.get("PROMOTE_PREP_LEASE_SECONDS") or "30")))


class InMemoryQueueBackend:
    def __init__(self) -> None:
        self._queue: queue.Queue[str] = queue.Queue()
        self._dedupe: dict[str, str] = {}
        self._leases: dict[str, str] = {}
        self._lock = threading.Lock()

    def claim_dedupe(self, dedupe_key: str, intent_id: str) -> str | None:
        with self._lock:
            existing = self._dedupe.get(dedupe_key)
            if existing is None:
                self._dedupe[dedupe_key] = intent_id
            return existing

    def enqueue(self, intent_id: str) -> None: self._queue.put(intent_id)

    def pop(self, timeout_seconds: float) -> str | None:
        try:
            return self._queue.get(timeout=max(timeout_seconds, 0.1))
        except queue.Empty:
            return None

    def acquire_lease(self, lease_key: str, token: str) -> bool:
        with self._lock:
            current = self._leases.get(lease_key)
            if current is not None and current != token:
                return False
            self._leases[lease_key] = token
            return True

    def release_lease(self, lease_key: str, token: str) -> None:
        with self._lock:
            if self._leases.get(lease_key) == token:
                self._leases.pop(lease_key, None)


class RedisQueueBackend:
    def __init__(self, redis_url: str, queue_name: str, lease_seconds: int) -> None:
        if redis_lib is None:
            raise RuntimeError("redis package is required when PROMOTE_PREP_REDIS_URL is configured")
        self._client = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        self._queue_name = queue_name
        self._lease_seconds = lease_seconds

    def claim_dedupe(self, dedupe_key: str, intent_id: str) -> str | None:
        key = f"{self._queue_name}:dedupe:{dedupe_key}"
        return None if self._client.set(key, intent_id, nx=True) else self._client.get(key)

    def enqueue(self, intent_id: str) -> None: self._client.lpush(f"{self._queue_name}:queue", intent_id)

    def pop(self, timeout_seconds: float) -> str | None:
        result = self._client.brpop(f"{self._queue_name}:queue", timeout=max(1, int(timeout_seconds)))
        return result[1] if result else None

    def acquire_lease(self, lease_key: str, token: str) -> bool:
        return bool(self._client.set(f"{self._queue_name}:lease:{lease_key}", token, nx=True, ex=self._lease_seconds))

    def release_lease(self, lease_key: str, token: str) -> None:
        key = f"{self._queue_name}:lease:{lease_key}"
        if self._client.get(key) == token:
            self._client.delete(key)


class PromotePrepService:
    def __init__(self, config: PromotePrepConfig) -> None:
        self.config = config
        self.config.runtime_root.mkdir(parents=True, exist_ok=True)
        (self.config.runtime_root / "intents").mkdir(parents=True, exist_ok=True)
        self._backend = RedisQueueBackend(config.redis_url, config.queue_name, config.lease_seconds) if config.redis_url else InMemoryQueueBackend()
        self._stop_event = threading.Event()
        self._consumer_thread: threading.Thread | None = None
        self._metrics_lock = threading.Lock()
        self._guard_audit_lock = threading.Lock()
        self._status_totals = {name: 0 for name in ("queued", "merged", "writing_shadow", "written", "superseded", "rejected")}
        self._failure_count = 0
        self._last_queue_lag_seconds: float | None = None
        self._max_queue_lag_seconds = 0.0
        self._last_error: str | None = None

    def start(self) -> None:
        if self._consumer_thread and self._consumer_thread.is_alive():
            return
        self._consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._consumer_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._consumer_thread:
            self._consumer_thread.join(timeout=2)

    def _intent_path(self, intent_id: str) -> Path: return self.config.runtime_root / "intents" / f"{intent_id}.json"
    def _save_intent(self, payload: dict[str, Any]) -> None: _atomic_write_json(self._intent_path(str(payload["intent_id"])), payload)

    def get_intent(self, intent_id: str, *, wait_for_completion: bool = False, wait_timeout_seconds: int = 0) -> dict[str, Any]:
        deadline = time.monotonic() + max(0, int(wait_timeout_seconds))
        while True:
            payload = _read_json(self._intent_path(intent_id))
            if payload is None:
                raise KeyError(intent_id)
            status = str(payload.get("status") or "").lower()
            if not wait_for_completion or status in TERMINAL_INTENT_STATUSES or time.monotonic() >= deadline:
                return payload
            time.sleep(min(max(self.config.consumer_poll_seconds, 0.05), 0.2))

    def _record_status(self, status: str) -> None:
        with self._metrics_lock:
            if status in self._status_totals:
                self._status_totals[status] += 1

    def _record_failure(self, error: str | None = None) -> None:
        with self._metrics_lock:
            self._failure_count += 1
            if error:
                self._last_error = error

    def _record_queue_lag(self, created_at: str | None) -> None:
        created = _parse_iso_datetime(created_at)
        if created is None:
            return
        lag_seconds = max(0.0, (datetime.now(timezone.utc) - created).total_seconds())
        with self._metrics_lock:
            self._last_queue_lag_seconds = lag_seconds
            self._max_queue_lag_seconds = max(self._max_queue_lag_seconds, lag_seconds)

    def get_metrics(self) -> dict[str, Any]:
        with self._metrics_lock:
            return {"status_totals": dict(self._status_totals), "failure_count": self._failure_count, "last_queue_lag_seconds": self._last_queue_lag_seconds, "max_queue_lag_seconds": self._max_queue_lag_seconds, "last_error": self._last_error}

    def _set_status(self, payload: dict[str, Any], status: str, *, error: str | None = None) -> None:
        payload["status"] = status
        payload["updated_at"] = _now_iso()
        if error is not None:
            payload["error"] = error
        self._record_status(status)

    def _progress_doc_path(self) -> Path:
        primary = (self.config.repo_root / CURRENT_PROGRESS_DOC).resolve()
        if primary.exists():
            return primary
        return (self.config.repo_root / LEGACY_PROGRESS_DOC).resolve()

    def _progress_doc_text(self) -> str: return self._progress_doc_path().read_text(encoding="utf-8")
    def _infra_status_path(self) -> Path: return (self.config.repo_root / INFRA_CONTROL_PLANE_STATUS).resolve()
    def _infra_state_path(self) -> Path: return (self.config.repo_root / INFRA_CONTROL_PLANE_STATE).resolve()

    def _guard_audit_path(self) -> Path: return (self.config.runtime_root / "guard_audit.jsonl").resolve()

    def _append_guard_audit_event(self, event: dict[str, Any]) -> None:
        path = self._guard_audit_path()
        with self._guard_audit_lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8", newline="\n") as file_obj:
                file_obj.write(json.dumps(event, ensure_ascii=True, sort_keys=True))
                file_obj.write("\n")

    def _get_promote_target_mode(self) -> tuple[str, str | None]:
        """Read promote_target_mode from control plane state file and fail closed on invalid state."""
        state_path = self._infra_state_path()
        if not state_path.exists():
            return "infra", "CONTROL_PLANE_STATE_MISSING"
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            return "infra", "CONTROL_PLANE_STATE_INVALID"
        if not isinstance(payload, dict):
            return "infra", "CONTROL_PLANE_STATE_INVALID"
        mode = str(payload.get("promote_target_mode") or "").strip().lower()
        if mode in {"infra", "doc22"}:
            return mode, None
        return "infra", "CONTROL_PLANE_STATE_INVALID"

    def _assert_doc22_promote_allowed(self, *, layer: str) -> None:
        """Hard block: reject any Doc-22 promote unless the control plane explicitly allows doc22 mode."""
        promote_target_mode, control_plane_reason = self._get_promote_target_mode()
        if promote_target_mode == "infra":
            self._append_guard_audit_event(
                {
                    "timestamp": _now_iso(),
                    "event": "doc22_promote_blocked",
                    "layer": layer,
                    "target_path": CURRENT_PROGRESS_DOC,
                    "promote_target_mode": promote_target_mode,
                    "control_plane_state_path": str(self._infra_state_path()),
                    "control_plane_state_reason": control_plane_reason,
                }
            )
            reason_suffix = ""
            if control_plane_reason:
                reason_suffix = f"; control_plane_state={control_plane_reason}"
            raise ValueError(
                f"DOC22_PROMOTE_BLOCKED_BY_INFRA_MODE: "
                f"layer={layer}, promote_target_mode=infra{reason_suffix} — "
                f"阶段一不允许写入 {CURRENT_PROGRESS_DOC}"
            )

    def _read_text_or_empty(self, path: Path) -> str: return path.read_text(encoding="utf-8") if path.exists() else ""
    def _run_root(self, run_id: str) -> Path: return (self.config.repo_root / "runtime" / "issue_mesh" / run_id).resolve()
    def _legacy_run_root(self, run_id: str) -> Path: return (self.config.repo_root / "runtime" / "issue_mesh" / "runs" / run_id).resolve()
    def _triage_root(self) -> Path: return (self.config.runtime_root / "triage").resolve()
    def _triage_path(self, run_id: str, layer: str) -> Path: return self._triage_root() / f"{_triage_record_id(run_id, layer)}.json"

    def _resolve_target_context(self, target_path: str, patch_text: str) -> dict[str, Any]:
        raw = Path(str(target_path or "").strip())
        resolved = raw.resolve() if raw.is_absolute() else (self.config.repo_root / raw).resolve()
        try:
            rel_path = resolved.relative_to(self.config.repo_root).as_posix()
            within_repo = True
        except ValueError:
            rel_path = _normalize_repo_rel_path(str(target_path or ""))
            within_repo = False

        current_text = ""
        if within_repo and resolved.exists():
            current_text = resolved.read_text(encoding="utf-8")

        return {
            "resolved_path": str(resolved),
            "relative_path": _normalize_repo_rel_path(rel_path),
            "within_repo": within_repo,
            "exists": bool(within_repo and resolved.exists()),
            "current_sha256": _sha256_text(current_text),
            "patch_delta": _patch_delta_summary(current_text, patch_text),
        }

    def _normalize_synthesized_code_patch(
        self,
        *,
        raw_target_path: str,
        raw_patch_text: str,
        explanation: str,
        file_contexts: list[dict[str, str]],
    ) -> tuple[str, str, str, str]:
        candidate_target = str(raw_target_path or "").strip()
        patch_text = str(raw_patch_text or "").strip()
        normalized_explanation = str(explanation or "").strip()
        if not candidate_target or not patch_text:
            return "", "", normalized_explanation, ""

        raw = Path(candidate_target)
        resolved = raw.resolve() if raw.is_absolute() else (self.config.repo_root / raw).resolve()
        try:
            rel_path = resolved.relative_to(self.config.repo_root).as_posix()
        except ValueError:
            return "", "", _merge_synthesis_explanation(normalized_explanation, "TARGET_OUTSIDE_REPO"), ""

        target_path = _normalize_repo_rel_path(rel_path)
        if not any(_path_matches_prefix(target_path, prefix) for prefix in CODE_FIX_ALLOWED_PREFIXES):
            return "", "", _merge_synthesis_explanation(normalized_explanation, "TARGET_ROOT_NOT_ALLOWED"), ""

        if _looks_like_diff_patch(patch_text):
            return "", "", _merge_synthesis_explanation(normalized_explanation, "PATCH_TEXT_NOT_FULL_FILE_CONTENT"), ""

        target_file = self.config.repo_root / target_path
        evidence_suffixes = {
            Path(str(item.get("path") or "")).suffix.lower()
            for item in file_contexts
            if Path(str(item.get("path") or "")).suffix
        }
        target_suffix = target_file.suffix.lower()

        if not target_file.exists():
            if target_suffix in UNGROUNDED_NEW_FILE_SUFFIXES and target_suffix not in evidence_suffixes:
                return "", "", _merge_synthesis_explanation(normalized_explanation, f"TARGET_SUFFIX_UNGROUNDED:{target_suffix}"), ""
            if evidence_suffixes and target_suffix and target_suffix not in evidence_suffixes:
                # Allow suffixes that are in the default code-fix set (e.g. .py)
                # even if evidence files don't include that suffix — code-fix
                # synthesis legitimately creates new .py files.
                if target_suffix not in DEFAULT_NEW_CODE_FIX_SUFFIXES:
                    return "", "", _merge_synthesis_explanation(normalized_explanation, f"TARGET_SUFFIX_MISMATCH:{target_suffix}"), ""
            if not evidence_suffixes and target_suffix and target_suffix not in DEFAULT_NEW_CODE_FIX_SUFFIXES:
                return "", "", _merge_synthesis_explanation(normalized_explanation, f"TARGET_SUFFIX_NOT_ALLOWED:{target_suffix}"), ""

        base_sha256 = ""
        if target_file.exists():
            base_sha256 = _sha256_text(target_file.read_text(encoding="utf-8"))
        return target_path, patch_text, normalized_explanation, base_sha256

    def _existing_run_root(self, run_id: str) -> Path:
        canonical = self._run_root(run_id)
        if canonical.exists():
            return canonical
        legacy = self._legacy_run_root(run_id)
        if legacy.exists():
            return legacy
        return canonical

    def _load_bundle(self, run_id: str) -> dict[str, Any]:
        bundle = _read_json(self.config.shadow_root / run_id / CANONICAL_BUNDLE_NAME) or _read_json(self.config.shadow_root / run_id / LEGACY_BUNDLE_NAME)
        if bundle is None:
            mesh_runtime_root = self.config.repo_root / "runtime" / "issue_mesh"
            bundle = (
                _read_json(mesh_runtime_root / run_id / CANONICAL_BUNDLE_NAME)
                or _read_json(mesh_runtime_root / run_id / LEGACY_BUNDLE_NAME)
            )
        if bundle is None:
            raise FileNotFoundError("SHADOW_BUNDLE_NOT_FOUND")
        return _sort_findings(bundle)

    def _load_bundle_for_synthesis(self, run_id: str) -> dict[str, Any]:
        """Load a findings bundle for code-fix synthesis.

        Checks shadow root first (existing promote workflow), then falls back to the
        mesh_runner's canonical storage at ``runtime/issue_mesh/{run_id}/bundle.json``.
        This allows the loop_controller to call synthesize-patches immediately after
        a mesh_runner audit without a prior shadow-write step.
        """
        # 1) shadow root (intent/promote workflow path)
        bundle = (
            _read_json(self.config.shadow_root / run_id / CANONICAL_BUNDLE_NAME)
            or _read_json(self.config.shadow_root / run_id / LEGACY_BUNDLE_NAME)
        )
        if bundle is not None:
            return _sort_findings(bundle)
        # 2) mesh_runner canonical storage (loop_controller audit path)
        mesh_runtime_root = self.config.repo_root / "runtime" / "issue_mesh"
        bundle = (
            _read_json(mesh_runtime_root / run_id / CANONICAL_BUNDLE_NAME)
            or _read_json(mesh_runtime_root / run_id / LEGACY_BUNDLE_NAME)
        )
        if bundle is not None:
            return _sort_findings(bundle)
        raise FileNotFoundError("BUNDLE_NOT_FOUND")

    def _shadow_snapshot(self, run_id: str) -> dict[str, Any]:
        run_root = self.config.shadow_root / run_id
        files = {}
        for name in ("summary.md", CANONICAL_BUNDLE_NAME, LEGACY_BUNDLE_NAME, "candidate_writeback.md", "candidate_blocks.json", "metadata.json"):
            path = run_root / name
            if path.exists():
                files[name] = {"path": str(path), "sha256": _sha256_text(path.read_text(encoding="utf-8"))}
        if not files:
            mesh_runtime_root = self.config.repo_root / "runtime" / "issue_mesh" / run_id
            for name in ("summary.md", CANONICAL_BUNDLE_NAME, LEGACY_BUNDLE_NAME, "candidate_writeback.md", "candidate_blocks.json", "metadata.json"):
                path = mesh_runtime_root / name
                if path.exists():
                    files[name] = {"path": str(path), "sha256": _sha256_text(path.read_text(encoding="utf-8"))}
        if not files:
            raise FileNotFoundError("SHADOW_SNAPSHOT_NOT_FOUND")
        return {"run_id": run_id, "shadow_root": str(run_root), "files": files}

    def _validate_status_note_prereqs(self, run_id: str) -> None:
        run_root = self._existing_run_root(run_id)
        if not run_root.exists():
            raise FileNotFoundError("ISSUE_MESH_RUN_NOT_FOUND")
        results = [_read_json(path) for path in sorted(run_root.glob("shard_*/result.json"))]
        if len(results) != EXPECTED_SHARD_COUNT:
            raise ValueError("ISSUE_MESH_SHARD_COUNT_MISMATCH")
        if any(not isinstance(item, dict) for item in results):
            raise ValueError("ISSUE_MESH_RESULT_INVALID")
        if any(str(item.get("status") or "").upper() not in FINAL_SHARD_STATUSES for item in results if isinstance(item, dict)):
            raise ValueError("ISSUE_MESH_SHARDS_NOT_FINAL")

    def verify_rollback_acceptance(self, *, run_id: str, layer: str, target_path: str, expected_base_sha256: str, expected_shadow_snapshot: dict[str, Any]) -> dict[str, Any]:
        resolved = (self.config.repo_root / target_path).resolve()
        try:
            resolved.relative_to(self.config.repo_root)
        except ValueError as exc:
            raise ValueError("ROLLBACK_TARGET_OUTSIDE_REPO") from exc
        if not resolved.exists():
            raise FileNotFoundError("ROLLBACK_TARGET_NOT_FOUND")
        current_text = resolved.read_text(encoding="utf-8")
        if _sha256_text(current_text) != expected_base_sha256:
            raise ValueError("ROLLBACK_BASE_SHA_MISMATCH")
        if run_id in current_text:
            raise ValueError("ROLLBACK_RUN_ID_STILL_PRESENT")
        if not isinstance(expected_shadow_snapshot, dict) or not expected_shadow_snapshot:
            raise ValueError("ROLLBACK_SHADOW_SNAPSHOT_REQUIRED")
        current_shadow_snapshot = self._shadow_snapshot(run_id)
        if current_shadow_snapshot != expected_shadow_snapshot:
            raise ValueError("ROLLBACK_SHADOW_CHANGED")
        return {"run_id": run_id, "layer": layer, "target_path": target_path, "current_sha256": expected_base_sha256, "acceptance_passed": True, "shadow_snapshot": current_shadow_snapshot}

    def _run_ai_triage_prompt(self, *, prompt: str, system_prompt: str) -> dict[str, Any]:
        from app.core.config import settings
        from app.services.codex_client import CodexAPIClient, discover_codex_provider_specs

        total_budget_seconds = max(30.0, float(getattr(settings, "codex_api_timeout_seconds", 300.0)))
        deadline = time.perf_counter() + total_budget_seconds
        gateway_error: Exception | None = None
        try:
            gateway = _resolve_triage_gateway(self.config.repo_root)
            request_started = time.perf_counter()
            # Responses API lives at /responses (NOT /v1/responses) on most relays
            responses_base = gateway['base_url'].rstrip('/').removesuffix('/v1')
            gateway_timeout = _bounded_stage_timeout(
                float(getattr(settings, "promote_prep_gateway_timeout_seconds", 180.0)),
                deadline=deadline,
            )
            response = httpx.post(
                f"{responses_base}/responses",
                headers={
                    "Authorization": f"Bearer {gateway['api_key']}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": DEFAULT_TRIAGE_MODEL,
                    "input": [
                        {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
                        {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
                    ],
                    "text": {"format": {"type": "text"}},
                    "reasoning": {"effort": "high"},
                },
                timeout=gateway_timeout,
            )
            response.raise_for_status()
            payload = _extract_json_dict_from_llm(_extract_response_text(response.json()))
            normalized = _normalize_triage_decision(
                payload,
                default_decision="freeze",
                default_reason="AI_TRIAGE_RESPONSE_INVALID",
            )
            normalized["provider_name"] = gateway["provider_name"]
            normalized["model"] = DEFAULT_TRIAGE_MODEL
            normalized["reasoning_effort"] = "high"
            normalized["pool_level"] = "direct_gateway"
            normalized["elapsed_s"] = round(time.perf_counter() - request_started, 3)
            return normalized
        except Exception as exc:
            gateway_error = exc

        canonical_provider = str(
            os.environ.get("CODEX_CANONICAL_PROVIDER") or settings.codex_canonical_provider or ""
        ).strip().lower()

        async def _run() -> dict[str, Any]:
            providers = discover_codex_provider_specs()
            if canonical_provider:
                providers = [
                    provider
                    for provider in providers
                    if str(provider.provider_name or "").strip().lower() == canonical_provider
                ]
            if not providers:
                raise RuntimeError("AI_TRIAGE_PROVIDER_UNAVAILABLE")
            client = CodexAPIClient(provider_specs=providers)
            try:
                return await client.analyze(
                    prompt=prompt,
                    system_prompt=system_prompt,
                    temperature=0.1,
                    max_tokens=800,
                )
            finally:
                await client.close()

        try:
            provider_timeout = _bounded_stage_timeout(
                float(getattr(settings, "codex_api_timeout_seconds", 300.0)),
                deadline=deadline,
            )
            result = asyncio.run(asyncio.wait_for(_run(), timeout=provider_timeout))
        except Exception as exc:
            provider_error = exc
            # ── Third fallback: Codex CLI subprocess ──
            try:
                cli_timeout = _bounded_stage_timeout(
                    float(getattr(settings, "codex_api_timeout_seconds", 300.0)),
                    deadline=deadline,
                )
                result = _run_codex_cli_fallback(
                    prompt,
                    system_prompt,
                    self.config.repo_root,
                    timeout_seconds=cli_timeout,
                )
            except Exception as cli_exc:
                raise RuntimeError(
                    f"DIRECT_GATEWAY_FAILED:{_clean_text(gateway_error)}; "
                    f"PROVIDER_FALLBACK_FAILED:{_clean_text(provider_error)}; "
                    f"CODEX_CLI_FALLBACK_FAILED:{_clean_text(cli_exc)}"
                ) from cli_exc

        payload = _extract_json_dict_from_text(result.get("response"))
        normalized = _normalize_triage_decision(
            payload,
            default_decision="freeze",
            default_reason="AI_TRIAGE_RESPONSE_INVALID",
        )
        normalized["provider_name"] = result.get("provider_name")
        normalized["model"] = result.get("model")
        normalized["reasoning_effort"] = result.get("reasoning_effort")
        normalized["pool_level"] = result.get("pool_level")
        normalized["elapsed_s"] = result.get("elapsed_s")
        if gateway_error is not None:
            normalized["gateway_fallback_error"] = _clean_text(gateway_error)
        return normalized

    def _call_ai_triage(self, *, run_id: str, layer: str, target_path: str, target_anchor: str, bundle: dict[str, Any], runtime_gates: dict[str, Any], audit_context: dict[str, Any], patch_text: str, base_sha256: str, semantic_fingerprint: str) -> dict[str, Any]:
        current_text = self._progress_doc_text()
        risk_summary = _risk_summary(bundle)
        patch_delta = _patch_delta_summary(current_text, patch_text)
        canonical_provider = str(os.environ.get("CODEX_CANONICAL_PROVIDER") or "newapi-192.168.232.141-3000-stable").strip().lower()
        prompt_payload = {
            "run_id": run_id,
            "layer": layer,
            "target_path": target_path,
            "target_anchor": target_anchor,
            "base_sha256": base_sha256,
            "semantic_fingerprint": semantic_fingerprint,
            "canonical_provider": canonical_provider,
            "risk_summary": risk_summary,
            "patch_delta": patch_delta,
            "runtime_gate_status": _clean_text(runtime_gates.get("status")).lower(),
            "shared_artifact_promote_allowed": bool((runtime_gates.get("shared_artifact_promote") or {}).get("allowed")),
            "public_runtime_status": _clean_text(audit_context.get("public_runtime_status")).lower(),
            "top_findings": [
                {
                    "issue_key": _clean_text(item.get("issue_key")),
                    "risk_level": _clean_text(item.get("risk_level")).upper(),
                    "issue_status": _clean_text(item.get("issue_status")).lower(),
                    "handling_path": _clean_text(item.get("handling_path")).lower(),
                    "recommended_action": _clean_text(item.get("recommended_action")),
                }
                for item in (bundle.get("findings") or [])[:8]
                if isinstance(item, dict)
            ],
        }
        prompt = (
            "你是 issue mesh 自动执行前的风险裁决器。"
            "你只能输出 JSON，对 controller-only 文档写回做 fail-close 决策。"
            "规则：任何 P0 一律 freeze；runtime gate 未 ready 且 layer=current-layer 一律 freeze；"
            "其余情况只有在风险充分可控时才 allow。"
            "输出格式必须是 {\"decision\":\"allow|shadow_only|freeze\",\"reason\":\"...\",\"confidence\":0.0}。\n\n"
            f"```json\n{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}\n```"
        )
        return self._run_ai_triage_prompt(
            prompt=prompt,
            system_prompt="你是严格的自动执行风险裁决器。必须输出 JSON，不要输出解释性文字。",
        )

    def triage_promote(self, *, run_id: str, layer: str, target_path: str, target_anchor: str, patch_text: str, base_sha256: str, runtime_gates: dict[str, Any], audit_context: dict[str, Any], semantic_fingerprint: str | None = None) -> dict[str, Any]:
        # Block Doc-22 triage when in infra mode
        normalized_target = _normalize_repo_rel_path(target_path)
        if normalized_target == _normalize_repo_rel_path(CURRENT_PROGRESS_DOC):
            self._assert_doc22_promote_allowed(layer=layer)
        bundle = self._load_bundle(run_id)
        fingerprint = semantic_fingerprint or _bundle_semantic_fingerprint(bundle, layer=layer, target_anchor=target_anchor)
        risk_summary = _risk_summary(bundle)
        patch_delta = _patch_delta_summary(self._progress_doc_text(), patch_text)
        issue_statuses = {
            _clean_text(item.get("issue_status")).lower()
            for item in (bundle.get("findings") or [])
            if isinstance(item, dict)
        }
        runtime_status = _clean_text(runtime_gates.get("status")).lower()
        promote_allowed = bool((runtime_gates.get("shared_artifact_promote") or {}).get("allowed"))
        preview_conflict = False
        default_decision = "allow"
        default_reason = "AI_TRIAGE_ALLOW"
        if risk_summary["highest_risk"] == "P0":
            default_decision = "freeze"
            default_reason = "P0_FINDINGS_REQUIRE_FREEZE"
        elif layer == "current-layer" and (runtime_status != "ready" or not promote_allowed):
            default_decision = "freeze"
            default_reason = "CURRENT_LAYER_RUNTIME_GATE_BLOCKED"
        elif risk_summary["highest_risk"] == "P3":
            default_decision = "shadow_only"
            default_reason = "P3_SHADOW_ONLY"

        decision_source = "ai"
        try:
            ai_decision = self._call_ai_triage(
                run_id=run_id,
                layer=layer,
                target_path=target_path,
                target_anchor=target_anchor,
                bundle=bundle,
                runtime_gates=runtime_gates,
                audit_context=audit_context,
                patch_text=patch_text,
                base_sha256=base_sha256,
                semantic_fingerprint=fingerprint,
            )
        except Exception as exc:
            # Allow code-fix / status-note with no P0 risk to proceed
            fallback_decision = "freeze"
            if layer in {"code-fix", "status-note"} and risk_summary["counts"]["P0"] == 0:
                fallback_decision = "allow"
            ai_decision = {
                "decision": fallback_decision,
                "reason": f"AI_TRIAGE_UNAVAILABLE:{_clean_text(exc)}",
                "confidence": 0.0,
                "raw": {},
            }
            decision_source = "fail_open_low_risk" if fallback_decision == "allow" else "fail_closed"

        final_decision = ai_decision["decision"]
        final_reason = ai_decision["reason"]
        low_risk_doc_correction = (
            layer == "doc-correction"
            and risk_summary["counts"]["P0"] == 0
            and risk_summary["counts"]["P1"] == 0
            and risk_summary["highest_risk"] in {"P2", "P3"}
            and issue_statuses.issubset({"stale", "narrow_required"})
            and patch_delta["changed_lines"] <= 4
        )
        status_note_auto_allow = (
            layer == "status-note"
            and risk_summary["counts"]["P0"] <= 1
            and risk_summary["counts"]["P1"] <= 12
            and not preview_conflict
            and final_decision != "allow"
        )
        if default_decision == "freeze":
            final_decision = "freeze"
            final_reason = default_reason
            decision_source = "policy_override"
        elif default_decision == "shadow_only" and final_decision == "allow":
            final_decision = "shadow_only"
            final_reason = default_reason
            decision_source = "policy_override"
        elif status_note_auto_allow:
            final_decision = "allow"
            final_reason = "STATUS_NOTE_AUTO_ALLOW"
            decision_source = "policy_override"
        elif low_risk_doc_correction and final_decision == "shadow_only":
            final_decision = "allow"
            final_reason = "LOW_RISK_DOC_CORRECTION_AUTO_ALLOW"
            decision_source = "policy_override"

        payload = {
            "triage_record_id": _triage_record_id(run_id, layer),
            "run_id": run_id,
            "layer": layer,
            "target_path": target_path,
            "target_anchor": target_anchor,
            "relative_target_path": _normalize_repo_rel_path(target_path),
            "base_sha256": base_sha256,
            "patch_hash": _sha256_text(patch_text),
            "semantic_fingerprint": fingerprint,
            "risk_summary": risk_summary,
            "patch_delta": patch_delta,
            "runtime_gate_status": runtime_status,
            "shared_artifact_promote_allowed": promote_allowed,
            "decision": final_decision,
            "reason": final_reason,
            "confidence": ai_decision.get("confidence", 0.0),
            "auto_commit": final_decision == "allow",
            "decision_source": decision_source,
            "ai_result": ai_decision,
            "triaged_at": _now_iso(),
        }
        _atomic_write_json(self._triage_path(run_id, layer), payload)
        return payload

    def _call_ai_writeback_triage(
        self,
        *,
        triage_id: str,
        layer: str,
        target_path: str,
        base_sha256: str,
        target_risk: str,
        runtime_gates: dict[str, Any],
        audit_context: dict[str, Any],
        patch_delta: dict[str, Any],
        preview_summary: dict[str, Any] | None,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        canonical_provider = str(os.environ.get("CODEX_CANONICAL_PROVIDER") or "newapi-192.168.232.141-3000-stable").strip().lower()
        prompt_payload = {
            "triage_id": triage_id,
            "layer": layer,
            "target_path": target_path,
            "base_sha256": base_sha256,
            "target_risk": target_risk,
            "canonical_provider": canonical_provider,
            "runtime_gate_status": _clean_text(runtime_gates.get("status")).lower(),
            "shared_artifact_promote_allowed": bool((runtime_gates.get("shared_artifact_promote") or {}).get("allowed")),
            "public_runtime_status": _clean_text(audit_context.get("public_runtime_status")).lower(),
            "patch_delta": patch_delta,
            "preview_conflict": bool((preview_summary or {}).get("conflict")),
            "metadata": metadata or {},
            "high_risk_prefixes": list(HIGH_RISK_WRITEBACK_PREFIXES),
            "runtime_gate_scope_note": "runtime gate only blocks docs/core/22_* and official shared artifacts, not code/doc allowlist targets by itself",
        }
        prompt = (
            "你是 writeback 自动执行前的风险裁决器。"
            "你只能输出 JSON，对 report/code/doc 写回做 fail-close 决策。"
            "规则：preview 冲突一律 freeze；"
            "对 docs/core/22_* 与 official shared artifacts，runtime gate 未 ready 时必须 freeze；"
            "对 app/tests/automation/scripts/LiteLLM/docs/_temp 这些 allowlist 高风险路径，runtime gate 只作为上下文，不是自动 freeze 条件；"
            "只要 patch 范围可控、目标未触碰 22/shared artifacts、preview 无冲突，允许给出 allow。"
            "输出格式必须是 {\"decision\":\"allow|shadow_only|freeze\",\"reason\":\"...\",\"confidence\":0.0}。\n\n"
            f"```json\n{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}\n```"
        )
        return self._run_ai_triage_prompt(
            prompt=prompt,
            system_prompt="你是严格的 writeback 风险裁决器。必须只输出 JSON，不要输出解释性文字。",
        )

    def triage_writeback(
        self,
        *,
        run_id: str | None,
        workflow_id: str | None,
        layer: str,
        target_path: str,
        patch_text: str,
        base_sha256: str,
        runtime_gates: dict[str, Any],
        audit_context: dict[str, Any],
        preview_summary: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not str(target_path or "").strip():
            raise ValueError("TRIAGE_TARGET_PATH_REQUIRED")
        if not str(base_sha256 or "").strip():
            raise ValueError("TRIAGE_BASE_SHA_REQUIRED")

        target_ctx = self._resolve_target_context(target_path, patch_text)
        target_risk = _target_path_risk_level(target_ctx["relative_path"])
        runtime_status = _clean_text(runtime_gates.get("status")).lower()
        promote_allowed = bool((runtime_gates.get("shared_artifact_promote") or {}).get("allowed"))
        preview_conflict = bool((preview_summary or {}).get("conflict"))
        triage_id = _safe_triage_token(
            str(run_id or "").strip()
            or str(workflow_id or "").strip()
            or f"writeback-{_sha256_text(json.dumps({'target': target_ctx['relative_path'], 'base_sha256': base_sha256}, ensure_ascii=True, sort_keys=True))[:12]}"
        )

        default_decision = "allow"
        default_reason = "AI_TRIAGE_ALLOW"
        if preview_conflict:
            default_decision = "freeze"
            default_reason = "PREVIEW_CONFLICT"
        elif not target_ctx["within_repo"]:
            default_decision = "freeze"
            default_reason = "TARGET_OUTSIDE_REPO"
        elif target_risk == "high":
            default_decision = "freeze"
            default_reason = "HIGH_RISK_REQUIRES_AI_ALLOW"

        decision_source = "ai"
        try:
            ai_decision = self._call_ai_writeback_triage(
                triage_id=triage_id,
                layer=layer,
                target_path=target_ctx["relative_path"],
                base_sha256=base_sha256,
                target_risk=target_risk,
                runtime_gates=runtime_gates,
                audit_context=audit_context,
                patch_delta=target_ctx["patch_delta"],
                preview_summary=preview_summary,
                metadata=metadata,
            )
        except Exception as exc:
            fallback_decision = "freeze"
            ai_decision = {
                "decision": fallback_decision,
                "reason": f"AI_TRIAGE_UNAVAILABLE:{_clean_text(exc)}",
                "confidence": 0.0,
                "raw": {},
            }
            decision_source = "fail_closed"

        final_decision = ai_decision["decision"]
        final_reason = ai_decision["reason"]
        confidence = float(ai_decision.get("confidence") or 0.0)

        if default_decision == "freeze" and default_reason in {"PREVIEW_CONFLICT", "TARGET_OUTSIDE_REPO"}:
            final_decision = "freeze"
            final_reason = default_reason
            decision_source = "policy_override"
        elif target_risk == "high":
            min_confidence = float(os.environ.get("PROMOTE_PREP_HIGH_RISK_ALLOW_CONFIDENCE") or DEFAULT_HIGH_RISK_ALLOW_CONFIDENCE)
            if final_decision != "allow":
                final_decision = "freeze"
                final_reason = "HIGH_RISK_REQUIRES_AI_ALLOW"
                decision_source = "policy_override"
            elif confidence < min_confidence:
                final_decision = "freeze"
                final_reason = "HIGH_RISK_CONFIDENCE_TOO_LOW"
                decision_source = "policy_override"

        payload = {
            "triage_record_id": _triage_record_id(triage_id, f"{layer}-writeback"),
            "triage_id": triage_id,
            "run_id": run_id,
            "workflow_id": workflow_id,
            "layer": layer,
            "target_path": target_path,
            "resolved_target_path": target_ctx["resolved_path"],
            "relative_target_path": target_ctx["relative_path"],
            "base_sha256": base_sha256,
            "patch_hash": _sha256_text(patch_text),
            "target_exists": target_ctx["exists"],
            "target_in_repo": target_ctx["within_repo"],
            "target_risk": target_risk,
            "runtime_gate_status": runtime_status,
            "shared_artifact_promote_allowed": promote_allowed,
            "preview_conflict": preview_conflict,
            "patch_delta": target_ctx["patch_delta"],
            "decision": final_decision,
            "reason": final_reason,
            "confidence": confidence,
            "auto_commit": final_decision == "allow",
            "decision_source": decision_source,
            "ai_result": ai_decision,
            "metadata": metadata or {},
            "triaged_at": _now_iso(),
        }
        _atomic_write_json(self._triage_path(triage_id, f"{layer}-writeback"), payload)
        return payload

    def prepare_status_note_promote(self, *, run_id: str, runtime_gates: dict[str, Any], audit_context: dict[str, Any]) -> dict[str, Any]:
        self._assert_doc22_promote_allowed(layer="status-note")
        self._validate_status_note_prereqs(run_id)
        current_text = self._progress_doc_text()
        bundle = self._load_bundle(run_id)
        shadow_snapshot = self._shadow_snapshot(run_id)
        target_anchor = "current-writeback-detail"
        idempotency_key = _promote_idempotency_key(run_id, "status-note", target_anchor)
        request_id = _promote_request_id(run_id, "status-note", target_anchor)
        semantic_fingerprint = _bundle_semantic_fingerprint(bundle, layer="status-note", target_anchor=target_anchor)
        patch_timestamp = _stable_patch_timestamp(bundle)
        if semantic_fingerprint in current_text:
            return {"run_id": run_id, "layer": "status-note", "target_path": CURRENT_PROGRESS_DOC, "target_anchor": target_anchor, "base_sha256": _sha256_text(current_text), "patch_text": current_text, "idempotency_key": idempotency_key, "request_id": request_id, "shadow_snapshot": shadow_snapshot, "semantic_fingerprint": semantic_fingerprint, "skip_commit": True, "skip_reason": "SEMANTIC_FINGERPRINT_ALREADY_PRESENT"}
        if _run_id_in_layer_scope(run_id, current_text, "status-note"):
            return {"run_id": run_id, "layer": "status-note", "target_path": CURRENT_PROGRESS_DOC, "target_anchor": target_anchor, "base_sha256": _sha256_text(current_text), "patch_text": current_text, "idempotency_key": idempotency_key, "request_id": request_id, "shadow_snapshot": shadow_snapshot, "semantic_fingerprint": semantic_fingerprint, "skip_commit": True, "skip_reason": "RUN_ID_ALREADY_PRESENT"}
        patch_text = _anchor_insert_before_heading(current_text, _build_status_note_markdown(run_id=run_id, bundle=bundle, runtime_gates=runtime_gates, audit_context=audit_context, semantic_fingerprint=semantic_fingerprint, patch_timestamp=patch_timestamp, target_anchor=target_anchor), "## 4.")
        return {"run_id": run_id, "layer": "status-note", "target_path": CURRENT_PROGRESS_DOC, "target_anchor": target_anchor, "base_sha256": _sha256_text(current_text), "patch_text": patch_text, "idempotency_key": idempotency_key, "request_id": request_id, "shadow_snapshot": shadow_snapshot, "semantic_fingerprint": semantic_fingerprint, "skip_commit": False, "skip_reason": None}

    def prepare_current_layer_promote(self, *, run_id: str, enabled: bool, runtime_gates: dict[str, Any], audit_context: dict[str, Any]) -> dict[str, Any]:
        self._assert_doc22_promote_allowed(layer="current-layer")
        current_text = self._progress_doc_text()
        target_anchor = "2.1|2.3|4.5"
        idempotency_key = _promote_idempotency_key(run_id, "current-layer", target_anchor)
        request_id = _promote_request_id(run_id, "current-layer", target_anchor)
        if not enabled:
            return {"run_id": run_id, "layer": "current-layer", "target_path": CURRENT_PROGRESS_DOC, "target_anchor": target_anchor, "base_sha256": _sha256_text(current_text), "patch_text": current_text, "idempotency_key": idempotency_key, "request_id": request_id, "shadow_snapshot": None, "semantic_fingerprint": None, "skip_commit": True, "skip_reason": "CURRENT_LAYER_PROMOTE_DISABLED"}
        runtime_status = _clean_text(runtime_gates.get("status")).lower()
        promote_allowed = bool((runtime_gates.get("shared_artifact_promote") or {}).get("allowed"))
        if runtime_status != "ready" or not promote_allowed:
            return {"run_id": run_id, "layer": "current-layer", "target_path": CURRENT_PROGRESS_DOC, "target_anchor": target_anchor, "base_sha256": _sha256_text(current_text), "patch_text": current_text, "idempotency_key": idempotency_key, "request_id": request_id, "shadow_snapshot": None, "semantic_fingerprint": None, "skip_commit": True, "skip_reason": "CURRENT_LAYER_PROMOTE_BLOCKED"}
        bundle = self._load_bundle(run_id)
        shadow_snapshot = self._shadow_snapshot(run_id)
        patch_timestamp = _stable_patch_timestamp(bundle)
        semantic_fingerprint = _bundle_semantic_fingerprint(bundle, layer="current-layer", target_anchor=target_anchor)
        if _run_id_in_layer_scope(run_id, current_text, "current-layer"):
            return {"run_id": run_id, "layer": "current-layer", "target_path": CURRENT_PROGRESS_DOC, "target_anchor": target_anchor, "base_sha256": _sha256_text(current_text), "patch_text": current_text, "idempotency_key": idempotency_key, "request_id": request_id, "shadow_snapshot": shadow_snapshot, "semantic_fingerprint": semantic_fingerprint, "skip_commit": True, "skip_reason": "RUN_ID_ALREADY_PRESENT"}
        if semantic_fingerprint in current_text:
            return {"run_id": run_id, "layer": "current-layer", "target_path": CURRENT_PROGRESS_DOC, "target_anchor": target_anchor, "base_sha256": _sha256_text(current_text), "patch_text": current_text, "idempotency_key": idempotency_key, "request_id": request_id, "shadow_snapshot": shadow_snapshot, "semantic_fingerprint": semantic_fingerprint, "skip_commit": True, "skip_reason": "CURRENT_LAYER_SEMANTIC_FINGERPRINT_ALREADY_PRESENT"}
        findings = list(bundle.get("findings") or [])
        finding_count = int(bundle.get("finding_count") or len(findings))
        highest_risk_level = _risk_summary(bundle)["highest_risk"]
        top_issue_keys = [_clean_text(item.get("issue_key")) for item in findings[:6]]
        public_runtime_status = _clean_text(audit_context.get("public_runtime_status"))
        try:
            patch_text = _replace_markdown_section(current_text, ANCHOR_21, _render_current_layer_21(run_id=run_id, target_anchor="2.1", patch_timestamp=patch_timestamp, semantic_fingerprint=semantic_fingerprint, finding_count=finding_count, runtime_status=runtime_status, public_runtime_status=public_runtime_status, highest_risk_level=highest_risk_level))
            patch_text = _replace_markdown_section(patch_text, ANCHOR_23, _render_current_layer_23(run_id=run_id, target_anchor="2.3", patch_timestamp=patch_timestamp, semantic_fingerprint=semantic_fingerprint, finding_count=finding_count, top_issue_keys=top_issue_keys, runtime_status=runtime_status, promote_allowed=promote_allowed, public_runtime_status=public_runtime_status))
            patch_text = _replace_markdown_section(patch_text, ANCHOR_45, _render_current_layer_45(run_id=run_id, target_anchor="4.5", patch_timestamp=patch_timestamp, semantic_fingerprint=semantic_fingerprint, finding_count=finding_count, highest_risk_level=highest_risk_level))
        except ValueError:
            return {"run_id": run_id, "layer": "current-layer", "target_path": CURRENT_PROGRESS_DOC, "target_anchor": target_anchor, "base_sha256": _sha256_text(current_text), "patch_text": current_text, "idempotency_key": idempotency_key, "request_id": request_id, "shadow_snapshot": shadow_snapshot, "semantic_fingerprint": semantic_fingerprint, "skip_commit": True, "skip_reason": "CURRENT_LAYER_ANCHOR_NOT_FOUND"}
        return {"run_id": run_id, "layer": "current-layer", "target_path": CURRENT_PROGRESS_DOC, "target_anchor": target_anchor, "base_sha256": _sha256_text(current_text), "patch_text": patch_text, "idempotency_key": idempotency_key, "request_id": request_id, "shadow_snapshot": shadow_snapshot, "semantic_fingerprint": semantic_fingerprint, "skip_commit": patch_text == current_text, "skip_reason": "CURRENT_LAYER_NO_CHANGE" if patch_text == current_text else None}

    def write_shadow_sync(self, *, run_id: str, summary_markdown: str, findings_bundle: dict[str, Any], logical_target: str = "issue_mesh_shadow", candidate_writeback_markdown: str | None = None) -> dict[str, Any]:
        run_root = self.config.shadow_root / run_id
        run_root.mkdir(parents=True, exist_ok=True)
        normalized_bundle = _sort_findings(findings_bundle)
        summary_path = run_root / "summary.md"
        bundle_path = run_root / CANONICAL_BUNDLE_NAME
        legacy_bundle_path = run_root / LEGACY_BUNDLE_NAME
        candidate_path = run_root / "candidate_writeback.md"
        blocks_path = run_root / "candidate_blocks.json"
        metadata_path = run_root / "metadata.json"
        _atomic_write_text(summary_path, summary_markdown)
        _atomic_write_json(bundle_path, normalized_bundle)
        _atomic_write_json(legacy_bundle_path, normalized_bundle)
        _atomic_write_text(candidate_path, candidate_writeback_markdown or _render_candidate_writeback(normalized_bundle))
        _atomic_write_json(blocks_path, _render_candidate_blocks(normalized_bundle, logical_target))
        _atomic_write_json(metadata_path, {"run_id": run_id, "generated_at": _now_iso(), "summary_path": str(summary_path), "bundle_path": str(bundle_path), "legacy_bundle_path": str(legacy_bundle_path), "candidate_writeback_path": str(candidate_path), "candidate_blocks_path": str(blocks_path)})
        return {"run_id": run_id, "shadow_root": str(run_root), "summary_path": str(summary_path), "bundle_path": str(bundle_path), "legacy_bundle_path": str(legacy_bundle_path), "candidate_writeback_path": str(candidate_path), "candidate_blocks_path": str(blocks_path)}

    def submit_intent(self, *, run_id: str, summary_markdown: str, findings_bundle: dict[str, Any], logical_target: str, candidate_writeback_markdown: str | None = None) -> dict[str, Any]:
        normalized_bundle = _sort_findings(findings_bundle)
        intent_id = str(uuid4())
        dedupe_key = _intent_dedupe_key(normalized_bundle, logical_target)
        existing_intent_id = self._backend.claim_dedupe(dedupe_key, intent_id)
        payload = {"intent_id": intent_id, "run_id": run_id, "logical_target": logical_target, "dedupe_key": dedupe_key, "status": "queued" if existing_intent_id is None else "superseded", "summary_markdown": summary_markdown, "findings_bundle": normalized_bundle, "candidate_writeback_markdown": candidate_writeback_markdown, "created_at": _now_iso(), "updated_at": _now_iso(), "shadow_paths": None, "superseded_by": existing_intent_id}
        self._save_intent(payload)
        self._record_status(str(payload["status"]))
        if existing_intent_id is None:
            self._backend.enqueue(intent_id)
        return payload

    def _consume_loop(self) -> None:
        while not self._stop_event.is_set():
            intent_id = self._backend.pop(self.config.consumer_poll_seconds)
            if not intent_id:
                continue
            try:
                self._process_intent(intent_id)
            except Exception as exc:
                payload = self.get_intent(intent_id)
                self._set_status(payload, "rejected", error=str(exc))
                self._record_failure(str(exc))
                self._save_intent(payload)

    def _process_intent(self, intent_id: str) -> None:
        payload = self.get_intent(intent_id)
        if payload["status"] != "queued":
            return
        self._record_queue_lag(payload.get("created_at"))
        self._set_status(payload, "merged")
        self._save_intent(payload)
        lease_key = str(payload["logical_target"])
        lease_token = str(uuid4())
        if not self._backend.acquire_lease(lease_key, lease_token):
            self._set_status(payload, "rejected", error="LEASE_NOT_ACQUIRED")
            self._record_failure("LEASE_NOT_ACQUIRED")
            self._save_intent(payload)
            return
        try:
            self._set_status(payload, "writing_shadow")
            self._save_intent(payload)
            payload["shadow_paths"] = self.write_shadow_sync(run_id=str(payload["run_id"]), summary_markdown=str(payload["summary_markdown"]), findings_bundle=dict(payload["findings_bundle"]), logical_target=str(payload.get("logical_target") or "issue_mesh_shadow"), candidate_writeback_markdown=payload.get("candidate_writeback_markdown"))
            self._set_status(payload, "written")
            self._save_intent(payload)
        finally:
            self._backend.release_lease(lease_key, lease_token)

    # ── Phase 1: Code-fix pipeline methods ─────────────────────────────

    def synthesize_code_fix_patches(
        self,
        *,
        source_run_id: str,
        fix_run_id: str,
        max_fix_items: int = 10,
        runtime_gates: dict[str, Any] | None = None,
        audit_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Extract fixable findings from a completed readonly mesh run and
        use the LLM gateway (New API) to synthesize concrete code patches.

        Returns a dict with ``patches`` list and metadata.
        """
        bundle = self._load_bundle_for_synthesis(source_run_id)
        target_issue_key = (audit_context or {}).get("issue_key") or (audit_context or {}).get("family")
        fixable = [
            f for f in (bundle.get("findings") or [])
            if isinstance(f, dict)
            and str(f.get("handling_path") or "").lower() in {"fix_code", "fix_then_rebuild"}
            and (not target_issue_key or str(f.get("issue_key") or "") == target_issue_key)
        ][:max_fix_items]

        if not fixable:
            all_findings = bundle.get("findings") or []
            reason = "NO_FINDINGS_IN_BUNDLE" if not all_findings else "NO_FIXABLE_FINDINGS"
            return {
                "fix_run_id": fix_run_id,
                "source_run_id": source_run_id,
                "patch_count": 0,
                "patches": [],
                "skip_reason": reason,
            }

        patches: list[dict[str, Any]] = []
        for finding in fixable:
            issue_key = _clean_text(finding.get("issue_key"))
            recommended_action = _clean_text(finding.get("recommended_action"))
            evidence_refs = finding.get("evidence_refs") or []
            ssot_refs = finding.get("ssot_refs") or []

            # Read referenced files for context
            file_contexts: list[dict[str, str]] = []
            loaded_paths: set[str] = set()
            for ref in evidence_refs[:5]:
                resolved_ref = _resolve_repo_evidence_ref(self.config.repo_root, str(ref))
                if resolved_ref is None:
                    continue
                normalized_ref, ref_path = resolved_ref
                if normalized_ref in loaded_paths:
                    continue
                if ref_path.exists() and ref_path.stat().st_size < 50_000:
                    file_contexts.append({
                        "path": normalized_ref,
                        "content": ref_path.read_text(encoding="utf-8", errors="replace")[:8000],
                    })
                    loaded_paths.add(normalized_ref)

            # Step 1.1: Build candidate target files for the prompt so the
            # LLM knows which concrete files it should modify.
            candidate_targets: list[str] = []
            _seen_candidates: set[str] = set()
            for _src in (
                [str(item.get("path") or "") for item in file_contexts],
                _extract_affected_files_from_finding(finding),
            ):
                for _cand in _src:
                    _norm_cand = _normalize_repo_rel_path(_cand)
                    if (
                        _norm_cand
                        and _norm_cand not in _seen_candidates
                        and any(_path_matches_prefix(_norm_cand, pfx) for pfx in CODE_FIX_ALLOWED_PREFIXES)
                    ):
                        _seen_candidates.add(_norm_cand)
                        candidate_targets.append(_norm_cand)

            # Load candidate target files into file_contexts so the LLM has
            # the current content of the files it needs to modify.
            for _ct in candidate_targets[:3]:
                if _ct in loaded_paths:
                    continue
                _ct_path = (self.config.repo_root / _ct).resolve()
                if _ct_path.exists() and _ct_path.is_file() and _ct_path.stat().st_size < 80_000:
                    file_contexts.append({
                        "path": _ct,
                        "content": _ct_path.read_text(encoding="utf-8", errors="replace")[:12000],
                    })
                    loaded_paths.add(_ct)

            prompt_payload = {
                "fix_run_id": fix_run_id,
                "issue_key": issue_key,
                "recommended_action": recommended_action,
                "evidence_refs": evidence_refs,
                "ssot_refs": ssot_refs,
                "file_contexts": file_contexts,
                "candidate_targets": candidate_targets,
                "constraints": [
                    "Only modify files under app/, tests/, automation/, or scripts/.",
                    "target_path MUST be a concrete repo-relative file path (e.g. app/services/xxx.py). Do NOT leave target_path empty.",
                    "patch_text must be the complete final content of the target file after the fix.",
                    "Do not output unified diff, git diff, apply_patch format, or fenced code blocks.",
                    "If creating a new file, use a repo-compatible path and provide the full new file content.",
                    "Produce minimal, focused changes.",
                    "Do not modify docs/core/22 or official shared artifacts.",
                    "Output JSON with keys: target_path, patch_text, explanation.",
                    "Choose target_path from candidate_targets when possible.",
                ],
            }
            prompt = (
                "你是代码修复工程师。根据以下 issue-mesh 发现，生成最小化的代码修复结果。\n"
                "只输出 JSON，格式: {\"target_path\": \"app/...\", \"patch_text\": \"目标文件修复后的完整内容\", \"explanation\": \"...\"}\n"
                "patch_text 必须是目标文件修复后的完整文本，不得输出 diff、apply_patch、git patch、@@ 块或代码围栏。\n"
                "如果无法生成有效结果，输出: {\"target_path\": \"\", \"patch_text\": \"\", \"explanation\": \"reason\"}\n\n"
                f"```json\n{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}\n```"
            )
            patch_data: dict[str, Any] = {}
            last_synth_error: str = ""
            for _attempt in range(3):
                try:
                    ai_result = self._run_ai_triage_prompt(
                        prompt=prompt,
                        system_prompt="你是精确的代码修复工程师。只输出 JSON，不要输出解释性文字。",
                    )
                    patch_data = _extract_json_dict_from_text(
                        ai_result.get("reason") or json.dumps(ai_result.get("raw") or {})
                    )
                    if not patch_data:
                        patch_data = ai_result.get("raw") or {}
                    last_synth_error = ""
                    break
                except Exception as exc:
                    last_synth_error = _clean_text(exc)
                    import time as _time
                    _time.sleep(min(2 ** _attempt, 8))
            if last_synth_error:
                patch_data = {
                    "target_path": "",
                    "patch_text": "",
                    "explanation": f"AI_SYNTHESIS_FAILED_AFTER_RETRIES: {last_synth_error}",
                }

            # Step 1.2: Post-synthesis target_path fallback resolution.
            # If the LLM returned empty target_path but provided patch_text,
            # try to infer the target from the explanation or file_contexts.
            raw_synth_target = str(patch_data.get("target_path") or "").strip()
            raw_synth_patch = str(patch_data.get("patch_text") or "").strip()
            if not raw_synth_target and raw_synth_patch:
                raw_synth_target = _infer_target_path_fallback(
                    explanation=str(patch_data.get("explanation") or ""),
                    file_contexts=file_contexts,
                    candidate_targets=candidate_targets,
                )

            target_path, patch_text, explanation, base_sha256 = self._normalize_synthesized_code_patch(
                raw_target_path=raw_synth_target,
                raw_patch_text=raw_synth_patch,
                explanation=str(patch_data.get("explanation") or "").strip(),
                file_contexts=file_contexts,
            )

            patches.append({
                "issue_key": issue_key,
                "target_path": target_path,
                "patch_text": patch_text,
                "base_sha256": base_sha256,
                "explanation": explanation,
                "valid": bool(target_path and patch_text),
            })

        valid_patches = [p for p in patches if p["valid"]]
        # Persist patch manifest
        manifest_dir = self.config.runtime_root / "code_fix" / fix_run_id
        manifest_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(manifest_dir / "patches.json", {
            "fix_run_id": fix_run_id,
            "source_run_id": source_run_id,
            "generated_at": _now_iso(),
            "patch_count": len(valid_patches),
            "total_findings": len(fixable),
            "patches": patches,
        })

        return {
            "fix_run_id": fix_run_id,
            "source_run_id": source_run_id,
            "patch_count": len(valid_patches),
            "total_findings": len(fixable),
            "patches": patches,
            "skip_reason": None,
        }

    def run_scoped_pytest(
        self,
        *,
        fix_run_id: str,
        changed_files: list[str],
        timeout_seconds: int = 120,
    ) -> dict[str, Any]:
        """Run pytest scoped to test files related to the changed files.

        Returns a dict with pass/fail status and output.
        """
        if not changed_files:
            return {
                "fix_run_id": fix_run_id,
                "passed": None,
                "test_count": 0,
                "failures": [],
                "stdout_tail": "",
                "skip_reason": "NO_CHANGED_FILES",
                "verify_status": "not_verified",
            }

        # Infer related test files from changed paths
        test_files: list[str] = []
        tests_dir = self.config.repo_root / "tests"
        for changed in changed_files:
            normalized = _normalize_repo_rel_path(changed)
            # If the changed file is itself a test file, include it
            if normalized.startswith("tests/"):
                full = self.config.repo_root / normalized
                if full.exists():
                    test_files.append(normalized)
                continue
            # Infer test file from app/ path
            if normalized.startswith("app/"):
                # app/services/foo.py -> tests/test_foo.py
                stem = Path(normalized).stem
                candidates = [
                    f"tests/test_{stem}.py",
                    f"tests/test_{stem.replace('_ssot', '')}.py",
                ]
                for candidate in candidates:
                    full = self.config.repo_root / candidate
                    if full.exists():
                        test_files.append(candidate)
            # Also try pattern matching
            if normalized.startswith(("app/", "automation/")):
                stem = Path(normalized).stem
                for test_path in tests_dir.glob(f"test_*{stem}*.py"):
                    rel = test_path.relative_to(self.config.repo_root).as_posix()
                    if rel not in test_files:
                        test_files.append(rel)

        # Deduplicate and limit
        seen: set[str] = set()
        unique_tests: list[str] = []
        for tf in test_files:
            if tf not in seen:
                seen.add(tf)
                unique_tests.append(tf)
        test_files = unique_tests[:20]

        if not test_files:
            return {
                "fix_run_id": fix_run_id,
                "passed": None,
                "test_count": 0,
                "failures": [],
                "stdout_tail": "",
                "skip_reason": "NO_RELATED_TESTS_FOUND",
                "verify_status": "not_verified",
            }

        cmd = [
            "python", "-m", "pytest",
            "--tb=short", "-q", "--no-header",
        ] + test_files

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                cwd=str(self.config.repo_root),
                timeout=timeout_seconds,
            )
            passed = result.returncode == 0
            stdout_tail = (result.stdout or "")[-2000:]
            stderr_tail = (result.stderr or "")[-500:]
        except subprocess.TimeoutExpired:
            passed = False
            stdout_tail = "PYTEST_TIMEOUT"
            stderr_tail = ""
        except Exception as exc:
            passed = False
            stdout_tail = f"PYTEST_EXEC_ERROR: {exc}"
            stderr_tail = ""

        # Parse failure count from pytest output
        failures: list[str] = []
        test_count = 0
        for line in stdout_tail.splitlines():
            if "passed" in line or "failed" in line:
                import re as _re
                passed_match = _re.search(r"(\d+) passed", line)
                failed_match = _re.search(r"(\d+) failed", line)
                if passed_match:
                    test_count += int(passed_match.group(1))
                if failed_match:
                    fail_count = int(failed_match.group(1))
                    test_count += fail_count
                    failures.append(f"{fail_count} tests failed")
            elif line.startswith("FAILED "):
                failures.append(line.strip())

        # Persist result
        result_dir = self.config.runtime_root / "code_fix" / fix_run_id
        result_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(result_dir / "pytest_result.json", {
            "fix_run_id": fix_run_id,
            "passed": passed,
            "test_count": test_count,
            "test_files": test_files,
            "failures": failures,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
            "ran_at": _now_iso(),
        })

        return {
            "fix_run_id": fix_run_id,
            "passed": passed,
            "test_count": test_count,
            "test_files": test_files,
            "failures": failures,
            "stdout_tail": stdout_tail,
            "skip_reason": None,
        }
