#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from scripts import queue22_system_fix_wave


MONITOR_ROOT = Path("output/task_runs") / queue22_system_fix_wave.RUN_LABEL / "controller04_monitor"
STATUS_ROOT = MONITOR_ROOT / "status"
SUPPORT_ROOT = MONITOR_ROOT / "support_runs"
WAVES_ROOT = MONITOR_ROOT / "waves"
ACTIVE_MONITOR_PATH = MONITOR_ROOT / "active_monitor.json"
DEFAULT_POLL_SECONDS = 60
DEFAULT_STATUS_INTERVAL_SECONDS = 60
DEFAULT_SUPPORT_INTERVAL_SECONDS = 90
LOCK_LEASE_HOURS = 4
FRESH_HEARTBEAT_SECONDS = 5 * 60
EXECUTOR_REQUIRED_FILES = [
    "preflight.json",
    "before_state.json",
    "process.json",
    "progress.json",
    "recovery.stdout.log",
    "after_state.json",
    "result.json",
]
WATCHDOG_REQUIRED_FILES = [
    "watchdog.jsonl",
    "watchdog_summary.json",
]
RUNTIME_REARM_CHOICES = ("executor", "watchdog", "both")
RUNTIME_REARM_TASKS = {
    "executor": ["111003"],
    "watchdog": ["111004"],
    "both": ["111003", "111004"],
}
SUPPORT_TASK_TO_SLOT = {
    "slot04-issue-miner": "04",
    "slot05-prompt-task-auditor": "05",
    "slot06-claim-resource-guard": "06",
    "slot07-runtime-evidence-verifier": "07",
}


def repo_root() -> Path:
    return _repo_root


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def timestamp_token() -> str:
    return datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False))
        handle.write("\n")


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _strip_ticks(raw: str) -> str:
    value = raw.strip()
    if value.startswith("`") and value.endswith("`") and len(value) >= 2:
        return value[1:-1]
    return value


def _field(text: str, name: str) -> str:
    match = re.search(rf"^- `{re.escape(name)}`:\s*(.*)$", text, flags=re.MULTILINE)
    if not match:
        return ""
    return _strip_ticks(match.group(1))


def _replace_field(text: str, name: str, value: str) -> str:
    pattern = re.compile(rf"(^- `{re.escape(name)}`:\s*)(.*)$", flags=re.MULTILINE)
    replacement = lambda match: f"{match.group(1)}`{value}`"
    updated, count = pattern.subn(replacement, text, count=1)
    if count:
        return updated
    suffix = "" if text.endswith("\n") else "\n"
    return f"{text}{suffix}- `{name}`: `{value}`\n"


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _latest_file(root: Path, pattern: str) -> Path | None:
    matches = sorted(root.glob(pattern))
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime)


def _parse_iso(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _age_seconds(value: str) -> float | None:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    return max(0.0, (datetime.now().astimezone() - parsed).total_seconds())


def _is_fresh(value: str, freshness_seconds: int = FRESH_HEARTBEAT_SECONDS) -> bool:
    age = _age_seconds(value)
    return age is not None and age <= freshness_seconds


def _normalize_relpath(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _resolved_attempt_dir(root: Path, attempt_run_dir: str) -> Path | None:
    if not attempt_run_dir:
        return None
    candidate = (root / attempt_run_dir).resolve()
    return candidate if candidate.exists() else None


def _json_changed(before: Any, after: Any) -> bool:
    if before in (None, {}) or after in (None, {}):
        return False
    return json.dumps(before, ensure_ascii=False, sort_keys=True) != json.dumps(
        after, ensure_ascii=False, sort_keys=True
    )


def _markdown_fields(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in text.splitlines():
        match = re.match(r"^- `([^`]+)`:\s*(.*)$", line.strip())
        if match:
            fields[match.group(1)] = _strip_ticks(match.group(2))
    return fields


def _bool_text(value: str) -> bool:
    lowered = str(value or "").strip().lower().rstrip(".")
    return lowered.startswith("true") or lowered in {"yes", "valid"}


def _has_named_violation(value: str) -> bool:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return False
    return not (
        lowered.startswith("none")
        or lowered.startswith("no ")
        or "none observed" in lowered
        or "no ordinary backlog task" in lowered
    )


def _task_filename_with_owner_status(path: Path, *, owner: str, status: str) -> Path:
    updated_name = re.sub(
        r"__owner-.*__status-[^.]+\.md$",
        f"__owner-{owner}__status-{status}.md",
        path.name,
    )
    return path.with_name(updated_name)


def _refresh_controller_lock(root: Path) -> None:
    path = root / queue22_system_fix_wave.ACTIVE_LOCK
    text = _read_text(path)
    heartbeat_at = now_iso()
    lease_until = datetime.fromisoformat(heartbeat_at).replace(microsecond=0) + timedelta(hours=LOCK_LEASE_HOURS)
    updated = _replace_field(text, "heartbeat_at", heartbeat_at)
    updated = _replace_field(updated, "lease_until", lease_until.isoformat())
    if updated != text:
        path.write_text(updated, encoding="utf-8")


def _write_active_monitor(root: Path, args: argparse.Namespace, support_state: dict[str, Any] | None) -> None:
    payload = {
        "pid": os.getpid(),
        "run_label": queue22_system_fix_wave.RUN_LABEL,
        "controller_id": queue22_system_fix_wave.CONTROLLER_ID,
        "heartbeat_at": now_iso(),
        "providers": list(args.providers),
        "max_workers": args.max_workers,
        "poll_seconds": args.poll_seconds,
        "status_interval_seconds": args.status_interval_seconds,
        "support_interval_seconds": args.support_interval_seconds,
        "wave_timeout_seconds": args.wave_timeout_seconds,
        "skip_support": bool(args.skip_support),
        "active_support_pid": support_state["process"].pid if support_state is not None else None,
    }
    _write_json(root / ACTIVE_MONITOR_PATH, payload)


def _task_snapshot(root: Path, task_id: str, control_state: str) -> dict[str, Any]:
    relpath = queue22_system_fix_wave._resolve_queue_task_file(root, task_id)
    path = root / relpath
    text = _read_text(path)
    status = _field(text, "status")
    control_state_required = _field(text, "control_state_required")
    heartbeat_at = _field(text, "heartbeat_at")
    attempt_run_dir = _field(text, "attempt_run_dir")
    file_state_claimable = status == "todo"
    controller_gate_allows = bool(control_state_required) and control_state == control_state_required
    resolved_attempt_dir = _resolved_attempt_dir(root, attempt_run_dir)
    return {
        "task_id": task_id,
        "path": relpath,
        "status": status,
        "owner": _field(text, "owner"),
        "run_label": _field(text, "run_label"),
        "claim_mode": _field(text, "claim_mode"),
        "control_state_required": control_state_required,
        "controller_gate_allows": controller_gate_allows,
        "file_state_claimable": file_state_claimable,
        "claimable_now": file_state_claimable and controller_gate_allows,
        "claim_token": _field(text, "claim_token"),
        "claim_time": _field(text, "claim_time"),
        "lease_until": _field(text, "lease_until"),
        "heartbeat_at": heartbeat_at,
        "heartbeat_age_seconds": _age_seconds(heartbeat_at),
        "heartbeat_fresh_under_5m": _is_fresh(heartbeat_at),
        "attempt_run_dir": attempt_run_dir,
        "task_run_dir": _field(text, "task_run_dir"),
        "resolved_attempt_dir": _normalize_relpath(resolved_attempt_dir, root) if resolved_attempt_dir else None,
        "current_state": _field(text, "current_state"),
        "blocked_reason": _field(text, "blocked_reason"),
        "review_status": _field(text, "review_status"),
    }


def _lock_snapshot(root: Path) -> dict[str, Any]:
    path = root / queue22_system_fix_wave.ACTIVE_LOCK
    text = _read_text(path)
    return {
        "path": queue22_system_fix_wave.ACTIVE_LOCK,
        "controller_id": _field(text, "controller_id"),
        "run_label": _field(text, "run_label"),
        "status": _field(text, "status"),
        "control_state": _field(text, "control_state"),
        "control_state_reason": _field(text, "control_state_reason"),
        "heartbeat_at": _field(text, "heartbeat_at"),
        "lease_until": _field(text, "lease_until"),
    }


def _slot_board_snapshot(root: Path) -> dict[str, Any]:
    relpath = "docs/_temp/problem/SLOT_OCCUPANCY.md"
    path = root / relpath
    text = _read_text(path)
    return {
        "path": relpath,
        "run_label": _field(text, "run_label"),
        "controller_id": _field(text, "controller_id"),
        "control_state": _field(text, "control_state"),
        "updated_at": _field(text, "updated_at"),
        "prompt_authority": _field(text, "prompt_authority"),
    }


def _latest_support_summary(root: Path) -> tuple[Path | None, dict[str, Any]]:
    summary_path = _latest_file(root / queue22_system_fix_wave.WAVE_EXPORT_ROOT / "support", "*/summary.json")
    if summary_path is None:
        return None, {}
    payload = _load_json(summary_path) or {}
    return summary_path, payload


def _support_summary_paths(root: Path) -> list[Path]:
    return sorted(
        (root / queue22_system_fix_wave.WAVE_EXPORT_ROOT / "support").glob("*/summary.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def _latest_support_wave(root: Path) -> dict[str, Any] | None:
    summary_path, payload = _latest_support_summary(root)
    if summary_path is None:
        return None
    return {
        "path": summary_path.relative_to(root).as_posix(),
        "run_id": payload.get("run_id"),
        "success": payload.get("success"),
        "started_at": payload.get("started_at"),
        "finished_at": payload.get("finished_at"),
        "task_count": payload.get("task_count"),
    }


def _latest_issue_findings(root: Path) -> dict[str, Any] | None:
    findings_path = _latest_file(root / queue22_system_fix_wave.ISSUE_MINER_EXPORT_ROOT, "*/findings.json")
    if findings_path is None:
        return None
    payload = _load_json(findings_path) or {}
    findings = payload.get("findings") or []
    return {
        "path": findings_path.relative_to(root).as_posix(),
        "session_id": payload.get("session_id"),
        "provider_id": payload.get("provider_id"),
        "generated_at": payload.get("generated_at"),
        "finding_count": len(findings),
    }


def _current_support_wave(root: Path) -> dict[str, Any] | None:
    payload = _load_json(root / MONITOR_ROOT / "current_support_run.json")
    return payload if isinstance(payload, dict) and payload else None


def _support_task_results(root: Path) -> dict[str, dict[str, Any]]:
    _, payload = _latest_support_summary(root)
    results: dict[str, dict[str, Any]] = {}
    for item in payload.get("tasks") or []:
        if isinstance(item, dict) and item.get("task_id"):
            results[str(item["task_id"])] = item
    return results


def _latest_task_message(root: Path, task_id: str) -> dict[str, Any] | None:
    for summary_path in _support_summary_paths(root):
        payload = _load_json(summary_path) or {}
        tasks = payload.get("tasks") or []
        task_payload = next((item for item in tasks if item.get("task_id") == task_id), None)
        if not isinstance(task_payload, dict):
            continue
        selected_provider = task_payload.get("selected_provider")
        attempts = list(task_payload.get("attempts") or [])
        if selected_provider:
            attempts.sort(key=lambda attempt: 0 if attempt.get("provider") == selected_provider else 1)
        for attempt in attempts:
            last_message_path = Path(str(attempt.get("last_message_path") or ""))
            if not last_message_path.exists():
                continue
            text = _read_text(last_message_path)
            if text.strip():
                return {
                    "provider_id": attempt.get("provider"),
                    "path": _normalize_relpath(last_message_path, root),
                    "summary_path": _normalize_relpath(summary_path, root),
                    "text": text,
                }
    return None


def _latest_verifier_verdict(root: Path) -> dict[str, Any] | None:
    message = _latest_task_message(root, "slot07-runtime-evidence-verifier")
    if message is None:
        return None
    fields = _markdown_fields(message["text"])
    return {
        "provider_id": message["provider_id"],
        "path": message["path"],
        "executor_artifact_completeness": fields.get("executor_artifact_completeness", ""),
        "watchdog_artifact_completeness": fields.get("watchdog_artifact_completeness", ""),
        "before_after_state_delta": fields.get("before_after_state_delta", ""),
        "progress_authenticity": fields.get("progress_authenticity", ""),
        "can_enter_recovery_executing": fields.get("can_enter_recovery_executing", ""),
        "can_enter_promote_ready": fields.get("can_enter_promote_ready", ""),
        "blocking_gaps": fields.get("blocking_gaps", ""),
    }


def _latest_claim_guard(root: Path) -> dict[str, Any] | None:
    message = _latest_task_message(root, "slot06-claim-resource-guard")
    if message is None:
        return None
    fields = _markdown_fields(message["text"])
    return {
        "provider_id": message["provider_id"],
        "path": message["path"],
        "control_state": fields.get("control_state", ""),
        "active_claims": fields.get("active_claims", ""),
        "write_scope_conflicts": fields.get("write_scope_conflicts", ""),
        "resource_scope_conflicts": fields.get("resource_scope_conflicts", ""),
        "unauthorized_writers": fields.get("unauthorized_writers", ""),
        "backlog_freeze_violations": fields.get("backlog_freeze_violations", ""),
        "slot_model_violations": fields.get("slot_model_violations", ""),
        "recommended_controller_actions": fields.get("recommended_controller_actions", ""),
    }


def _executor_runtime_evidence(root: Path, task: dict[str, Any]) -> dict[str, Any]:
    attempt_dir = _resolved_attempt_dir(root, str(task.get("attempt_run_dir") or ""))
    required_files = {name: False for name in EXECUTOR_REQUIRED_FILES}
    payload: dict[str, Any] = {
        "attempt_dir": None,
        "required_files": required_files,
        "after_state_changed": False,
        "progress_present": False,
    }
    if attempt_dir is None:
        return payload
    payload["attempt_dir"] = _normalize_relpath(attempt_dir, root)
    for name in EXECUTOR_REQUIRED_FILES:
        required_files[name] = (attempt_dir / name).exists()

    before_state = _load_json(attempt_dir / "before_state.json") if required_files["before_state.json"] else None
    after_state = _load_json(attempt_dir / "after_state.json") if required_files["after_state.json"] else None
    progress = _load_json(attempt_dir / "progress.json") if required_files["progress.json"] else None
    process = _load_json(attempt_dir / "process.json") if required_files["process.json"] else None
    result = _load_json(attempt_dir / "result.json") if required_files["result.json"] else None

    payload["after_state_changed"] = _json_changed(before_state, after_state)
    payload["progress_present"] = required_files["progress.json"]
    payload["progress_heartbeat_at"] = (
        progress.get("heartbeat_at") if isinstance(progress, dict) else None
    ) or (progress.get("last_progress_at") if isinstance(progress, dict) else None)
    payload["process_started_at"] = process.get("started_at") if isinstance(process, dict) else None
    payload["result_status"] = result.get("status") if isinstance(result, dict) else None
    payload["result_blocked_reason"] = result.get("blocked_reason") if isinstance(result, dict) else None
    payload["result_completed_at"] = result.get("completed_at") if isinstance(result, dict) else None
    return payload


def _watchdog_runtime_evidence(root: Path, task: dict[str, Any]) -> dict[str, Any]:
    attempt_dir = _resolved_attempt_dir(root, str(task.get("attempt_run_dir") or ""))
    payload: dict[str, Any] = {
        "attempt_dir": None,
        "watchdog_jsonl": False,
        "watchdog_summary": None,
    }
    if attempt_dir is None:
        return payload
    payload["attempt_dir"] = _normalize_relpath(attempt_dir, root)
    payload["watchdog_jsonl"] = (attempt_dir / "watchdog.jsonl").exists()
    summary = _load_json(attempt_dir / "watchdog_summary.json")
    payload["watchdog_summary"] = summary if isinstance(summary, dict) else None
    return payload


def _evaluate_control_state(snapshot: dict[str, Any]) -> dict[str, Any]:
    tasks = snapshot["tasks"]
    executor = tasks["111003"]
    watchdog = tasks["111004"]
    promote = tasks["111000"]
    executor_evidence = snapshot["runtime_evidence"]["111003"]
    watchdog_evidence = snapshot["runtime_evidence"]["111004"]
    verifier = snapshot.get("verifier_verdict") or {}
    claim_guard = snapshot.get("claim_guard") or {}
    watchdog_summary = watchdog_evidence.get("watchdog_summary") or {}

    fresh_executor_claim = bool(executor.get("claim_token")) and executor["status"] == "doing" and executor[
        "heartbeat_fresh_under_5m"
    ]
    executor_artifacts_ready = all(
        executor_evidence["required_files"].get(name, False)
        for name in ("preflight.json", "before_state.json", "process.json", "progress.json")
    )
    watchdog_sampling_started = bool(watchdog.get("claim_token")) and watchdog["status"] == "doing" and watchdog[
        "heartbeat_fresh_under_5m"
    ] and (
        bool(watchdog_evidence.get("watchdog_jsonl"))
        or bool((watchdog_summary.get("sampling_window") or {}).get("sample_count"))
    )
    live_state_moved = bool(executor_evidence.get("after_state_changed")) or bool(
        watchdog_summary.get("can_enter_recovery_executing")
    ) or _bool_text(verifier.get("can_enter_recovery_executing", ""))
    can_enter_recovery_executing = (
        fresh_executor_claim and executor_artifacts_ready and watchdog_sampling_started and live_state_moved
    )

    can_enter_promote_ready = (
        _bool_text(verifier.get("before_after_state_delta", ""))
        and _bool_text(verifier.get("progress_authenticity", ""))
        and _bool_text(verifier.get("can_enter_promote_ready", ""))
    )
    promote_round_complete = promote["status"] in {"review", "done"} and bool(
        promote.get("attempt_run_dir") or promote.get("claim_token")
    )
    can_enter_backlog_open = can_enter_promote_ready and promote_round_complete

    blocking_signals: list[str] = []
    if executor["status"] == "blocked":
        blocking_signals.append(f"111003:{executor.get('blocked_reason') or 'blocked'}")
    if watchdog["status"] == "blocked":
        blocking_signals.append(f"111004:{watchdog.get('blocked_reason') or 'blocked'}")
    if executor.get("claim_token") and not executor_evidence["required_files"].get("progress.json", False):
        blocking_signals.append("111003:progress_json_missing")
    if watchdog.get("claim_token") and not watchdog["heartbeat_fresh_under_5m"]:
        blocking_signals.append("111004:heartbeat_stale")
    if _has_named_violation(str(claim_guard.get("unauthorized_writers", ""))):
        blocking_signals.append("claim-guard:unauthorized_writers")
    if _has_named_violation(str(claim_guard.get("backlog_freeze_violations", ""))):
        blocking_signals.append("claim-guard:backlog_freeze_violations")
    if _has_named_violation(str(claim_guard.get("slot_model_violations", ""))):
        blocking_signals.append("claim-guard:slot_model_violations")

    if can_enter_backlog_open:
        recommended_control_state = "Backlog-Open"
        recommended_reason = "controller-promote-round-finished-and-current-layer-writeback-complete"
    elif can_enter_promote_ready:
        recommended_control_state = "Promote-Ready"
        recommended_reason = "runtime-evidence-verifier-approved-promote-ready"
    elif can_enter_recovery_executing:
        recommended_control_state = "Recovery-Executing"
        recommended_reason = "fresh-runtime-claim-artifacts-watchdog-and-live-state-movement-present"
    elif blocking_signals:
        recommended_control_state = "Recovery-Blocked"
        recommended_reason = ";".join(blocking_signals)
    else:
        recommended_control_state = "Recovery-Rearm"
        recommended_reason = "waiting-for-fresh-runtime-claim-artifacts-watchdog-and-live-state-movement"

    return {
        "fresh_executor_claim": fresh_executor_claim,
        "executor_artifacts_ready": executor_artifacts_ready,
        "watchdog_sampling_started": watchdog_sampling_started,
        "live_state_moved": live_state_moved,
        "can_enter_recovery_executing": can_enter_recovery_executing,
        "can_enter_promote_ready": can_enter_promote_ready,
        "can_enter_backlog_open": can_enter_backlog_open,
        "blocking_signals": blocking_signals,
        "recommended_control_state": recommended_control_state,
        "recommended_reason": recommended_reason,
    }


def _slot_summary(root: Path, snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    support_results = _support_task_results(root)
    current_support = _current_support_wave(root) or {}
    support_running = bool(current_support)
    support_running_slots = set(SUPPORT_TASK_TO_SLOT.values()) if support_running else set()
    control_state = snapshot["control_state"]
    tasks = snapshot["tasks"]

    slots: dict[str, dict[str, Any]] = {
        "01": {
            "role": "controller04",
            "status": "active",
            "mode": "controller-only",
            "source": "active_lock",
        },
        "02": {
            "role": "runtime-executor",
            "task_id": queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID,
            "status": tasks["111003"]["status"],
            "claimable_now": snapshot["claimability"]["111003"],
            "attempt_run_dir": tasks["111003"]["attempt_run_dir"],
            "source": tasks["111003"]["path"],
        },
        "03": {
            "role": "runtime-watchdog",
            "task_id": queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID,
            "status": tasks["111004"]["status"],
            "claimable_now": snapshot["claimability"]["111004"],
            "attempt_run_dir": tasks["111004"]["attempt_run_dir"],
            "source": tasks["111004"]["path"],
        },
    }

    for task_id, slot in SUPPORT_TASK_TO_SLOT.items():
        task_result = support_results.get(task_id) or {}
        if slot in support_running_slots:
            status = "running"
        elif task_result:
            status = "success" if task_result.get("success") else "failed"
        else:
            status = "standby"
        slots[slot] = {
            "role": task_id,
            "task_id": task_id,
            "status": status,
            "last_selected_provider": task_result.get("selected_provider"),
            "source": task_result.get("task_id") or "support_wave",
        }

    backlog_open = control_state == "Backlog-Open"
    reserve_roles = {
        "08": "isolated-capacity-a",
        "09": "isolated-capacity-b",
        "10": "isolated-capacity-c",
        "11": "isolated-capacity-d",
        "12": "isolated-capacity-e",
        "13": "isolated-capacity-f",
    }
    for slot, role in reserve_roles.items():
        slots[slot] = {
            "role": role,
            "status": "available-for-materialization" if backlog_open else "reserved-capacity",
            "mode": "isolated",
            "source": "slot_board_policy",
        }
    return slots


def _rearm_runtime_task(root: Path, queue_key: str) -> dict[str, Any]:
    task_id_map = {
        "111003": queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID,
        "111004": queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID,
    }
    current_state_text = {
        "111003": "rearmed for a fresh shared-root live attempt after the preserved blocked round-2 run",
        "111004": "rearmed for a fresh shared-root watchdog pass after the preserved blocked round-2 run",
    }
    task_id = task_id_map[queue_key]
    relpath = queue22_system_fix_wave._resolve_queue_task_file(root, task_id)
    path = root / relpath
    text = _read_text(path)
    prior_status = _field(text, "status")
    prior_owner = _field(text, "owner") or "unassigned"
    prior_attempt_run_dir = _field(text, "attempt_run_dir")
    task_lock_path = _field(text, "task_lock_path")
    prior_work_notes = _field(text, "work_notes")
    rearm_note = (
        f"Controller04 rearmed the task for a fresh shared-root claim after preserving prior evidence under "
        f"{prior_attempt_run_dir or 'task_run_dir history'}."
    )
    merged_work_notes = prior_work_notes.strip()
    if rearm_note not in merged_work_notes:
        merged_work_notes = (
            f"{merged_work_notes} {rearm_note}".strip() if merged_work_notes else rearm_note
        )

    for field in (
        "provider_id",
        "session_id",
        "claim_source",
        "session_label",
        "base_revision",
        "claim_time",
        "submit_review_time",
        "blocked_time",
        "claim_token",
        "lease_until",
        "heartbeat_at",
        "attempt_run_dir",
        "commands",
        "changed_files",
        "test_results",
        "blocked_reason",
        "reviewed_by",
        "review_notes",
    ):
        text = _replace_field(text, field, "")
    text = _replace_field(text, "status", "todo")
    text = _replace_field(text, "owner", "unassigned")
    text = _replace_field(text, "current_state", current_state_text[queue_key])
    text = _replace_field(text, "review_status", "pending")
    text = _replace_field(text, "work_notes", merged_work_notes)

    target_path = _task_filename_with_owner_status(path, owner="unassigned", status="todo")
    if target_path != path:
        path.rename(target_path)
    target_path.write_text(text, encoding="utf-8")

    removed_claim_lock = False
    if task_lock_path:
        lock_dir = root / task_lock_path
        if lock_dir.exists():
            for child in sorted(lock_dir.rglob("*"), reverse=True):
                if child.is_file():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
            if lock_dir.exists():
                lock_dir.rmdir()
            removed_claim_lock = True

    return {
        "task_id": task_id,
        "queue_key": queue_key,
        "previous_path": relpath,
        "current_path": _normalize_relpath(target_path, root),
        "previous_status": prior_status,
        "previous_owner": prior_owner,
        "preserved_attempt_run_dir": prior_attempt_run_dir,
        "claim_lock_removed": removed_claim_lock,
    }


def rearm_runtime_tasks(root: Path, target: str) -> dict[str, Any]:
    if target not in RUNTIME_REARM_TASKS:
        raise ValueError(f"Unsupported runtime rearm target: {target}")
    operations = [_rearm_runtime_task(root, queue_key) for queue_key in RUNTIME_REARM_TASKS[target]]
    snapshot = _reconciled_status_snapshot(root)
    payload = {
        "performed_at": now_iso(),
        "target": target,
        "operations": operations,
        "status_path": str(write_status_snapshot(root, snapshot)),
        "status": snapshot,
    }
    record_path = root / MONITOR_ROOT / "rearms" / f"{timestamp_token()}.json"
    _write_json(record_path, payload)
    payload["record_path"] = str(record_path)
    return payload


def _apply_control_state(root: Path, control_state: str, reason: str) -> None:
    updated_at = now_iso()
    lock_path = root / queue22_system_fix_wave.ACTIVE_LOCK
    lock_text = _read_text(lock_path)
    lock_text = _replace_field(lock_text, "control_state", control_state)
    lock_text = _replace_field(lock_text, "control_state_reason", reason)
    lock_text = _replace_field(lock_text, "control_state_updated_at", updated_at)
    lock_path.write_text(lock_text, encoding="utf-8")

    slot_path = root / "docs/_temp/problem/SLOT_OCCUPANCY.md"
    slot_text = _read_text(slot_path)
    slot_text = _replace_field(slot_text, "control_state", control_state)
    slot_text = _replace_field(slot_text, "updated_at", updated_at)
    slot_path.write_text(slot_text, encoding="utf-8")


def _required_controller_actions(snapshot: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    state = snapshot["state_evaluation"]["recommended_control_state"]
    latest_support_wave = snapshot.get("latest_support_wave") or {}
    tasks = snapshot["tasks"]
    executor = tasks["111003"]
    watchdog = tasks["111004"]
    promote = tasks["111000"]

    if state == "Recovery-Blocked":
        actions.append("Keep backlog frozen and leave `111000` unclaimable.")
        if executor["status"] in {"todo", "blocked"} and not executor.get("claim_token"):
            actions.append(
                "Rearm `111003` first and require a fresh executor claim with `claim_token + attempt_run_dir` before any new watchdog pass."
            )
        if watchdog["status"] in {"blocked", "todo"}:
            actions.append(
                "Do not relaunch `111004` until `111003` has a fresh claim and has written `preflight.json`, `before_state.json`, `process.json`, and `progress.json`."
            )
        if latest_support_wave and latest_support_wave.get("success") is False:
            actions.append("Rerun a readonly support wave to refresh verifier and claim-guard verdicts once provider capacity recovers.")
        return actions

    if state == "Recovery-Rearm":
        actions.append("Allow a fresh shared-root claim for `111003`.")
        actions.append(
            "Start `111004` only after `111003` has written `claim_token`, `attempt_run_dir`, `preflight.json`, `before_state.json`, `process.json`, and `progress.json`."
        )
        return actions

    if state == "Recovery-Executing":
        actions.append("Keep backlog frozen while `111003` and `111004` continue producing fresh live evidence.")
        actions.append("Wait for runtime evidence verifier approval before touching `111000`.")
        return actions

    if state == "Promote-Ready":
        if promote["status"] == "todo":
            actions.append("Claim `111000` as controller-only and refresh the four official shared artifacts in one round.")
        actions.append("After artifact refresh, write back only `22` sections `2.1 / 2.3 / 4.5 / 5.1`.")
        return actions

    if state == "Backlog-Open":
        actions.append("Materialize isolated fixer/reviewer capacity on demand for the first backlog wave.")
        actions.append("Prefer `git worktree` on a clean repo, fall back to `copy` on a dirty repo, and keep default ephemeral cleanup.")
        return actions

    actions.append("Keep controller lock fresh and re-evaluate runtime evidence before changing queue state.")
    return actions


def _reconciled_status_snapshot(root: Path) -> dict[str, Any]:
    snapshot = build_status_snapshot(root)
    recommended_state = snapshot["state_evaluation"]["recommended_control_state"]
    recommended_reason = snapshot["state_evaluation"]["recommended_reason"]
    current_reason = snapshot["active_lock"].get("control_state_reason") or ""
    if recommended_state != snapshot["control_state"] or recommended_reason != current_reason:
        _apply_control_state(root, recommended_state, recommended_reason)
        snapshot = build_status_snapshot(root)
    return snapshot


def build_status_snapshot(root: Path) -> dict[str, Any]:
    captured_at = now_iso()
    lock = _lock_snapshot(root)
    slot_board = _slot_board_snapshot(root)
    control_state = lock.get("control_state") or slot_board.get("control_state") or ""
    tasks = {
        "111000": _task_snapshot(root, queue22_system_fix_wave.PROMOTE_TASK_ID, control_state),
        "111003": _task_snapshot(root, queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID, control_state),
        "111004": _task_snapshot(root, queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID, control_state),
    }
    runtime_evidence = {
        "111003": _executor_runtime_evidence(root, tasks["111003"]),
        "111004": _watchdog_runtime_evidence(root, tasks["111004"]),
    }
    snapshot = {
        "run_label": queue22_system_fix_wave.RUN_LABEL,
        "generated_at": captured_at,
        "captured_at": captured_at,
        "control_state": control_state,
        "control_state_reason": lock.get("control_state_reason") or "",
        "lock_matches_slot_board": bool(control_state) and control_state == slot_board.get("control_state"),
        "active_lock": lock,
        "slot_board": slot_board,
        "tasks": tasks,
        "runtime_evidence": runtime_evidence,
        "latest_support_wave": _latest_support_wave(root),
        "current_support_wave": _current_support_wave(root),
        "latest_issue_findings": _latest_issue_findings(root),
        "verifier_verdict": _latest_verifier_verdict(root),
        "claim_guard": _latest_claim_guard(root),
    }
    state_evaluation = _evaluate_control_state(snapshot)
    snapshot["state_evaluation"] = state_evaluation
    snapshot["claimability"] = {
        "111003": state_evaluation["recommended_control_state"] == "Recovery-Rearm"
        and tasks["111003"]["status"] == "todo",
        "111004": state_evaluation["recommended_control_state"] == "Recovery-Rearm"
        and tasks["111004"]["status"] == "todo",
        "111000": state_evaluation["recommended_control_state"] == "Promote-Ready"
        and tasks["111000"]["status"] == "todo",
        "backlog_frozen": state_evaluation["recommended_control_state"] != "Backlog-Open",
    }
    snapshot["required_controller_actions"] = _required_controller_actions(snapshot)
    snapshot["slots"] = _slot_summary(root, snapshot)
    return snapshot


def write_status_snapshot(root: Path, payload: dict[str, Any]) -> Path:
    stamp = timestamp_token()
    target = root / STATUS_ROOT / f"{stamp}.json"
    _write_json(target, payload)
    _write_json(root / MONITOR_ROOT / "latest_status.json", payload)
    _write_json(root / WAVES_ROOT / stamp / "controller_tick.json", payload)
    _append_jsonl(root / MONITOR_ROOT / "history.jsonl", payload)
    _append_jsonl(
        root / MONITOR_ROOT / "status_history.jsonl",
        {
            "generated_at": payload["generated_at"],
            "control_state": payload["control_state"],
            "lock_matches_slot_board": payload["lock_matches_slot_board"],
            "task_statuses": {name: item["status"] for name, item in payload["tasks"].items()},
        },
    )
    return target


def _support_command(*, providers: list[str], max_workers: int, timeout_seconds: int) -> list[str]:
    command = [
        sys.executable,
        "scripts/queue22_system_fix_wave.py",
        "run",
        "--profile",
        "support",
        "--max-workers",
        str(max_workers),
        "--timeout-seconds",
        str(timeout_seconds),
        "--json",
    ]
    for provider in providers:
        command.extend(["--provider", provider])
    return command


def run_support_wave(
    root: Path,
    *,
    providers: list[str],
    max_workers: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    started_at = now_iso()
    command = _support_command(
        providers=providers,
        max_workers=max_workers,
        timeout_seconds=timeout_seconds,
    )
    completed = subprocess.run(
        command,
        cwd=root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    finished_at = now_iso()
    parsed_stdout = None
    if completed.stdout.strip():
        try:
            parsed_stdout = json.loads(completed.stdout)
        except json.JSONDecodeError:
            parsed_stdout = None
    payload = {
        "started_at": started_at,
        "finished_at": finished_at,
        "returncode": completed.returncode,
        "ok": completed.returncode == 0,
        "command": command,
        "providers": providers,
        "max_workers": max_workers,
        "timeout_seconds": timeout_seconds,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "parsed_stdout": parsed_stdout,
    }
    run_dir = root / SUPPORT_ROOT / timestamp_token()
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "stdout.log").write_text(completed.stdout, encoding="utf-8")
    (run_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")
    _write_json(run_dir / "result.json", payload)
    _write_json(root / MONITOR_ROOT / "latest_support_run.json", payload)
    return payload


def start_support_wave_process(
    root: Path,
    *,
    providers: list[str],
    max_workers: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    started_at = now_iso()
    stamp = timestamp_token()
    command = _support_command(
        providers=providers,
        max_workers=max_workers,
        timeout_seconds=timeout_seconds,
    )
    run_dir = root / SUPPORT_ROOT / stamp
    run_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = run_dir / "stdout.log"
    stderr_path = run_dir / "stderr.log"
    stdout_handle = stdout_path.open("w", encoding="utf-8")
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=root,
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    payload = {
        "run_dir": run_dir,
        "started_at": started_at,
        "command": command,
        "providers": providers,
        "max_workers": max_workers,
        "timeout_seconds": timeout_seconds,
        "stdout_path": stdout_path,
        "stderr_path": stderr_path,
        "stdout_handle": stdout_handle,
        "stderr_handle": stderr_handle,
        "process": process,
    }
    _write_json(
        root / MONITOR_ROOT / "current_support_run.json",
        {
            "started_at": started_at,
            "pid": process.pid,
            "command": command,
            "providers": providers,
            "max_workers": max_workers,
            "timeout_seconds": timeout_seconds,
            "run_dir": run_dir.relative_to(root).as_posix(),
            "status": "running",
        },
    )
    return payload


def finalize_support_wave_process(root: Path, run_state: dict[str, Any]) -> dict[str, Any]:
    process: subprocess.Popen[str] = run_state["process"]
    returncode = process.poll()
    if returncode is None:
        raise RuntimeError("support wave is still running")
    run_state["stdout_handle"].close()
    run_state["stderr_handle"].close()
    stdout_path: Path = run_state["stdout_path"]
    stderr_path: Path = run_state["stderr_path"]
    stdout = stdout_path.read_text(encoding="utf-8", errors="replace") if stdout_path.exists() else ""
    stderr = stderr_path.read_text(encoding="utf-8", errors="replace") if stderr_path.exists() else ""
    parsed_stdout = None
    if stdout.strip():
        try:
            parsed_stdout = json.loads(stdout)
        except json.JSONDecodeError:
            parsed_stdout = None
    payload = {
        "started_at": run_state["started_at"],
        "finished_at": now_iso(),
        "returncode": returncode,
        "ok": returncode == 0,
        "command": run_state["command"],
        "providers": run_state["providers"],
        "max_workers": run_state["max_workers"],
        "timeout_seconds": run_state["timeout_seconds"],
        "stdout": stdout,
        "stderr": stderr,
        "parsed_stdout": parsed_stdout,
    }
    run_dir: Path = run_state["run_dir"]
    _write_json(run_dir / "result.json", payload)
    _write_json(root / MONITOR_ROOT / "latest_support_run.json", payload)
    current_path = root / MONITOR_ROOT / "current_support_run.json"
    if current_path.exists():
        current_path.unlink()
    return payload


def monitor_once(
    root: Path,
    *,
    providers: list[str],
    max_workers: int,
    timeout_seconds: int,
    run_support: bool,
) -> dict[str, Any]:
    result: dict[str, Any] = {"support_run": None}
    if run_support:
        result["support_run"] = run_support_wave(
            root,
            providers=providers,
            max_workers=max_workers,
            timeout_seconds=timeout_seconds,
        )
    status = _reconciled_status_snapshot(root)
    result["status_path"] = str(write_status_snapshot(root, status))
    result["status"] = status
    return result


def monitor_loop(args: argparse.Namespace) -> int:
    root = args.repo_root.resolve()
    last_status_at = 0.0
    last_support_at = 0.0
    support_state: dict[str, Any] | None = None
    try:
        while True:
            now = time.monotonic()
            try:
                _refresh_controller_lock(root)
                _write_active_monitor(root, args, support_state)
                if support_state is not None and support_state["process"].poll() is not None:
                    finalize_support_wave_process(root, support_state)
                    write_status_snapshot(root, _reconciled_status_snapshot(root))
                    support_state = None
                if last_status_at == 0.0 or now - last_status_at >= args.status_interval_seconds:
                    write_status_snapshot(root, _reconciled_status_snapshot(root))
                    last_status_at = now
                if not args.skip_support and (
                    last_support_at == 0.0 or now - last_support_at >= args.support_interval_seconds
                ):
                    if support_state is None:
                        support_state = start_support_wave_process(
                            root,
                            providers=args.providers,
                            max_workers=args.max_workers,
                            timeout_seconds=args.wave_timeout_seconds,
                        )
                        _write_active_monitor(root, args, support_state)
                        write_status_snapshot(root, _reconciled_status_snapshot(root))
                        last_support_at = time.monotonic()
            except Exception as exc:
                error_payload = {
                    "generated_at": now_iso(),
                    "error": str(exc),
                }
                _append_jsonl(root / MONITOR_ROOT / "errors.jsonl", error_payload)
                _write_json(root / MONITOR_ROOT / "latest_error.json", error_payload)
            time.sleep(max(1, int(args.poll_seconds)))
    finally:
        active_monitor = root / ACTIVE_MONITOR_PATH
        if active_monitor.exists():
            active_monitor.unlink()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor Queue22 controller04 support waves and status snapshots.")
    parser.add_argument("--repo-root", type=Path, default=repo_root())
    parser.add_argument("--rearm-runtime", choices=RUNTIME_REARM_CHOICES)
    parser.add_argument("--provider", action="append", dest="providers")
    parser.add_argument("--max-workers", type=int)
    parser.add_argument("--wave-timeout-seconds", type=int, default=queue22_system_fix_wave.DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--status-interval-seconds", type=int, default=DEFAULT_STATUS_INTERVAL_SECONDS)
    parser.add_argument("--support-interval-seconds", type=int, default=DEFAULT_SUPPORT_INTERVAL_SECONDS)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--skip-support", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.rearm_runtime:
        payload = rearm_runtime_tasks(args.repo_root.resolve(), args.rearm_runtime)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    launch_error = queue22_system_fix_wave._validate_cli_launch_contract(
        providers=args.providers,
        max_workers=args.max_workers,
    )
    if launch_error:
        print(launch_error, file=sys.stderr)
        return 2
    args.providers = list(args.providers or [])
    args.max_workers = int(args.max_workers)
    if args.once:
        payload = monitor_once(
            args.repo_root.resolve(),
            providers=args.providers,
            max_workers=args.max_workers,
            timeout_seconds=args.wave_timeout_seconds,
            run_support=not args.skip_support,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    return monitor_loop(args)


if __name__ == "__main__":
    raise SystemExit(main())
