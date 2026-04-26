#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


REQUIRED_TASK_TEMPLATE_FIELDS = {
    "task_id",
    "created_at",
    "topic_slug",
    "dedupe_key",
    "title",
    "problem_family",
    "next_action",
    "why_now",
    "source_doc_section",
    "analysis_view_ids",
    "problem_type",
    "priority",
    "current_state",
    "status",
    "owner",
    "run_label",
    "claim_token",
    "lease_until",
    "heartbeat_at",
    "handling_path",
    "ssot_refs",
    "suggested_owner_role",
    "suggested_workspace_mode",
    "related_files",
    "write_scope",
    "acceptance_tests",
    "shared_artifacts_policy",
    "needs_shared_artifact_refresh",
    "task_run_dir",
    "attempt_run_dir",
    "writeback_target",
    "writeback_summary",
    "depends_on",
    "provider_id",
    "session_id",
    "claim_source",
    "session_label",
    "base_revision",
    "workspace_kind",
    "workspace_root",
    "claim_time",
    "submit_review_time",
    "blocked_time",
    "work_notes",
    "commands",
    "changed_files",
    "test_results",
    "blocked_reason",
    "need_main_writeback_to_22",
    "review_status",
    "reviewed_by",
    "review_notes",
}

REQUIRED_README_STATES = {"todo", "doing", "blocked", "review", "done", "obsolete", "rejected"}
REQUIRED_LOCK_FIELDS = {"run_label", "controller_id", "session_id", "base_revision", "started_at", "lease_until", "heartbeat_at", "status"}
REQUIRED_CLAIM_LOCK_FIELDS = {
    "task_id",
    "run_label",
    "owner",
    "provider_id",
    "session_id",
    "session_label",
    "base_revision",
    "claim_token",
    "claimed_at",
    "lease_until",
    "heartbeat_at",
    "status",
}
REQUIRED_JOURNAL_FIELDS = {
    "status",
    "writeback_at",
    "started_at",
    "finished_at",
    "run_label",
    "controller_id",
    "base_revision",
    "writeback_target",
    "before_hash",
    "after_hash",
    "reviewed_tasks",
    "notes",
}
TOPIC_SLUG_PATTERN = re.compile(r"^[a-z0-9-]{1,48}$")


@dataclass
class CheckIssue:
    severity: str
    scope: str
    message: str


@dataclass
class ClaimAttempt:
    mode: str
    owner: str
    success: bool
    target_name: str
    error: str | None = None


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def prompt_pack_path(root: Path) -> Path:
    return root / "docs" / "_temp" / "codex_parallel_prompt_pack_optimized.md"


def problem_dir(root: Path) -> Path:
    return root / "docs" / "_temp" / "problem"


def read_utf8(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def extract_backtick_keys(text: str) -> set[str]:
    keys: set[str] = set()
    for line in text.splitlines():
        line = line.strip()
        match = re.match(r"- `([^`]+)`:", line)
        if match:
            keys.add(match.group(1))
    return keys


def extract_state_names(readme_text: str) -> set[str]:
    states: set[str] = set()
    in_state_block = False
    for raw_line in readme_text.splitlines():
        line = raw_line.strip()
        if line == "## 允许的 `state`":
            in_state_block = True
            continue
        if in_state_block and line.startswith("## "):
            break
        if in_state_block:
            match = re.match(r"- `([^`]+)`", line)
            if match:
                states.add(match.group(1))
    return states


def field_name_consistency_checks(prompt_text: str, template_text: str) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    prompt_uses_plural = "needs_main_writeback" in prompt_text
    template_uses_singular = "need_main_writeback_to_22" in template_text
    template_uses_plural = "needs_main_writeback" in template_text
    if prompt_uses_plural and template_uses_singular and not template_uses_plural:
        issues.append(
            CheckIssue(
                severity="error",
                scope="prompt_vs_template",
                message="Prompt uses `needs_main_writeback`, but TASK_TEMPLATE only defines `need_main_writeback_to_22`.",
            )
        )
    return issues


def task_lease_recovery_checks(prompt_text: str, readme_text: str) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    combined = f"{prompt_text}\n{readme_text}"
    if "lease_until" in combined:
        recovery_markers = ("回收", "requeue", "重新开回", "lease 过期", "租约过期", "stale doing", "status-doing")
        if not any(marker in combined for marker in recovery_markers):
            issues.append(
                CheckIssue(
                    severity="error",
                    scope="lease_recovery",
                    message="Protocol defines `lease_until`, but does not define how expired `doing` tasks are reclaimed or requeued.",
                )
            )
    if "heartbeat_at" in combined:
        cadence_markers = ("每 5", "每5", "每 10", "每10", "heartbeat cadence", "心跳", "刷新 heartbeat", "更新 heartbeat")
        if not any(marker in combined for marker in cadence_markers):
            issues.append(
                CheckIssue(
                    severity="warning",
                    scope="lease_recovery",
                    message="Protocol defines `heartbeat_at`, but does not define a heartbeat cadence for long-running tasks.",
                )
            )
    return issues


def formatting_checks(prompt_text: str) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    marker = "## `docs/_temp/problem` 目录需要配套遵守的协议"
    idx = prompt_text.find(marker)
    if idx != -1:
        segment = prompt_text[idx : idx + 500]
        if "- 任务模板必须包含：" in segment and "\n- `dedupe_key`" in segment:
            issues.append(
                CheckIssue(
                    severity="warning",
                    scope="prompt_format",
                    message="The required-template-fields list is flattened at the same bullet level, which is easy to misread during execution.",
                )
            )
    return issues


def current_layer_checks(prompt_text: str) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    required_current_markers = ("`2.1`", "`2.2`", "`2.3`", "`4.5`", "`5.3`", "`current-writeback-detail`")
    missing = [marker for marker in required_current_markers if marker not in prompt_text]
    if missing:
        issues.append(
            CheckIssue(
                severity="error",
                scope="current_layer",
                message=f"Prompt pack current-layer rules are missing required section markers: {missing}",
            )
        )
    if "`2.4`" not in prompt_text:
        issues.append(
            CheckIssue(
                severity="warning",
                scope="current_layer",
                message="Prompt pack does not mention `2.4` as an anti-overreach filter for 'do not rewrite whole capabilities'.",
            )
        )
    return issues


def bootstrap_checks(prompt_text: str, readme_text: str) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    combined = f"{prompt_text}\n{readme_text}"
    required_markers = ("_claims/", "output/task_runs/", "`run_label`", "`base_revision`")
    missing = [marker for marker in required_markers if marker not in combined]
    if missing:
        issues.append(
            CheckIssue(
                severity="error",
                scope="bootstrap",
                message=f"Bootstrap protocol is missing required markers: {missing}",
            )
        )
    return issues


def dedupe_state_checks(prompt_text: str, readme_text: str) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    combined = f"{prompt_text}\n{readme_text}"
    required_states = ("todo", "doing", "review", "blocked")
    if "dedupe 活跃状态" not in combined:
        issues.append(
            CheckIssue(
                severity="error",
                scope="dedupe",
                message="Protocol does not define dedupe active states explicitly.",
            )
        )
        return issues
    missing = [state for state in required_states if f"`{state}`" not in combined and state not in combined]
    if missing:
        issues.append(
            CheckIssue(
                severity="error",
                scope="dedupe",
                message=f"Dedupe active states are missing required statuses: {missing}",
            )
        )
    return issues


def task_name_shape_checks(prompt_text: str) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    if "scan_truth-lineage" in prompt_text or "fix_<task_id>_<owner>_<nonce>" in prompt_text:
        issues.append(
            CheckIssue(
                severity="error",
                scope="task_name",
                message="Prompt pack still contains task_name examples that imply hyphens or raw task_id usage inside Codex subagent task_name.",
            )
        )
    if "scan_<area>_<topic_slug>_<session>_<seq>" not in prompt_text:
        issues.append(
            CheckIssue(
                severity="warning",
                scope="task_name",
                message="Prompt pack does not document the underscore-only analysis subagent task_name pattern.",
            )
        )
    return issues


def write_scope_normalization_checks(prompt_text: str, readme_text: str) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    combined = f"{prompt_text}\n{readme_text}"
    required_markers = ("case-insensitive", "repo-wide formatter", "git pull", "attempt_run_dir")
    missing = [marker for marker in required_markers if marker not in combined]
    if missing:
        issues.append(
            CheckIssue(
                severity="warning",
                scope="write_scope_normalization",
                message=f"Concurrent shared-root safeguards are missing some expected markers: {missing}",
            )
        )
    return issues


def task_identity_checks(prompt_text: str, readme_text: str) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    combined = f"{prompt_text}\n{readme_text}"
    identity_markers = ("task_id 固定等于", "`task_id` 固定等于", "canonical task_id")
    if "<task_id>" in combined and not any(marker in combined for marker in identity_markers):
        issues.append(
            CheckIssue(
                severity="error",
                scope="task_identity",
                message="Protocol uses `<task_id>` but does not define the canonical task_id shape separately from owner/status.",
            )
        )
    return issues


def claim_lock_shape_checks(readme_text: str, claim_lock_text: str, template_text: str) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    combined = f"{readme_text}\n{claim_lock_text}\n{template_text}"
    if "<task_id>.lock/" not in combined:
        issues.append(
            CheckIssue(
                severity="error",
                scope="claim_lock_shape",
                message="Claim lock path should be documented as a directory path ending in `<task_id>.lock/`.",
            )
        )
    if "CLAIM_LOCK.md" not in combined:
        issues.append(
            CheckIssue(
                severity="warning",
                scope="claim_lock_shape",
                message="Claim lock metadata file `CLAIM_LOCK.md` is not documented; lock acquisition details may be ambiguous on Windows.",
            )
        )
    return issues


def create_sample_task(temp_dir: Path) -> Path:
    temp_dir.mkdir(parents=True, exist_ok=True)
    file_path = temp_dir / "20260325-101530__P1__runtime__same-round-freeze__owner-unassigned__status-todo.md"
    file_path.write_text("# sample\n- `status`: `todo`\n- `owner`: `unassigned`\n", encoding="utf-8")
    return file_path


def simulate_rename_only_race(temp_dir: Path) -> list[ClaimAttempt]:
    source = create_sample_task(temp_dir)
    barrier = threading.Barrier(2)
    results: list[ClaimAttempt] = []
    lock = threading.Lock()

    def attempt(owner: str) -> None:
        target = temp_dir / f"20260325-101530__P1__runtime__same-round-freeze__owner-{owner}__status-doing.md"
        error: str | None = None
        success = False
        try:
            barrier.wait(timeout=5)
            os.replace(source, target)
            success = True
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
        with lock:
            results.append(
                ClaimAttempt(
                    mode="rename_only",
                    owner=owner,
                    success=success,
                    target_name=target.name,
                    error=error,
                )
            )

    threads = [threading.Thread(target=attempt, args=(owner,)) for owner in ("codex01", "codex02")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results


def simulate_claim_lock_race(temp_dir: Path) -> list[ClaimAttempt]:
    source = create_sample_task(temp_dir)
    claims_dir = temp_dir / "_claims"
    claims_dir.mkdir(parents=True, exist_ok=True)
    barrier = threading.Barrier(2)
    results: list[ClaimAttempt] = []
    lock = threading.Lock()

    def attempt(owner: str) -> None:
        target = temp_dir / f"20260325-101530__P1__runtime__same-round-freeze__owner-{owner}__status-doing.md"
        claim_lock = claims_dir / "20260325-101530.lock"
        error: str | None = None
        success = False
        try:
            barrier.wait(timeout=5)
            claim_lock.mkdir()
            os.replace(source, target)
            success = True
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
        with lock:
            results.append(
                ClaimAttempt(
                    mode="claim_lock",
                    owner=owner,
                    success=success,
                    target_name=target.name,
                    error=error,
                )
            )

    threads = [threading.Thread(target=attempt, args=(owner,)) for owner in ("codex01", "codex02")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results


def simulate_write_scope_conflict() -> list[CheckIssue]:
    active_scopes = [
        ("task-a", {"app/services/runtime_materialization.py", "tests/test_runtime_anchor_consistency.py"}),
        ("task-b", {"app/services/runtime_materialization.py", "tests/test_runtime_anchor_consistency.py"}),
    ]
    issues: list[CheckIssue] = []
    for i in range(len(active_scopes)):
        for j in range(i + 1, len(active_scopes)):
            left_name, left_scope = active_scopes[i]
            right_name, right_scope = active_scopes[j]
            overlap = sorted(left_scope & right_scope)
            if overlap:
                issues.append(
                    CheckIssue(
                        severity="info",
                        scope="write_scope_simulation",
                        message=f"Simulated active tasks `{left_name}` and `{right_name}` conflict on {overlap}.",
                    )
                )
    return issues


def run_checks(root: Path) -> dict[str, object]:
    prompt_path = prompt_pack_path(root)
    readme_path = problem_dir(root) / "README.md"
    template_path = problem_dir(root) / "TASK_TEMPLATE.md"
    lock_template_path = problem_dir(root) / "ANALYSIS_LOCK_TEMPLATE.md"
    claim_lock_template_path = problem_dir(root) / "CLAIM_LOCK_TEMPLATE.md"
    journal_path = problem_dir(root) / "WRITEBACK_JOURNAL.md"

    prompt_text = read_utf8(prompt_path)
    readme_text = read_utf8(readme_path)
    template_text = read_utf8(template_path)
    lock_text = read_utf8(lock_template_path)
    claim_lock_text = read_utf8(claim_lock_template_path)
    journal_text = read_utf8(journal_path)

    issues: list[CheckIssue] = []

    template_fields = extract_backtick_keys(template_text)
    missing_template_fields = sorted(REQUIRED_TASK_TEMPLATE_FIELDS - template_fields)
    if missing_template_fields:
        issues.append(
            CheckIssue(
                severity="error",
                scope="task_template",
                message=f"TASK_TEMPLATE is missing required fields: {missing_template_fields}",
            )
        )

    readme_states = extract_state_names(readme_text)
    missing_states = sorted(REQUIRED_README_STATES - readme_states)
    if missing_states:
        issues.append(
            CheckIssue(
                severity="error",
                scope="problem_readme",
                message=f"README is missing required task states: {missing_states}",
            )
        )

    lock_fields = extract_backtick_keys(lock_text)
    missing_lock_fields = sorted(REQUIRED_LOCK_FIELDS - lock_fields)
    if missing_lock_fields:
        issues.append(
            CheckIssue(
                severity="error",
                scope="analysis_lock_template",
                message=f"ANALYSIS_LOCK_TEMPLATE is missing required fields: {missing_lock_fields}",
            )
        )

    claim_lock_fields = extract_backtick_keys(claim_lock_text)
    missing_claim_lock_fields = sorted(REQUIRED_CLAIM_LOCK_FIELDS - claim_lock_fields)
    if missing_claim_lock_fields:
        issues.append(
            CheckIssue(
                severity="error",
                scope="claim_lock_template",
                message=f"CLAIM_LOCK_TEMPLATE is missing required fields: {missing_claim_lock_fields}",
            )
        )

    journal_fields = extract_backtick_keys(journal_text)
    missing_journal_fields = sorted(REQUIRED_JOURNAL_FIELDS - journal_fields)
    if missing_journal_fields:
        issues.append(
            CheckIssue(
                severity="error",
                scope="writeback_journal",
                message=f"WRITEBACK_JOURNAL is missing required fields: {missing_journal_fields}",
            )
        )

    issues.extend(field_name_consistency_checks(prompt_text, template_text))
    issues.extend(task_lease_recovery_checks(prompt_text, readme_text))
    issues.extend(formatting_checks(prompt_text))
    issues.extend(current_layer_checks(prompt_text))
    issues.extend(bootstrap_checks(prompt_text, readme_text))
    issues.extend(dedupe_state_checks(prompt_text, readme_text))
    issues.extend(task_name_shape_checks(prompt_text))
    issues.extend(write_scope_normalization_checks(prompt_text, readme_text))
    issues.extend(task_identity_checks(prompt_text, readme_text))
    issues.extend(claim_lock_shape_checks(readme_text, claim_lock_text, template_text))

    topic_examples = [
        "same-round-freeze",
        "freeze-round-divergence",
        "shared-artifacts-not-same-round",
    ]
    for slug in topic_examples:
        if not TOPIC_SLUG_PATTERN.fullmatch(slug):
            issues.append(
                CheckIssue(
                    severity="error",
                    scope="topic_slug",
                    message=f"Configured example slug `{slug}` does not match the documented TOPIC_SLUG regex.",
                )
            )

    temp_root = root / "runtime" / "prompt_pack_checks"
    run_id = datetime.now(timezone.utc).astimezone().strftime("%Y%m%dT%H%M%S")
    run_dir = temp_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=run_dir) as temp_name:
        rename_only_results = simulate_rename_only_race(Path(temp_name) / "rename_only")
    with tempfile.TemporaryDirectory(dir=run_dir) as temp_name:
        claim_lock_results = simulate_claim_lock_race(Path(temp_name) / "claim_lock")
    rename_only_success_count = sum(1 for item in rename_only_results if item.success)
    claim_lock_success_count = sum(1 for item in claim_lock_results if item.success)
    if claim_lock_success_count != 1:
        issues.append(
            CheckIssue(
                severity="error",
                scope="claim_simulation",
                message=f"Claim-lock claim simulation expected exactly 1 winner, got {claim_lock_success_count}.",
            )
        )
    if rename_only_success_count > 1:
        issues.append(
            CheckIssue(
                severity="info",
                scope="rename_only_simulation",
                message=f"Rename-only simulation produced {rename_only_success_count} apparent winners on this filesystem; this is why the protocol now requires claim lock first.",
            )
        )

    issues.extend(simulate_write_scope_conflict())

    severity_rank = {"error": 3, "warning": 2, "info": 1}
    highest_severity = "ok"
    if issues:
        highest = max(issues, key=lambda item: severity_rank.get(item.severity, 0))
        highest_severity = highest.severity

    payload = {
        "checked_at": datetime.now(timezone.utc).astimezone().isoformat(),
        "root": str(root),
        "paths": {
            "prompt_pack": str(prompt_path),
            "problem_readme": str(readme_path),
            "task_template": str(template_path),
            "analysis_lock_template": str(lock_template_path),
            "claim_lock_template": str(claim_lock_template_path),
            "writeback_journal": str(journal_path),
        },
        "rename_only_race": [asdict(item) for item in rename_only_results],
        "claim_lock_race": [asdict(item) for item in claim_lock_results],
        "issues": [asdict(item) for item in issues],
        "highest_severity": highest_severity,
        "ok": highest_severity not in {"error"},
    }
    (run_dir / "result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the Codex parallel prompt pack and queue protocol.")
    parser.add_argument("--root", type=Path, default=repo_root())
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--max-rounds", type=int, default=0, help="0 means run forever in watch mode")
    return parser.parse_args()


def print_payload(payload: dict[str, object], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    print(f"checked_at={payload['checked_at']}")
    print(f"ok={str(payload['ok']).lower()}")
    print(f"highest_severity={payload['highest_severity']}")
    print("rename_only_race:")
    for item in payload["rename_only_race"]:
        print(
            f"  owner={item['owner']} success={str(item['success']).lower()} "
            f"target={item['target_name']} error={item['error']}"
        )
    print("claim_lock_race:")
    for item in payload["claim_lock_race"]:
        print(
            f"  owner={item['owner']} success={str(item['success']).lower()} "
            f"target={item['target_name']} error={item['error']}"
        )
    if payload["issues"]:
        print("issues:")
        for issue in payload["issues"]:
            print(f"  [{issue['severity']}] {issue['scope']}: {issue['message']}")
    else:
        print("issues: none")


def main() -> int:
    args = parse_args()
    root = args.root.resolve()

    if not args.watch:
        payload = run_checks(root)
        print_payload(payload, args.json)
        return 0 if payload["ok"] else 1

    rounds = 0
    while True:
        rounds += 1
        payload = run_checks(root)
        print_payload(payload, args.json)
        if not payload["ok"]:
            return 1
        if args.max_rounds > 0 and rounds >= args.max_rounds:
            return 0
        if not args.json:
            print(f"watch_sleep_seconds={args.interval_seconds}")
        time.sleep(max(1, args.interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
