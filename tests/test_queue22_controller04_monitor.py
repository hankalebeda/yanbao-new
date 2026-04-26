from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from scripts import queue22_controller04_monitor
from scripts import queue22_system_fix_wave


def _write_text(root: Path, relpath: str, text: str) -> None:
    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(root: Path, relpath: Path, payload: dict[str, object]) -> None:
    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _now_iso() -> str:
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def _stale_iso(minutes: int = 6) -> str:
    return (datetime.now().astimezone() - timedelta(minutes=minutes)).replace(microsecond=0).isoformat()


def _write_support_summary(
    root: Path,
    *,
    run_id: str,
    task_id: str,
    provider: str,
    last_message_text: str,
) -> None:
    message_path = root / "runtime" / f"{task_id}-{provider}-last_message.txt"
    message_path.parent.mkdir(parents=True, exist_ok=True)
    message_path.write_text(last_message_text, encoding="utf-8")
    _write_json(
        root,
        queue22_system_fix_wave.WAVE_EXPORT_ROOT / "support" / run_id / "summary.json",
        {
            "run_id": run_id,
            "success": True,
            "started_at": "2026-03-27T04:01:00+00:00",
            "finished_at": "2026-03-27T04:01:20+00:00",
            "task_count": 4,
            "tasks": [
                {
                    "task_id": task_id,
                    "success": True,
                    "selected_provider": provider,
                    "attempts": [
                        {
                            "provider": provider,
                            "last_message_path": str(message_path),
                        }
                    ],
                }
            ],
        },
    )


def test_build_status_snapshot_reads_current_gate_state(tmp_path: Path) -> None:
    _write_text(
        tmp_path,
        queue22_system_fix_wave.ACTIVE_LOCK,
        "\n".join(
            [
                "# lock",
                "",
                "- `controller_id`: `controller04`",
                "- `run_label`: `queue22-20260327b`",
                "- `status`: `active`",
                "- `control_state`: `Recovery-Blocked`",
                "- `control_state_reason`: `runtime evidence still blocked`",
                "- `heartbeat_at`: `2026-03-27T12:00:00+08:00`",
                "- `lease_until`: `2026-03-27T16:00:00+08:00`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        "docs/_temp/problem/SLOT_OCCUPANCY.md",
        "\n".join(
            [
                "# slots",
                "",
                "- `run_label`: `queue22-20260327b`",
                "- `controller_id`: `controller04`",
                "- `control_state`: `Recovery-Blocked`",
                "- `updated_at`: `2026-03-27T12:00:05+08:00`",
                "- `prompt_authority`: `docs/_temp/problem/launch_prompts/queue22_system_fix_prompt_manifest_20260327b.md`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.PROMOTE_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111000",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.PROMOTE_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Promote-Ready`",
                "- `claim_token`: ``",
                "- `heartbeat_at`: ``",
                "- `attempt_run_dir`: ``",
                "- `blocked_reason`: ``",
                "- `review_status`: `pending`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}__owner-slot02-runtime-executor-r2__status-blocked.md",
        "\n".join(
            [
                "# 111003",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}`",
                "- `status`: `blocked`",
                "- `owner`: `slot02-runtime-executor-r2`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `claim_token`: `slot02r2-demo`",
                "- `heartbeat_at`: `2026-03-27T11:35:00+08:00`",
                "- `attempt_run_dir`: `output/task_runs/20260326-111003/demo/`",
                "- `blocked_reason`: `sqlite_locked`",
                "- `review_status`: `pending`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}__owner-slot03-runtime-watchdog-r2__status-blocked.md",
        "\n".join(
            [
                "# 111004",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}`",
                "- `status`: `blocked`",
                "- `owner`: `slot03-runtime-watchdog-r2`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `claim_token`: `slot03r2-demo`",
                "- `heartbeat_at`: `2026-03-27T11:35:30+08:00`",
                "- `attempt_run_dir`: `output/task_runs/20260326-111004/demo/`",
                "- `blocked_reason`: `missing_terminal_artifacts`",
                "- `review_status`: `pending`",
            ]
        ),
    )
    _write_json(
        tmp_path,
        queue22_system_fix_wave.WAVE_EXPORT_ROOT / "support" / "20260327T120100" / "summary.json",
        {
            "run_id": "20260327T120100",
            "success": True,
            "started_at": "2026-03-27T04:01:00+00:00",
            "finished_at": "2026-03-27T04:01:20+00:00",
            "task_count": 4,
        },
    )
    _write_json(
        tmp_path,
        queue22_system_fix_wave.ISSUE_MINER_EXPORT_ROOT / "20260327T120100" / "findings.json",
        {
            "run_label": "queue22-20260327b",
            "session_id": "20260327T120100",
            "provider_id": "119.8.113.226",
            "generated_at": "2026-03-27T12:01:20+08:00",
            "findings": [{"problem": "sample"}],
        },
    )

    snapshot = queue22_controller04_monitor.build_status_snapshot(tmp_path)

    assert snapshot["control_state"] == "Recovery-Blocked"
    assert snapshot["lock_matches_slot_board"] is True
    assert snapshot["state_evaluation"]["recommended_control_state"] == "Recovery-Blocked"
    assert snapshot["claimability"]["backlog_frozen"] is True
    assert snapshot["required_controller_actions"][0] == "Keep backlog frozen and leave `111000` unclaimable."
    assert snapshot["current_support_wave"] is None
    assert snapshot["slots"]["01"]["status"] == "active"
    assert snapshot["slots"]["08"]["status"] == "reserved-capacity"
    assert snapshot["slots"]["13"]["status"] == "reserved-capacity"
    assert snapshot["tasks"]["111000"]["claimable_now"] is False
    assert snapshot["tasks"]["111003"]["file_state_claimable"] is False
    assert snapshot["tasks"]["111003"]["controller_gate_allows"] is False
    assert snapshot["latest_support_wave"]["run_id"] == "20260327T120100"
    assert snapshot["latest_issue_findings"]["finding_count"] == 1

    status_path = queue22_controller04_monitor.write_status_snapshot(tmp_path, snapshot)
    assert status_path.exists()
    assert (tmp_path / queue22_controller04_monitor.MONITOR_ROOT / "latest_status.json").exists()
    assert (tmp_path / queue22_controller04_monitor.MONITOR_ROOT / "history.jsonl").exists()
    assert list((tmp_path / queue22_controller04_monitor.WAVES_ROOT).glob("*/controller_tick.json"))


def test_run_support_wave_uses_explicit_providers_and_writes_artifacts(
    tmp_path: Path, monkeypatch
) -> None:
    recorded: dict[str, object] = {}

    def fake_run(
        command: list[str],
        *,
        cwd: Path,
        capture_output: bool,
        text: bool,
        encoding: str,
        errors: str,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        recorded["command"] = command
        recorded["cwd"] = cwd
        assert capture_output is True
        assert text is True
        assert encoding == "utf-8"
        assert errors == "replace"
        assert check is False
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=json.dumps({"run_id": "20260327T130000", "success": True}, ensure_ascii=False),
            stderr="",
        )

    monkeypatch.setattr(queue22_controller04_monitor.subprocess, "run", fake_run)

    payload = queue22_controller04_monitor.run_support_wave(
        tmp_path,
        providers=["119.8.113.226", "api.925214.xyz"],
        max_workers=6,
        timeout_seconds=300,
    )

    assert recorded["cwd"] == tmp_path
    assert recorded["command"] == [
        queue22_controller04_monitor.sys.executable,
        "scripts/queue22_system_fix_wave.py",
        "run",
        "--profile",
        "support",
        "--max-workers",
        "6",
        "--timeout-seconds",
        "300",
        "--json",
        "--provider",
        "119.8.113.226",
        "--provider",
        "api.925214.xyz",
    ]
    assert payload["ok"] is True
    assert payload["parsed_stdout"]["run_id"] == "20260327T130000"
    assert (tmp_path / queue22_controller04_monitor.MONITOR_ROOT / "latest_support_run.json").exists()
    assert len(list((tmp_path / queue22_controller04_monitor.SUPPORT_ROOT).glob("*/result.json"))) == 1


def test_refresh_controller_lock_updates_heartbeat_and_lease(tmp_path: Path) -> None:
    _write_text(
        tmp_path,
        queue22_system_fix_wave.ACTIVE_LOCK,
        "\n".join(
            [
                "# lock",
                "",
                "- `controller_id`: `controller04`",
                "- `run_label`: `queue22-20260327b`",
                "- `status`: `active`",
                "- `control_state`: `Recovery-Blocked`",
                "- `control_state_reason`: `runtime evidence still blocked`",
                "- `heartbeat_at`: `2026-03-27T12:00:00+08:00`",
                "- `lease_until`: `2026-03-27T16:00:00+08:00`",
            ]
        ),
    )

    queue22_controller04_monitor._refresh_controller_lock(tmp_path)

    text = (tmp_path / queue22_system_fix_wave.ACTIVE_LOCK).read_text(encoding="utf-8")
    heartbeat = queue22_controller04_monitor._field(text, "heartbeat_at")
    lease_until = queue22_controller04_monitor._field(text, "lease_until")

    assert heartbeat
    assert heartbeat != "2026-03-27T12:00:00+08:00"
    assert lease_until


def test_build_status_snapshot_reads_verifier_and_recommends_promote_ready(tmp_path: Path) -> None:
    fresh = _now_iso()
    _write_text(
        tmp_path,
        queue22_system_fix_wave.ACTIVE_LOCK,
        "\n".join(
            [
                "# lock",
                "",
                "- `controller_id`: `controller04`",
                "- `run_label`: `queue22-20260327b`",
                "- `status`: `active`",
                "- `control_state`: `Recovery-Rearm`",
                "- `control_state_reason`: `waiting`",
                f"- `heartbeat_at`: `{fresh}`",
                f"- `lease_until`: `{fresh}`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        "docs/_temp/problem/SLOT_OCCUPANCY.md",
        "\n".join(
            [
                "# slots",
                "",
                "- `run_label`: `queue22-20260327b`",
                "- `controller_id`: `controller04`",
                "- `control_state`: `Recovery-Rearm`",
                f"- `updated_at`: `{fresh}`",
                "- `prompt_authority`: `docs/_temp/problem/launch_prompts/queue22_system_fix_prompt_manifest_20260327b.md`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.PROMOTE_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111000",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.PROMOTE_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Promote-Ready`",
                "- `claim_token`: ``",
                "- `heartbeat_at`: ``",
                "- `attempt_run_dir`: ``",
                "- `blocked_reason`: ``",
                "- `review_status`: `pending`",
            ]
        ),
    )
    executor_attempt = (
        "output/task_runs/20260326-111003__P1__runtime__runtime-rearm-executor-r2/demo-executor/"
    )
    watchdog_attempt = (
        "output/task_runs/20260326-111004__P1__runtime__runtime-rearm-watchdog-r2/demo-watchdog/"
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}__owner-slot02-runtime-executor-r2__status-doing.md",
        "\n".join(
            [
                "# 111003",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}`",
                "- `status`: `doing`",
                "- `owner`: `slot02-runtime-executor-r2`",
                "- `run_label`: `queue22-20260327b`",
                "- `claim_mode`: `repair`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `claim_token`: `demo-executor`",
                f"- `heartbeat_at`: `{fresh}`",
                f"- `attempt_run_dir`: `{executor_attempt}`",
                "- `blocked_reason`: ``",
                "- `review_status`: `pending`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}__owner-slot03-runtime-watchdog-r2__status-doing.md",
        "\n".join(
            [
                "# 111004",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}`",
                "- `status`: `doing`",
                "- `owner`: `slot03-runtime-watchdog-r2`",
                "- `run_label`: `queue22-20260327b`",
                "- `claim_mode`: `readonly-watch`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `claim_token`: `demo-watchdog`",
                f"- `heartbeat_at`: `{fresh}`",
                f"- `attempt_run_dir`: `{watchdog_attempt}`",
                "- `blocked_reason`: ``",
                "- `review_status`: `pending`",
            ]
        ),
    )
    _write_json(tmp_path, Path(executor_attempt) / "preflight.json", {"ok": True})
    _write_json(tmp_path, Path(executor_attempt) / "before_state.json", {"runtime": {"state": "degraded"}})
    _write_json(tmp_path, Path(executor_attempt) / "process.json", {"started_at": fresh})
    _write_json(tmp_path, Path(executor_attempt) / "progress.json", {"heartbeat_at": fresh, "step": "live-execute"})
    _write_text(tmp_path, executor_attempt + "recovery.stdout.log", "step ok\n")
    _write_json(tmp_path, Path(executor_attempt) / "after_state.json", {"runtime": {"state": "ready"}})
    _write_json(tmp_path, Path(executor_attempt) / "result.json", {"status": "review"})
    _write_text(tmp_path, watchdog_attempt + "watchdog.jsonl", "{\"sample\": 1}\n")
    _write_json(
        tmp_path,
        Path(watchdog_attempt) / "watchdog_summary.json",
        {
            "sampling_window": {"sample_count": 2},
            "can_enter_recovery_executing": True,
            "can_enter_promote_ready": True,
        },
    )
    _write_support_summary(
        tmp_path,
        run_id="20260327T121500",
        task_id="slot07-runtime-evidence-verifier",
        provider="119.8.113.226",
        last_message_text="\n".join(
            [
                "## evidence_audit",
                "- `executor_artifact_completeness`: `valid`",
                "- `watchdog_artifact_completeness`: `valid`",
                "- `before_after_state_delta`: `valid`",
                "- `progress_authenticity`: `valid`",
                "- `can_enter_recovery_executing`: `true`",
                "- `can_enter_promote_ready`: `true`",
                "- `blocking_gaps`: `none`",
            ]
        ),
    )

    snapshot = queue22_controller04_monitor.build_status_snapshot(tmp_path)

    assert snapshot["verifier_verdict"]["before_after_state_delta"] == "valid"
    assert snapshot["slots"]["07"]["status"] == "success"
    assert snapshot["state_evaluation"]["can_enter_promote_ready"] is True
    assert snapshot["state_evaluation"]["recommended_control_state"] == "Promote-Ready"
    assert snapshot["claimability"]["111000"] is True
    assert snapshot["required_controller_actions"][0].startswith("Claim `111000`")


def test_load_json_accepts_utf8_bom(tmp_path: Path) -> None:
    path = tmp_path / "bom.json"
    path.write_bytes(b"\xef\xbb\xbf" + json.dumps({"ok": True}).encode("utf-8"))

    payload = queue22_controller04_monitor._load_json(path)

    assert payload == {"ok": True}


def test_build_status_snapshot_uses_latest_available_verifier_message_across_runs(tmp_path: Path) -> None:
    fresh = _now_iso()
    _write_text(
        tmp_path,
        queue22_system_fix_wave.ACTIVE_LOCK,
        "\n".join(
            [
                "# lock",
                "",
                "- `controller_id`: `controller04`",
                "- `run_label`: `queue22-20260327b`",
                "- `status`: `active`",
                "- `control_state`: `Recovery-Rearm`",
                "- `control_state_reason`: `waiting`",
                f"- `heartbeat_at`: `{fresh}`",
                f"- `lease_until`: `{fresh}`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        "docs/_temp/problem/SLOT_OCCUPANCY.md",
        "\n".join(
            [
                "# slots",
                "",
                "- `run_label`: `queue22-20260327b`",
                "- `controller_id`: `controller04`",
                "- `control_state`: `Recovery-Rearm`",
                f"- `updated_at`: `{fresh}`",
                "- `prompt_authority`: `docs/_temp/problem/launch_prompts/queue22_system_fix_prompt_manifest_20260327b.md`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.PROMOTE_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111000",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.PROMOTE_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Promote-Ready`",
                "- `claim_token`: ``",
                "- `heartbeat_at`: ``",
                "- `attempt_run_dir`: ``",
                "- `blocked_reason`: ``",
                "- `review_status`: `pending`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}__owner-slot02-runtime-executor-r2__status-blocked.md",
        "\n".join(
            [
                "# 111003",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}`",
                "- `status`: `blocked`",
                "- `owner`: `slot02-runtime-executor-r2`",
                "- `run_label`: `queue22-20260327b`",
                "- `claim_mode`: `repair`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `claim_token`: `demo-executor`",
                f"- `heartbeat_at`: `{fresh}`",
                "- `attempt_run_dir`: `output/task_runs/20260326-111003__P1__runtime__runtime-rearm-executor-r2/demo/`",
                "- `blocked_reason`: `sqlite_locked`",
                "- `review_status`: `pending`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}__owner-slot03-runtime-watchdog-r2__status-blocked.md",
        "\n".join(
            [
                "# 111004",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}`",
                "- `status`: `blocked`",
                "- `owner`: `slot03-runtime-watchdog-r2`",
                "- `run_label`: `queue22-20260327b`",
                "- `claim_mode`: `readonly-watch`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `claim_token`: `demo-watchdog`",
                f"- `heartbeat_at`: `{fresh}`",
                "- `attempt_run_dir`: `output/task_runs/20260326-111004__P1__runtime__runtime-rearm-watchdog-r2/demo/`",
                "- `blocked_reason`: `missing_terminal_artifacts`",
                "- `review_status`: `pending`",
            ]
        ),
    )
    _write_support_summary(
        tmp_path,
        run_id="20260327T120100",
        task_id="slot07-runtime-evidence-verifier",
        provider="119.8.113.226",
        last_message_text="\n".join(
            [
                "## evidence_audit",
                "- `executor_artifact_completeness`: `valid`",
                "- `watchdog_artifact_completeness`: `valid`",
                "- `before_after_state_delta`: `valid`",
                "- `progress_authenticity`: `valid`",
                "- `can_enter_recovery_executing`: `true`",
                "- `can_enter_promote_ready`: `true`",
                "- `blocking_gaps`: `none`",
            ]
        ),
    )
    _write_json(
        tmp_path,
        queue22_system_fix_wave.WAVE_EXPORT_ROOT / "support" / "20260327T120200" / "summary.json",
        {
            "run_id": "20260327T120200",
            "success": False,
            "started_at": "2026-03-27T04:02:00+00:00",
            "finished_at": "2026-03-27T04:02:20+00:00",
            "task_count": 4,
            "tasks": [
                {
                    "task_id": "slot07-runtime-evidence-verifier",
                    "selected_provider": None,
                    "attempts": [
                        {
                            "provider": "freeapi.dgbmc.top",
                            "last_message_path": str(tmp_path / "runtime" / "missing-last-message.txt"),
                        }
                    ],
                }
            ],
        },
    )

    snapshot = queue22_controller04_monitor.build_status_snapshot(tmp_path)

    assert snapshot["verifier_verdict"]["provider_id"] == "119.8.113.226"
    assert snapshot["state_evaluation"]["can_enter_promote_ready"] is True


def test_monitor_once_reconciles_control_state_to_blocked_on_missing_progress(tmp_path: Path) -> None:
    fresh = _now_iso()
    stale = _stale_iso()
    _write_text(
        tmp_path,
        queue22_system_fix_wave.ACTIVE_LOCK,
        "\n".join(
            [
                "# lock",
                "",
                "- `controller_id`: `controller04`",
                "- `run_label`: `queue22-20260327b`",
                "- `status`: `active`",
                "- `control_state`: `Recovery-Rearm`",
                "- `control_state_reason`: `waiting`",
                f"- `heartbeat_at`: `{fresh}`",
                f"- `lease_until`: `{fresh}`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        "docs/_temp/problem/SLOT_OCCUPANCY.md",
        "\n".join(
            [
                "# slots",
                "",
                "- `run_label`: `queue22-20260327b`",
                "- `controller_id`: `controller04`",
                "- `control_state`: `Recovery-Rearm`",
                f"- `updated_at`: `{fresh}`",
                "- `prompt_authority`: `docs/_temp/problem/launch_prompts/queue22_system_fix_prompt_manifest_20260327b.md`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.PROMOTE_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111000",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.PROMOTE_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Promote-Ready`",
                "- `claim_token`: ``",
                "- `heartbeat_at`: ``",
                "- `attempt_run_dir`: ``",
                "- `blocked_reason`: ``",
                "- `review_status`: `pending`",
            ]
        ),
    )
    executor_attempt = "output/task_runs/20260326-111003__P1__runtime__runtime-rearm-executor-r2/demo-blocked/"
    watchdog_attempt = "output/task_runs/20260326-111004__P1__runtime__runtime-rearm-watchdog-r2/demo-blocked/"
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}__owner-slot02-runtime-executor-r2__status-doing.md",
        "\n".join(
            [
                "# 111003",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}`",
                "- `status`: `doing`",
                "- `owner`: `slot02-runtime-executor-r2`",
                "- `run_label`: `queue22-20260327b`",
                "- `claim_mode`: `repair`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `claim_token`: `demo-blocked`",
                f"- `heartbeat_at`: `{fresh}`",
                f"- `attempt_run_dir`: `{executor_attempt}`",
                "- `blocked_reason`: ``",
                "- `review_status`: `pending`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}__owner-slot03-runtime-watchdog-r2__status-doing.md",
        "\n".join(
            [
                "# 111004",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}`",
                "- `status`: `doing`",
                "- `owner`: `slot03-runtime-watchdog-r2`",
                "- `run_label`: `queue22-20260327b`",
                "- `claim_mode`: `readonly-watch`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `claim_token`: `demo-watchdog`",
                f"- `heartbeat_at`: `{stale}`",
                f"- `attempt_run_dir`: `{watchdog_attempt}`",
                "- `blocked_reason`: ``",
                "- `review_status`: `pending`",
            ]
        ),
    )
    _write_json(tmp_path, Path(executor_attempt) / "preflight.json", {"ok": True})
    _write_json(tmp_path, Path(executor_attempt) / "before_state.json", {"runtime": {"state": "degraded"}})
    _write_json(tmp_path, Path(executor_attempt) / "process.json", {"started_at": fresh})
    _write_text(tmp_path, watchdog_attempt + "watchdog.jsonl", "{\"sample\": 1}\n")

    payload = queue22_controller04_monitor.monitor_once(
        tmp_path,
        providers=["119.8.113.226"],
        max_workers=6,
        timeout_seconds=300,
        run_support=False,
    )

    assert payload["status"]["control_state"] == "Recovery-Blocked"
    assert payload["status"]["state_evaluation"]["recommended_control_state"] == "Recovery-Blocked"
    assert "progress_json_missing" in payload["status"]["control_state_reason"]
    assert "heartbeat_stale" in payload["status"]["control_state_reason"]
    lock_text = (tmp_path / queue22_system_fix_wave.ACTIVE_LOCK).read_text(encoding="utf-8")
    slot_text = (tmp_path / "docs/_temp/problem/SLOT_OCCUPANCY.md").read_text(encoding="utf-8")
    assert "- `control_state`: `Recovery-Blocked`" in lock_text
    assert "- `control_state`: `Recovery-Blocked`" in slot_text


def test_build_status_snapshot_marks_support_slots_running_when_wave_active(tmp_path: Path) -> None:
    fresh = _now_iso()
    _write_text(
        tmp_path,
        queue22_system_fix_wave.ACTIVE_LOCK,
        "\n".join(
            [
                "# lock",
                "",
                "- `controller_id`: `controller04`",
                "- `run_label`: `queue22-20260327b`",
                "- `status`: `active`",
                "- `control_state`: `Recovery-Rearm`",
                "- `control_state_reason`: `waiting`",
                f"- `heartbeat_at`: `{fresh}`",
                f"- `lease_until`: `{fresh}`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        "docs/_temp/problem/SLOT_OCCUPANCY.md",
        "\n".join(
            [
                "# slots",
                "",
                "- `run_label`: `queue22-20260327b`",
                "- `controller_id`: `controller04`",
                "- `control_state`: `Recovery-Rearm`",
                f"- `updated_at`: `{fresh}`",
                "- `prompt_authority`: `docs/_temp/problem/launch_prompts/queue22_system_fix_prompt_manifest_20260327b.md`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.PROMOTE_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111000",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.PROMOTE_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Promote-Ready`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111003",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Recovery-Rearm`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111004",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Recovery-Rearm`",
            ]
        ),
    )
    _write_json(
        tmp_path,
        queue22_controller04_monitor.MONITOR_ROOT / "current_support_run.json",
        {
            "started_at": fresh,
            "pid": 123,
            "status": "running",
        },
    )

    snapshot = queue22_controller04_monitor.build_status_snapshot(tmp_path)

    assert snapshot["current_support_wave"]["status"] == "running"
    assert snapshot["slots"]["04"]["status"] == "running"
    assert snapshot["slots"]["05"]["status"] == "running"
    assert snapshot["slots"]["06"]["status"] == "running"
    assert snapshot["slots"]["07"]["status"] == "running"


def test_rearm_runtime_tasks_resets_watchdog_and_reopens_recovery_rearm(tmp_path: Path) -> None:
    fresh = _now_iso()
    _write_text(
        tmp_path,
        queue22_system_fix_wave.ACTIVE_LOCK,
        "\n".join(
            [
                "# lock",
                "",
                "- `controller_id`: `controller04`",
                "- `run_label`: `queue22-20260327b`",
                "- `status`: `active`",
                "- `control_state`: `Recovery-Blocked`",
                "- `control_state_reason`: `watchdog blocked`",
                f"- `heartbeat_at`: `{fresh}`",
                f"- `lease_until`: `{fresh}`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        "docs/_temp/problem/SLOT_OCCUPANCY.md",
        "\n".join(
            [
                "# slots",
                "",
                "- `run_label`: `queue22-20260327b`",
                "- `controller_id`: `controller04`",
                "- `control_state`: `Recovery-Blocked`",
                f"- `updated_at`: `{fresh}`",
                "- `prompt_authority`: `docs/_temp/problem/launch_prompts/queue22_system_fix_prompt_manifest_20260327b.md`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.PROMOTE_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111000",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.PROMOTE_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Promote-Ready`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111003",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `claim_mode`: `repair`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `claim_token`: ``",
                "- `heartbeat_at`: ``",
                "- `attempt_run_dir`: ``",
                "- `blocked_reason`: ``",
                "- `review_status`: `pending`",
                "- `work_notes`: ``",
            ]
        ),
    )
    blocked_relpath = (
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}"
        "__owner-slot03-runtime-watchdog-r2__status-blocked.md"
    )
    _write_text(
        tmp_path,
        blocked_relpath,
        "\n".join(
            [
                "# 111004",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}`",
                "- `status`: `blocked`",
                "- `owner`: `slot03-runtime-watchdog-r2`",
                "- `run_label`: `queue22-20260327b`",
                "- `claim_mode`: `readonly-watch`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `claim_token`: `slot03r2-demo`",
                f"- `heartbeat_at`: `{fresh}`",
                "- `attempt_run_dir`: `output/task_runs/20260326-111004__P1__runtime__runtime-rearm-watchdog-r2/demo/`",
                "- `task_lock_path`: `docs/_temp/problem/_claims/20260326-111004__P1__runtime__runtime-rearm-watchdog-r2.lock/`",
                "- `blocked_reason`: `executor_missing`",
                "- `review_status`: `pending`",
                "- `work_notes`: `Previous blocked run preserved.`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        "docs/_temp/problem/_claims/20260326-111004__P1__runtime__runtime-rearm-watchdog-r2.lock/CLAIM_LOCK.md",
        "# claim\n",
    )

    payload = queue22_controller04_monitor.rearm_runtime_tasks(tmp_path, "watchdog")

    reopened = (
        tmp_path
        / "docs"
        / "_temp"
        / "problem"
        / f"{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}__owner-unassigned__status-todo.md"
    )
    assert reopened.exists()
    reopened_text = reopened.read_text(encoding="utf-8")
    assert "- `status`: `todo`" in reopened_text
    assert "- `owner`: `unassigned`" in reopened_text
    assert "- `claim_token`: ``" in reopened_text
    assert "- `attempt_run_dir`: ``" in reopened_text
    assert "Controller04 rearmed the task for a fresh shared-root claim" in reopened_text
    assert not (
        tmp_path
        / "docs"
        / "_temp"
        / "problem"
        / "_claims"
        / "20260326-111004__P1__runtime__runtime-rearm-watchdog-r2.lock"
    ).exists()
    assert payload["status"]["control_state"] == "Recovery-Rearm"
    assert payload["status"]["claimability"]["111003"] is True
    assert payload["status"]["claimability"]["111004"] is True


def test_main_allows_rearm_runtime_without_provider(tmp_path: Path, capsys) -> None:
    fresh = _now_iso()
    _write_text(
        tmp_path,
        queue22_system_fix_wave.ACTIVE_LOCK,
        "\n".join(
            [
                "# lock",
                "",
                "- `controller_id`: `controller04`",
                "- `run_label`: `queue22-20260327b`",
                "- `status`: `active`",
                "- `control_state`: `Recovery-Blocked`",
                "- `control_state_reason`: `watchdog blocked`",
                f"- `heartbeat_at`: `{fresh}`",
                f"- `lease_until`: `{fresh}`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        "docs/_temp/problem/SLOT_OCCUPANCY.md",
        "\n".join(
            [
                "# slots",
                "",
                "- `run_label`: `queue22-20260327b`",
                "- `controller_id`: `controller04`",
                "- `control_state`: `Recovery-Blocked`",
                f"- `updated_at`: `{fresh}`",
                "- `prompt_authority`: `docs/_temp/problem/launch_prompts/queue22_system_fix_prompt_manifest_20260327b.md`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.PROMOTE_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111000",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.PROMOTE_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `control_state_required`: `Promote-Ready`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}__owner-unassigned__status-todo.md",
        "\n".join(
            [
                "# 111003",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_EXECUTOR_TASK_ID}`",
                "- `status`: `todo`",
                "- `owner`: `unassigned`",
                "- `run_label`: `queue22-20260327b`",
                "- `claim_mode`: `repair`",
                "- `control_state_required`: `Recovery-Rearm`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        f"docs/_temp/problem/{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}__owner-slot03-runtime-watchdog-r2__status-blocked.md",
        "\n".join(
            [
                "# 111004",
                "",
                f"- `task_id`: `{queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID}`",
                "- `status`: `blocked`",
                "- `owner`: `slot03-runtime-watchdog-r2`",
                "- `run_label`: `queue22-20260327b`",
                "- `claim_mode`: `readonly-watch`",
                "- `control_state_required`: `Recovery-Rearm`",
                "- `task_lock_path`: `docs/_temp/problem/_claims/20260326-111004__P1__runtime__runtime-rearm-watchdog-r2.lock/`",
            ]
        ),
    )
    _write_text(
        tmp_path,
        "docs/_temp/problem/_claims/20260326-111004__P1__runtime__runtime-rearm-watchdog-r2.lock/CLAIM_LOCK.md",
        "# claim\n",
    )

    code = queue22_controller04_monitor.main(
        [
            "--repo-root",
            str(tmp_path),
            "--rearm-runtime",
            "watchdog",
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert "\"target\": \"watchdog\"" in captured.out


def test_main_requires_explicit_providers(capsys) -> None:
    code = queue22_controller04_monitor.main(
        [
            "--once",
            "--skip-support",
            "--max-workers",
            "6",
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert "--provider explicitly" in captured.err


def test_main_requires_explicit_max_workers(capsys) -> None:
    code = queue22_controller04_monitor.main(
        [
            "--once",
            "--skip-support",
            "--provider",
            "119.8.113.226",
            "--max-workers",
            "5",
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert "--max-workers 6 explicitly" in captured.err
