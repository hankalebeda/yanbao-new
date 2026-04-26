#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.db import SessionLocal
from app.services.observability import runtime_metrics_summary
from app.services.runtime_anchor_service import RuntimeAnchorService


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Supervise a runtime recovery command and materialize standard attempt artifacts."
    )
    parser.add_argument("--attempt-run-dir", required=True)
    parser.add_argument("--task-id", default="")
    parser.add_argument("--control-state", default="Recovery-Rearm")
    parser.add_argument("--stdout-log", default="recovery.stdout.log")
    parser.add_argument("--stderr-log", default="recovery.stderr.log")
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument(
        "--command",
        nargs=argparse.REMAINDER,
        required=True,
        help="Command to supervise. Prefix with --command, e.g. --command python -u scripts/repair_runtime_history.py",
    )
    return parser.parse_args(argv)


def capture_runtime_state() -> dict[str, Any]:
    with SessionLocal() as db:
        service = RuntimeAnchorService(db)
        metrics = runtime_metrics_summary(db, runtime_anchor_service=service)
        return {
            "captured_at": now_iso(),
            "runtime_anchor_dates": service.runtime_anchor_dates(),
            "public_runtime_status": service.public_runtime_status(),
            "dashboard_30d": metrics.get("dashboard_30d") or {},
            "settlement_pipeline": metrics.get("settlement_pipeline") or {},
            "data_quality": metrics.get("data_quality") or {},
        }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def blocked_reason(stderr_text: str, returncode: int | None) -> str | None:
    lowered = stderr_text.lower()
    if "database is locked" in lowered:
        return "database_locked_runtime_write_conflict"
    if returncode and returncode != 0:
        return f"runtime_command_failed_rc_{returncode}"
    return None


def terminal_status(reason: str | None, returncode: int | None) -> str:
    if reason:
        return "blocked"
    if returncode == 0:
        return "review"
    return "blocked"


def build_process_payload(
    *,
    pid: int,
    started_at: str,
    last_progress_at: str,
    last_progress_step: str,
    stdout_path: Path,
    control_state: str,
    finished_at: str | None = None,
    returncode: int | None = None,
    state: str = "running",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "pid": pid,
        "started_at": started_at,
        "last_progress_at": last_progress_at,
        "last_progress_step": last_progress_step,
        "stdout_path": stdout_path.as_posix(),
        "observed_control_state": control_state,
        "command": [],
    }
    if finished_at is not None:
        payload["finished_at"] = finished_at
    if returncode is not None:
        payload["returncode"] = returncode
    if state:
        payload["state"] = state
    return payload


def build_progress_payload(
    *,
    pid: int,
    started_at: str,
    heartbeat_at: str,
    last_progress_step: str,
    stdout_path: Path,
    control_state: str,
    stdout_bytes: int,
    finished_at: str | None = None,
    returncode: int | None = None,
    progress_source: str = "supervisor_heartbeat",
    blocked: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "pid": pid,
        "started_at": started_at,
        "last_progress_at": heartbeat_at,
        "last_progress_step": last_progress_step,
        "stdout_path": stdout_path.as_posix(),
        "observed_control_state": control_state,
        "heartbeat_at": heartbeat_at,
        "stdout_bytes": stdout_bytes,
        "progress_source": progress_source,
    }
    if finished_at is not None:
        payload["finished_at"] = finished_at
    if returncode is not None:
        payload["returncode"] = returncode
    if blocked is not None:
        payload["blocked_reason"] = blocked
    return payload


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("--command must not be empty")

    attempt_dir = Path(args.attempt_run_dir).resolve()
    attempt_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = attempt_dir / args.stdout_log
    stderr_path = attempt_dir / args.stderr_log
    process_path = attempt_dir / "process.json"
    progress_path = attempt_dir / "progress.json"
    before_state_path = attempt_dir / "before_state.json"
    after_state_path = attempt_dir / "after_state.json"
    result_path = attempt_dir / "result.json"

    started_at = now_iso()
    if not before_state_path.exists():
        write_json(before_state_path, capture_runtime_state())

    with stdout_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open("w", encoding="utf-8") as stderr_file:
        proc = subprocess.Popen(
            command,
            cwd=str(PROJECT_ROOT),
            stdout=stdout_file,
            stderr=stderr_file,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        last_step = "supervisor_started"
        while True:
            returncode = proc.poll()
            heartbeat_at = now_iso()
            stdout_bytes = stdout_path.stat().st_size if stdout_path.exists() else 0
            if returncode is None:
                if stdout_bytes == 0:
                    last_step = "waiting_for_first_stdout_heartbeat_1205"
                else:
                    last_step = "stdout_seen_supervisor_heartbeat"
                write_json(
                    process_path,
                    build_process_payload(
                        pid=proc.pid,
                        started_at=started_at,
                        last_progress_at=heartbeat_at,
                        last_progress_step=last_step,
                        stdout_path=stdout_path,
                        control_state=args.control_state,
                    ),
                )
                write_json(
                    progress_path,
                    build_progress_payload(
                        pid=proc.pid,
                        started_at=started_at,
                        heartbeat_at=heartbeat_at,
                        last_progress_step=last_step,
                        stdout_path=stdout_path,
                        control_state=args.control_state,
                        stdout_bytes=stdout_bytes,
                    ),
                )
                time.sleep(max(1, int(args.poll_seconds)))
                continue

            finished_at = now_iso()
            stderr_text = read_text(stderr_path)
            reason = blocked_reason(stderr_text, returncode)
            final_state = "blocked" if reason else ("completed" if returncode == 0 else "failed")
            last_step = "command_finished" if returncode == 0 else (reason or f"command_failed_rc_{returncode}")
            write_json(
                process_path,
                build_process_payload(
                    pid=proc.pid,
                    started_at=started_at,
                    last_progress_at=finished_at,
                    last_progress_step=last_step,
                    stdout_path=stdout_path,
                    control_state=args.control_state,
                    finished_at=finished_at,
                    returncode=returncode,
                    state=final_state,
                ),
            )
            write_json(
                progress_path,
                build_progress_payload(
                    pid=proc.pid,
                    started_at=started_at,
                    heartbeat_at=finished_at,
                    last_progress_step=last_step,
                    stdout_path=stdout_path,
                    control_state=args.control_state,
                    stdout_bytes=stdout_bytes,
                    finished_at=finished_at,
                    returncode=returncode,
                    blocked=reason,
                ),
            )
            break

    after_state = capture_runtime_state()
    write_json(after_state_path, after_state)

    stderr_text = read_text(stderr_path)
    stdout_text = read_text(stdout_path)
    reason = blocked_reason(stderr_text, proc.returncode)
    result = {
        "task_id": args.task_id or None,
        "status": terminal_status(reason, proc.returncode),
        "completed_at": now_iso(),
        "command": command,
        "returncode": proc.returncode,
        "stdout_log_bytes": stdout_path.stat().st_size if stdout_path.exists() else 0,
        "stderr_log_bytes": stderr_path.stat().st_size if stderr_path.exists() else 0,
        "stdout_tail": stdout_text.strip().splitlines()[-5:],
        "stderr_tail": stderr_text.strip().splitlines()[-12:],
        "blocked_reason": reason,
        "requires_followup": [],
    }
    if reason == "database_locked_runtime_write_conflict":
        result["requires_followup"] = [
            "stop competing runtime DB writers",
            "rerun runtime recovery under single-writer supervision",
            "preserve terminal after_state/result artifacts for controller adjudication",
        ]
    write_json(result_path, result)
    return 0 if result["status"] == "review" else 1


if __name__ == "__main__":
    raise SystemExit(main())
