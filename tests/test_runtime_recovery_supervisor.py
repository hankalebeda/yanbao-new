from __future__ import annotations

import json
from pathlib import Path

from scripts import runtime_recovery_supervisor


def test_blocked_reason_detects_database_lock():
    reason = runtime_recovery_supervisor.blocked_reason("sqlite3.OperationalError: database is locked", 1)

    assert reason == "database_locked_runtime_write_conflict"


def test_supervisor_writes_review_result_for_successful_command(tmp_path):
    attempt_dir = tmp_path / "attempt"
    rc = runtime_recovery_supervisor.main(
        [
            "--attempt-run-dir",
            str(attempt_dir),
            "--task-id",
            "task-ok",
            "--control-state",
            "Recovery-Rearm",
            "--command",
            "python",
            "-c",
            "print('ok')",
        ]
    )

    assert rc == 0
    assert (attempt_dir / "before_state.json").exists()
    assert (attempt_dir / "after_state.json").exists()
    assert (attempt_dir / "process.json").exists()
    assert (attempt_dir / "progress.json").exists()
    result = json.loads((attempt_dir / "result.json").read_text(encoding="utf-8"))
    assert result["status"] == "review"
    assert result["blocked_reason"] is None


def test_supervisor_writes_blocked_result_for_database_lock(tmp_path):
    attempt_dir = tmp_path / "attempt"
    rc = runtime_recovery_supervisor.main(
        [
            "--attempt-run-dir",
            str(attempt_dir),
            "--task-id",
            "task-locked",
            "--control-state",
            "Recovery-Executing",
            "--command",
            "python",
            "-c",
            "import sys; sys.stderr.write('database is locked\\n'); sys.exit(1)",
        ]
    )

    assert rc == 1
    result = json.loads((attempt_dir / "result.json").read_text(encoding="utf-8"))
    assert result["status"] == "blocked"
    assert result["blocked_reason"] == "database_locked_runtime_write_conflict"
    assert "stop competing runtime DB writers" in result["requires_followup"]
