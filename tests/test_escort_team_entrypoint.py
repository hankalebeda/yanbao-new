from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_escort_team_entrypoint_dry_run() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "Escort_Team.py"

    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(repo_root) if not pythonpath else str(repo_root) + os.pathsep + pythonpath

    result = subprocess.run(
        [sys.executable, str(script_path), "--dry-run"],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Escort Team Config" in result.stdout
    assert "Agents:" in result.stdout