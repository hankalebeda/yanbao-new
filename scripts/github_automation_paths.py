from __future__ import annotations

import shutil
from pathlib import Path


def github_root(root: Path) -> Path:
    return root / "github"


def github_docs_root(root: Path) -> Path:
    return github_root(root) / "docs"


def automation_root(root: Path) -> Path:
    return github_root(root) / "automation"


def continuous_audit_dir(root: Path) -> Path:
    return automation_root(root) / "continuous_audit"


def legacy_continuous_audit_dir(root: Path) -> Path:
    return root / "docs" / "_temp" / "continuous_audit"


def live_fix_loop_dir(root: Path) -> Path:
    return automation_root(root) / "live_fix_loop"


def legacy_live_fix_loop_dir(root: Path) -> Path:
    return root / "docs" / "_temp" / "live_fix_loop"


def live_fix_loop_runs_dir(root: Path) -> Path:
    return live_fix_loop_dir(root) / "automation_runs"


def live_fix_loop_state_file(root: Path) -> Path:
    return live_fix_loop_dir(root) / "automation_state.json"


def live_fix_loop_lock_file(root: Path) -> Path:
    return live_fix_loop_dir(root) / "automation.lock"


def seed_dir_from_legacy(current_dir: Path, legacy_dir: Path) -> bool:
    if current_dir.exists() or not legacy_dir.exists() or not legacy_dir.is_dir():
        return False
    current_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(legacy_dir, current_dir, dirs_exist_ok=True)
    return True
