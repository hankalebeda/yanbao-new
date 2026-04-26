from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts import git_hourly_sync


def _git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if check and completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or f"git {' '.join(args)} failed")
    return completed


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _init_repo(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Test User")
    _git(repo, "config", "user.email", "test@example.com")
    _write(
        repo / ".gitignore",
        "\n".join(
            (
                ".env",
                "docs/old/",
                "docs/_temp/",
                "docs/core/test_results/",
                "docs/core/test_*.py",
                "tests/legacy_*.py",
                "tests/gemini_results/",
                ".cursor/*",
                "!.cursor/rules/",
                "!.cursor/rules/**",
                "!.cursor/skills/",
                "!.cursor/skills/**",
                "github/automation/continuous_audit/*",
                "!github/automation/continuous_audit/latest_run.json",
            )
        )
        + "\n",
    )
    _write(repo / "README.md", "# repo\n")
    _write(repo / "AGENTS.md", "agents\n")
    _write(repo / "app" / "main.py", "print('base')\n")
    _write(repo / "scripts" / "tool.py", "print('tool')\n")
    _write(repo / "tests" / "test_alpha.py", "def test_alpha():\n    assert True\n")
    _write(repo / "docs" / "core" / "01_spec.md", "spec\n")
    _write(repo / "docs" / "提示词" / "18_prompt.md", "prompt\n")
    _write(repo / "github" / "README.md", "github docs\n")
    _write(repo / "github" / "docs" / "01_intro.md", "docs\n")
    _write(repo / "app" / "governance" / "catalog_snapshot.json", "{}\n")
    _write(repo / "output" / "junit.xml", "<testsuite />\n")
    _write(repo / "output" / "blind_spot_audit.json", "{}\n")
    _write(repo / "github" / "automation" / "continuous_audit" / "latest_run.json", "{}\n")
    _write(repo / ".cursor" / "rules" / "rule.mdc", "rule\n")
    _write(repo / ".cursor" / "skills" / "skill" / "SKILL.md", "skill\n")
    _write(repo / "notes" / "local.md", "local\n")
    _write(repo / ".aiexclude", "data/\n")
    _write(repo / ".cursorignore", "data/\n")
    _write(repo / ".ignore", "data/\n")
    _write(repo / "requirements.txt", "pytest\n")
    _write(repo / "package.json", "{}\n")
    _write(repo / "package-lock.json", "{}\n")
    _write(repo / "pytest.ini", "[pytest]\n")
    _write(repo / "docs" / "old" / "legacy.md", "legacy\n")
    _write(repo / "tests" / "legacy_old.py", "legacy\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "init")


def test_build_commit_message_contains_expected_prefix():
    message = git_hourly_sync.build_commit_message()

    assert message.startswith("chore(sync): hourly repo sync ")


def test_is_whitelisted_rejects_excluded_subpaths():
    assert git_hourly_sync.is_whitelisted("app/main.py") is True
    assert git_hourly_sync.is_whitelisted("docs/core/test_results/old.json") is False
    assert git_hourly_sync.is_whitelisted("tests/legacy_trade_calendar.py") is False
    assert git_hourly_sync.is_whitelisted("tests/test_api.py") is False


def test_run_sync_commits_whitelist_changes_only(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    _init_repo(repo)
    _write(repo / "app" / "main.py", "print('changed')\n")
    _write(repo / "notes" / "local.md", "still local\n")

    monkeypatch.setattr(git_hourly_sync, "repo_root", lambda: repo)
    args = git_hourly_sync.parse_args(["--skip-pull", "--no-push"])

    summary = git_hourly_sync.run_sync(args)

    assert summary.created_commit is True
    assert "app/main.py" in summary.staged_paths
    log_output = _git(repo, "log", "--oneline", "--decorate=no", "-1").stdout.strip()
    assert "chore(sync): hourly repo sync" in log_output
    status_output = _git(repo, "status", "--short").stdout
    assert " M notes/local.md" in status_output


def test_run_sync_is_noop_when_only_non_whitelist_paths_change(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    _init_repo(repo)
    _write(repo / "docs" / "old" / "legacy.md", "still local\n")

    monkeypatch.setattr(git_hourly_sync, "repo_root", lambda: repo)
    args = git_hourly_sync.parse_args(["--skip-pull", "--no-push"])

    summary = git_hourly_sync.run_sync(args)

    assert summary.created_commit is False
    assert summary.staged_paths == ()


def test_run_sync_fails_when_forbidden_path_is_staged(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    _init_repo(repo)
    _write(repo / ".env", "TOKEN=secret\n")
    _git(repo, "add", "--force", ".env")

    monkeypatch.setattr(git_hourly_sync, "repo_root", lambda: repo)
    args = git_hourly_sync.parse_args(["--skip-pull", "--no-push"])

    with pytest.raises(git_hourly_sync.SyncError):
        git_hourly_sync.run_sync(args)


def test_run_sync_ignores_staged_non_whitelist_paths_when_committing(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    _init_repo(repo)
    _write(repo / "notes" / "todo.txt", "todo\n")
    _git(repo, "add", "notes/todo.txt")
    _git(repo, "commit", "-m", "notes")
    _write(repo / "notes" / "todo.txt", "todo changed\n")
    _git(repo, "add", "notes/todo.txt")
    _write(repo / "app" / "main.py", "print('changed')\n")

    monkeypatch.setattr(git_hourly_sync, "repo_root", lambda: repo)
    args = git_hourly_sync.parse_args(["--skip-pull", "--no-push"])

    summary = git_hourly_sync.run_sync(args)

    assert summary.created_commit is True
    assert "app/main.py" in summary.staged_paths
    assert "notes/todo.txt" not in summary.staged_paths
    last_commit = _git(repo, "show", "--stat", "--oneline", "HEAD").stdout
    assert "app/main.py" in last_commit
    assert "notes/todo.txt" not in last_commit
    status_output = _git(repo, "status", "--short").stdout
    assert "M  notes/todo.txt" in status_output


def test_register_git_hourly_sync_task_supports_whatif():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "scripts" / "register_git_hourly_sync_task.ps1"

    completed = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script_path),
            "-WhatIf",
            "-Branch",
            "main",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )

    assert completed.returncode == 0
    assert "schtasks /Create" in completed.stdout
    assert "run_git_hourly_sync.ps1" in completed.stdout
    assert "-Branch" in completed.stdout
