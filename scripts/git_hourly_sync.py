from __future__ import annotations

import argparse
import fnmatch
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence


DIRECTORY_WHITELIST = (
    "app",
    "scripts",
    "tests",
    "docs/core",
    "docs/提示词",
    "github/docs",
    ".cursor/rules",
    ".cursor/skills",
)

FILE_WHITELIST = (
    "github/README.md",
    "app/governance/catalog_snapshot.json",
    "output/junit.xml",
    "output/blind_spot_audit.json",
    "github/automation/continuous_audit/latest_run.json",
    "AGENTS.md",
    "README.md",
    ".gitignore",
    ".aiexclude",
    ".cursorignore",
    ".ignore",
    "requirements.txt",
    "package.json",
    "package-lock.json",
    "pytest.ini",
)

FORBIDDEN_STAGE_PATTERNS = (
    ".env",
    ".claude/**",
    "data/**",
    "runtime/**",
    "github/token.txt",
    "docs/old/**",
    "docs/_temp/**",
    "tests/legacy_*.py",
    "*.db",
    "*.sqlite",
    "*.sqlite3",
    "*.log",
    "*.tgz",
    "*.tar.gz",
)

SYNC_EXCLUDE_PATTERNS = (
    "docs/core/test_results/**",
    "docs/core/test_*.py",
    "tests/legacy_*.py",
    "tests/gemini_results/**",
    "tests/demo_spacing.spec.js",
    "tests/verify_demo_spacing.py",
    "tests/test_ai_api_contract.py",
    "tests/test_api.py",
    "tests/test_data_sources.py",
    "tests/test_e2e_sim.py",
    "tests/test_trade_calendar.py",
)


class SyncError(RuntimeError):
    """Raised when the hourly sync cannot continue safely."""


@dataclass(frozen=True)
class SyncSummary:
    staged_paths: tuple[str, ...]
    commit_message: str | None
    created_commit: bool
    pushed: bool


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def normalize_rel(path: str) -> str:
    return path.replace("\\", "/").strip("/")


def whitelist_pathspecs() -> list[str]:
    return [*DIRECTORY_WHITELIST, *FILE_WHITELIST]


def existing_include_pathspecs(root: Path) -> list[str]:
    return [path for path in whitelist_pathspecs() if (root / path).exists()]


def sync_pathspecs(root: Path) -> list[str]:
    includes = existing_include_pathspecs(root)
    if not includes:
        return []
    excludes = [f":(exclude,glob){pattern}" for pattern in SYNC_EXCLUDE_PATTERNS]
    return [*includes, *excludes]


def build_commit_message(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now()).strftime("%Y-%m-%d %H:%M")
    return f"chore(sync): hourly repo sync {timestamp}"


def is_whitelisted(path: str) -> bool:
    rel = normalize_rel(path)
    for pattern in SYNC_EXCLUDE_PATTERNS:
        if fnmatch.fnmatch(rel, pattern):
            return False
    if rel in FILE_WHITELIST:
        return True
    for prefix in DIRECTORY_WHITELIST:
        if rel == prefix or rel.startswith(prefix + "/"):
            return True
    return False


def is_forbidden(path: str) -> bool:
    rel = normalize_rel(path)
    for pattern in FORBIDDEN_STAGE_PATTERNS:
        if fnmatch.fnmatch(rel, pattern):
            return True
    return False


def git(
    root: Path,
    *args: str,
    check: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        env=env,
    )
    if check and completed.returncode != 0:
        raise SyncError(
            completed.stderr.strip()
            or completed.stdout.strip()
            or f"git {' '.join(args)} failed with exit code {completed.returncode}"
        )
    return completed


def staged_paths(root: Path) -> list[str]:
    result = git(root, "diff", "--cached", "--name-only", "--diff-filter=ACMRD", check=True)
    return [normalize_rel(line) for line in result.stdout.splitlines() if line.strip()]


def working_tree_paths(root: Path) -> list[str]:
    result = git(root, "status", "--porcelain", "--untracked-files=all", check=True)
    paths: list[str] = []
    for line in result.stdout.splitlines():
        if len(line) < 4:
            continue
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.append(normalize_rel(path))
    return paths


def classify_paths(paths: Iterable[str]) -> tuple[list[str], list[str]]:
    forbidden = sorted({path for path in paths if is_forbidden(path)})
    out_of_scope = sorted({path for path in paths if not is_whitelisted(path)})
    return forbidden, out_of_scope


def pull_rebase(root: Path, remote: str, branch: str) -> None:
    git(root, "pull", "--rebase", "--autostash", remote, branch, check=True)


def stage_whitelist(root: Path) -> None:
    pathspecs = sync_pathspecs(root)
    if not pathspecs:
        return
    git(root, "add", "--all", "--", *pathspecs, check=True)


def commit_whitelist(root: Path, message: str) -> None:
    pathspecs = sync_pathspecs(root)
    if not pathspecs:
        raise SyncError("no existing whitelist pathspecs available for commit")
    git(root, "commit", "--only", "-m", message, "--", *pathspecs, check=True)


def push_branch(root: Path, remote: str, branch: str) -> None:
    git(root, "push", remote, branch, check=True)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Whitelist-only hourly GitHub sync.")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--commit-message", default=None)
    parser.add_argument("--skip-pull", action="store_true")
    parser.add_argument("--no-push", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def run_sync(args: argparse.Namespace) -> SyncSummary:
    root = repo_root()
    existing_staged = staged_paths(root)
    forbidden_existing, _ = classify_paths(existing_staged)
    if forbidden_existing:
        raise SyncError(
            "refusing to continue because forbidden staged paths exist: "
            + ", ".join(forbidden_existing)
        )

    if not args.skip_pull:
        pull_rebase(root, args.remote, args.branch)

    if args.dry_run:
        current_paths = working_tree_paths(root)
        staged_after = sorted({path for path in current_paths if is_whitelisted(path)})
    else:
        stage_whitelist(root)
        staged_after = staged_paths(root)

    forbidden_after, _ = classify_paths(staged_after)
    if forbidden_after:
        raise SyncError(
            "refusing to continue because forbidden paths were staged: "
            + ", ".join(forbidden_after)
        )

    commit_scope_paths = tuple(path for path in staged_after if is_whitelisted(path))

    if not commit_scope_paths:
        return SyncSummary(staged_paths=(), commit_message=None, created_commit=False, pushed=False)

    commit_message = args.commit_message or build_commit_message()
    if args.dry_run:
        return SyncSummary(
            staged_paths=commit_scope_paths,
            commit_message=commit_message,
            created_commit=False,
            pushed=False,
        )

    commit_whitelist(root, commit_message)

    pushed = False
    if not args.no_push:
        push_branch(root, args.remote, args.branch)
        pushed = True

    return SyncSummary(
        staged_paths=commit_scope_paths,
        commit_message=commit_message,
        created_commit=True,
        pushed=pushed,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = run_sync(args)
    except SyncError as exc:
        print(f"[git-hourly-sync] ERROR: {exc}", file=sys.stderr)
        return 1

    if not summary.created_commit and not summary.staged_paths:
        print("[git-hourly-sync] No whitelist changes to sync.")
        return 0

    print(f"[git-hourly-sync] staged={len(summary.staged_paths)}")
    for path in summary.staged_paths:
        print(f"  - {path}")

    if summary.commit_message is not None:
        print(f"[git-hourly-sync] commit_message={summary.commit_message}")

    if summary.created_commit:
        print("[git-hourly-sync] commit_created=true")
    else:
        print("[git-hourly-sync] commit_created=false")

    print(f"[git-hourly-sync] pushed={str(summary.pushed).lower()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
