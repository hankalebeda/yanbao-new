"""
Repo Hygiene — classify and manage uncommitted workspace items.

Classifies git status items into categories:
  - build_output: compiled/generated files that can be safely cleaned
  - temp_files: temporary files from pytest, editors, etc.
  - runtime_state: service PID files, logs, leases
  - config_drift: .env, config changes that need review
  - code_changes: actual source code changes (app/, tests/, automation/)
  - doc_changes: documentation changes (docs/)
  - unknown: unclassified items

Usage:
  python -m automation.diagnostics.repo_hygiene              # report only
  python -m automation.diagnostics.repo_hygiene --clean-safe  # clean build_output + temp_files
  python -m automation.diagnostics.repo_hygiene --json        # machine-readable
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(os.environ.get("LOOP_CONTROLLER_REPO_ROOT", "")).resolve() or Path(__file__).resolve().parents[2]

# Classification rules: (category, compiled regex pattern list)
_RULES: list[tuple[str, list[re.Pattern[str]]]] = [
    ("build_output", [
        re.compile(r"__pycache__/"),
        re.compile(r"\.pyc$"),
        re.compile(r"\.pyo$"),
        re.compile(r"\.egg-info/"),
        re.compile(r"dist/"),
        re.compile(r"build/"),
        re.compile(r"\.whl$"),
    ]),
    ("temp_files", [
        re.compile(r"^tmp_"),
        re.compile(r"\.tmp$"),
        re.compile(r"basetemp_audit/"),
        re.compile(r"tmp_pytest_run"),
        re.compile(r"\.log$"),
        re.compile(r"\.log\.err$"),
        re.compile(r"DIRS_CREATED/"),
        re.compile(r"echo/"),
        re.compile(r"mkdir/"),
        re.compile(r"-p/"),
    ]),
    ("runtime_state", [
        re.compile(r"runtime/services/"),
        re.compile(r"runtime/loop_controller/"),
        re.compile(r"runtime/writeback_coordination/"),
        re.compile(r"\.pid$"),
        re.compile(r"start_.*\.cmd$", re.IGNORECASE),
    ]),
    ("config_drift", [
        re.compile(r"\.env$"),
        re.compile(r"\.env\.local$"),
        re.compile(r"automation/deploy/\.env"),
        re.compile(r"automation/control_plane/"),
    ]),
    ("code_changes", [
        re.compile(r"^app/"),
        re.compile(r"^tests/"),
        re.compile(r"^automation/"),
        re.compile(r"^scripts/"),
    ]),
    ("doc_changes", [
        re.compile(r"^docs/"),
        re.compile(r"\.md$"),
        re.compile(r"AGENTS\.md$"),
    ]),
]


def _classify(path: str) -> str:
    """Return the category for a git status path."""
    norm = path.replace("\\", "/").strip()
    for category, patterns in _RULES:
        for pat in patterns:
            if pat.search(norm):
                return category
    return "unknown"


def get_git_status() -> list[dict[str, str]]:
    """Run git status --porcelain and return parsed entries."""
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )
    entries = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        status_code = line[:2]
        filepath = line[3:].strip()
        # Handle renamed files
        if " -> " in filepath:
            filepath = filepath.split(" -> ")[1]
        entries.append({
            "status": status_code.strip(),
            "path": filepath,
            "category": _classify(filepath),
        })
    return entries


def classify_status() -> dict[str, Any]:
    """Get classified git status report."""
    entries = get_git_status()
    categories: dict[str, list[dict[str, str]]] = {}
    for entry in entries:
        cat = entry["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(entry)

    return {
        "total": len(entries),
        "categories": {cat: len(items) for cat, items in categories.items()},
        "details": categories,
        "safe_to_clean": len(categories.get("build_output", [])) + len(categories.get("temp_files", [])),
    }


def clean_safe(dry_run: bool = True) -> dict[str, Any]:
    """Clean build_output and temp_files (untracked only)."""
    entries = get_git_status()
    cleanable = [
        e for e in entries
        if e["category"] in ("build_output", "temp_files")
        and e["status"] == "??"  # only untracked
    ]

    cleaned = []
    errors = []
    for entry in cleanable:
        full_path = REPO_ROOT / entry["path"]
        if dry_run:
            cleaned.append(entry["path"])
            continue
        try:
            if full_path.is_dir():
                import shutil
                shutil.rmtree(str(full_path), ignore_errors=True)
            elif full_path.exists():
                full_path.unlink()
            cleaned.append(entry["path"])
        except Exception as exc:
            errors.append({"path": entry["path"], "error": str(exc)[:100]})

    return {
        "dry_run": dry_run,
        "cleaned_count": len(cleaned),
        "cleaned": cleaned[:50],
        "errors": errors,
    }


def print_report(report: dict[str, Any]) -> None:
    print(f"\n{'='*60}")
    print(f"  REPO HYGIENE — {report['total']} uncommitted items")
    print(f"{'='*60}")

    for cat, count in sorted(report["categories"].items(), key=lambda x: -x[1]):
        print(f"\n  [{cat}] ({count} items)")
        for item in report["details"][cat][:10]:
            print(f"    {item['status']:>2} {item['path']}")
        if count > 10:
            print(f"    ... and {count - 10} more")

    print(f"\n  Safe to clean: {report['safe_to_clean']} items (build_output + temp_files)")
    print(f"{'='*60}\n")


def main():
    as_json = "--json" in sys.argv
    do_clean = "--clean-safe" in sys.argv

    report = classify_status()

    if as_json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print_report(report)

    if do_clean:
        print("\nCleaning safe items (untracked build_output + temp_files)...")
        result = clean_safe(dry_run=False)
        if as_json:
            print(json.dumps(result, indent=2))
        else:
            print(f"  Cleaned: {result['cleaned_count']} items")
            if result["errors"]:
                print(f"  Errors: {len(result['errors'])}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
