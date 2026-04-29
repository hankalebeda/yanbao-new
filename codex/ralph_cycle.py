from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from codex import ralph_compile


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BRANCH = "main"
CONFIG_PATH = Path(".claude/ralph/config.json")
LAST_BRANCH_PATH = Path(".claude/ralph/loop/.last-branch")
LOOP_PRD_PATH = Path(".claude/ralph/loop/prd.json")
TARGETED_PYTEST_ARGS = [
    sys.executable,
    "-m",
    "pytest",
    "tests/test_ralph_compile.py",
    "tests/test_ralph_cycle.py",
    "tests/test_history_guardian.py",
    "tests/test_repair_runtime_history.py",
    "-q",
    "--tb=short",
]
MAX_OUTPUT_CHARS = 4000
TRANSIENT_WORKSPACE_RECHECK_DELAY_SEC = 0.35


def _configure_stdio_utf8() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except OSError:
                pass


@dataclass(slots=True)
class PreflightCheck:
    name: str
    status: str
    detail: str
    returncode: int | None = None
    output: str | None = None
    data: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
        }
        if self.returncode is not None:
            payload["returncode"] = self.returncode
        if self.output:
            payload["output"] = self.output
        if self.data is not None:
            payload["data"] = self.data
        return payload


@dataclass(slots=True)
class CycleDecision:
    cycle_index: int
    status: str
    rebuild_before: dict[str, Any]
    step2: dict[str, Any] | None
    rebuild_after: dict[str, Any] | None


@dataclass(slots=True)
class CycleSummary:
    cycles_run: int
    final_status: str
    stories_total: int
    stories_passed: int
    stories_remaining: int
    new_story_ids_last_cycle: list[str]
    regressed_story_ids_last_cycle: list[str]
    history: list[CycleDecision]
    expected_branch: str | None = None
    last_branch: str | None = None
    current_branch: str | None = None
    branch_distance: dict[str, int] | None = None
    tracked_changes: list[str] = field(default_factory=list)
    status_reason: str | None = None
    preflight: list[PreflightCheck] = field(default_factory=list)
    initial_tracked_changes: list[str] | None = None
    rechecked_tracked_changes: list[str] | None = None
    transient_workspace_dirty_recovered: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "cycles_run": self.cycles_run,
            "final_status": self.final_status,
            "stories_total": self.stories_total,
            "stories_passed": self.stories_passed,
            "stories_remaining": self.stories_remaining,
            "new_story_ids_last_cycle": self.new_story_ids_last_cycle,
            "regressed_story_ids_last_cycle": self.regressed_story_ids_last_cycle,
            "history": [asdict(item) for item in self.history],
            "expected_branch": self.expected_branch,
            "last_branch": self.last_branch,
            "current_branch": self.current_branch,
            "branch_distance": self.branch_distance,
            "tracked_changes": self.tracked_changes,
            "status_reason": self.status_reason,
            "preflight": [item.to_dict() for item in self.preflight],
        }
        if self.initial_tracked_changes is not None:
            payload["initial_tracked_changes"] = self.initial_tracked_changes
        if self.rechecked_tracked_changes is not None:
            payload["rechecked_tracked_changes"] = self.rechecked_tracked_changes
        if self.transient_workspace_dirty_recovered is not None:
            payload["transient_workspace_dirty_recovered"] = self.transient_workspace_dirty_recovered
        return payload


def _trim_output(*parts: str | None) -> str:
    text = "\n".join(part.strip() for part in parts if part and part.strip())
    return text[:MAX_OUTPUT_CHARS]


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _expected_branch(repo_root: Path) -> str:
    config = _load_json_dict(repo_root / CONFIG_PATH)
    branch_policy = config.get("branchNamePolicy")
    if isinstance(branch_policy, dict):
        current_value = str(branch_policy.get("currentValue") or "").strip()
        if current_value:
            return current_value
    last_branch = _read_last_branch(repo_root)
    if last_branch:
        return last_branch
    runtime_prd = _load_json_dict(repo_root / LOOP_PRD_PATH)
    branch_name = str(runtime_prd.get("branchName") or "").strip()
    return branch_name or DEFAULT_BRANCH


def _read_last_branch(repo_root: Path) -> str | None:
    path = repo_root / LAST_BRANCH_PATH
    if not path.exists():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def _git_run(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", "-c", "core.quotepath=false", *args],
        cwd=str(repo_root),
        capture_output=True,
        check=False,
    )
    return subprocess.CompletedProcess(
        args=result.args,
        returncode=result.returncode,
        stdout=(result.stdout or b"").decode("utf-8", errors="replace"),
        stderr=(result.stderr or b"").decode("utf-8", errors="replace"),
    )


def _current_branch(repo_root: Path) -> str:
    result = _git_run(repo_root, "branch", "--show-current")
    if result.returncode != 0:
        raise RuntimeError(_trim_output(result.stdout, result.stderr) or "git_branch_show_current_failed")
    return result.stdout.strip()


def _branch_distance(repo_root: Path, expected_branch: str) -> dict[str, int]:
    result = _git_run(repo_root, "rev-list", "--left-right", "--count", f"HEAD...{expected_branch}")
    if result.returncode != 0:
        raise RuntimeError(_trim_output(result.stdout, result.stderr) or "git_rev_list_failed")
    parts = result.stdout.strip().split()
    if len(parts) != 2:
        raise RuntimeError(f"git_rev_list_parse_failed:{result.stdout.strip()}")
    return {
        "head_only_count": int(parts[0]),
        "expected_branch_only_count": int(parts[1]),
    }


def _tracked_changes(repo_root: Path) -> list[str]:
    tracked: set[str] = set()
    for args in (("diff", "--name-only", "-z"), ("diff", "--cached", "--name-only", "-z")):
        result = _git_run(repo_root, *args)
        if result.returncode not in (0, 1):
            raise RuntimeError(_trim_output(result.stdout, result.stderr) or f"git_{'_'.join(args)}_failed")
        tracked.update(item for item in result.stdout.split("\0") if item)
    return sorted(tracked)


def _resolve_tracked_changes(repo_root: Path) -> dict[str, Any]:
    initial_tracked_changes = _tracked_changes(repo_root)
    if not initial_tracked_changes:
        return {
            "tracked_changes": [],
            "initial_tracked_changes": None,
            "rechecked_tracked_changes": None,
            "transient_workspace_dirty_recovered": None,
        }

    workspace_state: dict[str, Any] = {
        "tracked_changes": list(initial_tracked_changes),
        "initial_tracked_changes": list(initial_tracked_changes),
        "rechecked_tracked_changes": None,
        "transient_workspace_dirty_recovered": None,
    }
    time.sleep(TRANSIENT_WORKSPACE_RECHECK_DELAY_SEC)
    rechecked_tracked_changes = _tracked_changes(repo_root)
    workspace_state["tracked_changes"] = list(rechecked_tracked_changes)
    workspace_state["rechecked_tracked_changes"] = list(rechecked_tracked_changes)
    workspace_state["transient_workspace_dirty_recovered"] = not rechecked_tracked_changes
    return workspace_state


def _branch_check(branch_state: dict[str, Any]) -> PreflightCheck:
    expected_branch = str(branch_state.get("expected_branch") or "").strip()
    last_branch = str(branch_state.get("last_branch") or "").strip()
    current_branch = str(branch_state.get("current_branch") or "").strip()
    distance = branch_state.get("branch_distance") or {}
    head_only = int(distance.get("head_only_count") or 0)
    expected_only = int(distance.get("expected_branch_only_count") or 0)
    detail = (
        f"expected={expected_branch}; current={current_branch or '<detached>'}; "
        f"last={last_branch or '<missing>'}; head_only={head_only}; expected_only={expected_only}"
    )
    if not last_branch:
        return PreflightCheck("branch_policy", "fail", f".last-branch missing; {detail}")
    if last_branch != expected_branch:
        return PreflightCheck("branch_policy", "fail", f".last-branch mismatch; {detail}")
    if current_branch != expected_branch:
        return PreflightCheck("branch_policy", "fail", f"branch drift detected; {detail}")
    if head_only != 0 or expected_only != 0:
        return PreflightCheck("branch_policy", "fail", f"branch tip drift detected; {detail}")
    return PreflightCheck("branch_policy", "pass", detail, data=branch_state)


def _workspace_check(workspace_state: dict[str, Any]) -> PreflightCheck:
    tracked_changes = list(workspace_state.get("tracked_changes") or [])
    initial_tracked_changes = workspace_state.get("initial_tracked_changes")
    rechecked_tracked_changes = workspace_state.get("rechecked_tracked_changes")
    recovered = workspace_state.get("transient_workspace_dirty_recovered")
    data = {
        "tracked_changes": tracked_changes,
    }
    if initial_tracked_changes is not None:
        data["initial_tracked_changes"] = list(initial_tracked_changes)
    if rechecked_tracked_changes is not None:
        data["rechecked_tracked_changes"] = list(rechecked_tracked_changes)
    if recovered is not None:
        data["transient_workspace_dirty_recovered"] = bool(recovered)
    if tracked_changes:
        preview = ", ".join(tracked_changes[:10])
        if len(tracked_changes) > 10:
            preview += ", ..."
        return PreflightCheck(
            "workspace_clean",
            "fail",
            f"tracked changes present after recheck: {preview}" if rechecked_tracked_changes is not None else f"tracked changes present: {preview}",
            data=data,
        )
    if recovered:
        return PreflightCheck(
            "workspace_clean",
            "pass",
            "tracked git diff is clean after transient recheck",
            data=data,
        )
    return PreflightCheck("workspace_clean", "pass", "tracked git diff is clean")


def _run_python_file(repo_root: Path, path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(path)],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )


def _run_check_state(repo_root: Path) -> PreflightCheck:
    result = _run_python_file(repo_root, repo_root / "check_state.py")
    output = _trim_output(result.stdout, result.stderr)
    if result.returncode != 0:
        return PreflightCheck("check_state", "fail", "check_state.py failed", result.returncode, output)
    lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    detail = "; ".join(lines[:2]) if lines else "check_state.py returned no output"
    return PreflightCheck("check_state", "pass", detail, result.returncode, output)


def _run_verify(repo_root: Path) -> tuple[dict[str, Any] | None, PreflightCheck]:
    try:
        summary = ralph_compile.verify_repo(repo_root=repo_root)
    except Exception as exc:  # pragma: no cover - exercised by unit tests via monkeypatch
        return None, PreflightCheck("verify", "fail", f"verify_repo failed: {exc}", output=str(exc))
    payload = summary.to_dict()
    detail = (
        f"stories_total={summary.stories_total}; "
        f"stories_passed={summary.stories_passed}; "
        f"stories_failed={summary.stories_failed}"
    )
    status = "pass" if summary.stories_failed == 0 else "fail"
    return payload, PreflightCheck("verify", status, detail, data=payload)


def _run_runner_dry_run(repo_root: Path, tool: str) -> PreflightCheck:
    result = subprocess.run(
        [
            "powershell",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(repo_root / ".claude" / "ralph" / "run-ralph.ps1"),
            "-Tool",
            tool,
            "-MaxIterations",
            "1",
            "-DryRun",
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    output = _trim_output(result.stdout, result.stderr)
    status = "pass" if result.returncode == 0 else "fail"
    detail = "runner dry-run passed" if status == "pass" else "runner dry-run failed"
    return PreflightCheck("runner_dry_run", status, detail, result.returncode, output)


def _run_targeted_pytest(repo_root: Path) -> PreflightCheck:
    result = subprocess.run(
        TARGETED_PYTEST_ARGS,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    output = _trim_output(result.stdout, result.stderr)
    status = "pass" if result.returncode == 0 else "fail"
    detail = "targeted Ralph pytest passed" if status == "pass" else "targeted Ralph pytest failed"
    return PreflightCheck("targeted_pytest", status, detail, result.returncode, output)


def _collect_branch_state(repo_root: Path) -> dict[str, Any]:
    expected_branch = _expected_branch(repo_root)
    return {
        "expected_branch": expected_branch,
        "last_branch": _read_last_branch(repo_root),
        "current_branch": _current_branch(repo_root),
        "branch_distance": _branch_distance(repo_root, expected_branch),
        "tracked_changes": [],
        "initial_tracked_changes": None,
        "rechecked_tracked_changes": None,
        "transient_workspace_dirty_recovered": None,
    }


def _run_preflight_checks(*, repo_root: Path, tool: str) -> tuple[dict[str, Any], list[PreflightCheck], str | None, str | None]:
    checks: list[PreflightCheck] = []
    try:
        branch_state = _collect_branch_state(repo_root)
    except Exception as exc:
        fallback_state = {
            "expected_branch": _expected_branch(repo_root),
            "last_branch": _read_last_branch(repo_root),
            "current_branch": None,
            "branch_distance": None,
            "tracked_changes": [],
            "initial_tracked_changes": None,
            "rechecked_tracked_changes": None,
            "transient_workspace_dirty_recovered": None,
        }
        message = f"branch policy check failed: {exc}"
        checks.append(PreflightCheck("branch_policy", "fail", message, output=str(exc)))
        return fallback_state, checks, "branch_drift", message

    branch_check = _branch_check(branch_state)
    checks.append(branch_check)
    if branch_check.status != "pass":
        return branch_state, checks, "branch_drift", branch_check.detail

    try:
        workspace_state = _resolve_tracked_changes(repo_root)
    except Exception as exc:
        message = f"workspace check failed: {exc}"
        checks.append(PreflightCheck("workspace_clean", "fail", message, output=str(exc)))
        return branch_state, checks, "preflight_failed", message

    branch_state["tracked_changes"] = list(workspace_state.get("tracked_changes") or [])
    branch_state["initial_tracked_changes"] = workspace_state.get("initial_tracked_changes")
    branch_state["rechecked_tracked_changes"] = workspace_state.get("rechecked_tracked_changes")
    branch_state["transient_workspace_dirty_recovered"] = workspace_state.get("transient_workspace_dirty_recovered")
    workspace_check = _workspace_check(workspace_state)
    checks.append(workspace_check)
    if workspace_check.status != "pass":
        return branch_state, checks, "workspace_dirty", workspace_check.detail

    check_state_check = _run_check_state(repo_root)
    checks.append(check_state_check)
    if check_state_check.status != "pass":
        return branch_state, checks, "preflight_failed", check_state_check.detail

    verify_payload, verify_check = _run_verify(repo_root)
    checks.append(verify_check)
    if verify_check.status != "pass":
        return branch_state, checks, "preflight_failed", verify_check.detail
    branch_state["verify_summary"] = verify_payload

    runner_check = _run_runner_dry_run(repo_root, tool)
    checks.append(runner_check)
    if runner_check.status != "pass":
        return branch_state, checks, "preflight_failed", runner_check.detail

    pytest_check = _run_targeted_pytest(repo_root)
    checks.append(pytest_check)
    if pytest_check.status != "pass":
        return branch_state, checks, "preflight_failed", pytest_check.detail

    return branch_state, checks, None, None


def run_ralph_step2(*, repo_root: Path = REPO_ROOT, tool: str = "claude") -> dict[str, Any]:
    result = subprocess.run(
        [
            "powershell",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(repo_root / ".claude" / "ralph" / "run-ralph.ps1"),
            "-Tool",
            tool,
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    output = "\n".join(part for part in [(result.stdout or "").strip(), (result.stderr or "").strip()] if part)
    status = "complete" if result.returncode == 0 else ("blocked" if result.returncode == 2 else "failed")
    return {
        "status": status,
        "returncode": result.returncode,
        "output": output[:MAX_OUTPUT_CHARS],
    }


def _pre_step1_summary(*, repo_root: Path, tool: str) -> tuple[dict[str, Any], str]:
    try:
        verified = ralph_compile.verify_repo(repo_root=repo_root)
    except Exception:
        rebuilt = ralph_compile.rebuild_repo(repo_root=repo_root, tool=tool).to_dict()
        return rebuilt, "rebuild"
    if verified.stories_failed == 0:
        adjudicated = ralph_compile.adjudicate_repo(repo_root=repo_root).to_dict()
        return adjudicated, "adjudicate"
    rebuilt = ralph_compile.rebuild_repo(repo_root=repo_root, tool=tool).to_dict()
    return rebuilt, "rebuild"


def _run_cycles_core(*, repo_root: Path = REPO_ROOT, tool: str = "claude", max_cycles: int = 5) -> CycleSummary:
    history: list[CycleDecision] = []
    final_status = "incomplete"
    latest_summary: dict[str, Any] = {
        "stories_total": 0,
        "stories_passed": 0,
        "stories_failed": 0,
        "new_story_ids": [],
        "regressed_story_ids": [],
    }
    for cycle_index in range(1, max_cycles + 1):
        rebuild_before, pre_mode = _pre_step1_summary(repo_root=repo_root, tool=tool)
        if rebuild_before["stories_failed"] == 0:
            ralph_compile.verify_repo(repo_root=repo_root)
            final_status = "complete"
            history.append(CycleDecision(cycle_index, "complete", rebuild_before, None, None))
            latest_summary = rebuild_before
            break

        step2 = run_ralph_step2(repo_root=repo_root, tool=tool)
        if pre_mode == "adjudicate":
            rebuild_after = ralph_compile.adjudicate_repo(repo_root=repo_root).to_dict()
        else:
            rebuild_after = ralph_compile.rebuild_repo(repo_root=repo_root, tool=tool).to_dict()
        latest_summary = rebuild_after

        if rebuild_after["stories_failed"] == 0:
            ralph_compile.verify_repo(repo_root=repo_root)
            final_status = "complete"
            history.append(CycleDecision(cycle_index, "complete", rebuild_before, step2, rebuild_after))
            break

        if (
            step2["status"] in {"blocked", "failed"}
            and rebuild_after["story_set_hash"] == rebuild_before["story_set_hash"]
            and not rebuild_after["new_story_ids"]
        ):
            final_status = "blocked"
            history.append(CycleDecision(cycle_index, "blocked", rebuild_before, step2, rebuild_after))
            break

        history.append(CycleDecision(cycle_index, "continue", rebuild_before, step2, rebuild_after))
    else:
        final_status = "incomplete"

    return CycleSummary(
        cycles_run=len(history),
        final_status=final_status,
        stories_total=int(latest_summary.get("stories_total") or 0),
        stories_passed=int(latest_summary.get("stories_passed") or 0),
        stories_remaining=int(latest_summary.get("stories_failed") or 0),
        new_story_ids_last_cycle=list(latest_summary.get("new_story_ids") or []),
        regressed_story_ids_last_cycle=list(latest_summary.get("regressed_story_ids") or []),
        history=history,
    )


def run_cycles(
    *,
    repo_root: Path = REPO_ROOT,
    tool: str = "claude",
    max_cycles: int = 5,
    enforce_preflight: bool = True,
) -> CycleSummary:
    branch_state = {
        "expected_branch": None,
        "last_branch": None,
        "current_branch": None,
        "branch_distance": None,
        "tracked_changes": [],
        "initial_tracked_changes": None,
        "rechecked_tracked_changes": None,
        "transient_workspace_dirty_recovered": None,
    }
    preflight: list[PreflightCheck] = []
    if enforce_preflight:
        branch_state, preflight, failure_status, status_reason = _run_preflight_checks(repo_root=repo_root, tool=tool)
        if failure_status is not None:
            verify_summary = branch_state.get("verify_summary") or {}
            return CycleSummary(
                cycles_run=0,
                final_status=failure_status,
                stories_total=int(verify_summary.get("stories_total") or 0),
                stories_passed=int(verify_summary.get("stories_passed") or 0),
                stories_remaining=int(verify_summary.get("stories_failed") or 0),
                new_story_ids_last_cycle=[],
                regressed_story_ids_last_cycle=[],
                history=[],
                expected_branch=branch_state.get("expected_branch"),
                last_branch=branch_state.get("last_branch"),
                current_branch=branch_state.get("current_branch"),
                branch_distance=branch_state.get("branch_distance"),
                tracked_changes=list(branch_state.get("tracked_changes") or []),
                status_reason=status_reason,
                preflight=preflight,
                initial_tracked_changes=(
                    list(branch_state.get("initial_tracked_changes") or [])
                    if branch_state.get("initial_tracked_changes") is not None
                    else None
                ),
                rechecked_tracked_changes=(
                    list(branch_state.get("rechecked_tracked_changes") or [])
                    if branch_state.get("rechecked_tracked_changes") is not None
                    else None
                ),
                transient_workspace_dirty_recovered=branch_state.get("transient_workspace_dirty_recovered"),
            )

    summary = _run_cycles_core(repo_root=repo_root, tool=tool, max_cycles=max_cycles)
    summary.expected_branch = branch_state.get("expected_branch")
    summary.last_branch = branch_state.get("last_branch")
    summary.current_branch = branch_state.get("current_branch")
    summary.branch_distance = branch_state.get("branch_distance")
    summary.tracked_changes = list(branch_state.get("tracked_changes") or [])
    summary.preflight = preflight
    summary.initial_tracked_changes = (
        list(branch_state.get("initial_tracked_changes") or [])
        if branch_state.get("initial_tracked_changes") is not None
        else None
    )
    summary.rechecked_tracked_changes = (
        list(branch_state.get("rechecked_tracked_changes") or [])
        if branch_state.get("rechecked_tracked_changes") is not None
        else None
    )
    summary.transient_workspace_dirty_recovered = branch_state.get("transient_workspace_dirty_recovered")
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ralph outer-loop convergence controller")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--tool", default="claude")
    run_parser.add_argument("--max-cycles", type=int, default=5)
    return parser


def main(argv: list[str] | None = None) -> int:
    _configure_stdio_utf8()
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command != "run":
        parser.error("unknown command")
        return 2
    summary = run_cycles(repo_root=args.repo_root.resolve(), tool=args.tool, max_cycles=args.max_cycles)
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
    return 0 if summary.final_status == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
