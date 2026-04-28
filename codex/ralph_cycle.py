from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from codex import ralph_compile


REPO_ROOT = Path(__file__).resolve().parents[1]


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

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycles_run": self.cycles_run,
            "final_status": self.final_status,
            "stories_total": self.stories_total,
            "stories_passed": self.stories_passed,
            "stories_remaining": self.stories_remaining,
            "new_story_ids_last_cycle": self.new_story_ids_last_cycle,
            "regressed_story_ids_last_cycle": self.regressed_story_ids_last_cycle,
            "history": [asdict(item) for item in self.history],
        }


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
        "output": output[:4000],
    }


def run_cycles(*, repo_root: Path = REPO_ROOT, tool: str = "claude", max_cycles: int = 5) -> CycleSummary:
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
        rebuild_before = ralph_compile.rebuild_repo(repo_root=repo_root, tool=tool).to_dict()
        if rebuild_before["stories_failed"] == 0:
            ralph_compile.verify_repo(repo_root=repo_root)
            final_status = "complete"
            history.append(CycleDecision(cycle_index, "complete", rebuild_before, None, None))
            latest_summary = rebuild_before
            break

        step2 = run_ralph_step2(repo_root=repo_root, tool=tool)
        rebuild_after = ralph_compile.rebuild_repo(repo_root=repo_root, tool=tool).to_dict()
        latest_summary = rebuild_after

        if rebuild_after["stories_failed"] == 0:
            ralph_compile.verify_repo(repo_root=repo_root)
            final_status = "complete"
            history.append(CycleDecision(cycle_index, "complete", rebuild_before, step2, rebuild_after))
            break

        if (
            step2["status"] == "blocked"
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ralph outer-loop convergence controller")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--tool", default="claude")
    run_parser.add_argument("--max-cycles", type=int, default=5)
    return parser


def main(argv: list[str] | None = None) -> int:
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
