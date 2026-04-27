"""Ralph Step-2 progression runner.

Walks .claude/ralph/loop/prd.json story by story (priority order). For each
story with passes=false, parses notes['pytest'], runs it, and only on real
green flips passes=true (and syncs the named copy + appends to progress.txt).
"""
from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".claude/ralph/loop/prd.json"
NAMED = ROOT / ".claude/ralph/prd/yanbao-platform-enhancement.json"
PROGRESS = ROOT / ".claude/ralph/loop/progress.txt"
PYTHON = sys.executable


def load_prd() -> dict:
    return json.loads(RUNTIME.read_text(encoding="utf-8"))


def save_prd(prd: dict) -> None:
    text = json.dumps(prd, ensure_ascii=False, indent=2) + "\n"
    RUNTIME.write_text(text, encoding="utf-8")
    NAMED.write_text(text, encoding="utf-8")


def append_progress(story_id: str, pytest_cmd: str, summary: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M")
    block = f"""## {ts} - {story_id}
- Story focused pytest passed; flipped passes=true.
- Command: `{pytest_cmd}`
- Summary: {summary}
- Git: skipped (no explicit authorization in this run).
---
"""
    with PROGRESS.open("a", encoding="utf-8") as fh:
        fh.write(block)


def run_pytest(cmd: str) -> tuple[bool, str]:
    parts = shlex.split(cmd, posix=False)
    if parts[:2] == ["python", "-m"]:
        parts = [PYTHON, "-m"] + parts[2:]
    elif parts[:1] == ["python"]:
        parts = [PYTHON] + parts[1:]
    env = os.environ.copy()
    env.setdefault("MOCK_LLM", "true")
    env.setdefault("ENABLE_SCHEDULER", "false")
    env.setdefault("SETTLEMENT_INLINE_EXECUTION", "true")
    proc = subprocess.run(
        parts,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out = proc.stdout + proc.stderr
    last = "\n".join(out.splitlines()[-3:])
    ok = proc.returncode == 0 and " passed" in out and " failed" not in out
    return ok, last


def main() -> int:
    target_id = sys.argv[1] if len(sys.argv) > 1 else None
    max_count = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    prd = load_prd()
    flipped = 0
    failures = []
    cache: dict[str, tuple[bool, str]] = {}

    for story in prd["userStories"]:
        if story.get("passes"):
            continue
        if target_id and story["id"] != target_id:
            continue
        notes = json.loads(story["notes"])
        cmd = notes.get("pytest", "").strip()
        if not cmd:
            failures.append((story["id"], "no pytest command"))
            continue
        if cmd not in cache:
            print(f"[run] {story['id']}: {cmd}", flush=True)
            cache[cmd] = run_pytest(cmd)
        ok, last = cache[cmd]
        if ok:
            story["passes"] = True
            save_prd(prd)
            append_progress(story["id"], cmd, last.replace("\n", " | "))
            flipped += 1
            print(f"[OK]  {story['id']} flipped -> {last}", flush=True)
            if flipped >= max_count:
                break
        else:
            failures.append((story["id"], last))
            print(f"[FAIL]{story['id']} :: {last}", flush=True)
            if target_id:
                break
            # Continue with remaining stories; do not stop on first failure.

    print("--- summary ---")
    print(f"flipped: {flipped}")
    print(f"failures: {len(failures)}")
    for sid, msg in failures[:30]:
        print(f"  {sid}: {msg[-200:]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
