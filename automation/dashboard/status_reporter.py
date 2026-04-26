"""
Status Reporter — unified dashboard for the autonomous multi-agent stack.

Aggregates data from:
  - infra_doctor (service health, control plane, writeback, New API, git)
  - repo_hygiene (workspace classification)
  - control_plane/current_state.json (loop state, round history)
  - service_health.json (watchdog degradation state)

Output: Markdown dashboard + JSON snapshot.

Usage:
  python -m automation.dashboard.status_reporter               # print to stdout
  python -m automation.dashboard.status_reporter --json        # JSON to stdout
  python -m automation.dashboard.status_reporter --write       # write to output/
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(os.environ.get("LOOP_CONTROLLER_REPO_ROOT", "")).resolve() or Path(__file__).resolve().parents[2]


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _load_json_safe(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def gather_status() -> dict[str, Any]:
    """Gather all status data into a unified snapshot."""
    snapshot: dict[str, Any] = {
        "timestamp": _now_iso(),
        "repo_root": str(REPO_ROOT),
    }

    # 1. Control plane state
    state = _load_json_safe(REPO_ROOT / "automation" / "control_plane" / "current_state.json")
    if state:
        summary = state.get("last_round_summary", {})
        snapshot["loop"] = {
            "mode": state.get("mode"),
            "phase": state.get("phase"),
            "promote_target": state.get("promote_target_mode"),
            "consecutive_fix_success": state.get("consecutive_fix_success_count", 0),
            "consecutive_verified_fixes": state.get("consecutive_verified_problem_fixes", 0),
            "fix_goal": state.get("fix_goal", 0),
            "goal_reached": state.get("goal_reached", False),
            "goal_ever_reached": state.get("goal_ever_reached", False),
            "total_fixes": state.get("total_fixes", 0),
            "total_failures": state.get("total_failures", 0),
            "blocked_reason": state.get("blocked_reason"),
            "provider_pool": state.get("provider_pool", {}),
            "current_round_id": state.get("current_round_id"),
            "last_round": {
                "round_id": summary.get("round_id"),
                "success": summary.get("all_success"),
                "error": summary.get("error"),
                "problems_found": summary.get("problems_found", 0),
                "problems_fixed": summary.get("problems_fixed", 0),
                "problems_failed": summary.get("problems_failed", 0),
                "finished_at": summary.get("finished_at"),
            } if summary else None,
        }
    else:
        snapshot["loop"] = {"status": "state_file_missing"}

    # 2. Service health (from watchdog)
    health = _load_json_safe(REPO_ROOT / "automation" / "control_plane" / "service_health.json")
    snapshot["watchdog_health"] = health or {}

    # 3. Infra doctor report (latest)
    doctor = _load_json_safe(REPO_ROOT / "output" / "infra_doctor_report.json")
    if doctor:
        snapshot["infra_doctor"] = {
            "timestamp": doctor.get("timestamp"),
            "overall_status": doctor.get("overall_status"),
            "critical_issues": doctor.get("critical_issues", []),
            "warnings": doctor.get("warnings", []),
            "services_down": [
                s["name"] for s in doctor.get("checks", {}).get("service_health", [])
                if s.get("status") == "down"
            ],
        }

    # 4. Round history stats (from state)
    if state:
        history = state.get("round_history", [])
        recent = history[-10:] if history else []
        success_count = sum(1 for r in recent if r.get("all_success"))
        fail_count = len(recent) - success_count
        snapshot["recent_rounds"] = {
            "total_history_size": len(history),
            "last_10_success": success_count,
            "last_10_fail": fail_count,
            "recent": [
                {
                    "id": r.get("round_id"),
                    "success": r.get("all_success"),
                    "fixed": r.get("problems_fixed", 0),
                    "failed": r.get("problems_failed", 0),
                    "error": (r.get("error") or "")[:80] if r.get("error") else None,
                }
                for r in recent
            ],
        }

    return snapshot


def render_markdown(snapshot: dict[str, Any]) -> str:
    """Render the snapshot as a Markdown dashboard."""
    lines: list[str] = []
    lines.append(f"# Autonomous Stack Dashboard")
    lines.append(f"")
    lines.append(f"Generated: {snapshot['timestamp']}")
    lines.append(f"")

    # Loop state
    loop = snapshot.get("loop", {})
    lines.append(f"## Loop Controller")
    lines.append(f"")
    lines.append(f"| Key | Value |")
    lines.append(f"|-----|-------|")
    lines.append(f"| Mode | `{loop.get('mode', '?')}` |")
    lines.append(f"| Phase | `{loop.get('phase', '?')}` |")
    lines.append(f"| Promote Target | `{loop.get('promote_target', '?')}` |")
    lines.append(f"| Consecutive Fix Success | **{loop.get('consecutive_fix_success', 0)}** / {loop.get('fix_goal', '?')} |")
    lines.append(f"| Verified Problem Fixes | **{loop.get('consecutive_verified_fixes', 0)}** |")
    lines.append(f"| Goal Reached | {'Yes' if loop.get('goal_reached') else 'No'} |")
    lines.append(f"| Total Fixes | {loop.get('total_fixes', 0)} |")
    lines.append(f"| Total Failures | {loop.get('total_failures', 0)} |")
    if loop.get("blocked_reason"):
        lines.append(f"| **Blocked** | `{loop['blocked_reason']}` |")
    lines.append(f"")

    # Provider pool
    pool = loop.get("provider_pool", {})
    if pool:
        status_emoji = {"ok": "OK", "degraded": "WARN", "down": "DOWN"}.get(pool.get("status", ""), "?")
        lines.append(f"**Provider Pool:** {status_emoji} (`{pool.get('status', '?')}`)")
        if pool.get("error"):
            lines.append(f"  - Error: `{pool['error']}`")
        lines.append(f"")

    # Last round
    last = loop.get("last_round")
    if last and last.get("round_id"):
        lines.append(f"## Last Round: `{last['round_id']}`")
        lines.append(f"")
        result = "SUCCESS" if last.get("success") else "FAILED"
        lines.append(f"- Result: **{result}**")
        lines.append(f"- Found: {last.get('problems_found', 0)}, Fixed: {last.get('problems_fixed', 0)}, Failed: {last.get('problems_failed', 0)}")
        if last.get("error"):
            lines.append(f"- Error: `{last['error'][:120]}`")
        lines.append(f"- Finished: {last.get('finished_at', '?')}")
        lines.append(f"")

    # Recent rounds
    recent = snapshot.get("recent_rounds", {})
    if recent.get("recent"):
        lines.append(f"## Recent Rounds (last 10)")
        lines.append(f"")
        lines.append(f"Success: {recent.get('last_10_success', 0)} / {len(recent['recent'])}")
        lines.append(f"")
        lines.append(f"| Round | Result | Fixed | Failed | Error |")
        lines.append(f"|-------|--------|-------|--------|-------|")
        for r in recent["recent"]:
            result = "OK" if r.get("success") else "FAIL"
            err = r.get("error") or ""
            lines.append(f"| `{r.get('id', '?')}` | {result} | {r.get('fixed', 0)} | {r.get('failed', 0)} | {err[:50]} |")
        lines.append(f"")

    # Infra doctor
    doctor = snapshot.get("infra_doctor")
    if doctor:
        lines.append(f"## Infra Doctor ({doctor.get('timestamp', '?')})")
        lines.append(f"")
        lines.append(f"Overall: **{doctor.get('overall_status', '?').upper()}**")
        if doctor.get("critical_issues"):
            lines.append(f"")
            lines.append(f"Critical Issues:")
            for c in doctor["critical_issues"]:
                lines.append(f"- {c}")
        if doctor.get("services_down"):
            lines.append(f"")
            lines.append(f"Services Down: {', '.join(doctor['services_down'])}")
        lines.append(f"")

    # Watchdog health
    wh = snapshot.get("watchdog_health", {})
    degraded = {k: v for k, v in wh.items() if isinstance(v, dict) and v.get("status") == "degraded"}
    if degraded:
        lines.append(f"## Watchdog Degraded Services")
        lines.append(f"")
        for name, info in degraded.items():
            lines.append(f"- **{name}**: {info.get('reason', '?')} (since {info.get('timestamp', '?')})")
        lines.append(f"")

    return "\n".join(lines)


def main():
    as_json = "--json" in sys.argv
    do_write = "--write" in sys.argv

    snapshot = gather_status()

    if as_json:
        output = json.dumps(snapshot, indent=2, ensure_ascii=False)
        print(output)
    else:
        md = render_markdown(snapshot)
        print(md)

    if do_write:
        out_dir = REPO_ROOT / "output"
        out_dir.mkdir(parents=True, exist_ok=True)

        json_path = out_dir / "dashboard_snapshot.json"
        json_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")

        md_path = REPO_ROOT / "automation" / "control_plane" / "dashboard.md"
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(render_markdown(snapshot), encoding="utf-8")

        print(f"\nWritten to:\n  {json_path}\n  {md_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
