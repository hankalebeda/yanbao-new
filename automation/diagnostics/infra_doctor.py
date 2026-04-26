"""
Infra Doctor — autonomous diagnostic module for the Kestra + New API + Writeback stack.

Checks:
  1. Service health (6 Windows services)
  2. Control plane state anomalies (stale rounds, stuck phases)
  3. Writeback 403 error patterns
  4. New API channel availability
  5. Git repo status (uncommitted changes, conflicts)

Usage:
  python -m automation.diagnostics.infra_doctor          # full report
  python -m automation.diagnostics.infra_doctor --json    # machine-readable
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import httpx

    _HTTPX = True
except ImportError:
    _HTTPX = False

REPO_ROOT = Path(os.environ.get("LOOP_CONTROLLER_REPO_ROOT", "")).resolve() or Path(__file__).resolve().parents[2]

SERVICES = [
    {"name": "app", "port": 38001},
    {"name": "writeback_a", "port": 8092},
    {"name": "writeback_b", "port": 8095},
    {"name": "mesh_runner", "port": 8093},
    {"name": "promote_prep", "port": 8094},
    {"name": "loop_controller", "port": 8096},
]

STALE_ROUND_THRESHOLD_MINUTES = 60
STUCK_PHASE_THRESHOLD_MINUTES = 30


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# 1. Service Health
# ---------------------------------------------------------------------------


def check_service_health() -> list[dict[str, Any]]:
    results = []
    for svc in SERVICES:
        entry: dict[str, Any] = {"name": svc["name"], "port": svc["port"]}
        url = f"http://127.0.0.1:{svc['port']}/health"
        try:
            if _HTTPX:
                resp = httpx.get(url, timeout=5)
                resp.raise_for_status()
                entry["status"] = "ok"
                entry["detail"] = resp.json()
            else:
                import urllib.request

                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=5) as r:
                    entry["status"] = "ok"
                    entry["detail"] = json.loads(r.read())
        except Exception as exc:
            entry["status"] = "down"
            entry["error"] = str(exc)[:200]
        results.append(entry)
    return results


# ---------------------------------------------------------------------------
# 2. Control Plane State
# ---------------------------------------------------------------------------


def check_control_plane() -> dict[str, Any]:
    state_path = REPO_ROOT / "automation" / "control_plane" / "current_state.json"
    result: dict[str, Any] = {"path": str(state_path)}

    if not state_path.exists():
        result["status"] = "missing"
        return result

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as exc:
        result["status"] = "parse_error"
        result["error"] = str(exc)[:200]
        return result

    result["status"] = "ok"
    result["mode"] = state.get("mode")
    result["phase"] = state.get("phase")
    result["promote_target_mode"] = state.get("promote_target_mode")
    result["consecutive_fix_success_count"] = state.get("consecutive_fix_success_count", 0)
    result["total_failures"] = state.get("total_failures", 0)
    result["goal_reached"] = state.get("goal_reached", False)
    result["blocked_reason"] = state.get("blocked_reason")

    anomalies: list[str] = []

    # Check for stuck phase
    summary = state.get("last_round_summary", {})
    if summary:
        finished = summary.get("finished_at")
        if finished:
            try:
                finished_dt = _dt.datetime.fromisoformat(finished.replace("Z", "+00:00"))
                age = _dt.datetime.now(_dt.timezone.utc) - finished_dt
                result["last_round_age_minutes"] = round(age.total_seconds() / 60, 1)
                if age.total_seconds() > STALE_ROUND_THRESHOLD_MINUTES * 60:
                    anomalies.append(f"last_round_stale ({result['last_round_age_minutes']}m ago)")
            except (ValueError, TypeError):
                pass

        if summary.get("error"):
            anomalies.append(f"last_round_error: {summary['error'][:120]}")

    phase = state.get("phase", "idle")
    if phase not in ("idle",):
        # If phase is active but no recent round, it might be stuck
        anomalies.append(f"active_phase: {phase}")

    if state.get("blocked_reason"):
        anomalies.append(f"blocked: {state['blocked_reason']}")

    provider = state.get("provider_pool", {})
    if provider.get("status") not in (None, "ok"):
        anomalies.append(f"provider_pool: {provider.get('status')}")

    result["anomalies"] = anomalies
    return result


# ---------------------------------------------------------------------------
# 3. Writeback Error Patterns
# ---------------------------------------------------------------------------


def check_writeback_errors() -> dict[str, Any]:
    result: dict[str, Any] = {"services": {}}

    for port, name in [(8092, "writeback_a"), (8095, "writeback_b")]:
        entry: dict[str, Any] = {"port": port}
        url = f"http://127.0.0.1:{port}/health"
        try:
            if _HTTPX:
                resp = httpx.get(url, timeout=5)
                resp.raise_for_status()
                health = resp.json()
                entry["status"] = "ok"
                entry["auth_enabled"] = health.get("auth_enabled")
                entry["require_triage"] = health.get("require_triage")
                entry["require_fencing"] = health.get("require_fencing")
            else:
                entry["status"] = "unreachable"
        except Exception as exc:
            entry["status"] = "unreachable"
            entry["error"] = str(exc)[:200]
        result["services"][name] = entry

    # Check audit trail for recent 403s
    audit_dir = REPO_ROOT / "automation" / "writeback_service" / ".audit_writeback_b"
    if audit_dir.exists():
        recent_403s = 0
        try:
            for f in sorted(audit_dir.iterdir(), reverse=True)[:50]:
                if f.suffix == ".json":
                    try:
                        data = json.loads(f.read_text(encoding="utf-8"))
                        if data.get("http_status") == 403:
                            recent_403s += 1
                    except Exception:
                        pass
        except Exception:
            pass
        result["recent_403_count"] = recent_403s
    else:
        result["recent_403_count"] = "audit_dir_missing"

    return result


# ---------------------------------------------------------------------------
# 4. New API Channel
# ---------------------------------------------------------------------------


def check_newapi_channel() -> dict[str, Any]:
    base_url = os.environ.get("NEW_API_BASE_URL", "http://192.168.232.141:3000")
    result: dict[str, Any] = {"base_url": base_url}

    # Check /v1/models endpoint
    url = f"{base_url}/v1/models"
    token = os.environ.get("NEW_API_TOKEN", "")
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        if _HTTPX:
            resp = httpx.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                models = data.get("data", [])
                result["status"] = "ok"
                result["model_count"] = len(models)
                result["models"] = [m.get("id") for m in models[:10]]
            else:
                result["status"] = "error"
                result["http_status"] = resp.status_code
        else:
            result["status"] = "httpx_not_available"
    except Exception as exc:
        result["status"] = "unreachable"
        result["error"] = str(exc)[:200]

    return result


# ---------------------------------------------------------------------------
# 5. Git Repo Status
# ---------------------------------------------------------------------------


def check_git_status() -> dict[str, Any]:
    result: dict[str, Any] = {"repo_root": str(REPO_ROOT)}
    try:
        proc = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=15,
        )
        lines = [l for l in proc.stdout.strip().split("\n") if l.strip()]
        result["uncommitted_count"] = len(lines)
        # Classify changes
        modified = [l for l in lines if l.startswith(" M") or l.startswith("M ")]
        added = [l for l in lines if l.startswith("A ") or l.startswith("??")]
        deleted = [l for l in lines if l.startswith(" D") or l.startswith("D ")]
        conflicts = [l for l in lines if l.startswith("UU") or l.startswith("AA")]
        result["modified"] = len(modified)
        result["untracked_or_added"] = len(added)
        result["deleted"] = len(deleted)
        result["conflicts"] = len(conflicts)
        if conflicts:
            result["conflict_files"] = [l[3:].strip() for l in conflicts[:10]]
        result["status"] = "ok" if not conflicts else "conflicts_detected"
    except Exception as exc:
        result["status"] = "error"
        result["error"] = str(exc)[:200]
    return result


# ---------------------------------------------------------------------------
# 6. Service Health File (from watchdog)
# ---------------------------------------------------------------------------


def check_degraded_services() -> dict[str, Any]:
    health_file = REPO_ROOT / "automation" / "control_plane" / "service_health.json"
    if not health_file.exists():
        return {"status": "no_health_file"}
    try:
        data = json.loads(health_file.read_text(encoding="utf-8"))
        degraded = {k: v for k, v in data.items() if isinstance(v, dict) and v.get("status") == "degraded"}
        return {"status": "ok" if not degraded else "degraded", "degraded_services": degraded}
    except Exception as exc:
        return {"status": "parse_error", "error": str(exc)[:200]}


# ---------------------------------------------------------------------------
# Full Diagnostic Report
# ---------------------------------------------------------------------------


def run_full_diagnostic() -> dict[str, Any]:
    report: dict[str, Any] = {
        "timestamp": _now_iso(),
        "repo_root": str(REPO_ROOT),
        "checks": {},
        "overall_status": "ok",
        "critical_issues": [],
        "warnings": [],
    }

    # 1. Service health
    health = check_service_health()
    report["checks"]["service_health"] = health
    down = [s for s in health if s["status"] == "down"]
    if down:
        svc_names = [s["name"] for s in down]
        report["critical_issues"].append(f"services_down: {', '.join(svc_names)}")

    # 2. Control plane
    cp = check_control_plane()
    report["checks"]["control_plane"] = cp
    if cp.get("status") != "ok":
        report["critical_issues"].append(f"control_plane: {cp.get('status')}")
    if cp.get("anomalies"):
        report["warnings"].extend(cp["anomalies"])

    # 3. Writeback errors
    wb = check_writeback_errors()
    report["checks"]["writeback"] = wb
    r403 = wb.get("recent_403_count", 0)
    if isinstance(r403, int) and r403 > 0:
        report["warnings"].append(f"writeback_b_recent_403s: {r403}")

    # 4. New API
    api = check_newapi_channel()
    report["checks"]["newapi_channel"] = api
    if api.get("status") != "ok":
        report["critical_issues"].append(f"newapi_channel: {api.get('status')}")

    # 5. Git
    git = check_git_status()
    report["checks"]["git_status"] = git
    if git.get("conflicts", 0) > 0:
        report["critical_issues"].append(f"git_conflicts: {git.get('conflicts')}")

    # 6. Degraded services (from watchdog)
    degraded = check_degraded_services()
    report["checks"]["degraded_services"] = degraded
    if degraded.get("status") == "degraded":
        report["critical_issues"].append(f"watchdog_degraded: {list(degraded.get('degraded_services', {}).keys())}")

    # Overall
    if report["critical_issues"]:
        report["overall_status"] = "critical"
    elif report["warnings"]:
        report["overall_status"] = "warning"

    return report


def print_report(report: dict[str, Any]) -> None:
    print(f"\n{'='*60}")
    print(f"  INFRA DOCTOR — {report['timestamp']}")
    print(f"  Overall: {report['overall_status'].upper()}")
    print(f"{'='*60}")

    if report["critical_issues"]:
        print("\n  CRITICAL ISSUES:")
        for issue in report["critical_issues"]:
            print(f"    [!] {issue}")

    if report["warnings"]:
        print("\n  WARNINGS:")
        for w in report["warnings"]:
            print(f"    [~] {w}")

    # Service health table
    health = report["checks"].get("service_health", [])
    print(f"\n  SERVICE HEALTH:")
    for svc in health:
        symbol = "OK" if svc["status"] == "ok" else "DOWN"
        print(f"    [{symbol:>4}] {svc['name']:20s} :{svc['port']}")

    # Control plane
    cp = report["checks"].get("control_plane", {})
    print(f"\n  CONTROL PLANE:")
    print(f"    mode={cp.get('mode')} phase={cp.get('phase')} target={cp.get('promote_target_mode')}")
    print(f"    consecutive_fix_success={cp.get('consecutive_fix_success_count')} total_failures={cp.get('total_failures')}")
    if cp.get("last_round_age_minutes") is not None:
        print(f"    last_round_age={cp.get('last_round_age_minutes')}m")

    # New API
    api = report["checks"].get("newapi_channel", {})
    print(f"\n  NEW API CHANNEL: {api.get('status')} ({api.get('base_url')})")
    if api.get("model_count") is not None:
        print(f"    models_available: {api.get('model_count')}")

    # Git
    git = report["checks"].get("git_status", {})
    print(f"\n  GIT STATUS: {git.get('uncommitted_count', '?')} uncommitted changes")
    if git.get("conflicts", 0) > 0:
        print(f"    CONFLICTS: {git['conflicts']} files")

    print(f"\n{'='*60}\n")


def main():
    as_json = "--json" in sys.argv
    report = run_full_diagnostic()

    if as_json:
        # Write to output file as well
        out_path = REPO_ROOT / "output" / "infra_doctor_report.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(report, indent=2, ensure_ascii=False))
        print(f"\nReport saved to: {out_path}")
    else:
        print_report(report)

    return 0 if report["overall_status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
