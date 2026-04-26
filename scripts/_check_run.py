#!/usr/bin/env python
import json, os, sys, glob

run_dirs = sorted(glob.glob("runtime/codex_mesh/runs/*/summary.json"))
if not run_dirs:
    print("No completed runs found")
    sys.exit(0)

latest = run_dirs[-1]
s = json.load(open(latest, encoding="utf-8"))
print("Run:", os.path.dirname(latest))
print("Status:", s.get("status", "?"))
print("Duration:", s.get("duration_seconds", "?"))
print("Success:", s.get("success_count", "?"))
print("Failure:", s.get("failure_count", "?"))
for t in s.get("tasks", []):
    tid = t.get("task_id", "?")
    status = t.get("status", "?")
    dur = t.get("duration_seconds", 0) or 0
    prov = t.get("selected_provider", "?")
    print("  %s: status=%s dur=%ds provider=%s" % (tid, status, dur, prov))
