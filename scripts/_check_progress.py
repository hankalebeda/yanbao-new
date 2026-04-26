"""Check progress of codex mesh runs and issue-mesh runs."""
import json
import os
import glob
import urllib.request

# Check loop state
try:
    req = urllib.request.Request(
        "http://127.0.0.1:8096/v1/state",
        headers={"Authorization": "Bearer kestra-internal-20260327"},
    )
    resp = urllib.request.urlopen(req, timeout=5)
    state = json.loads(resp.read().decode())
    print(f"Loop: phase={state['phase']} mode={state['mode']} fixes={state.get('total_fixes',0)} failures={state.get('total_failures',0)} round={state.get('current_round_id','?')}")
except Exception as e:
    print(f"Loop state error: {e}")

# Check codex_mesh runs
print("\n--- Codex Mesh Runs ---")
runs = sorted(glob.glob(r"D:\yanbao\runtime\codex_mesh\runs\*"), reverse=True)[:5]
for r in runs:
    td = os.path.join(r, "tasks")
    if os.path.isdir(td):
        tasks = [d for d in os.listdir(td) if os.path.isdir(os.path.join(td, d))]
        done = sum(1 for d in tasks if os.path.exists(os.path.join(td, d, "summary.json")))
        total = len(tasks)
        has_sum = os.path.exists(os.path.join(r, "summary.json"))
        print(f"  {os.path.basename(r)}: tasks={done}/{total} summary={'Y' if has_sum else 'N'}")

# Check issue-mesh runs
print("\n--- Issue Mesh Runs ---")
im_runs = sorted(glob.glob(r"D:\yanbao\runtime\issue_mesh\issue-mesh-*"), reverse=True)[:5]
for im in im_runs:
    sf = os.path.join(im, "status.json")
    if os.path.exists(sf):
        st = json.load(open(sf, encoding="utf-8"))
        rid = st.get("run_id", "?")
        status = st.get("status", "?")
        err = st.get("error", "")
        bundle = "Y" if os.path.exists(os.path.join(im, "bundle.json")) else "N"
        print(f"  {rid}: status={status} bundle={bundle}" + (f" err={err[:80]}" if err else ""))
