"""Quick check of task status for latest codex mesh run."""
import os, json, sys

run_dir = sys.argv[1] if len(sys.argv) > 1 else None
if not run_dir:
    base = r"D:\yanbao\runtime\codex_mesh\runs"
    runs = sorted(os.listdir(base))
    run_dir = os.path.join(base, runs[-1])

tasks_dir = os.path.join(run_dir, "tasks")
print(f"Run: {os.path.basename(run_dir)}")
print(f"{'Task':<30} {'Status':<10} {'ok':<6} {'rc':<6} {'dur':<8} {'msg_len':<8}")
print("-" * 80)

done = 0
total = 0
for t in sorted(os.listdir(tasks_dir)):
    td = os.path.join(tasks_dir, t)
    if not os.path.isdir(td):
        continue
    total += 1
    sf = os.path.join(td, "summary.json")
    if os.path.isfile(sf):
        s = json.load(open(sf, encoding="utf-8"))
        a = s.get("attempts", [{}])[-1]
        ok = a.get("ok", "?")
        rc = a.get("returncode", "?")
        dur = a.get("duration_seconds", 0)
        err = a.get("error", "")
        done += 1
        row = "{:<30} {:<10} {:<6} {:<6} {:<8.0f} {}".format(t, "DONE", str(ok), str(rc), dur, err or "")
        print(row)
    else:
        provs = [d for d in os.listdir(td) if os.path.isdir(os.path.join(td, d))]
        row = "{:<30} {:<10} {}".format(t, "RUNNING", "providers=" + str(provs))
        print(row)

print("\n{}/{} tasks done".format(done, total))
