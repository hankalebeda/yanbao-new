"""Mark orphaned issue-mesh runs as failed."""
import json
import os

base = r"D:\yanbao\runtime\issue_mesh"
for name in os.listdir(base):
    sf = os.path.join(base, name, "status.json")
    if not os.path.isfile(sf):
        continue
    s = json.load(open(sf, encoding="utf-8"))
    if s.get("status") == "running":
        s["status"] = "failed"
        s["error"] = "orphaned by service restart"
        open(sf, "w", encoding="utf-8").write(json.dumps(s, indent=2))
        print("Marked", name, "as failed")
