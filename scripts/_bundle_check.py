"""Check issue-mesh runs and bundle findings."""
import json, os

mesh_dir = r"D:\yanbao\runtime\issue_mesh"
runs = sorted([d for d in os.listdir(mesh_dir) 
               if d.startswith("issue-mesh-") and os.path.isdir(os.path.join(mesh_dir, d))])

for r in runs[-5:]:
    rd = os.path.join(mesh_dir, r)
    sf = os.path.join(rd, "status.json")
    if not os.path.isfile(sf):
        continue
    s = json.load(open(sf, encoding="utf-8"))
    has_bundle = os.path.isfile(os.path.join(rd, "bundle.json"))
    has_summary = os.path.isfile(os.path.join(rd, "summary.json"))
    print("{} status={} bundle={} summary={}".format(r, s.get("status","?"), has_bundle, has_summary))
    
    if has_bundle:
        b = json.load(open(os.path.join(rd, "bundle.json"), encoding="utf-8"))
        findings = b.get("findings", [])
        actionable = [f for f in findings if f.get("issue_status") != "narrow_required"]
        narrow = [f for f in findings if f.get("issue_status") == "narrow_required"]
        print("  findings={} actionable={} narrow={}".format(len(findings), len(actionable), len(narrow)))
        for f in findings[:5]:
            iid = f.get("issue_key", "?")
            ist = f.get("issue_status", "?")
            hp = f.get("handling_path", "?")
            ra = f.get("recommended_action", "?")[:80]
            print("    {} [{}] {} -> {}".format(iid, ist, hp, ra))
