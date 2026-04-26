"""Collect raw_results.json from junit XML."""
import json
import re
import xml.etree.ElementTree as ET

tree = ET.parse("output/junit_verify.xml")
results = []
for tc in tree.findall(".//testcase"):
    name = tc.get("name", "")
    cls = tc.get("classname", "")
    nodeid = f"{cls}::{name}"
    failure = tc.find("failure")
    passed = failure is None
    msg = failure.get("message", "")[:200] if failure is not None else ""

    m = re.search(r"test_(FR\d+)_(\w+?)_(\d+)", name)
    fid = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""
    if not fid:
        m2 = re.search(r"test_(FR\d+)", name)
        fid = m2.group(1) if m2 else ""

    kind = "general"
    for k in ("browser", "dom", "contract", "api"):
        if k in name.lower():
            kind = k
            break

    results.append({
        "feature_id": fid,
        "verify_kind": kind,
        "passed": passed,
        "failure_msg": msg,
        "nodeid": nodeid,
    })

with open("output/raw_results.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

passed = sum(1 for r in results if r["passed"])
failed = sum(1 for r in results if not r["passed"])
print(f"Collected {len(results)} results: {passed} passed, {failed} failed")
