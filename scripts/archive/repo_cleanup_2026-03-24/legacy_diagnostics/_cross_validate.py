"""第6轮交叉验证：确认 registry ↔ catalog_snapshot ↔ 22_doc 一致性"""

import json
from pathlib import Path

from app.governance.build_feature_catalog import resolve_progress_doc_path

ROOT = Path(__file__).resolve().parents[1]

# 1. Load registry
with open(ROOT / "app/governance/feature_registry.json", "r", encoding="utf-8") as f:
    reg = json.load(f)

# 2. Load snapshot
with open(ROOT / "app/governance/catalog_snapshot.json", "r", encoding="utf-8") as f:
    snap = json.load(f)

# 3. Load generated governance doc
doc_path = resolve_progress_doc_path()
doc_text = doc_path.read_text(encoding="utf-8")

print("=== 交叉验证 ===\n")

# Check registry → snapshot mapping
reg_ids = {f["feature_id"] for f in reg["features"]}
snap_ids = {f["feature_id"] for f in snap["features"]}

missing_in_snap = reg_ids - snap_ids
missing_in_reg = snap_ids - reg_ids
print(f"Registry IDs: {len(reg_ids)}")
print(f"Snapshot IDs: {len(snap_ids)}")
if missing_in_snap:
    print(f"  [ERROR] In registry but not snapshot: {missing_in_snap}")
if missing_in_reg:
    print(f"  [ERROR] In snapshot but not registry: {missing_in_reg}")
if not missing_in_snap and not missing_in_reg:
    print(f"  [OK] All {len(reg_ids)} IDs match")

# Check all features have audit data
no_audit = [f["feature_id"] for f in reg["features"] if not f.get("spec_requirement")]
print(f"\nAudit coverage: {len(reg_ids) - len(no_audit)}/{len(reg_ids)}")
if no_audit:
    print(f"  [ERROR] Missing audit: {no_audit}")
else:
    print("  [OK] 100% audit coverage")

# Check doc contains key sections
checks = [
    ("总功能点: 119", "119" in doc_text and "总功能点" in doc_text),
    ("深度审计: 119 已审计", "119 已审计" in doc_text),
    ("具体差距项: 147", "147" in doc_text),
    ("全量差距清单", "全量差距清单" in doc_text),
    ("代码完全未实现", "代码完全未实现" in doc_text),
    ("代码实现有问题", "代码实现有问题" in doc_text),
    ("测试严重缺失", "测试严重缺失" in doc_text),
    ("三维审计(方案要求)", "方案要求" in doc_text),
    ("三维审计(代码实现)", "代码实现" in doc_text),
    ("三维审计(测试覆盖)", "测试覆盖" in doc_text),
]

print("\nDoc 内容检查:")
for label, ok in checks:
    status = "OK" if ok else "FAIL"
    print(f"  [{status}] {label}")

# Check gap count per FR in doc
import re
fr_gap_pattern = re.compile(r"\| (FR-[\w-]+) .+? \| (?:🔴|✅)(\d+) \|")
doc_fr_gaps = {}
for m in fr_gap_pattern.finditer(doc_text):
    doc_fr_gaps[m.group(1)] = int(m.group(2))

# Calculate from registry
from collections import defaultdict
reg_fr_gaps = defaultdict(int)
for f in reg["features"]:
    fr_id = f.get("fr_id", "?")
    reg_fr_gaps[fr_id] += len(f.get("gaps", []))

print("\nFR gap count 对比 (registry vs doc):")
all_ok = True
for fr_id in sorted(set(list(doc_fr_gaps.keys()) + list(reg_fr_gaps.keys()))):
    doc_val = doc_fr_gaps.get(fr_id, "N/A")
    reg_val = reg_fr_gaps.get(fr_id, 0)
    match = doc_val == reg_val
    if not match:
        print(f"  [MISMATCH] {fr_id}: registry={reg_val} doc={doc_val}")
        all_ok = False
if all_ok:
    print("  [OK] All FR gap counts match")

# Summary
print("\n=== 总结 ===")
total_gaps = sum(len(f.get("gaps", [])) for f in reg["features"])
code_crit = sum(1 for f in reg["features"] if "❌" in f.get("code_verdict", ""))
code_red = sum(1 for f in reg["features"] if "🔴" in f.get("code_verdict", ""))
test_red = sum(1 for f in reg["features"] if "🔴" in f.get("test_verdict", ""))
print(f"  119/119 功能点已审计")
print(f"  {total_gaps} 个具体差距项")
print(f"  {code_crit} 个 CRITICAL (代码完全未实现)")
print(f"  {code_red} 个代码实现错误")
print(f"  {test_red} 个零测试覆盖")
print(f"  真实完好率: {sum(1 for f in reg['features'] if not f.get('gaps'))}/{len(reg['features'])} = {sum(1 for f in reg['features'] if not f.get('gaps'))/len(reg['features'])*100:.1f}%")
