"""v24 硬闸直连验证：调用 _collect_generation_input_issues，确认 3 个新增 capital 数据集缺失时能正确阻断。"""
import sys
sys.path.insert(0, r"d:\yanbao-new")
from app.services.report_generation_ssot import (
    _collect_generation_input_issues,
    _REPORT_REQUIRED_INPUT_DATASETS,
)

print("[v24-gate-test] required datasets:", sorted(_REPORT_REQUIRED_INPUT_DATASETS))

# 情景 A：仅老 5 项全 ok（缺 3 个新资金数据集）
case_a = [
    {"dataset_name": "kline_daily", "status": "ok"},
    {"dataset_name": "hotspot_top50", "status": "ok"},
    {"dataset_name": "northbound_summary", "status": "ok"},
    {"dataset_name": "etf_flow_summary", "status": "ok"},
    {"dataset_name": "market_state_input", "status": "ok"},
]
issues_a = _collect_generation_input_issues(used_data=case_a, market_state_row={"market_state_degraded": False, "state_reason": None})
print("\n[case-A old-5-ok only, missing 3 capital] issues:")
for i in issues_a: print("  -", i)
assert any("main_force_flow" in i for i in issues_a), "main_force_flow gate not enforced"
assert any("dragon_tiger_list" in i for i in issues_a), "dragon_tiger_list gate not enforced"
assert any("margin_financing" in i for i in issues_a), "margin_financing gate not enforced"

# 情景 B：全 8 项 ok
case_b = case_a + [
    {"dataset_name": "main_force_flow", "status": "ok"},
    {"dataset_name": "dragon_tiger_list", "status": "ok"},
    {"dataset_name": "margin_financing", "status": "ok"},
]
issues_b = _collect_generation_input_issues(used_data=case_b, market_state_row={"market_state_degraded": False, "state_reason": None})
print(f"\n[case-B all 8 ok] issues: {issues_b}")
assert not issues_b, f"clean path should pass, got {issues_b}"

print("\n[v24-gate-test] PASS: hard gate blocks on missing capital datasets; clean path passes.")
