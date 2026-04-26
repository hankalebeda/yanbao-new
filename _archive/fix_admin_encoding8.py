"""Replace entire garbled CN dict lines in admin.html with correct versions."""

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# TIER_CN: Free, Pro, Enterprise, 10k, 100k, 500k
old_tier = "var TIER_CN = {'Free':'免费','Pro':'涓撲笟鐗?,'Enterprise':'企业版,'10k':'1个,'100k':'10个,'500k':'50个};"
new_tier = "var TIER_CN = {'Free':'免费','Pro':'专业版','Enterprise':'企业版','10k':'1万','100k':'10万','500k':'50万'};"
c = text.count(old_tier)
print(f"TIER_CN: {c} occurrences")
text = text.replace(old_tier, new_tier)

# REVIEW_CN
old_review = "var REVIEW_CN = {'PENDING_REVIEW':'寰呭瀹?,'APPROVED':'宸查€氳繃','REJECTED':'已拒绝,'AUTO_APPROVED':'鑷姩閫氳繃'};"
new_review = "var REVIEW_CN = {'PENDING_REVIEW':'待审核','APPROVED':'已通过','REJECTED':'已拒绝','AUTO_APPROVED':'自动通过'};"
c = text.count(old_review)
print(f"REVIEW_CN: {c} occurrences")
text = text.replace(old_review, new_review)

# QF_CN
old_qf = "var QF_CN = {'ok':'正常','stale_ok':'数据略旧','degraded':'已降级,'failed':'失败','llm_degraded':'LLM降级','rule_fallback':'规则兜底'};"
new_qf = "var QF_CN = {'ok':'正常','stale_ok':'数据略旧','degraded':'已降级','failed':'失败','llm_degraded':'LLM降级','rule_fallback':'规则兜底'};"
c = text.count(old_qf)
print(f"QF_CN: {c} occurrences")
text = text.replace(old_qf, new_qf)

# REASON_CN - this is the longest one
old_reason_start = "var REASON_CN = {'lock_held':'锁已持有','non_trade_day':'非交易日'"
# Find the line containing this
lines = text.split("\n")
new_reason = "var REASON_CN = {'lock_held':'锁已持有','non_trade_day':'非交易日','upstream_not_ready':'上游未就绪','waiting_upstream':'等待上游完成','upstream_ready':'上游已就绪','timeout':'超时','ok':'正常','no_error':'无错误','kline_fetch_failed':'K线抓取失败','no_items_fetched':'未获取到数据','degraded':'降级','self_lock_bug_cleanup':'自锁修复清理','retries_exhausted':'重试次数已用尽','upstream_timeout_next_open':'上游超时','no_handler':'无处理函数','unexpected_error':'意外错误','idempotent_skip':'幂等跳过','admin_manual_retrigger':'管理员补跑','KLINE_COVERAGE_INSUFFICIENT':'当日行情覆盖不足','home_source_inconsistent':'公开读模型已回退到稳定批次','home_snapshot_not_ready':'公开批次尚未就绪','stats_history_truncated':'历史窗口覆盖不足','stats_source_degraded':'部分历史批次未完整回填'};"

reason_count = 0
new_lines = []
for line in lines:
    stripped = line.strip()
    if stripped.startswith("var REASON_CN") and "lock_held" in stripped:
        indent = line[:len(line) - len(line.lstrip())]
        new_lines.append(indent + new_reason)
        reason_count += 1
    else:
        new_lines.append(line)
text = "\n".join(new_lines)
print(f"REASON_CN: {reason_count} occurrences")

# Verify all CN dicts are valid JS syntax (no garbled chars in them)
import re
for name in ["TIER_CN", "REVIEW_CN", "QF_CN", "REASON_CN", "STATUS_CN", "PUBLIC_STATUS_CN", "TRIGGER_CN", "ROLE_CN", "TASK_CN"]:
    for i, line in enumerate(text.split("\n"), 1):
        if f"var {name} = " in line:
            has_pua = any("\ue000" <= c <= "\uf8ff" for c in line)
            has_garbled_q = bool(re.search(r"[\u4e00-\u9fff]\?", line))
            has_missing_quote = bool(re.search(r"[\u4e00-\u9fff],", line))
            status = "OK" if not (has_pua or has_garbled_q or has_missing_quote) else "BROKEN"
            if status == "BROKEN":
                print(f"\n  BROKEN Line {i} ({name}): {line.strip()[:150]}")
            else:
                print(f"  OK Line {i} ({name})")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("\nSaved.")
