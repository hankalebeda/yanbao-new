"""
审计结果注入 — 第三轮：覆盖全部剩余 48 个功能点

FR-04(7) + FR-06 Provider(16) + FR-09 Billing/Auth(5)
+ FR-09B(4) + FR-10 Page/Platform(8) + FR-11(2)
+ LEGACY(4) + OOS(2)
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REG_PATH = ROOT / "app" / "governance" / "feature_registry.json"

AUDIT_ROUND3 = {
    # ═══ FR-04 数据采集 ═══
    "FR04-DATA-01": {
        "spec_requirement": "7源优先级采集热搜(eastmoney→weibo→…)，去重合并Top50；≥3源=ok/<3=stale_ok/全失败=degraded",
        "code_verdict": "✅ multisource_ingest完整实现7源+合并+quality_flag三级",
        "test_verdict": "✅ test_fr04_normalized_output验证Top50+rank连续",
        "gaps": ["registry primary_api误指旧端点(/hotspot/collect)"]
    },
    "FR04-DATA-02": {
        "spec_requirement": "热点富化：关联股票代码、标注event_type、计算衰减权重/情绪分",
        "code_verdict": "⚠️ /hotspot/enrich端点只做COUNT(*)返回数量，未调用实际富化函数",
        "test_verdict": "🔴 零测试",
        "gaps": ["enrich端点是空壳(只返回count)", "compute_decay_weight/score_sentiment/infer_event_type未串联调用", "零测试"]
    },
    "FR04-DATA-03": {
        "spec_requirement": "热点健康检查：返回各源最近采集时间与状态",
        "code_verdict": "⚠️ /hotspot/health只查MAX(fetch_time)返回ok/degraded，未检查逐源状态/数据新鲜度阈值",
        "test_verdict": "🔴 零测试",
        "gaps": ["未返回逐源健康状况", "无数据新鲜度阈值检查", "零测试"]
    },
    "FR04-DATA-04": {
        "spec_requirement": "实时查询各数据源降级/circuit_breaker状态",
        "code_verdict": "⚠️ /source/fallback-status返回硬编码字符串，内存状态与DB circuit_state不同步",
        "test_verdict": "🔴 零测试",
        "gaps": ["返回硬编码值非实时状态", "内存RuntimeState与DB DataSourceCircuitState不同步", "零测试"]
    },
    "FR04-DATA-05": {
        "spec_requirement": "单源连续失败≥3→熔断300s→半开探测→恢复/冷却；非核心池Partial Commit，单股失败不回滚全批",
        "code_verdict": "✅ CLOSED→OPEN→HALF_OPEN完整状态机+300s cooldown+partial commit",
        "test_verdict": "✅ test_fr04验证熔断全流程+尾部失败不影响核心池",
        "gaps": ["熔断事件未写入NFR-13告警日志"]
    },
    "FR04-DATA-06": {
        "spec_requirement": "公开端点GET /market/hot-stocks返回热门股票列表",
        "code_verdict": "⚠️ 端点返回股票池前N只而非热搜数据，字段仅stock_code+name，limit max=10(应Top50)",
        "test_verdict": "🔴 零测试",
        "gaps": ["返回池子样本而非热搜数据(名实不符)", "缺少rank/topic_title/source_name等字段", "limit max=10远小于Top50", "零测试"]
    },
    "FR04-DATA-07": {
        "spec_requirement": "采集northbound_summary+etf_flow_summary；status∈{ok,missing,degraded}；缺失不阻塞主链；血缘登记report_data_usage",
        "code_verdict": "✅ _collect_summary正确处理ok/missing/degraded+report_data_usage记录",
        "test_verdict": "✅ test_fr04验证northbound=ok+etf_flow异常=degraded+usage行",
        "gaps": ["registry primary_api误标为/hotspot/collect"]
    },

    # ═══ FR-06 LLM Provider (16个，已退出研报生成主路径，作为独立AI调用工具) ═══
    "FR06-LLM-GEMINI-01": {
        "spec_requirement": "Gemini Web自动化分析：POST /api/v1/gemini/analyze→Playwright驱动真实连接",
        "code_verdict": "✅ 完整实现(Playwright→gemini.google.com)",
        "test_verdict": "⚠️ 仅手动test.py脚本，无pytest用例",
        "gaps": ["无pytest验收测试", "已退出研报生成主路径"]
    },
    "FR06-LLM-GEMINI-02": {
        "spec_requirement": "Gemini批量分析：POST /api/v1/gemini/analyze/batch→asyncio.gather并发",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-GEMINI-03": {
        "spec_requirement": "Gemini会话管理：DELETE /api/v1/gemini/session→关闭Chrome进程",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-GEMINI-04": {
        "spec_requirement": "Gemini会话状态：GET /api/v1/gemini/session/status",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "🔴 零测试",
        "gaps": ["零测试"]
    },
    "FR06-LLM-CHATGPT-01": {
        "spec_requirement": "ChatGPT Web自动化分析：POST /api/v1/chatgpt/analyze→Playwright+GPT-5.x探测",
        "code_verdict": "✅ 完整实现(含GPT-5模型版本探测)",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-CHATGPT-02": {
        "spec_requirement": "ChatGPT批量分析：POST /api/v1/chatgpt/analyze/batch",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-CHATGPT-03": {
        "spec_requirement": "ChatGPT会话管理：DELETE /api/v1/chatgpt/session",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-CHATGPT-04": {
        "spec_requirement": "ChatGPT会话状态：GET /api/v1/chatgpt/session/status",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "🔴 零测试",
        "gaps": ["零测试"]
    },
    "FR06-LLM-DEEPSEEK-01": {
        "spec_requirement": "DeepSeek Web自动化分析：POST /api/v1/deepseek/analyze",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-DEEPSEEK-02": {
        "spec_requirement": "DeepSeek批量分析：POST /api/v1/deepseek/analyze/batch",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-DEEPSEEK-03": {
        "spec_requirement": "DeepSeek会话管理：DELETE /api/v1/deepseek/session",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-DEEPSEEK-04": {
        "spec_requirement": "DeepSeek会话状态：GET /api/v1/deepseek/session/status",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "🔴 零测试",
        "gaps": ["零测试"]
    },
    "FR06-LLM-QWEN-01": {
        "spec_requirement": "Qwen Web自动化分析：POST /api/v1/qwen/analyze→支持CDP模式",
        "code_verdict": "✅ 完整实现(含CDP附加浏览器模式)",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-QWEN-02": {
        "spec_requirement": "Qwen批量分析：POST /api/v1/qwen/analyze/batch",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-QWEN-03": {
        "spec_requirement": "Qwen会话管理：DELETE /api/v1/qwen/session",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "⚠️ 仅手动test.py脚本",
        "gaps": ["无pytest验收测试"]
    },
    "FR06-LLM-QWEN-04": {
        "spec_requirement": "Qwen会话状态：GET /api/v1/qwen/session/status",
        "code_verdict": "✅ 完整实现",
        "test_verdict": "🔴 零测试",
        "gaps": ["零测试"]
    },

    # ═══ FR-09 Billing/Auth 补充 ═══
    "FR09-BILLING-01": {
        "spec_requirement": "POST /billing/create_order→201+BillingOrderSummary；tier_id=Free→422；同级已有效→409 TIER_ALREADY_ACTIVE；PENDING复用",
        "code_verdict": "✅ 完整实现(鉴权+FREE拦截+409+PENDING复用+15min过期)",
        "test_verdict": "⚠️ 仅通过webhook测试间接验证201，缺独立test_create_order用例",
        "gaps": ["缺独立测试：tier_id=Free→422", "缺独立测试：已有效权益→409"]
    },
    "FR09-BILLING-02": {
        "spec_requirement": "POST /billing/webhook→签名校验+event_id幂等+原子事务(PAID+权益发放)；签名失败400；内部失败500",
        "code_verdict": "✅ 完整实现(双签名校验+幂等+rollback+降级防御)",
        "test_verdict": "⚠️ 间接验证PAID路径，缺幂等/签名/回滚/降级专项测试",
        "gaps": ["缺webhook幂等重复→duplicate=true测试", "缺签名失败→400测试", "缺原子回滚测试"]
    },
    "FR09-BILLING-03": {
        "spec_requirement": "GET /membership/subscription/status→tier/plan_code/status/tier_expires_at(05未冻结此路由)",
        "code_verdict": "✅ 实现正确(仅本人或管理员可查)",
        "test_verdict": "🔴 零测试",
        "gaps": ["非SSOT冻结路由", "零测试"]
    },
    "FR09-AUTH-08": {
        "spec_requirement": "GET /auth/me→UserAuthProfile(tier/email/role)；多验收用例依赖此验证JWT有效性",
        "code_verdict": "✅ 正确实现+NFR-14封装",
        "test_verdict": "✅ test_fr09_refresh间接验证200+email+tier",
        "gaps": ["返回字段有冗余(membership_level与tier重复)"]
    },
    "FR09-AUTH-09": {
        "spec_requirement": "GET /platform/plans→Free/Pro/Enterprise三档套餐信息(05未冻结此路由)",
        "code_verdict": "✅ 正确实现(无需鉴权)",
        "test_verdict": "🔴 零测试",
        "gaps": ["非SSOT冻结路由", "无价格精确断言(29.9/79.9等)", "零测试"]
    },

    # ═══ FR-09B 清理 ═══
    "FR09B-CLEAN-01": {
        "spec_requirement": "POST /internal/reports/clear→调试工具(非FR-09-b冻结功能)；SSOT软删除/非SSOT硬删除",
        "code_verdict": "⚠️ 调试路由(internal_auth保护)，非SSOT硬删与'永久保留'规则矛盾",
        "test_verdict": "🔴 无独立FR验收测试",
        "gaps": ["调试工具非FR-09-b验收范围", "非SSOT硬删与保留规则矛盾"]
    },
    "FR09B-CLEAN-02": {
        "spec_requirement": "POST /internal/stats/clear→调试工具(非FR-09-b冻结功能)；清空settlement/feedback/sim等表",
        "code_verdict": "⚠️ 调试路由(internal_auth保护)，不检查活跃仓位引用",
        "test_verdict": "🔴 无独立FR验收测试",
        "gaps": ["调试工具非FR-09-b验收范围", "清空时不检查活跃引用"]
    },
    "FR09B-CLEAN-03": {
        "spec_requirement": "run_cleanup分级保留：session/temp_token/atl 7d，task 30d，stale 3d，notification 30d，unverified 24h；分批≤500+sleep 100ms；互斥锁",
        "code_verdict": "⚠️ 保留周期正确，但分批删除未实现(全量DELETE)、互斥锁未实现",
        "test_verdict": "✅ 4个测试覆盖字段/stale/unverified/研报保留",
        "gaps": ["分批删除≤500未实现(SSOT边界第1条)", "互斥锁未实现(SSOT边界第4条)", "status_reason='stale_cleanup'应为'stale_task_expired'"]
    },
    "FR09B-CLEAN-04": {
        "spec_requirement": "email_verified=false超24h物理删除释放邮箱占用；count>0时记录脱敏email列表",
        "code_verdict": "✅ UNVERIFIED_ACCOUNT_HOURS=24物理DELETE正确",
        "test_verdict": "✅ 完整验证(创建2天前未激活→清理→消失+count≥1)",
        "gaps": ["脱敏审计缺失(首3字符+***+域名)"]
    },

    # ═══ FR-10 页面/平台 ═══
    "FR10-HOME-01": {
        "spec_requirement": "GET /→渲染首页；GET /api/v1/home→最新5条摘要+market_state+pool_size+data_status，5min缓存",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr10_home_api_fields覆盖",
        "gaps": ["today_report_count字段(05§7.1冻结)未见测试断言"]
    },
    "FR10-LIST-01": {
        "spec_requirement": "GET /reports→列表HTML；GET /api/v1/reports→分页+筛选(stock_code/recommendation/strategy_type等)，默认page_size=20",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr10_reports_list_contract",
        "gaps": ["quality_flag=missing→items=[]语义无专项测试"]
    },
    "FR10-DETAIL-01": {
        "spec_requirement": "GET /report/{id}→详情HTML；GET /api/v1/reports/{id}→ReportDetail含instruction_card/term_context；Free脱敏'¥**.**'",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ Free脱敏+Pro完整+term_context",
        "gaps": ["capital_game_summary字段无专项测试"]
    },
    "FR10-BOARD-01": {
        "spec_requirement": "GET /dashboard→看板HTML；GET /api/v1/dashboard/stats?window_days={1,7,14,30,60}→by_strategy_type A/B/C+baseline",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ COMPUTING态+baseline null",
        "gaps": ["window_days不同值切换隔离无专项测试"]
    },
    "FR10-BOARD-02": {
        "spec_requirement": "GET /portfolio/sim-dashboard→需登录；API返回equity_curve+baseline三线+is_simulated_only=true；Free仅100k",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ tier限制+equity_curve连续+三线+空数据态",
        "gaps": []
    },
    "FR10-DETAIL-02": {
        "spec_requirement": "GET /api/v1/reports/{id}/advanced→JWT；Free≤200字+is_truncated=true；Pro完整；401未登录",
        "code_verdict": "✅ 正确(197+'...'截断)",
        "test_verdict": "🔴 无FR-10级专项测试(401/Free截断/Pro三场景全缺)",
        "gaps": ["advanced端点三场景无FR-10测试(401/Free截断200/Pro完整)"]
    },
    "FR10-PLATFORM-01": {
        "spec_requirement": "GET /api/v1/platform/config→capital_tiers/labels等配置(05未冻结此路由)",
        "code_verdict": "⚠️ 功能可用但非SSOT冻结路由",
        "test_verdict": "🔴 零测试",
        "gaps": ["非SSOT冻结路由(05_API未定义)", "零测试"]
    },
    "FR10-PLATFORM-02": {
        "spec_requirement": "GET /api/v1/platform/summary→win_rate/pnl_ratio/alpha公开汇总(05未冻结此路由)",
        "code_verdict": "⚠️ 功能可用但非SSOT冻结路由",
        "test_verdict": "🔴 零测试",
        "gaps": ["非SSOT冻结路由(05_API未定义)", "零测试"]
    },

    # ═══ FR-11 反馈 ═══
    "FR11-FEEDBACK-01": {
        "spec_requirement": "POST /reports/{id}/feedback→JWT；positive仅记录；negative去重≥3置PENDING_REVIEW；返回FeedbackSubmitResult",
        "code_verdict": "✅ 完整实现(JWT+存在性检查+去重+3阈值+FR-13联动)",
        "test_verdict": "✅ 401/去重/negative_count/review_flag/联动全覆盖",
        "gaps": []
    },
    "FR11-FEEDBACK-02": {
        "spec_requirement": "单用户negative每日≤20次(可配)，超限429；原子计数(Lock+BEGIN IMMEDIATE)；50并发后≤20",
        "code_verdict": "✅ 完整实现(threading.Lock+SQLite BEGIN IMMEDIATE原子性)",
        "test_verdict": "✅ 20次成功→第21次429+50并发断言≤20",
        "gaps": []
    },

    # ═══ LEGACY 旧路由 ═══
    "LEGACY-REPORT-01": {
        "spec_requirement": "[兼容] GET /report/{stock_code}→查最新report_id→302重定向",
        "code_verdict": "✅ 05§05.1已冻结为兼容入口",
        "test_verdict": "⚠️ 无专项测试",
        "gaps": ["兼容入口无测试"]
    },
    "LEGACY-REPORT-02": {
        "spec_requirement": "[兼容] GET /demo/report/{stock_code}/status→302委托到canonical_report_status",
        "code_verdict": "✅ 05§05.1已冻结为兼容入口",
        "test_verdict": "⚠️ 无专项测试",
        "gaps": ["兼容入口无测试"]
    },
    "LEGACY-REPORT-03": {
        "spec_requirement": "[兼容] GET /report/实时研报/{stock_code}→302到/report/{stock_code}",
        "code_verdict": "✅ 05§05.1已冻结为兼容入口",
        "test_verdict": "⚠️ 无专项测试",
        "gaps": ["兼容入口无测试"]
    },
    "LEGACY-REPORT-04": {
        "spec_requirement": "[兼容] GET /demo/report/{stock_code}→302到/report/{stock_code}",
        "code_verdict": "✅ 05§05.1已冻结为兼容入口",
        "test_verdict": "⚠️ 无专项测试",
        "gaps": ["兼容入口无测试"]
    },

    # ═══ OOS ═══
    "OOS-MOCK-PAY-01": {
        "spec_requirement": "[OUT_OF_SSOT] GET /billing/mock-pay/{order_id}→开发/测试用mock支付页面",
        "code_verdict": "✅ 功能完整(需登录+显示订单+确认按钮)",
        "test_verdict": "⚠️ 无专项测试(OOS范围可接受)",
        "gaps": []
    },
    "OOS-MOCK-PAY-02": {
        "spec_requirement": "[OUT_OF_SSOT] POST /billing/mock-pay/{order_id}/confirm→触发mock webhook完成支付闭环",
        "code_verdict": "✅ 功能完整",
        "test_verdict": "⚠️ 无专项测试(OOS范围可接受)",
        "gaps": []
    },
}

def main():
    with open(REG_PATH, "r", encoding="utf-8") as f:
        registry = json.load(f)

    updated = 0
    for feat in registry["features"]:
        fid = feat["feature_id"]
        if fid in AUDIT_ROUND3:
            a = AUDIT_ROUND3[fid]
            feat["spec_requirement"] = a["spec_requirement"]
            feat["code_verdict"] = a["code_verdict"]
            feat["test_verdict"] = a["test_verdict"]
            feat["gaps"] = a["gaps"]
            updated += 1

    # Update version
    registry["version"] = "3.0.0"

    with open(REG_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)

    total_gaps = sum(len(a["gaps"]) for a in AUDIT_ROUND3.values())
    print(f"[OK] 第三轮更新 {updated}/{len(registry['features'])} 条 ({total_gaps} 个差距项)")

    # Check completeness
    no_audit = [feat["feature_id"] for feat in registry["features"] if not feat.get("spec_requirement")]
    if no_audit:
        print(f"[WARN] 仍有 {len(no_audit)} 条无审计: {no_audit}")
    else:
        print("[OK] 全部 119 条功能点已审计覆盖!")

if __name__ == "__main__":
    main()
