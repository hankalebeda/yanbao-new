"""
审计结果注入 — 第二轮：映射到真实 feature_id

将 FR-06~FR-13 审计发现映射到 feature_registry.json 中的实际功能点 ID。
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REG_PATH = ROOT / "app" / "governance" / "feature_registry.json"

# 基于真实 feature_id 的审计数据
AUDIT_ROUND2 = {
    # ═══ FR-06 ═══
    "FR06-LLM-01": {  # 研报生成触发
        "spec_requirement": "POST /reports/generate→recommendation∈{BUY,SELL,HOLD}；citations非空；幂等键daily:{stock_code}:{trade_date}→同report_id",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr06_idempotency验证幂等+citations",
        "gaps": []
    },
    "FR06-LLM-05": {  # 降级梯度策略
        "spec_requirement": "LLM降级链primary→backup→cli→local→failed；llm_fallback_level追踪",
        "code_verdict": "✅ 降级链完整实现(codex→deepseek→gemini→ollama)",
        "test_verdict": "⚠️ 仅枚举范围断言，未测逐级降级(mock primary fail→backup触发)；测试走mock_llm直接到local",
        "gaps": ["无逐级降级E2E测试", "测试实际走mock_llm绕过真实降级链"]
    },
    "FR06-LLM-06": {  # 辩证审阅与自我批评
        "spec_requirement": "BUY+confidence≥0.65→副模型风险审计；成功→completed+高级区'风险补充审计'章节；超时→skipped+skip_reason非空",
        "code_verdict": "❌ 完全未实现：无第二LLM端点调用，risk_audit_status只是字符串标记",
        "test_verdict": "🔴 零测试覆盖",
        "gaps": ["副模型调用未实现(CRITICAL)", "risk_audit_status为伪实现(只设字符串)", "高级区无'风险补充审计'章节"]
    },
    "FR06-LLM-07": {  # 策略引擎与买卖信号
        "spec_requirement": "strategy_type∈{A,B,C}规则引擎判(非LLM)：A=事件命中(×1.5)；B=MA20向上+5日涨>3%(×2.0)；C=ATR<2%+波动率后30%分位(×2.5)；atr_multiplier差异化",
        "code_verdict": "🔴 三处不一致：C用固定0.02非市场分位；B缺5日涨>3%；atr_multiplier硬编码1.5不区分A/B/C",
        "test_verdict": "🔴 无strategy_type判定测试",
        "gaps": ["C类应为波动率后30%分位实为固定0.02", "B类缺近5日涨幅>3%", "atr_multiplier硬编码1.5(应A=1.5/B=2.0/C=2.5)", "无测试"]
    },
    "FR06-LLM-08": {  # 熊市短路机制
        "spec_requirement": "BEAR+B/C类→不调LLM，published=false，quality_flag=degraded，status_reason=BEAR_MARKET_FILTERED",
        "code_verdict": "✅ 代码正确实现",
        "test_verdict": "🔴 完全无测试覆盖(SSOT明确要求mock BEAR+B类断言)",
        "gaps": ["BEAR防损场景零测试"]
    },
    "FR06-LLM-09": {  # 实操指令卡
        "spec_requirement": "instruction_card必含6字段；BUY强信号→三挡(10k/100k/500k)；1W买不起→SKIPPED；防倒挂stop_loss>entry→全挡SKIPPED+logic_inversion_fallback；ATR缺失→92%fallback",
        "code_verdict": "🔴 防倒挂实现错误：代码将倒挂修正为92%继续执行，SSOT要求全部SKIPPED",
        "test_verdict": "⚠️ 6字段+三挡+INSUFFICIENT_FUNDS已测；倒挂行为错误",
        "gaps": ["防倒挂：SSOT要求→全挡SKIPPED+logic_inversion_fallback；代码→修正继续执行"]
    },
    "FR06-LLM-10": {  # 同日同股幂等
        "spec_requirement": "幂等：同stock_code+trade_date→同report_id；并发→409",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr06_idempotency+concurrent_conflict",
        "gaps": []
    },
    "FR06-LLM-02": {  # 内部LLM生成
        "spec_requirement": "先验数据：已结算历史样本≥30→prior_stats(含sample_count+data_cutoff<月初)；<30→null；全局LLM熔断(连续N次→llm_circuit_open→Pending→Suspended)",
        "code_verdict": "❌ 全局LLM熔断完全未实现(无llm_circuit_open字段/机制，无Pending→Suspended)；prior_stats代码正确",
        "test_verdict": "🔴 prior_stats三个断言(≥30/null/cutoff)全缺；LLM熔断零测试",
        "gaps": ["全局LLM熔断完全未实现(CRITICAL)", "无Pending→Suspended批量转换", "prior_stats验收断言全缺"]
    },
    "FR06-LLM-03": {  # LLM 健康检查
        "spec_requirement": "LLM健康检查接口",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 已测",
        "gaps": []
    },
    "FR06-LLM-04": {  # LLM版本查询
        "spec_requirement": "LLM版本查询接口",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 已测",
        "gaps": []
    },

    # FR-06 provider-level features (4 providers × 4 features = 16)
    # 这些是AI提供者的具体实现，审计聚焦在统一层
    "FR06-LLM-WEBAI-01": {
        "spec_requirement": "WebAI统一调用网关",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 已测",
        "gaps": []
    },

    # ═══ FR-07 ═══
    "FR07-SETTLE-01": {
        "spec_requirement": "异步结算HTTP 202+task_id；结算公式：佣金=max(金额×0.025%,5元)；印花税=卖出金额×0.5‰；滑点=0.05%；shares固定100",
        "code_verdict": "🔴 滑点率使用0.1%(0.001)是SSOT要求0.05%的2倍",
        "test_verdict": "🔴 测试跟着错误值断言(buy_slippage==1.0应为0.5)",
        "gaps": ["滑点率0.001应为0.0005", "测试断言值连带错误", "FR-08的_buy_cost/_sell_proceeds同样错误"]
    },
    "FR07-SETTLE-02": {
        "spec_requirement": "四维度统计：样本≥30→非null+display_hint=null；<30→null+display_hint='样本积累中'",
        "code_verdict": "🔴 display_hint使用英文'sample_lt_30'而非SSOT要求的中文'样本积累中'",
        "test_verdict": "🔴 四维度阈值场景零测试",
        "gaps": ["display_hint值不符SSOT", "四维度阈值判定零测试"]
    },
    "FR07-SETTLE-03": {
        "spec_requirement": "win_rate口径：仅net_return_pct>0计胜，排除return=0(阈值abs<0.0001)；profit_loss_ratio同",
        "code_verdict": "🔴 分母未排除return=0；未使用0.0001阈值",
        "test_verdict": "🔴 零测试",
        "gaps": ["win_rate分母应count(>0)/count(!=0)", "零收益阈值0.0001未实现", "profit_loss_ratio同问题"]
    },
    "FR07-SETTLE-04": {
        "spec_requirement": "幂等：force=false+已settled→skipped_count>0",
        "code_verdict": "✅ 跳过逻辑正确",
        "test_verdict": "🔴 无测试",
        "gaps": ["force=false幂等跳过无测试"]
    },
    "FR07-SETTLE-05": {
        "spec_requirement": "结算互斥锁：并发→仅1成功(202)另1返回409",
        "code_verdict": "⚠️ 基于DB状态检查(非真正互斥锁，有竞态窗口)",
        "test_verdict": "🔴 零测试",
        "gaps": ["非真正互斥锁(check-then-act竞态)", "并发409零测试"]
    },
    "FR07-SETTLE-06": {
        "spec_requirement": "对照组：样本≥30→baseline_random(runs=500蒙特卡洛)+baseline_ma_cross；<30→null",
        "code_verdict": "🔴 不检查len<30(5条也返回baseline)；蒙特卡洛伪实现(hardcoded win_rate=0.55)",
        "test_verdict": "⚠️ runs=500已测，但baseline本身是伪实现",
        "gaps": ["baseline不检查sample<30", "蒙特卡洛伪实现(固定0.55)", "baseline_ma_cross未见独立实现"]
    },
    "FR07-SETTLE-07": {
        "spec_requirement": "信号有效性：AI跑输随机基线→signal_validity_warning:true；coverage_pct计算",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ monkeypatch验证",
        "gaps": []
    },

    # ═══ FR-08 ═══
    "FR08-SIM-01": {
        "spec_requirement": "回撤熔断：drawdown≤-0.20→HALT(不新开仓)；-0.20<dd≤-0.12→REDUCE(仓位×0.5)；>-0.12→NORMAL",
        "code_verdict": "✅ 阈值+因子正确",
        "test_verdict": "⚠️ HALT已测；REDUCE场景无测试",
        "gaps": ["REDUCE仓位减半场景无测试"]
    },
    "FR08-SIM-02": {
        "spec_requirement": "三挡并发capital=10k/100k/500k；最大持仓1W=2/10W=5/50W=10",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 间接测试",
        "gaps": []
    },
    "FR08-SIM-03": {
        "spec_requirement": "1W INSUFFICIENT_FUNDS→SKIPPED；碎股向下取整至100股；佣金≥5元",
        "code_verdict": "✅ 逻辑正确",
        "test_verdict": "🔴 三项均无FR-08专项测试",
        "gaps": ["INSUFFICIENT_FUNDS无测试", "碎股取整无测试", "佣金≥5无FR-08测试"]
    },
    "FR08-SIM-04": {
        "spec_requirement": "持仓≥180日→TIMEOUT平仓；T+1卖出限定",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr08_timeout_180d完整验证",
        "gaps": []
    },
    "FR08-SIM-05": {
        "spec_requirement": "开仓→平仓E2E(TAKE_PROFIT/STOP_LOSS/TIMEOUT/DELISTED)全覆盖",
        "code_verdict": "⚠️ TAKE_PROFIT/STOP_LOSS/TIMEOUT已实现；DELISTED完全未实现",
        "test_verdict": "⚠️ 仅覆盖TAKE_PROFIT路径",
        "gaps": ["STOP_LOSS E2E链路无测试", "DELISTED E2E链路无测试"]
    },
    "FR08-SIM-06": {
        "spec_requirement": "停牌/一字涨跌停/volume=0→OPEN+pending顺延；退市→DELISTED_LIQUIDATED(exit_price=0,return=-1)",
        "code_verdict": "🔴 退市未实现(无DELISTED检测)；涨跌停锁定未实现",
        "test_verdict": "🔴 零测试",
        "gaps": ["DELISTED_LIQUIDATED未实现(CRITICAL)", "涨跌停锁定未实现", "停牌pending仅部分实现"]
    },
    "FR08-SIM-07": {
        "spec_requirement": "除权复权：前复权动态折算adj_entry_price→重算dynamic_stop_loss/target_price",
        "code_verdict": "❌ 完全未实现：使用冻结的stop_loss_price/target_price，无前复权折算",
        "test_verdict": "🔴 零测试",
        "gaps": ["前复权动态折算完全未实现(CRITICAL)", "10送10等除权事件会误触止损"]
    },
    "FR08-SIM-08": {
        "spec_requirement": "悲观撮合：High≥TP AND Low≤SL→强制STOP_LOSS",
        "code_verdict": "✅ 代码逻辑正确",
        "test_verdict": "🔴 无测试",
        "gaps": ["悲观撮合逻辑无测试"]
    },
    "FR08-SIM-09": {
        "spec_requirement": "模拟看板/portfolio/sim-dashboard；?capital_tier切换；多信号竞争满仓→SKIPPED；confidence优先排序",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 已测",
        "gaps": []
    },
    "FR08-SIM-10": {
        "spec_requirement": "日内先平后开(释放cash→按confidence降序开仓)",
        "code_verdict": "✅ _close_positions→_open_positions顺序正确",
        "test_verdict": "✅ 间接验证(资金释放→开仓)",
        "gaps": []
    },

    # ═══ FR-09 ═══
    "FR09-AUTH-01": {
        "spec_requirement": "注册→201+email+tier=Free；未激活→401(EMAIL_NOT_VERIFIED)；OAuth provider_user_id幂等",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 完整覆盖",
        "gaps": []
    },
    "FR09-AUTH-02": {
        "spec_requirement": "refresh轮转：60s宽限内旧token→401但不登出；超60s→401+全设备登出(revoke_all)",
        "code_verdict": "✅ REFRESH_GRACE_SECONDS=60正确",
        "test_verdict": "🔴 宽限期内401已测；超60s全设备登出零测试",
        "gaps": ["超宽限期重放→全设备登出无测试"]
    },
    "FR09-AUTH-03": {
        "spec_requirement": "forgot-password无论存在→200；reset-password expired→400 RESET_TOKEN_EXPIRED；logout→旧token拦截",
        "code_verdict": "✅ 代码均已正确实现",
        "test_verdict": "🔴 三个端点零测试",
        "gaps": ["forgot-password→200无测试", "reset-password expired→400无测试", "logout→旧token拦截无测试"]
    },
    "FR09-AUTH-04": {
        "spec_requirement": "登录速率：5次失败→第6次429；10分钟后自动解锁；同IP+同email双维度",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ IP维度429已测；10分钟解锁+email维度无测试",
        "gaps": ["10分钟自动解锁无测试", "email维度速率限制无测试"]
    },
    "FR09-AUTH-05": {
        "spec_requirement": "Webhook原子事务：权益写入失败→回滚+500；幂等(同event_id→仅首个发放)；防掉单轮询",
        "code_verdict": "⚠️ 回滚逻辑简单(非严格ACID)；幂等通过查existing实现",
        "test_verdict": "🔴 原子回滚+幂等均无直接测试",
        "gaps": ["Webhook原子回滚无测试", "同event_id幂等无直接测试"]
    },
    "FR09-AUTH-06": {
        "spec_requirement": "定价：Pro 29.9/月；Enterprise 99.9/月；tier_id=Free→422；已有同级→409",
        "code_verdict": "✅ 价格表正确",
        "test_verdict": "⚠️ tier=Free→422已测；具体价格+已有同级409无测试",
        "gaps": ["定价金额无精确断言"]
    },

    # ═══ FR-10 ═══
    "FR10-SITE-01": {
        "spec_requirement": "首页GET /home→latest_reports≤5+market_state∈{BULL,NEUTRAL,BEAR}",
        "code_verdict": "✅ limit=5+fallback NEUTRAL",
        "test_verdict": "✅ 已测",
        "gaps": []
    },
    "FR10-SITE-02": {
        "spec_requirement": "统计看板→default window_days=30，by_strategy_type含A/B/C全8字段",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 已测",
        "gaps": []
    },
    "FR10-SITE-03": {
        "spec_requirement": "模拟看板→is_simulated_only=true+signal_validity_warning+equity_curve连续",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr10_equity_curve_continuous",
        "gaps": []
    },
    "FR10-SITE-04": {
        "spec_requirement": "E2E各页面可访问无500：/ /reports /report/{id} /dashboard /portfolio/sim-dashboard",
        "code_verdict": "✅ 路由已注册",
        "test_verdict": "🔴 无E2E页面渲染测试(5个页面HTTP 200检查全缺)",
        "gaps": ["5个页面E2E渲染测试全缺"]
    },
    "FR10-SITE-05": {
        "spec_requirement": "高级区：未登录→401；Free→≤200字+省略；Pro→全文",
        "code_verdict": "✅ 截断197+'...'正确",
        "test_verdict": "🔴 401/Free截断/Pro三场景零测试",
        "gaps": ["advanced端点三场景零测试"]
    },
    "FR10-SITE-06": {
        "spec_requirement": "Free价格脱敏'¥**.**'+sim_trade_instruction=null；Pro→float",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ masks+pro双测试",
        "gaps": []
    },
    "FR10-SITE-07": {
        "spec_requirement": "term_context为dict+ATR含具体数值",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 已测",
        "gaps": []
    },
    "FR10-SITE-08": {
        "spec_requirement": "Dashboard三线图(AI+random+MA)；AI跑输random→前端⚠️告警",
        "code_verdict": "⚠️ 3线绘制OK；前端AI<random告警逻辑缺失",
        "test_verdict": "⚠️ 后端数据已测；前端告警无测试",
        "gaps": ["前端无AI跑输random→⚠️告警的JS逻辑"]
    },
    "FR10-SITE-09": {
        "spec_requirement": "骨架屏：数据未就绪→'数据结算中/生成中'提示",
        "code_verdict": "⚠️ 降级提示存在(非标准skeleton)",
        "test_verdict": "⚠️ data_status=COMPUTING已测",
        "gaps": ["非标准骨架屏效果"]
    },

    # ═══ FR-12 ═══
    "FR12-ADMIN-01": {
        "spec_requirement": "非admin→403",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 已测",
        "gaps": []
    },
    "FR12-ADMIN-02": {
        "spec_requirement": "GET admin/users→items/total分页+tier筛选",
        "code_verdict": "✅ 正确",
        "test_verdict": "🔴 无专项测试",
        "gaps": ["GET admin/users无专项测试"]
    },
    "FR12-ADMIN-03": {
        "spec_requirement": "PATCH admin/reports/{id}→review_flag/published修改+审计记录",
        "code_verdict": "✅ 双表审计",
        "test_verdict": "🔴 无测试",
        "gaps": ["PATCH review_flag+audit无测试"]
    },
    "FR12-ADMIN-04": {
        "spec_requirement": "GET admin/overview→pool_size(int)+active_positions(Dict,keys=10k/100k/500k)",
        "code_verdict": "✅ 动态tier_keys",
        "test_verdict": "✅ test_fr12_overview_min_fields",
        "gaps": []
    },
    "FR12-ADMIN-05": {
        "spec_requirement": "force-regenerate：非super_admin→403；成功→旧软删+新ID+审计；级联阻断(sim引用→409)",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 三场景全覆盖",
        "gaps": []
    },
    "FR12-ADMIN-06": {
        "spec_requirement": "补单reconcile→权益发放+幂等+审计",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 幂等已测",
        "gaps": []
    },
    "FR12-ADMIN-07": {
        "spec_requirement": "PATCH admin/users/{id}(修改tier/role)+审计",
        "code_verdict": "✅ 正确",
        "test_verdict": "🔴 无专项测试",
        "gaps": ["PATCH admin/users/{id}无测试"]
    },
    "FR12-ADMIN-08": {
        "spec_requirement": "GET admin/reports(review_flag筛选)+所有PATCH审计记录完整",
        "code_verdict": "✅ create_audit_log统一写入",
        "test_verdict": "⚠️ FR-00审计已测；admin层PATCH审计无独立测试",
        "gaps": ["admin层PATCH审计无独立测试"]
    },

    # ═══ FR-13 ═══
    "FR13-EVENT-01": {
        "spec_requirement": "平仓→POSITION_CLOSED通知；DELISTED时exit_price=0.0",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ TAKE_PROFIT已测；DELISTED+exit_price=0无测试",
        "gaps": ["DELISTED+exit_price=0场景无测试"]
    },
    "FR13-EVENT-02": {
        "spec_requirement": "幂等去重：同事件二次→status=skipped",
        "code_verdict": "✅ 去重键检查",
        "test_verdict": "✅ test_fr13_notification_idempotent",
        "gaps": []
    },
    "FR13-EVENT-03": {
        "spec_requirement": "DRAWDOWN_ALERT：同capital_tier 4h内仅首条sent，后续skipped",
        "code_verdict": "✅ SUPPRESS_HOURS=4",
        "test_verdict": "✅ 已测",
        "gaps": []
    },
    "FR13-EVENT-04": {
        "spec_requirement": "推送失败不阻塞结算(Outbox模式)；事务回滚→无sent记录",
        "code_verdict": "✅ Outbox隔离",
        "test_verdict": "⚠️ 回滚已测；推送失败不阻塞无测试",
        "gaps": ["推送失败不阻塞结算无测试"]
    },
    "FR13-EVENT-05": {
        "spec_requirement": "BUY_SIGNAL_DAILY→confidence倒序≤5条；signal=0→不创建事件；email_enabled=false→skipped",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ ≤5+空信号已测；email_enabled=false无测试",
        "gaps": ["user_email_enabled=false→skipped无测试"]
    },
}

def main():
    with open(REG_PATH, "r", encoding="utf-8") as f:
        registry = json.load(f)

    updated = 0
    for feat in registry["features"]:
        fid = feat["feature_id"]
        if fid in AUDIT_ROUND2:
            a = AUDIT_ROUND2[fid]
            feat["spec_requirement"] = a["spec_requirement"]
            feat["code_verdict"] = a["code_verdict"]
            feat["test_verdict"] = a["test_verdict"]
            feat["gaps"] = a["gaps"]
            updated += 1

    with open(REG_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)

    total_gaps = sum(len(a["gaps"]) for a in AUDIT_ROUND2.values())
    print(f"[OK] 第二轮更新 {updated}/{len(registry['features'])} 条 ({total_gaps} 个差距项)")

if __name__ == "__main__":
    main()
