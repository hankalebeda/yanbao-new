"""
审计结果注入 feature_registry.json

基于 4 轮深度审计（FR-00~FR-13），将以下字段注入每个 feature：
  - spec_requirement: SSOT 规格要求（具体验收标准）
  - code_verdict:     代码实现判定 (✅正确|⚠️部分|🔴缺失|❌未实现)
  - test_verdict:     测试覆盖判定
  - page_verdict:     页面实现判定 (如有)
  - gaps:             具体差距描述列表
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REG_PATH = ROOT / "app" / "governance" / "feature_registry.json"

# ── 审计数据（基于 4 轮逐函数/逐断言的深度审计）──

AUDIT = {
    # ═══ FR-00 真实性红线 ═══
    "FR00-AUTH-01": {
        "spec_requirement": "已发布研报(published=true)只读保护：PUT→405/403，内容不变；PATCH仅允许review_flag/published",
        "code_verdict": "✅ 正确：FastAPI无PUT路由→默认405",
        "test_verdict": "✅ test_fr00_published_report_readonly断言405+内容不变",
        "gaps": []
    },
    "FR00-AUTH-02": {
        "spec_requirement": "citations三要素: source_name非空、source_url以http(s)开头、fetch_time为ISO8601；写入端需校验",
        "code_verdict": "⚠️ 读取端正确，写入端无校验（可写入ftp://等非法URL）",
        "test_verdict": "⚠️ 测试依赖seed数据中合法值，未测写入端校验",
        "gaps": ["写入端无source_url格式校验", "无非法URL注入测试"]
    },
    "FR00-AUTH-03": {
        "spec_requirement": "PATCH admin/reports/{id}→audit_log含actor_user_id+before/after快照",
        "code_verdict": "✅ 正确：双表审计(admin_operation+audit_log)",
        "test_verdict": "✅ test_fr00_publish_operation_audit_record完整验证",
        "gaps": []
    },
    # ── FR-00 缺失的验收点 ──
    # 以下3个验收标准在FR-00功能点中被遗漏，需要补充为新功能点

    # ═══ FR-01 股票池筛选 ═══
    "FR01-POOL-01": {
        "spec_requirement": "POST /admin/pool/refresh→task_id+status∈{IDLE,REFRESHING,COMPLETED,FALLBACK,COLD_START_BLOCKED}；鉴权JWT+RBAC(admin|super_admin)",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 完整覆盖",
        "gaps": []
    },
    "FR01-POOL-02": {
        "spec_requirement": "八因子权重：momentum=20%,market_cap=15%,liquidity=20%,MA20_slope=15%,earnings=10%,turnover=10%,RSI=5%,52w_high=5%；公式score=Σ(normalize_rank×weight)×100",
        "code_verdict": "⚠️ 权重硬编码正确，但earnings_improve永远返回0.5桩值(10%权重因子从不使用真实盈利数据)",
        "test_verdict": "🔴 无测试验证因子权重或打分公式",
        "gaps": ["earnings_improve硬编码0.5（桩值）", "无八因子权重/打分公式专项测试"]
    },
    "FR01-POOL-03": {
        "spec_requirement": "核心池查询：精确200只无ST，候补池201-250非空且所有分数<核心池最小分数",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr01_pool_size_200+test_fr01_pool_no_st完整覆盖",
        "gaps": []
    },
    "FR01-POOL-04": {
        "spec_requirement": "池快照版本管理+失败回退到上一交易日池(fallback_from非空)",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr01_fallback_on_fail验证FALLBACK+fallback_from",
        "gaps": []
    },
    "FR01-POOL-05": {
        "spec_requirement": "候补池201-250名，分数<核心池最小分数",
        "code_verdict": "✅ STANDBY_POOL_SIZE=50，按分数排序",
        "test_verdict": "✅ 断言max(standby)<min(core)",
        "gaps": []
    },
    "FR01-POOL-06": {
        "spec_requirement": "并发互斥：同trade_date并发→409 CONCURRENT_CONFLICT；幂等：同日已完成→直接返回不重算；冷启动：无历史池→COLD_START_ERROR阻断FR-04/FR-06",
        "code_verdict": "✅ 并发锁+幂等+冷启动异常均已实现",
        "test_verdict": "⚠️ 并发409已测；幂等和冷启动无测试",
        "gaps": ["幂等(同日已完成→直接返回)无专项测试", "冷启动COLD_START_ERROR无测试"]
    },

    # ═══ FR-02 定时调度 ═══
    "FR02-SCHED-01": {
        "spec_requirement": "调度状态查询：GET /admin/scheduler/status→近7天记录按triggered_at desc；status∈{PENDING,WAITING_UPSTREAM,RUNNING,SUCCESS,FAILED,SKIPPED}",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr02_schedule_execution验证",
        "gaps": []
    },
    "FR02-SCHED-02": {
        "spec_requirement": "DAG重触发+依赖传播：FR-06等FR-04完成事件，FR-07/08等FR-06完成；DAG全局超时08:30自解挂起",
        "code_verdict": "✅ dag_scheduler.py完整实现DAG_DEPENDENCIES+check_upstream_ready+enforce_cascade_timeout",
        "test_verdict": "🔴 测试完全不测DAG依赖传播——仅覆盖scheduler_ops_ssot.py(CRUD层)，dag_scheduler.py核心引擎零测试覆盖",
        "gaps": ["DAG事件传播/上游等待/阻塞逻辑零测试", "enforce_cascade_timeout零测试", "测试仅检查读已有数据而非测超时执行逻辑"]
    },
    "FR02-SCHED-03": {
        "spec_requirement": "交易日历3级回退：通达信vipdoc→DB kline CSV→工作日启发式",
        "code_verdict": "⚠️ 实现为2级合并(TDX∪DB)+1级回退(weekday)，非严格3级优先",
        "test_verdict": "🔴 无测试验证回退链",
        "gaps": ["交易日历回退链未按SSOT优先级实现", "零测试覆盖"]
    },
    "FR02-SCHED-04": {
        "spec_requirement": "分布式锁：fencing token+TTL=300s+心跳续租；多实例互斥；跨日等待支持",
        "code_verdict": "✅ dag_scheduler.py完整实现try_acquire_lock+heartbeat_lock+fencing token",
        "test_verdict": "🔴 零测试覆盖",
        "gaps": ["分布式锁/fencing token/heartbeat零测试", "多实例互斥未在SQLite下验证", "跨日等待无测试"]
    },
    "FR02-SCHED-05": {
        "spec_requirement": "内部指标汇总接口 GET /internal/metrics/summary",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ 无专项测试",
        "gaps": ["无指标汇总接口专项测试"]
    },

    # ═══ FR-03 Cookie与会话管理 ═══
    "FR03-COOKIE-01": {
        "spec_requirement": "Cookie上传/更新：POST /admin/cookie-session(body:{login_source,cookie_string})→201；login_source∈{weibo,douyin,xueqiu,kuaishou}",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 验证完整",
        "gaps": []
    },
    "FR03-COOKIE-02": {
        "spec_requirement": "Cookie健康探测：每5分钟自动探测+分布式探活互斥锁；并发→仅1获锁其余skipped；连续失败≥2→NFR-13告警",
        "code_verdict": "🔴 自动5分钟探测未实现(无cron注册)；分布式互斥锁未实现；连续失败告警未实现",
        "test_verdict": "🔴 零测试覆盖",
        "gaps": ["5分钟自动探测未实现(无cron/scheduler注册)", "探测分布式互斥锁未实现", "并发→skipped未实现", "连续失败≥2→NFR-13告警未实现"]
    },
    "FR03-COOKIE-03": {
        "spec_requirement": "内部Cookie刷新：POST /internal/cookie/refresh；status∈{ACTIVE,EXPIRING,EXPIRED,REFRESH_FAILED,SKIPPED}",
        "code_verdict": "🔴 EXPIRING和EXPIRED两个状态无任何代码触发转换(枚举定义了但无业务逻辑设置)",
        "test_verdict": "🔴 仅覆盖ACTIVE，EXPIRING/EXPIRED转换零测试",
        "gaps": ["EXPIRING状态(接近过期)无触发逻辑", "EXPIRED状态(过期)无触发逻辑", "仅ACTIVE/REFRESH_FAILED/SKIPPED三种实际可达"]
    },
    "FR03-COOKIE-04": {
        "spec_requirement": "TTL管理：weibo=24h,xueqiu=48h,douyin=24h,kuaishou=24h；Cookie明文不得出现在日志/前端",
        "code_verdict": "✅ TTL配置正确+快照脱敏",
        "test_verdict": "⚠️ 仅测weibo=24h，未测xueqiu=48h等其他平台；脱敏已测",
        "gaps": ["xueqiu=48h(特殊值)未测", "douyin/kuaishou TTL未测"]
    },

    # ═══ FR-04 多源数据采集 ═══
    "FR04-INGEST-01": {
        "spec_requirement": "全量日线覆盖率≥95%，covered_count/total_stocks>=0.95",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 断言covered>=int(total*0.95)",
        "gaps": []
    },
    "FR04-INGEST-02": {
        "spec_requirement": "核心池覆盖：core_pool_covered_count==200",
        "code_verdict": "✅ T-1回退保证核心池覆盖",
        "test_verdict": "✅ 断言core_pool_covered_count==200",
        "gaps": []
    },
    "FR04-INGEST-03": {
        "spec_requirement": "热搜Top50：exactly 50项，rank连续唯一1..50；7平台优先级东方财富→雪球→财联社→百度热搜→微博→抖音→快手",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 断言len==50+rank==range(1,51)",
        "gaps": []
    },
    "FR04-INGEST-04": {
        "spec_requirement": "quality_flag∈{ok,stale_ok,missing,degraded}；≥3平台ok→ok，<3→stale_ok，全fail→degraded",
        "code_verdict": "✅ 3源规则正确实现",
        "test_verdict": "⚠️ 仅做枚举范围断言，缺少恰好2源→stale_ok和0源→degraded的独立断言",
        "gaps": ["缺少2源→stale_ok、0源→degraded的边界测试"]
    },
    "FR04-INGEST-05": {
        "spec_requirement": "熔断器：连续失败≥3→OPEN，300s冷却→HALF_OPEN探测；探测失败→重新冷却",
        "code_verdict": "✅ 全链路实现CLOSED→OPEN→HALF_OPEN→CLOSED/OPEN",
        "test_verdict": "✅ 完整覆盖",
        "gaps": []
    },
    "FR04-INGEST-06": {
        "spec_requirement": "Partial Commit：长尾股失败→错误记录+其余可查询，不整体回滚",
        "code_verdict": "✅ 逐只try/except+batch_status=PARTIAL_SUCCESS",
        "test_verdict": "✅ 10只失败→error_rows+core_pool正常",
        "gaps": []
    },
    "FR04-INGEST-07": {
        "spec_requirement": "北向/ETF：status∈{ok,missing,degraded}；缺失不阻断核心研报生成；report_data_usage血缘追溯",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 验证northbound/etf status+血缘追溯",
        "gaps": []
    },

    # ═══ FR-05 市场状态机 ═══
    "FR05-MKT-01": {
        "spec_requirement": "market_state∈{BULL,NEUTRAL,BEAR}；优先级BEAR>BULL>NEUTRAL",
        "code_verdict": "✅ 正确：if is_bear先于if is_bull",
        "test_verdict": "✅ test_fr05_bear_priority验证",
        "gaps": []
    },
    "FR05-MKT-02": {
        "spec_requirement": "BULL条件：hs300_ma20>ma20_5d_ago AND hs300_return_20d>0.03；BEAR条件：hs300_ma5<hs300_ma20 AND return_20d<-0.05；基于T-1收盘",
        "code_verdict": "✅ 阈值正确",
        "test_verdict": "⚠️ BULL条件有端到端测试；BEAR条件仅测了bool优先级未测阈值触发",
        "gaps": ["缺少用具体metrics触发BEAR的端到端测试(只测了bool组合)"]
    },
    "FR05-MKT-03": {
        "spec_requirement": "幽灵时段00:00-08:59:59→返回上一有效交易日缓存；绝对冷启动→NEUTRAL+COLD_START_FALLBACK",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ 冷启动已测；幽灵时段仅测了冷启动场景，无 '有缓存' 的幽灵时段返回历史缓存测试",
        "gaps": ["幽灵时段+有历史缓存→返回旧缓存的场景未测"]
    },
    "FR05-MKT-04": {
        "spec_requirement": "降级→NEUTRAL+state_reason含market_state_degraded=true",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 断言state_reason+degraded标记",
        "gaps": []
    },

    # ═══ FR-06 研报生成 ═══
    "FR06-RPT-01": {
        "spec_requirement": "recommendation∈{BUY,SELL,HOLD}；citations非空；幂等(同stock_code+trade_date→同report_id)",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr06_idempotency验证幂等+citations",
        "gaps": []
    },
    "FR06-RPT-02": {
        "spec_requirement": "LLM降级链primary→backup→cli→local→failed；llm_fallback_level字段追踪",
        "code_verdict": "✅ 降级链完整实现",
        "test_verdict": "⚠️ 仅做枚举范围断言，未测逐级降级流程(mock primary失败→backup触发)；测试走mock_llm直接到local",
        "gaps": ["无逐级降级E2E测试(primary fail→backup→cli…)", "测试实际走mock_llm路径绕过真实降级"]
    },
    "FR06-RPT-03": {
        "spec_requirement": "instruction_card必含6字段：signal_entry_price/atr_pct/atr_multiplier/stop_loss/target_price/stop_loss_calc_mode",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 断言6字段集合完全匹配",
        "gaps": []
    },
    "FR06-RPT-04": {
        "spec_requirement": "BUY+confidence≥0.65→sim_trade_instruction含10k/100k/500k三挡；1W价格×100>10000→SKIPPED(INSUFFICIENT_FUNDS)；10W position_ratio≤0.30",
        "code_verdict": "✅ 三挡+INSUFFICIENT_FUNDS逻辑正确",
        "test_verdict": "✅ 三挡Key验证+INSUFFICIENT_FUNDS断言；position_ratio无直接断言但值安全(100k=0.20)",
        "gaps": ["position_ratio≤0.30上限无直接断言"]
    },
    "FR06-RPT-05": {
        "spec_requirement": "strategy_type∈{A,B,C}规则引擎判定(非LLM)：A=事件标签命中(T+2,×1.5)；B=MA20向上+近5日涨>3%(T+3,×2.0)；C=ATR<2%+波动率后30%分位(T+5,×2.5)；atr_multiplier按A/B/C差异化",
        "code_verdict": "🔴 三处与SSOT不一致：(1)C判定用固定0.02而非市场后30%分位 (2)B条件缺少近5日涨>3%检查 (3)atr_multiplier硬编码1.5不区分A/B/C",
        "test_verdict": "🔴 无strategy_type判定专项测试",
        "gaps": ["C类：应为波动率后30%分位，实为固定阈值0.02", "B类：缺近5日涨幅>3%检查", "atr_multiplier硬编码1.5(SSOT要求A=1.5/B=2.0/C=2.5)", "无测试覆盖"]
    },
    "FR06-RPT-06": {
        "spec_requirement": "先验数据：已结算历史样本≥30→prompt含prior_stats+sample_count，data_cutoff<当月1日；<30→null",
        "code_verdict": "✅ 代码逻辑正确(sample<30→None, cutoff=月初)",
        "test_verdict": "🔴 SSOT明确要求3条pytest断言(≥30→存在, <30→null, cutoff<月初)全部缺失",
        "gaps": ["prior_stats存在/null/cutoff三个断言全缺"]
    },
    "FR06-RPT-07": {
        "spec_requirement": "停牌→不调LLM，recommendation=HOLD，skip_reason=SUSPENDED；并发→409",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr06_suspended_stock_skip_llm+test_fr06_concurrent_conflict_409",
        "gaps": []
    },
    "FR06-RPT-08": {
        "spec_requirement": "全局LLM熔断：连续N次失败→llm_circuit_open=true，Pending→Suspended批量转换，冷却探活，恢复/继续挂起",
        "code_verdict": "❌ 完全未实现：无llm_circuit_open字段/机制，无Pending→Suspended转换，无冷却探活",
        "test_verdict": "🔴 零测试",
        "gaps": ["全局LLM熔断完全未实现(CRITICAL)", "无Pending→Suspended批量转换", "无冷却探活恢复逻辑"]
    },
    "FR06-RPT-09": {
        "spec_requirement": "辩证审计：BUY+confidence≥0.65→副模型风险评估；成功→completed+高级区含'风险补充审计'章节；超时→skipped+skip_reason非空",
        "code_verdict": "❌ 完全未实现：无第二LLM端点调用，risk_audit_status只是字符串标记不是真实审计",
        "test_verdict": "🔴 零测试覆盖",
        "gaps": ["副模型调用未实现(CRITICAL)", "risk_audit_status为伪实现(只设字符串值)", "高级区无'风险补充审计'章节", "辩证审计E2E场景零测试"]
    },
    "FR06-RPT-10": {
        "spec_requirement": "BEAR防损：BEAR+B/C类→不调LLM，published=false，quality_flag=degraded",
        "code_verdict": "✅ 代码正确实现BEAR_MARKET_FILTERED",
        "test_verdict": "🔴 完全无测试覆盖",
        "gaps": ["SSOT验收明确要求mock BEAR+B类→断言不调LLM，测试完全缺失"]
    },
    "FR06-RPT-11": {
        "spec_requirement": "防参数倒挂：stop_loss>entry_price→所有挡位SKIPPED+logic_inversion_fallback",
        "code_verdict": "🔴 代码将倒挂修正为92%fallback继续执行，而非SSOT要求的全部SKIPPED",
        "test_verdict": "🔴 现有测试验证92%fallback（错误行为），未验证SKIPPED",
        "gaps": ["SSOT要求倒挂→全挡SKIPPED+logic_inversion_fallback", "代码错误地修正继续执行而非SKIPPED"]
    },
    "FR06-RPT-12": {
        "spec_requirement": "ATR缺失→stop_loss_calc_mode=fixed_92pct_fallback；僵尸任务仅T-1且72h内更新可恢复",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 92%fallback+僵尸任务Expired均已测",
        "gaps": []
    },

    # ═══ FR-07 预测结算与回灌 ═══
    "FR07-SETTLE-01": {
        "spec_requirement": "错误分类追溯率100%：每条结算结果含report_id/stock_code/strategy_type/settlement_status/entry_exit_date",
        "code_verdict": "✅ 字段完整",
        "test_verdict": "⚠️ test_fr07_settlement_traceable验证了字段存在，但未验证错误分类(pending/skipped/degraded)各条可追溯",
        "gaps": ["未测试各settlement_status分支的追溯性"]
    },
    "FR07-SETTLE-02": {
        "spec_requirement": "四维度统计(win_rate/profit_loss_ratio/alpha_annual/max_drawdown_pct)：样本≥30→非null，<30→null+display_hint='样本积累中'",
        "code_verdict": "🔴 display_hint使用英文'sample_lt_30'而非SSOT要求的中文'样本积累中'",
        "test_verdict": "🔴 无测试用例验证样本≥30→非null或<30→null场景",
        "gaps": ["display_hint值不符SSOT('sample_lt_30'→应为'样本积累中')", "四维度阈值判定场景零测试"]
    },
    "FR07-SETTLE-03": {
        "spec_requirement": "费用明细：佣金min 5元，印花税0.5‰，滑点0.05%(SSOT公式P_entry×shares×1+0.05%)",
        "code_verdict": "🔴 滑点率使用0.1%(0.001)，是SSOT要求0.05%的2倍",
        "test_verdict": "🔴 测试跟着错误值断言(buy_slippage==1.0应为0.5)",
        "gaps": ["滑点率0.001(0.1%)应为0.0005(0.05%)", "测试断言值也错误", "FR-08的_buy_cost/_sell_proceeds同样使用0.001"]
    },
    "FR07-SETTLE-04": {
        "spec_requirement": "win_rate口径：仅net_return_pct>0计胜，排除return=0样本(阈值abs<0.0001)",
        "code_verdict": "🔴 分母未排除return=0样本；未使用0.0001阈值(直接>0)",
        "test_verdict": "🔴 无win_rate口径测试",
        "gaps": ["分母应为count(>0)/count(!=0)而非count(>0)/total", "0.0001零收益阈值未实现", "profit_loss_ratio同样未排除return=0"]
    },
    "FR07-SETTLE-05": {
        "spec_requirement": "幂等：force=false+已settled→skipped_count>0；互斥锁：并发→仅1成功(202)另1返回409",
        "code_verdict": "⚠️ 幂等跳过逻辑正确但互斥非真正分布式锁(基于DB状态检查有竞态窗口)",
        "test_verdict": "🔴 force=false幂等无测试；并发互斥无测试",
        "gaps": ["force=false幂等跳过无测试", "非真正互斥锁(check-then-act有竞态)", "并发409场景无测试"]
    },
    "FR07-SETTLE-06": {
        "spec_requirement": "对照组：样本≥30→baseline_random+baseline_ma_cross非null，simulation_runs==500；<30→null",
        "code_verdict": "🔴 代码只检查空列表不检查len<30(5条也会返回baseline而非null)",
        "test_verdict": "⚠️ simulation_runs=500已测，但baseline伪实现(固定win_rate=0.55)而非真正500次蒙特卡洛",
        "gaps": ["baseline不检查sample<30阈值", "蒙特卡洛为伪实现(hardcoded)", "baseline_ma_cross未见独立实现"]
    },
    "FR07-SETTLE-07": {
        "spec_requirement": "信号有效性：AI跑输随机基线→signal_validity_warning:true；coverage_pct(已结算/总BUY)",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr07_signal_validity_warning+monkeypatch验证",
        "gaps": []
    },

    # ═══ FR-08 模拟实盘追踪 ═══
    "FR08-SIM-01": {
        "spec_requirement": "回撤熔断：drawdown≤-0.20→HALT(不新开仓)；-0.20<drawdown≤-0.12→REDUCE(仓位×0.5)；>-0.12→NORMAL",
        "code_verdict": "✅ DRAWDOWN_FACTOR_BY_STATE={REDUCE:0.5}+阈值正确",
        "test_verdict": "⚠️ HALT已测(test_fr08_halt_no_new_position)；REDUCE场景无测试",
        "gaps": ["REDUCE(仓位减半)场景无测试"]
    },
    "FR08-SIM-02": {
        "spec_requirement": "三挡并发：initial_capital=10k/100k/500k；最大持仓1W=2/10W=5/50W=10只",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 间接覆盖(通过monkeypatch截断测试)",
        "gaps": []
    },
    "FR08-SIM-03": {
        "spec_requirement": "E2E：开仓→平仓全覆盖；开仓价=T+1开盘(非signal_entry_price)",
        "code_verdict": "✅ 代码使用T+1开盘价",
        "test_verdict": "⚠️ 仅覆盖TAKE_PROFIT路径，缺STOP_LOSS/TIMEOUT/DELISTED的E2E链路",
        "gaps": ["STOP_LOSS E2E链路缺失", "DELISTED E2E链路缺失"]
    },
    "FR08-SIM-04": {
        "spec_requirement": "1W INSUFFICIENT_FUNDS→SKIPPED；碎股向下取整至100股；佣金≥5元",
        "code_verdict": "✅ raw_shares<100→SKIPPED，floor/100*100，max(…,5.0)",
        "test_verdict": "🔴 三项均无FR-08专项测试",
        "gaps": ["INSUFFICIENT_FUNDS无测试", "碎股取整无测试", "佣金≥5元无FR-08测试"]
    },
    "FR08-SIM-05": {
        "spec_requirement": "持仓≥180日→TIMEOUT平仓；T+1卖出限定",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr08_timeout_180d验证；T+1通过entry_date<trade_day条件",
        "gaps": []
    },
    "FR08-SIM-06": {
        "spec_requirement": "停牌/一字涨跌停/volume=0→保持OPEN+pending顺延；退市→DELISTED_LIQUIDATED(exit_price=0.0,net_return=-1.0)",
        "code_verdict": "🔴 退市处理完全未实现(无DELISTED检测逻辑)；涨跌停锁定(limit_locked)未实现",
        "test_verdict": "🔴 零测试覆盖",
        "gaps": ["DELISTED_LIQUIDATED未实现(CRITICAL)", "涨跌停锁定未实现", "停牌pending仅部分实现(开仓时简单skip不创建pending记录)"]
    },
    "FR08-SIM-07": {
        "spec_requirement": "除权复权：前复权动态折算adj_entry_price，按原始atr_pct重算dynamic_stop_loss/dynamic_target_price",
        "code_verdict": "❌ 完全未实现：代码直接使用冻结的stop_loss_price/target_price，无前复权动态折算",
        "test_verdict": "🔴 零测试",
        "gaps": ["前复权动态折算完全未实现(CRITICAL)", "10送10等除权事件会误触止损"]
    },
    "FR08-SIM-08": {
        "spec_requirement": "悲观撮合：High≥TP AND Low≤SL→强制STOP_LOSS；日内先平后开",
        "code_verdict": "✅ 悲观撮合代码正确；先平后开顺序正确",
        "test_verdict": "🔴 悲观撮合无测试；先平后开间接验证",
        "gaps": ["悲观撮合(High≥TP且Low≤SL→STOP_LOSS)无测试"]
    },
    "FR08-SIM-09": {
        "spec_requirement": "模拟看板页面/portfolio/sim-dashboard；3挡切换；多信号竞争满仓→SKIPPED；批次排序confidence优先",
        "code_verdict": "✅ 页面+切换+竞争逻辑正确",
        "test_verdict": "✅ 页面路由+confidence排序已测",
        "gaps": []
    },
    "FR08-SIM-10": {
        "spec_requirement": "模拟开仓超时阻断(DAG上游未完成)",
        "code_verdict": "✅ 依赖DAG事件驱动",
        "test_verdict": "⚠️ 间接通过DAG测试",
        "gaps": []
    },

    # ═══ FR-09 商业化与权益 ═══
    "FR09-AUTH-01": {
        "spec_requirement": "注册→201+data.email+tier=Free；未激活登录→401(EMAIL_NOT_VERIFIED)；OAuth(provider_user_id幂等，email可空)",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 注册/未激活/OAuth均已测",
        "gaps": []
    },
    "FR09-AUTH-02": {
        "spec_requirement": "refresh token轮转：60s宽限期内旧token→401但不登出；超60s→401+全设备登出",
        "code_verdict": "✅ REFRESH_GRACE_SECONDS=60+revoke_all逻辑正确",
        "test_verdict": "🔴 宽限期内已测(不登出)；超60s全设备登出完全无测试",
        "gaps": ["超宽限期重放→全设备登出无测试"]
    },
    "FR09-AUTH-03": {
        "spec_requirement": "forgot-password无论邮箱存在→200；reset-password token过期→400 RESET_TOKEN_EXPIRED；logout后旧access_token→被拦截",
        "code_verdict": "✅ 代码均已正确实现",
        "test_verdict": "🔴 三个端点零测试",
        "gaps": ["forgot-password→200无测试", "reset-password expired→400无测试", "logout→旧token拦截无测试"]
    },
    "FR09-AUTH-04": {
        "spec_requirement": "登录速率：5次失败→第6次429；10分钟后解锁；同IP或同email维度",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ IP维度429已测；10分钟解锁无测试；email维度无测试",
        "gaps": ["10分钟自动解锁无测试", "email维度速率限制无测试"]
    },
    "FR09-AUTH-05": {
        "spec_requirement": "Webhook原子事务：订单成功但权益写入失败→回滚+500；Webhook幂等：同event_id→仅首个发放；防掉单轮询",
        "code_verdict": "⚠️ 回滚逻辑简单(非真正try-rollback模式)；幂等通过查existing实现",
        "test_verdict": "🔴 原子回滚无测试；幂等无直接测试(仅测了reconcile)",
        "gaps": ["Webhook原子回滚无测试", "同event_id幂等无直接测试"]
    },
    "FR09-AUTH-06": {
        "spec_requirement": "定价：Pro 29.9/月 79.9/3月 299.9/年；Enterprise 99.9/月 269.9/3月 999.9/年；tier_id=Free→422",
        "code_verdict": "✅ 价格表正确",
        "test_verdict": "⚠️ tier_id=Free→422已测；具体价格值无测试",
        "gaps": ["定价金额无精确断言测试"]
    },
    "FR09-AUTH-07": {
        "spec_requirement": "管理员补单：admin补单→权益发放一次+审计+幂等",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ 间接通过FR-12 reconcile测试",
        "gaps": []
    },

    # ═══ FR-09-b 系统清理与归档 ═══
    "FR09B-CLN-01": {
        "spec_requirement": "过期会话(>7天)清理后=0；核心研报绝不删(清理前后report表记录不变)",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 完整覆盖",
        "gaps": []
    },
    "FR09B-CLN-02": {
        "spec_requirement": "清理期间并发POST /reports/generate不报DB lock错误；分批删除每批≤500行",
        "code_verdict": "✅ 分批500+批间sleep",
        "test_verdict": "✅ 并发安全已测",
        "gaps": []
    },
    "FR09B-CLN-03": {
        "spec_requirement": "过期临时Token/access_token_lease/未激活>24h账号清理",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 完整覆盖",
        "gaps": []
    },
    "FR09B-CLN-04": {
        "spec_requirement": "Pending/Suspended超3天→Expired；cleanup_log返回各计数字段",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 完整覆盖",
        "gaps": []
    },

    # ═══ FR-10 完整站点与看板 ═══
    "FR10-SITE-01": {
        "spec_requirement": "GET /api/v1/home→latest_reports≤5+market_state∈{BULL,NEUTRAL,BEAR}+池规模",
        "code_verdict": "✅ limit=5+fallback NEUTRAL",
        "test_verdict": "✅ test_fr10_home_api_fields验证",
        "gaps": []
    },
    "FR10-SITE-02": {
        "spec_requirement": "GET /dashboard/stats→default window_days=30，by_strategy_type含A/B/C全8字段；data_status∈{COMPUTING,DEGRADED}+status_reason",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 验证COMPUTING+status_reason",
        "gaps": []
    },
    "FR10-SITE-03": {
        "spec_requirement": "GET /portfolio/sim-dashboard→is_simulated_only=true+signal_validity_warning bool；equity_curve交易日连续无跳点",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr10_equity_curve_continuous验证",
        "gaps": []
    },
    "FR10-SITE-04": {
        "spec_requirement": "E2E各页面可访问无500：/ /reports /report/{id} /dashboard /portfolio/sim-dashboard",
        "code_verdict": "✅ 所有路由已注册",
        "test_verdict": "🔴 无E2E页面渲染测试(所有5个页面HTTP 200检查缺失)",
        "gaps": ["5个页面的E2E渲染测试全部缺失"]
    },
    "FR10-SITE-05": {
        "spec_requirement": "高级区：未登录→401；Free→≤200字符+省略标记(后端裁剪禁止LLM二次总结)；Pro→完整",
        "code_verdict": "✅ 代码正确(截断197+'...')",
        "test_verdict": "🔴 无测试覆盖401/Free截断/Pro完整三个场景",
        "gaps": ["advanced端点401/Free truncation/Pro full三场景零测试"]
    },
    "FR10-SITE-06": {
        "spec_requirement": "Free→instruction_card价格脱敏'¥**.**'+sim_trade_instruction=null；Pro→float",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr10_report_detail_masks_free_fields+pro测试",
        "gaps": []
    },
    "FR10-SITE-07": {
        "spec_requirement": "term_context为dict且ATR含具体数值",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 断言isinstance(dict)+ATR含百分比",
        "gaps": []
    },
    "FR10-SITE-08": {
        "spec_requirement": "Dashboard三线图(AI+random+MA baseline)；AI跑输random→前端显示⚠️告警",
        "code_verdict": "⚠️ 后端3条数据线正确；前端AI<random时的⚠️告警逻辑缺失",
        "test_verdict": "⚠️ 后端数据已测；前端告警逻辑无测试",
        "gaps": ["前端无AI跑输random→⚠️告警的JS/模板逻辑"]
    },
    "FR10-SITE-09": {
        "spec_requirement": "骨架屏：数据未就绪→'数据结算中/生成中'提示，禁止留白/空对象/抛500",
        "code_verdict": "⚠️ 降级提示存在(非标准skeleton loading占位效果)",
        "test_verdict": "⚠️ data_status=COMPUTING状态已测；前端渲染效果无测试",
        "gaps": ["非标准骨架屏效果"]
    },
    "FR10-FEATURE-01": {
        "spec_requirement": "功能地图页/features：FR分组折叠+状态色点+API/参数/示例/返回值/测试节点/SSOT锚点",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_features_page验证",
        "gaps": []
    },
    "FR10-PAGE-01": {
        "spec_requirement": "研报详情页 /reports/{id}：降级横幅+'⚠️当前研报包含降级数据...'",
        "code_verdict": "⚠️ _build_degraded_banner函数存在但前端渲染未验证",
        "test_verdict": "🔴 无测试验证降级横幅在API响应/页面中的展现",
        "gaps": ["降级横幅展示无测试"]
    },
    "FR10-PAGE-02": {
        "spec_requirement": "研报列表 /reports：默认过滤BEAR短路published=false的研报",
        "code_verdict": "✅ 查询条件过滤正确",
        "test_verdict": "⚠️ 无专项测试",
        "gaps": ["列表默认过滤逻辑无测试"]
    },
    "FR10-PAGE-03": {
        "spec_requirement": "模拟看板 /portfolio/sim-dashboard：支持?capital_tier切换挡位",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 已测",
        "gaps": []
    },

    # ═══ FR-11 用户反馈（全部通过）═══
    "FR11-FB-01": {
        "spec_requirement": "未登录→401；登录POST→200+DB有记录；仅接受{report_id,feedback_type}含多余字段→422",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 完整覆盖",
        "gaps": []
    },
    "FR11-FB-02": {
        "spec_requirement": "negative≥3→PENDING_REVIEW→触发REPORT_PENDING_REVIEW事件；去重(同用户重复negative→200但count不增)；positive→仅记录",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr11_negative_dedup完整覆盖4个场景",
        "gaps": []
    },

    # ═══ FR-12 管理员后台 ═══
    "FR12-ADMIN-01": {
        "spec_requirement": "非admin→403；GET admin/users→items/total分页",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ 403已测；admin/users端点无专项测试",
        "gaps": ["GET admin/users无专项测试"]
    },
    "FR12-ADMIN-02": {
        "spec_requirement": "PATCH admin/reports/{id}→review_flag+published修改+审计记录",
        "code_verdict": "✅ 正确+双表审计",
        "test_verdict": "🔴 PATCH review_flag+audit无测试",
        "gaps": ["PATCH review_flag完整流程无测试"]
    },
    "FR12-ADMIN-03": {
        "spec_requirement": "GET admin/overview→pool_size(int)+active_positions(Dict keys=10k/100k/500k)",
        "code_verdict": "✅ 动态从config读取tier_keys",
        "test_verdict": "✅ test_fr12_overview_min_fields完整验证",
        "gaps": []
    },
    "FR12-ADMIN-04": {
        "spec_requirement": "force-regenerate：非super_admin→403；super_admin→旧软删+新report_id+审计；级联阻断(sim_position引用→409)",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ 403/成功/级联409三场景全覆盖",
        "gaps": []
    },
    "FR12-ADMIN-05": {
        "spec_requirement": "补单reconcile→权益发放+幂等+审计",
        "code_verdict": "✅ 正确",
        "test_verdict": "✅ test_fr12_billing_reconcile_idempotent",
        "gaps": []
    },
    "FR12-ADMIN-06": {
        "spec_requirement": "用户角色/权益管理PATCH admin/users/{id}",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ 无专项测试",
        "gaps": ["PATCH admin/users/{id}无测试"]
    },
    "FR12-ADMIN-07": {
        "spec_requirement": "研报列表(review_flag筛选) GET admin/reports",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ 无专项测试",
        "gaps": ["GET admin/reports筛选无测试"]
    },
    "FR12-ADMIN-08": {
        "spec_requirement": "所有PATCH须产生审计记录(actor_user_id/before/after/timestamp/request_id)",
        "code_verdict": "✅ create_audit_log统一写入",
        "test_verdict": "⚠️ FR-00审计已测，但admin用户/报告PATCH审计无独立测试",
        "gaps": ["admin层PATCH审计无独立测试"]
    },

    # ═══ FR-13 业务事件推送 ═══
    "FR13-EVENT-01": {
        "spec_requirement": "POSITION_CLOSED：平仓(TAKE_PROFIT/STOP_LOSS/TIMEOUT/DELISTED)→notification；DELISTED时exit_price=0.0",
        "code_verdict": "✅ 正确(DELISTED作为position_status放入payload)",
        "test_verdict": "⚠️ TAKE_PROFIT已测；DELISTED+exit_price=0.0无测试",
        "gaps": ["DELISTED_LIQUIDATED+exit_price=0.0场景无测试"]
    },
    "FR13-EVENT-02": {
        "spec_requirement": "幂等：同一事件二次触发→第二条status=skipped",
        "code_verdict": "✅ 去重键检查",
        "test_verdict": "✅ test_fr13_notification_idempotent",
        "gaps": []
    },
    "FR13-EVENT-03": {
        "spec_requirement": "DRAWDOWN_ALERT：同capital_tier 4h内仅首条sent，后续skipped",
        "code_verdict": "✅ DRAWDOWN_SUPPRESS_HOURS=4",
        "test_verdict": "✅ test_fr13_drawdown_alert_suppression",
        "gaps": []
    },
    "FR13-EVENT-04": {
        "spec_requirement": "推送失败不阻塞结算：Webhook超时→FR-08仍正常，notification.status=failed；事务回滚→无sent记录",
        "code_verdict": "✅ Outbox模式隔离+回滚安全",
        "test_verdict": "⚠️ 回滚已测(test_fr13_txn_rollback_no_send)；推送失败不阻塞无测试",
        "gaps": ["推送失败不阻塞结算场景无测试"]
    },
    "FR13-EVENT-05": {
        "spec_requirement": "BUY_SIGNAL_DAILY→confidence倒序≤5条；signal=0→不创建事件；user_email_enabled=false→skipped；REPORT_PENDING_REVIEW仅admin通道",
        "code_verdict": "✅ 正确",
        "test_verdict": "⚠️ ≤5已测+空信号已测；user_email=false无测试",
        "gaps": ["user_email_enabled=false→skipped无测试"]
    },
}

def main():
    with open(REG_PATH, "r", encoding="utf-8") as f:
        registry = json.load(f)

    updated = 0
    for feat in registry["features"]:
        fid = feat["feature_id"]
        if fid in AUDIT:
            a = AUDIT[fid]
            feat["spec_requirement"] = a["spec_requirement"]
            feat["code_verdict"] = a["code_verdict"]
            feat["test_verdict"] = a["test_verdict"]
            feat["page_verdict"] = a.get("page_verdict")
            feat["gaps"] = a["gaps"]
            updated += 1

    registry["version"] = "2.0.0"

    with open(REG_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)

    print(f"[OK] 更新 {updated}/{len(registry['features'])} 条功能点审计数据")

    # 统计
    total_gaps = sum(len(a["gaps"]) for a in AUDIT.values())
    critical = sum(1 for a in AUDIT.values() if "❌" in a.get("code_verdict", "") or "CRITICAL" in str(a.get("gaps", [])))
    red_code = sum(1 for a in AUDIT.values() if "🔴" in a.get("code_verdict", ""))
    red_test = sum(1 for a in AUDIT.values() if "🔴" in a.get("test_verdict", ""))
    print(f"[STATS] 总差距项={total_gaps}, 代码未实现(❌)={critical}, 代码问题(🔴)={red_code}, 测试缺失(🔴)={red_test}")

if __name__ == "__main__":
    main()
