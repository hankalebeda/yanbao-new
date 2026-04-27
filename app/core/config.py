from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "A-Share Research Platform"
    api_prefix: str = "/api/v1"
    ollama_base_url: str = "http://127.0.0.1:11434"
    ollama_model_test: str = "qwen3:8b"   # 文档 SSOT：多模型路由设计.md §2
    ollama_model_prod: str = "qwen3:8b"   # 本地兜底统一使用 qwen3:8b（见 §2.3）
    use_test_model: bool = True
    database_url: str = "sqlite:///./data/app.db"
    request_timeout_seconds: int = 60
    max_llm_retries: int = 2
    enable_scheduler: bool = True
    scheduler_job_timeout_seconds: int = 300
    scheduler_retry_count: int = 2
    scheduler_backoff_base_seconds: int = 2
    dag_cascade_timeout_before_open: str = "08:30"
    mock_llm: bool = False
    stock_pool: str = "600519.SH,000001.SZ,300750.SZ"  # 研报生成股票池（逗号分隔），前50=Tier-1，其余=Tier-2
    tier2_batch_mod: int = 5    # Tier-2 轮转分组数（5 日覆盖一轮）
    tier2_batch_size: int = 50  # Tier-2 当日批次（250÷5），03 §8.2
    tier2_signal_gate_top_n: int = 20  # Tier-2 升级 API 的 Top-N，13 §3.2
    hourly_collect_top_n: int = 50
    market_provider_order: str = "eastmoney,tdx"
    source_fail_open_threshold: int = 3
    source_recover_success_threshold: int = 2
    source_circuit_cooldown_seconds: int = 300
    internal_api_key: str = ""
    jwt_secret: str = ""
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_hours: int = 12
    membership_free_trial_days: int = 7  # 17 §6 注册赠送会员天数（0=关闭）
    audit_log_enabled: bool = True
    strict_real_data: bool = True
    enable_browser_fallback: bool = True
    browser_fallback_timeout_seconds: int = 20
    hotspot_relevance_threshold: float = 0.6
    hotspot_max_age_hours: int = 72
    hotspot_realtime_top_n: int = 20
    hotspot_min_related_topics: int = 5
    trusted_hosts: str = "127.0.0.1,localhost,testserver,*"
    expose_error_details: bool = False

    # ---- Gemini Web（Playwright 网页端）----
    gemini_chrome_user_data: str = ""   # 留空则自动使用当前用户 Chrome Default 目录
    gemini_chrome_profile: str = "Default"
    gemini_max_concurrency: int = 5     # 最大并发标签数

    # ---- ChatGPT Web（Playwright 网页端）----
    chatgpt_chrome_user_data: str = ""  # 留空则自动使用当前用户 Chrome Default 目录
    chatgpt_chrome_profile: str = "Default"
    chatgpt_max_concurrency: int = 5
    chatgpt_require_5x: bool = True

    # ---- Qwen Web (Playwright) ----
    qwen_chrome_user_data: str = ""
    qwen_chrome_profile: str = "Default"
    qwen_max_concurrency: int = 5
    qwen_service_user_data: str = ""
    qwen_proxy_url: str = ""
    qwen_force_no_proxy: bool = True
    qwen_cdp_url: str = ""
    qwen_cdp_hide_window: bool = True

    # ---- DeepSeek Web（Playwright 网页端）----
    deepseek_chrome_user_data: str = ""
    deepseek_chrome_profile: str = "Default"
    deepseek_chrome_max_concurrency: int = 5
    deepseek_chrome_service_user_data: str = ""
    deepseek_chrome_force_no_proxy: bool = True
    deepseek_edge_user_data: str = ""   # 留空则自动使用当前用户 Edge Default 目录
    deepseek_edge_profile: str = "Default"
    deepseek_max_concurrency: int = 5
    # 可选：指定“服务专用”的 Edge User Data 目录（避免每次从本机 Default 拷贝）
    # 适合长跑/重启后继续复用登录态；留空则走拷贝 profile 的兼容路径
    deepseek_edge_service_user_data: str = ""
    # 代理 URL，例如 "http://127.0.0.1:7890" 或 "socks5://127.0.0.1:7890"
    # 留空则依赖系统环境变量 HTTPS_PROXY / HTTP_PROXY
    deepseek_proxy_url: str = ""

    # ---- Gemini LLM API ----
    gemini_api_key: str = ""
    # 代理 URL，例如 "http://127.0.0.1:7890" 或 "socks5://127.0.0.1:7890"
    # 留空则依赖系统环境变量 HTTPS_PROXY / HTTP_PROXY
    gemini_proxy_url: str = ""
    gemini_model_test: str = "gemini-2.0-flash"
    gemini_model_prod: str = "gemini-2.0-flash"
    # "ollama" | "gemini" | "router" — 切换 LLM 引擎；router=场景路由模式
    llm_backend: str = "ollama"

    # ---- DeepSeek 官方 API ----
    deepseek_api_key: str = ""
    deepseek_api_base_url: str = "https://api.deepseek.com/v1"
    deepseek_api_model: str = "deepseek-chat"   # deepseek-chat = DeepSeek-V3

    # ---- 场景路由器配置 ----
    # 活跃主链统一收口到 NewAPI provider pool（codex_api）→ ollama 兜底。
    router_primary: str = "codex_api"          # 主力：研报生成 / 公告 / 情绪 / 风险
    router_longctx: str = "codex_api"          # 长上下文优先仍走同一 provider pool
    router_bulk: str = "ollama"                # 批量初筛/低优先级
    router_confidence_threshold: float = 0.55   # 低于此置信度触发多模型投票
    router_max_context_tokens: int = 8000       # 超过此值切换长上下文模型
    codex_api_parallel_enabled: bool = True     # 同池并发竞速：首个成功结果胜出
    codex_api_parallel_max_providers: int = 6   # 并发 provider 上限（12路relay×6渠道可支撑6并发竞速）
    report_batch_max_concurrent: int = 6        # 批量研报生成最大并发数（与 parallel_max_providers 协同）

    forecast_target_accuracy: float = 0.55  # 商业底线：真实胜率≥55%（见系统目标与范围整合 §3）

    # ---- 冷启动期展示（05 §7.5a）----
    # 约需交易日公式 M = ceil((30 - N) / daily_avg)；daily_avg 可用本配置或近7日实际日均 BUY 强信号笔数
    cold_start_daily_signal_avg: float = 1.5   # 冷启动期「约需M个交易日」估算用的日均 BUY 强信号笔数（经验值）

    # ---- 实操指令卡触发门槛 ----
    # 置信度映射最高"高"=0.72，"中高"=0.65，故门槛定为0.65可让"高/中高"信号均触发。
    # 旧值0.85已废弃（与_conf_map最高值0.78不兼容，导致BUY信号永远无法触发指令卡）。
    sim_instruction_confidence_threshold: float = 0.65  # BUY强信号生成实操指令卡的置信度下限

    # ---- 账户级最大回撤熔断 ----
    max_drawdown_halt_threshold: float = -0.20   # 账户回撤达到 -20% 触发全面暂停开仓
    max_drawdown_reduce_threshold: float = -0.12  # 账户回撤达到 -12% 触发减仓（新开仓仓位×0.5）
    drawdown_halt_review_days: int = 3            # HALT状态持续N交易日后自动解除（等待人工复审）

    # ---- 告警通知（见 06_SLO §5.2） ----
    alert_webhook_url: str = ""           # 企业微信/钉钉 机器人 Webhook URL（留空=仅写日志）
    alert_webhook_enabled: bool = False   # 是否启用 Webhook 推送
    alert_s0_cooldown_minutes: int = 30   # S0 告警静默窗口（分钟，防重复推送）
    alert_email: str = ""                 # 备用邮件告警（仅 S0 级）
    forecast_min_samples: int = 10
    forecast_min_coverage: float = 0.03
    forecast_history_days: int = 365
    quality_gate_min_score: float = 60.0
    daily_idem_rebuild_after_minutes: int = 120

    # ---- 通达信本地数据（历史因子预计算依赖）----
    # 重要：本系统"历史信号先验"（近90日胜率/ATR分位/量比分位）依赖通达信本地 .day 文件。
    # 用户需先安装通达信金融终端并完成数据同步（每交易日盘后同步一次）。
    # Windows 常见路径：C:\new_tdx 或 C:\TDX 或 D:\new_tdx
    # 若留空，系统将仅使用 mootdx 网络模式（实时报价可用，历史因子缺失）。
    tdx_install_dir: str = ""    # 例：C:\new_tdx
    # 历史因子计算使用的最大交易日窗口（建议>=252，即约1年）
    tdx_history_days: int = 252
    # 历史因子缺失时的先验填充值（该值代表"略优于随机"的基准胜率）
    tdx_fallback_win_rate: float = 0.52

    # ---- 行情数据采集速率控制 ----
    # 东方财富接口虽无需 API Key，但 IP 级有频率限制（约每秒5~10次）
    # 当监控股票池扩大到Tier-2（250只）时，批量采集需控制速率避免429/封禁
    market_data_request_delay_ms: int = 200   # 每只股票请求间隔（毫秒），0=不限速
    market_data_max_concurrency: int = 5      # 最大并发采集数

    # ---- 社交舆情数据 Cookie 配置 ----
    # 微博：登录后从浏览器开发者工具 Network 中获取 Cookie 字段值
    # 所需字段：SUB（必填），SUBP（可选，增强稳定性）
    weibo_cookie_sub: str = ""     # 微博 Cookie: SUB=xxx
    weibo_cookie_subp: str = ""    # 微博 Cookie: SUBP=xxx（可选）
    # 抖音：主链需要 Cookie（ms_token/ttwid），备链通过 Playwright Chrome Profile 复用
    # 建议使用备链（enable_browser_fallback=True），主链因反爬措施不稳定
    douyin_cookie: str = ""        # 抖音 Cookie（主链，可选，备链无需）

    # ---- 沪深300及上证指数数据 ----
    # 用于：① Alpha计算基准 ② 市场状态机（BULL/NEUTRAL/BEAR）判断
    # 数据来源：东方财富市场数据接口（无需Key）
    # URL格式：https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.000001&klt=101&...
    hs300_code: str = "000300"     # 沪深300指数代码
    sh_index_code: str = "000001"  # 上证指数代码（用于MA5/MA20计算）
    # 指数行情采集间隔（每日收盘后采集，与股票行情同步）

    # ---- 模拟结算任务 ----
    sim_settle_timeout_seconds: int = 1800   # 日度结算任务超时（秒，默认30分钟）

    # ---- 用户通知 ----
    notification_enabled: bool = False       # 是否开启用户侧通知
    admin_alert_webhook_url: str = ""        # 管理员专用告警Webhook（可与alert_webhook_url不同）

    # ---- E8.5 策略失效监测（12 §10.2）----
    strategy_failure_alert_enabled: bool = True   # 是否启用策略失效告警
    strategy_failure_rolling_window: int = 20     # 滚动窗口笔数
    strategy_failure_continuous_loss: int = 10    # 连续净亏笔数触发暂停
    strategy_failure_auto_pause: bool = False     # 默认仅告警，不自动暂停

    # ---- 前端/平台配置（供 /platform/config 使用）----
    capital_tiers: str = '{"10k":{"label":"1 万档","amount":10000},"100k":{"label":"10 万档","amount":100000},"500k":{"label":"50 万档","amount":500000}}'
    stock_aliases: str = '{"600519":["贵州茅台","茅台","白酒","酱香"],"000001":["平安银行","银行","金融"],"300750":["宁德时代","动力电池","锂电"]}'
    stock_industry_keywords: str = '{"600519":["白酒","消费","酱香","高端白酒"],"000001":["银行","信贷","金融"],"300750":["锂电","动力电池","新能源车"]}'

    # ---- Codex / Mesh Runner ----
    codex_canonical_provider: str = "openai"

    # ---- OAuth (17 §2.1a) ----
    oauth_callback_base: str = ""
    qq_app_id: str = ""
    qq_app_key: str = ""
    wechat_app_id: str = ""
    wechat_app_secret: str = ""
    # JWT refresh (17 §3)
    jwt_refresh_token_expire_days: int = 7

    # ---- Codex API ----
    codex_api_key: str = ""
    codex_api_base_url: str = ""
    codex_api_model: str = "gpt-5.4"
    codex_api_fallback_model: str = ""
    codex_api_reasoning_effort: str = "high"
    codex_api_timeout_seconds: float = 300.0
    promote_prep_gateway_timeout_seconds: float = 180.0
    codex_wire_api: str = "responses"
    codex_audit_gateway_only: bool = False
    codex_provider_failure_cooldown_seconds: int = 300
    codex_provider_root: str = ""

    # ---- LLM Audit ----
    llm_audit_enabled: bool = False
    llm_audit_provider: str = ""
    llm_audit_fallback_chain: str = ""

    # ---- User Email ----
    user_email_enabled: bool = False
    user_email_from_address: str = ""
    user_email_from_name: str = ""
    user_email_smtp_host: str = ""

    # ---- Feedback ----
    feedback_negative_daily_limit: int = 20

    # ---- Billing ----
    enable_mock_billing: bool = False
    billing_webhook_secret: str = ""
    alipay_app_id: str = ""
    alipay_gateway_url: str = ""
    wechat_pay_app_id: str = ""
    wechat_pay_gateway_url: str = ""

    # ---- Internal Cron ----
    internal_cron_token: str = ""

    # ---- Report Generation ----
    report_generation_llm_timeout_seconds: int = 120
    report_generation_active_task_stale_seconds: int = 1800
    llm_circuit_cooldown_seconds: int = 300
    market_state_fetch_timeout_seconds: int = 10
    market_state_write_timeout_seconds: int = 3

    # ---- Autonomy Loop ----
    autonomy_loop_enabled: bool = False
    autonomy_loop_mode: str = "monitor"
    autonomy_loop_fix_goal: int = 10
    autonomy_loop_audit_interval_seconds: int = 300
    autonomy_loop_monitor_interval_seconds: int = 1800
    autonomy_loop_lease_seconds: int = 600
    autonomy_loop_heartbeat_seconds: int = 30

    # ---- Debug ----
    debug: bool = False

    @field_validator("debug", mode="before")
    @classmethod
    def _normalize_debug(cls, v: object) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.lower() in ("true", "1", "yes", "on")
        return bool(v)

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    def validate_runtime_security(self) -> None:
        """Raise RuntimeError if critical security settings are missing or insecure."""
        if not self.jwt_secret:
            raise RuntimeError("JWT_SECRET must be set for production")
        if not self.billing_webhook_secret:
            raise RuntimeError("BILLING_WEBHOOK_SECRET must be set for production")
        if self.billing_webhook_secret == "dev-billing-secret":
            raise RuntimeError("BILLING_WEBHOOK_SECRET uses legacy development default")
        if self.trusted_hosts and "*" in [h.strip() for h in self.trusted_hosts.split(",")]:
            raise RuntimeError("TRUSTED_HOSTS must not contain wildcard '*' in production")


settings = Settings()
