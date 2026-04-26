from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import JSON, Boolean, CheckConstraint, Column, Date, DateTime, Float, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import synonym

from app.core.db import Base


def utc_now():
    return datetime.now(timezone.utc)


class OauthAccount(Base):
    """OAuth 第三方登录绑定 (17 §1.2)：provider+open_id → user_id (legacy table)"""

    __tablename__ = "oauth_account"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, index=True)
    provider = Column(String(16), nullable=False)
    open_id = Column(String(128), nullable=False)
    union_id = Column(String(128), nullable=True)
    created_at = Column(DateTime, default=utc_now)

    __table_args__ = (UniqueConstraint("provider", "open_id", name="uq_oauth_provider_openid"),)


class PasswordResetToken(Base):
    """忘记密码重置 Token (17 找回流程)"""

    __tablename__ = "password_reset_token"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, index=True)
    token_hash = Column(String(256), nullable=False, unique=True, index=True)
    expires_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utc_now)


class User(Base):
    """用户表 (17_用户系统设计 §1.1, E6)；支持邮箱或手机号注册。映射至 app_user 表。"""
    __tablename__ = "app_user"

    user_id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    email = Column(String(256), unique=True, nullable=True, index=True)
    phone = Column(String(32), unique=True, nullable=True, index=True)  # 手机号，与email二选一
    password_hash = Column(String(256), nullable=True)
    nickname = Column(String(64), nullable=True)
    role = Column(String(16), default="user", nullable=False)  # user | admin
    tier = Column(String(16), default="Free", nullable=False)  # Free | Pro | Enterprise
    tier_expires_at = Column(DateTime, nullable=True)
    membership_level = Column(String(16), default="free", nullable=True)  # free | monthly | annual
    membership_expires_at = Column(DateTime, nullable=True)  # NULL=免费用户
    email_verified = Column(Boolean, default=False, nullable=False)
    last_login_at = Column(DateTime, nullable=True)
    locked_until = Column(DateTime, nullable=True)
    failed_login_count = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)

    # Backward compatibility properties
    @property
    def id(self):
        return self.user_id

    @property
    def lockout_until(self):
        return self.locked_until

    @property
    def login_attempt_count(self):
        return self.failed_login_count


class HotspotRaw(Base):
    __tablename__ = "hotspot_raw"

    id = Column(Integer, primary_key=True, index=True)
    platform = Column(String(16), index=True, nullable=False)
    rank = Column(Integer, nullable=False)
    title = Column(String(512), nullable=False)
    raw_heat = Column(String(64), nullable=True)
    fetch_time = Column(DateTime, default=utc_now, nullable=False)
    source_url = Column(String(1024), nullable=True)
    cookie_version = Column(String(64), nullable=True)


class HotspotNormalized(Base):
    __tablename__ = "hotspot_normalized"

    topic_id = Column(String(64), primary_key=True)
    canonical_topic = Column(String(512), nullable=False)
    heat_score = Column(Float, default=0)
    sentiment_score = Column(Float, default=0)
    event_type = Column(String(64), nullable=False)
    decay_weight = Column(Float, default=1)
    created_at = Column(DateTime, default=utc_now)


class HotspotStockLink(Base):
    __tablename__ = "hotspot_stock_link"

    id = Column(Integer, primary_key=True, index=True)
    topic_id = Column(String(64), index=True, nullable=False)
    stock_code = Column(String(16), index=True, nullable=False)
    relevance_score = Column(Float, default=0)
    match_method = Column(String(64), nullable=False)


class CookieSession(Base):
    __tablename__ = "cookie_session"

    cookie_session_id = Column(String(36), primary_key=True)
    provider = Column(String(16), nullable=False, index=True)
    platform = synonym("provider")
    account_key = Column(String(128), nullable=True)
    status = Column(String(32), default="ACTIVE")
    cookie_blob = Column(Text, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    refresh_at = Column(DateTime, nullable=True)
    last_probe_at = Column(DateTime, nullable=True)
    last_refresh_at = Column(DateTime, nullable=True)
    failure_count = Column(Integer, default=0)
    status_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)

    __table_args__ = (
        CheckConstraint(
            "status IN ('ACTIVE', 'EXPIRING', 'EXPIRED', 'REFRESH_FAILED', 'SKIPPED')",
            name="ck_cookie_session_status_enum",
        ),
    )


class Report(Base):
    __tablename__ = "report"

    report_id = Column(String(64), primary_key=True)
    generation_task_id = Column(String(36), nullable=True)
    stock_code = Column(String(16), index=True, nullable=False)
    stock_name_snapshot = Column(String(64), nullable=True)
    pool_version = Column(Integer, nullable=True)
    idempotency_key = Column(String(64), nullable=True)
    generation_seq = Column(Integer, nullable=True)
    run_mode = Column(String(16), default="hourly")
    source = Column(String(16), default="real", nullable=True)
    published = Column(Boolean, nullable=True)
    publish_status = Column(String(32), nullable=True)
    published_at = Column(DateTime, nullable=True)
    recommendation = Column(String(8), nullable=False)
    confidence = Column(Float, default=0)
    quality_flag = Column(String(16), nullable=True)
    status_reason = Column(Text, nullable=True)
    llm_fallback_level = Column(String(16), nullable=True)
    strategy_type = Column(String(32), nullable=True)
    market_state = Column(String(32), nullable=True)
    market_state_reference_date = Column(Date, nullable=True)
    market_state_degraded = Column(Boolean, nullable=True)
    market_state_reason_snapshot = Column(Text, nullable=True)
    market_state_trade_date = Column(Date, nullable=True)
    conclusion_text = Column(Text, nullable=True)
    reasoning_chain_md = Column(Text, nullable=True)
    prior_stats_snapshot = Column(JSON, nullable=True)
    risk_audit_status = Column(String(32), nullable=True)
    risk_audit_skip_reason = Column(Text, nullable=True)
    review_flag = Column(String(16), nullable=True)
    failure_category = Column(String(32), nullable=True)
    negative_feedback_count = Column(Integer, default=0)
    reviewed_by = Column(String(64), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    is_deleted = Column(Boolean, default=False)
    deleted_at = Column(DateTime, nullable=True)
    superseded_by_report_id = Column(String(64), nullable=True)
    content_json = Column(JSON, nullable=True)
    # v26 P0: 真实命中的 LLM 模型 / 网关名 / 端点（用于审计 "是否真用 gpt-5.4"）
    llm_actual_model = Column(String(64), nullable=True)
    llm_provider_name = Column(String(64), nullable=True)
    llm_endpoint = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=utc_now)
    updated_at = Column(DateTime, nullable=True, default=utc_now, onupdate=utc_now)
    trade_date = Column(String(10), index=True, nullable=False)  # N-01: 防复发，不允许 NULL


class PredictionOutcome(Base):
    __tablename__ = "prediction_outcome"

    id = Column(Integer, primary_key=True, index=True)
    report_id = Column(String(64), index=True, nullable=False)
    stock_code = Column(String(16), index=True, nullable=False)
    window_days = Column(Integer, nullable=False)
    actual_result = Column(Float, nullable=True)
    is_correct = Column(Integer, nullable=True)
    error_type = Column(String(64), nullable=True)
    settled_at = Column(DateTime, nullable=True)


class ModelRunLog(Base):
    __tablename__ = "model_run_log"

    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(String(64), index=True)
    model_version = Column(String(64), nullable=False)
    prompt_version = Column(String(64), nullable=False)
    latency_ms = Column(Integer, nullable=False)
    token_in = Column(Integer, default=0)
    token_out = Column(Integer, default=0)
    status = Column(String(16), nullable=False)
    error_type = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=utc_now)


class ModelVersionRegistry(Base):
    __tablename__ = "model_version_registry"

    id = Column(Integer, primary_key=True, index=True)
    model_name = Column(String(64), nullable=False)
    version = Column(String(64), nullable=False)
    active = Column(Integer, default=0)
    created_at = Column(DateTime, default=utc_now)


class EnhancementExperiment(Base):
    __tablename__ = "enhancement_experiment"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(128), nullable=False)
    baseline_metrics = Column(JSON, nullable=False)
    candidate_metrics = Column(JSON, nullable=False)
    decision = Column(String(16), nullable=False)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=utc_now)


class MembershipOrder(Base):
    __tablename__ = "membership_order"

    order_id = Column(String(64), primary_key=True)
    user_id = Column(String(64), index=True, nullable=False)
    plan_code = Column(String(32), nullable=False)
    amount = Column(Float, nullable=False)
    status = Column(String(16), nullable=False, default="created")
    channel = Column(String(32), nullable=False, default="mock")
    created_at = Column(DateTime, default=utc_now)
    paid_at = Column(DateTime, nullable=True)


class MembershipSubscription(Base):
    __tablename__ = "membership_subscription"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String(64), index=True, nullable=False)
    plan_code = Column(String(32), nullable=False)
    status = Column(String(16), nullable=False, default="inactive")
    start_at = Column(DateTime, nullable=True)
    end_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=utc_now)


class ReportIdempotency(Base):
    __tablename__ = "report_idempotency"

    idempotency_key = Column(String(128), primary_key=True)
    stock_code = Column(String(16), index=True, nullable=False)
    run_mode = Column(String(16), nullable=False)
    report_id = Column(String(64), nullable=False)
    created_at = Column(DateTime, default=utc_now)


class SimPosition(Base):
    """模拟持仓记录 (FR-07)"""

    __tablename__ = "sim_position"

    position_id = Column(String(36), primary_key=True)
    report_id = Column(String(36), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    capital_tier = Column(String(16), nullable=False)
    position_status = Column(String(32), nullable=False)
    signal_date = Column(Date, nullable=True)
    entry_date = Column(Date, nullable=True)
    actual_entry_price = Column(Numeric(18, 4), nullable=True)
    signal_entry_price = Column(Numeric(18, 4), nullable=True)
    position_ratio = Column(Numeric(8, 6), nullable=True)
    shares = Column(Integer, nullable=True)
    atr_pct_snapshot = Column(Numeric(8, 6), nullable=True)
    atr_multiplier_snapshot = Column(Numeric(4, 2), nullable=True)
    stop_loss_price = Column(Numeric(18, 4), nullable=True)
    target_price = Column(Numeric(18, 4), nullable=True)
    exit_date = Column(Date, nullable=True)
    exit_price = Column(Numeric(18, 4), nullable=True)
    holding_days = Column(Integer, nullable=True)
    net_return_pct = Column(Numeric(8, 6), nullable=True)
    commission_total = Column(Numeric(18, 4), nullable=True)
    stamp_duty = Column(Numeric(18, 4), nullable=True)
    slippage_total = Column(Numeric(18, 4), nullable=True)
    take_profit_pending_t1 = Column(Boolean, nullable=False, default=False)
    stop_loss_pending_t1 = Column(Boolean, nullable=False, default=False)
    suspended_pending = Column(Boolean, nullable=False, default=False)
    limit_locked_pending = Column(Boolean, nullable=False, default=False)
    skip_reason = Column(Text, nullable=True)
    status_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)

    # legacy columns for backward compat with app code
    strategy_type = Column(String(4), nullable=True)
    stock_name = Column(String(64), nullable=True)
    sim_open_date = Column(String(10), nullable=True)
    sim_open_price = Column(Float, nullable=True)
    sim_qty = Column(Integer, nullable=True)
    target_price_1 = Column(Float, nullable=True)
    target_price_2 = Column(Float, nullable=True)
    valid_until = Column(String(10), nullable=True)
    execution_blocked = Column(Boolean, default=False)
    close_blocked = Column(Boolean, default=False)
    suspended_days = Column(Integer, default=0)
    sim_close_date = Column(String(10), nullable=True)
    sim_close_price = Column(Float, nullable=True)
    sim_pnl_gross = Column(Float, nullable=True)
    sim_pnl_net = Column(Float, nullable=True)
    sim_pnl_pct = Column(Float, nullable=True)
    hold_days = Column(Integer, nullable=True)

    # backward-compat aliases for legacy app code
    id = synonym("position_id")
    status = synonym("position_status")


class SimPositionBacktest(Base):
    """Walk-Forward 回测结果（12 §6.0.1，E7）；与 sim_position 同结构，增加 source 区分。"""

    __tablename__ = "sim_position_backtest"

    id = Column(Integer, primary_key=True, index=True)
    report_id = Column(String(64), index=True, nullable=False)
    stock_code = Column(String(16), index=True, nullable=False)
    stock_name = Column(String(64), nullable=True)
    strategy_type = Column(String(4), nullable=False)
    signal_date = Column(String(10), nullable=False)
    sim_open_date = Column(String(10), nullable=False)
    sim_open_price = Column(Float, nullable=False)
    actual_entry_price = Column(Float, nullable=True)
    sim_qty = Column(Integer, nullable=False)
    capital_tier = Column(String(8), nullable=False)
    stop_loss_price = Column(Float, nullable=False)
    target_price_1 = Column(Float, nullable=True)
    target_price_2 = Column(Float, nullable=True)
    valid_until = Column(String(10), nullable=True)
    status = Column(String(20), nullable=False, default="OPEN")
    execution_blocked = Column(Boolean, default=False)
    close_blocked = Column(Boolean, default=False)
    suspended_days = Column(Integer, default=0)
    sim_close_date = Column(String(10), nullable=True)
    sim_close_price = Column(Float, nullable=True)
    sim_pnl_gross = Column(Float, nullable=True)
    sim_pnl_net = Column(Float, nullable=True)
    sim_pnl_pct = Column(Float, nullable=True)
    hold_days = Column(Integer, nullable=True)
    source = Column(String(16), nullable=False, default="walkforward")
    created_at = Column(DateTime, default=utc_now)


class SimBaseline(Base):
    """对照组基线（12 §6.1，E8）：random/ma_cross/hs300 验证 LLM 增量 Alpha。"""

    __tablename__ = "sim_baseline"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    baseline_type = Column(String(20), nullable=False)  # 'random' | 'ma_cross' | 'hs300'
    trade_date = Column(String(10), nullable=False)
    stock_code = Column(String(16), nullable=True)  # hs300 时 NULL
    open_price = Column(Float, nullable=True)
    close_price = Column(Float, nullable=True)
    pnl_pct = Column(Float, nullable=True)
    hold_days = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=utc_now)


class ReportFeedback(Base):
    """用户研报反馈 (05 §10, FR-07)"""

    __tablename__ = "report_feedback"

    feedback_id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    report_id = Column(String(36), index=True, nullable=False)
    user_id = Column(String(36), nullable=False, default="0")  # 0=匿名/演示，JWT启用后从token解析
    is_helpful = Column(Integer, nullable=True)  # 1=有帮助，0=无帮助
    feedback_type = Column(String(16), nullable=False)  # direction|data|logic|other
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime, default=utc_now, nullable=False)


class SimAccount(Base):
    """模拟账户 (FR-07)"""

    __tablename__ = "sim_account"

    id = Column(Integer, primary_key=True, autoincrement=True)
    capital_tier = Column(String(16), nullable=False, index=True)
    # NEW schema columns
    initial_cash = Column(Numeric(18, 2), nullable=True)
    cash_available = Column(Numeric(18, 2), nullable=True)
    total_asset = Column(Numeric(18, 2), nullable=True)
    peak_total_asset = Column(Numeric(18, 2), nullable=True)
    max_drawdown_pct = Column(Float, nullable=True, default=0)
    drawdown_state = Column(String(16), nullable=False, default="normal")
    drawdown_state_factor = Column(Numeric(4, 2), nullable=True, default=1)
    active_position_count = Column(Integer, nullable=True, default=0)
    last_reconciled_trade_date = Column(Date, nullable=True)
    # OLD schema columns for backward compat
    snapshot_date = Column(String(10), nullable=True)
    initial_capital = Column(Float, nullable=True)
    cash = Column(Float, nullable=True)
    position_value = Column(Float, nullable=True)
    daily_return_pct = Column(Float, nullable=True)
    cumulative_return_pct = Column(Float, nullable=True)
    hs300_daily_pct = Column(Float, nullable=True)
    hs300_cum_pct = Column(Float, nullable=True)
    alpha_pct = Column(Float, nullable=True)
    open_positions = Column(Integer, nullable=True)
    settled_trades = Column(Integer, nullable=True)
    win_rate = Column(Float, nullable=True)
    pnl_ratio = Column(Float, nullable=True)
    updated_at = Column(DateTime, nullable=True, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)

    __table_args__ = (UniqueConstraint("snapshot_date", "capital_tier", name="uq_sim_account_date_tier"),)


# ---------------------------------------------------------------------------
# SSOT 补齐：以下 ORM 类为 v12+ 新增，供 admin_audit / membership / settlement 等模块使用
# ---------------------------------------------------------------------------


class DataBatch(Base):
    """多源数据采集批处理记录 (FR-04)"""

    __tablename__ = "data_batch"

    batch_id = Column(String(36), primary_key=True)
    source_name = Column(String(32), nullable=False)
    trade_date = Column(Date, nullable=False)
    batch_scope = Column(String(32), nullable=False)
    batch_seq = Column(Integer, nullable=False)
    batch_status = Column(String(32), nullable=False)
    quality_flag = Column(String(16), nullable=False)
    covered_stock_count = Column(Integer, nullable=True)
    core_pool_covered_count = Column(Integer, nullable=True)
    records_total = Column(Integer, nullable=True)
    records_success = Column(Integer, nullable=True)
    records_failed = Column(Integer, nullable=True)
    status_reason = Column(Text, nullable=True)
    trigger_task_run_id = Column(String(36), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class DataBatchError(Base):
    """多源数据采集批处理错误明细 (FR-04)"""

    __tablename__ = "data_batch_error"

    batch_error_id = Column(String(64), primary_key=True)
    batch_id = Column(String(64), nullable=False, index=True)
    stock_code = Column(String(16), nullable=True)
    record_key = Column(String(128), nullable=False, default="")
    error_stage = Column(String(32), nullable=False, default="unknown")
    error_code = Column(String(32), nullable=False, default="UNKNOWN")
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=utc_now)


class DataBatchLineage(Base):
    """多源数据采集血缘关系 (FR-04)"""

    __tablename__ = "data_batch_lineage"

    batch_lineage_id = Column(String(64), primary_key=True)
    child_batch_id = Column(String(64), nullable=False, index=True)
    parent_batch_id = Column(String(64), nullable=False, index=True)
    lineage_role = Column(String(32), nullable=False, default="source")
    created_at = Column(DateTime, default=utc_now)


class DataSourceCircuitState(Base):
    """数据源熔断器状态 (FR-04)"""

    __tablename__ = "data_source_circuit_state"

    source_name = Column(String(64), primary_key=True)
    circuit_state = Column(String(16), nullable=False, default="CLOSED")
    consecutive_failures = Column(Integer, default=0)
    circuit_open_at = Column(DateTime, nullable=True)
    cooldown_until = Column(DateTime, nullable=True)
    last_probe_at = Column(DateTime, nullable=True)
    last_failure_reason = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, default=utc_now)


class MarketStateCache(Base):
    """大盘市场状态缓存 (FR-05)"""

    __tablename__ = "market_state_cache"

    trade_date = Column(Date, primary_key=True)
    market_state = Column(String(16), nullable=False)
    cache_status = Column(String(32), nullable=False, default="fresh")
    state_reason = Column(Text, nullable=True)
    reference_date = Column(Date, nullable=True)
    market_state_degraded = Column(Boolean, nullable=False, default=False)
    a_type_pct = Column(Numeric(8, 6), nullable=True)
    b_type_pct = Column(Numeric(8, 6), nullable=True)
    c_type_pct = Column(Numeric(8, 6), nullable=True)
    kline_batch_id = Column(String(36), nullable=True)
    hotspot_batch_id = Column(String(36), nullable=True)
    computed_at = Column(DateTime, nullable=False, default=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class AdminOperation(Base):
    """管理端操作审计 (FR-12 / NFR-19)"""

    __tablename__ = "admin_operation"

    operation_id = Column(String(64), primary_key=True)
    action_type = Column(String(64), nullable=False, index=True)
    actor_user_id = Column(String(64), nullable=False, index=True)
    target_table = Column(String(64), nullable=False)
    target_pk = Column(String(128), nullable=False)
    status = Column(String(16), nullable=False, default="PENDING")
    reason_code = Column(String(64), nullable=True)
    failure_category = Column(String(64), nullable=True)
    status_reason = Column(String(256), nullable=True)
    request_id = Column(String(64), nullable=True, index=True)
    before_snapshot = Column(JSON, nullable=True)
    after_snapshot = Column(JSON, nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utc_now)


class AuditLog(Base):
    """审计日志 (NFR-19)"""

    __tablename__ = "audit_log"

    audit_log_id = Column(String(64), primary_key=True)
    operation_id = Column(String(64), nullable=True, index=True)
    actor_user_id = Column(String(64), nullable=False, index=True)
    action_type = Column(String(64), nullable=False, index=True)
    target_table = Column(String(64), nullable=False)
    target_pk = Column(String(128), nullable=False)
    request_id = Column(String(64), nullable=True, index=True)
    reason_code = Column(String(64), nullable=True)
    failure_category = Column(String(64), nullable=True)
    before_snapshot = Column(JSON, nullable=True)
    after_snapshot = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=utc_now)


class BillingOrder(Base):
    """支付订单 (FR-09 Billing)"""

    __tablename__ = "billing_order"

    order_id = Column(String(64), primary_key=True, default=lambda: str(uuid4()))
    user_id = Column(String(64), nullable=False, index=True)
    provider = Column(String(32), nullable=False)
    expected_tier = Column(String(32), nullable=True)
    period_months = Column(Integer, nullable=True)
    granted_tier = Column(String(32), nullable=True)
    amount_cny = Column(Float, nullable=False)
    currency = Column(String(8), nullable=False, default="CNY")
    payment_url = Column(String(512), nullable=True)
    status = Column(String(16), nullable=False, default="CREATED")
    status_reason = Column(String(256), nullable=True)
    provider_order_id = Column(String(128), nullable=True)
    created_at = Column(DateTime, default=utc_now)
    paid_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)


class AccessTokenLease(Base):
    """Token 租约表 (NFR-17)"""

    __tablename__ = "access_token_lease"

    jti = Column(String(128), primary_key=True)
    user_id = Column(String(36), nullable=False, index=True)
    session_id = Column(String(36), nullable=True)
    refresh_token_id = Column(String(36), nullable=True)
    issued_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=False)
    revoked_at = Column(DateTime, nullable=True)
    revoke_source = Column(String(32), nullable=True)
    created_at = Column(DateTime, default=utc_now)


class StockPoolRefreshTask(Base):
    """股票池刷新任务 (FR-01)"""

    __tablename__ = "stock_pool_refresh_task"

    task_id = Column(String(64), primary_key=True)
    trade_date = Column(String(10), nullable=False, index=True)
    status = Column(String(16), nullable=False, default="PENDING")
    pool_version = Column(Integer, nullable=True)
    fallback_from = Column(String(10), nullable=True)
    filter_params_json = Column(JSON, nullable=True)
    core_pool_size = Column(Integer, nullable=True)
    standby_pool_size = Column(Integer, nullable=True)
    evicted_stocks_json = Column(JSON, nullable=True)
    status_reason = Column(String(256), nullable=True)
    request_id = Column(String(64), nullable=True, index=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, default=utc_now)


class PaymentWebhookEvent(Base):
    """支付回调事件 (FR-09 Billing)"""

    __tablename__ = "payment_webhook_event"

    event_id = Column(String(64), primary_key=True)
    order_id = Column(String(64), nullable=True, index=True)
    user_id = Column(String(64), nullable=True, index=True)
    provider = Column(String(32), nullable=False)
    event_type = Column(String(32), nullable=True)
    tier_id = Column(String(32), nullable=True)
    paid_amount = Column(Float, nullable=True)
    payload_json = Column(JSON, nullable=True)
    request_id = Column(String(64), nullable=True)
    status = Column(String(16), nullable=False, default="RECEIVED")
    status_reason = Column(String(256), nullable=True)
    processing_succeeded = Column(Boolean, default=False)
    duplicate_count = Column(Integer, default=0)
    received_at = Column(DateTime, default=utc_now)
    processed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("event_id", name="uq_payment_webhook_event_id"),
    )


from sqlalchemy import Index  # noqa: E402
Index("idx_payment_webhook_event_request", PaymentWebhookEvent.request_id, PaymentWebhookEvent.received_at)


# ---------------------------------------------------------------------------
# 行情与数据模型 (FR-01 / FR-04)
# ---------------------------------------------------------------------------


class StockMaster(Base):
    """股票主数据 (FR-01)"""

    __tablename__ = "stock_master"

    stock_code = Column(String(16), primary_key=True)
    stock_name = Column(String(64), nullable=False)
    exchange = Column(String(8), nullable=False)
    industry = Column(String(64), nullable=True)
    list_date = Column(Date, nullable=True)
    circulating_shares = Column(Numeric(20, 2), nullable=True)
    is_st = Column(Boolean, nullable=False, default=False)
    is_suspended = Column(Boolean, nullable=False, default=False)
    is_delisted = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)


class KlineDaily(Base):
    """日K线数据 (FR-04)"""

    __tablename__ = "kline_daily"

    kline_id = Column(String(36), primary_key=True)
    stock_code = Column(String(16), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    open = Column(Numeric(18, 4), nullable=False)
    high = Column(Numeric(18, 4), nullable=False)
    low = Column(Numeric(18, 4), nullable=False)
    close = Column(Numeric(18, 4), nullable=False)
    volume = Column(Numeric(20, 2), nullable=False)
    amount = Column(Numeric(20, 2), nullable=False)
    adjust_type = Column(String(32), nullable=False)
    atr_pct = Column(Numeric(8, 6), nullable=True)
    turnover_rate = Column(Numeric(8, 6), nullable=True)
    ma5 = Column(Numeric(18, 4), nullable=True)
    ma10 = Column(Numeric(18, 4), nullable=True)
    ma20 = Column(Numeric(18, 4), nullable=True)
    ma60 = Column(Numeric(18, 4), nullable=True)
    volatility_20d = Column(Numeric(8, 6), nullable=True)
    hs300_return_20d = Column(Numeric(8, 6), nullable=True)
    is_suspended = Column(Boolean, nullable=False, default=False)
    source_batch_id = Column(String(36), nullable=False)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class MarketHotspotItem(Base):
    """市场热点条目 (FR-04)"""

    __tablename__ = "market_hotspot_item"

    hotspot_item_id = Column(String(36), primary_key=True)
    batch_id = Column(String(36), nullable=False, index=True)
    source_name = Column(String(32), nullable=False)
    merged_rank = Column(Integer, nullable=False)
    source_rank = Column(Integer, nullable=True)
    topic_title = Column(String(256), nullable=False)
    news_event_type = Column(String(32), nullable=True)
    hotspot_tags_json = Column(JSON, nullable=True)
    source_url = Column(Text, nullable=False, default="")
    fetch_time = Column(DateTime, nullable=False, default=utc_now)
    quality_flag = Column(String(16), nullable=False, default="OK")
    created_at = Column(DateTime, nullable=False, default=utc_now)


class MarketHotspotItemSource(Base):
    """热点条目来源明细 (FR-04)"""

    __tablename__ = "market_hotspot_item_source"

    hotspot_item_source_id = Column(String(36), primary_key=True)
    hotspot_item_id = Column(String(36), nullable=False, index=True)
    batch_id = Column(String(36), nullable=False)
    source_name = Column(String(32), nullable=False)
    source_rank = Column(Integer, nullable=True)
    source_url = Column(Text, nullable=False, default="")
    fetch_time = Column(DateTime, nullable=False, default=utc_now)
    quality_flag = Column(String(16), nullable=False, default="OK")
    created_at = Column(DateTime, nullable=False, default=utc_now)


class MarketHotspotItemStockLink(Base):
    """热点条目-个股关联 (FR-04)"""

    __tablename__ = "market_hotspot_item_stock_link"

    hotspot_item_stock_link_id = Column(String(36), primary_key=True)
    hotspot_item_id = Column(String(36), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    relation_role = Column(String(16), nullable=False, default="related")
    match_confidence = Column(Numeric(5, 4), nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class ReportDataUsage(Base):
    """研报数据使用记录 (FR-04)"""

    __tablename__ = "report_data_usage"

    usage_id = Column(String(36), primary_key=True)
    trade_date = Column(Date, nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    dataset_name = Column(String(32), nullable=False)
    source_name = Column(String(32), nullable=False)
    batch_id = Column(String(36), nullable=False)
    fetch_time = Column(DateTime, nullable=False, default=utc_now)
    status = Column(String(16), nullable=False, default="OK")
    status_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)

    __table_args__ = (
        CheckConstraint(
            "status IN ('ok', 'stale_ok', 'missing', 'degraded', 'proxy_ok', 'realtime_only')",
            name="ck_report_data_usage_status_enum",
        ),
    )


# ---------------------------------------------------------------------------
# 以下为从生产 DB 同步补全的 ORM 定义 (40 tables)
# ---------------------------------------------------------------------------


# AppUser is now unified with User (both map to app_user table).
# Keep alias for backward compatibility with any code importing AppUser.
AppUser = User


class AuthTempToken(Base):
    __tablename__ = "auth_temp_token"
    temp_token_id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    user_id = Column(String(36), nullable=False)
    token_type = Column(String(32), nullable=False)
    token_hash = Column(String(256), nullable=False)
    sent_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class BaselineEquityCurvePoint(Base):
    __tablename__ = "baseline_equity_curve_point"
    baseline_equity_curve_point_id = Column(String(36), primary_key=True)
    capital_tier = Column(String(16), nullable=False)
    baseline_type = Column(String(32), nullable=False)
    trade_date = Column(Date, nullable=False)
    equity = Column(Numeric(18, 2), nullable=False)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class BaselineMetricSnapshot(Base):
    __tablename__ = "baseline_metric_snapshot"
    baseline_metric_snapshot_id = Column(String(36), primary_key=True)
    snapshot_date = Column(Date, nullable=False)
    window_days = Column(Integer, nullable=False)
    baseline_type = Column(String(32), nullable=False)
    simulation_runs = Column(Integer, nullable=True)
    sample_size = Column(Integer, nullable=False)
    win_rate = Column(Numeric(8, 6), nullable=True)
    profit_loss_ratio = Column(Numeric(8, 6), nullable=True)
    alpha_annual = Column(Numeric(8, 6), nullable=True)
    max_drawdown_pct = Column(Numeric(8, 6), nullable=True)
    cumulative_return_pct = Column(Numeric(8, 6), nullable=True)
    display_hint = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class BaselineResult(Base):
    __tablename__ = "baseline_result"
    baseline_id = Column(Text, primary_key=True)
    trade_date = Column(Text, nullable=False)
    strategy_type = Column(Text, nullable=False)
    window_days = Column(Text, nullable=False)
    baseline_type = Column(Text, nullable=False)
    cumulative_return_pct = Column(Text, nullable=False)
    simulation_runs = Column(Text, nullable=True)
    created_at = Column(Text, nullable=False)


class BaselineTask(Base):
    __tablename__ = "baseline_task"
    baseline_task_id = Column(String(36), primary_key=True)
    snapshot_date = Column(Date, nullable=False)
    window_days = Column(Integer, nullable=False)
    baseline_type = Column(String(32), nullable=False)
    simulation_runs = Column(Integer, nullable=True)
    status = Column(String(32), nullable=False)
    status_reason = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class BusinessEvent(Base):
    __tablename__ = "business_event"
    business_event_id = Column(String(36), primary_key=True)
    event_type = Column(String(32), nullable=False)
    projection_cursor_id = Column(String(36), nullable=False)
    event_projection_key = Column(String(160), nullable=False)
    event_status = Column(String(32), nullable=False)
    source_table = Column(String(64), nullable=False)
    source_pk = Column(String(128), nullable=False)
    stock_code = Column(String(16), nullable=True)
    trade_date = Column(Date, nullable=True)
    capital_tier = Column(String(16), nullable=True)
    payload_json = Column(JSON, nullable=False)
    dedup_until = Column(DateTime, nullable=True)
    status_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    enqueued_at = Column(DateTime, nullable=True)


class CleanupTask(Base):
    __tablename__ = "cleanup_task"
    cleanup_id = Column(String(36), primary_key=True)
    cleanup_date = Column(Date, nullable=False)
    status = Column(String(32), nullable=False)
    request_id = Column(String(64), nullable=True)
    lock_key = Column(String(128), nullable=True)
    deleted_session_count = Column(Integer, nullable=False, default=0)
    deleted_temp_token_count = Column(Integer, nullable=False, default=0)
    deleted_access_token_lease_count = Column(Integer, nullable=False, default=0)
    deleted_report_generation_task_count = Column(Integer, nullable=False, default=0)
    expired_stale_task_count = Column(Integer, nullable=False, default=0)
    deleted_unverified_user_count = Column(Integer, nullable=False, default=0)
    deleted_notification_count = Column(Integer, nullable=False, default=0)
    duration_ms = Column(Integer, nullable=True)
    status_reason = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class CleanupTaskItem(Base):
    __tablename__ = "cleanup_task_item"
    cleanup_task_item_id = Column(String(36), primary_key=True)
    cleanup_id = Column(String(36), nullable=False)
    step_no = Column(Integer, nullable=False)
    target_domain = Column(String(64), nullable=False)
    result = Column(String(16), nullable=False)
    affected_count = Column(Integer, nullable=False)
    status_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class CookieProbeLog(Base):
    __tablename__ = "cookie_probe_log"
    probe_log_id = Column(String(36), primary_key=True)
    cookie_session_id = Column(String(36), nullable=False)
    probe_outcome = Column(String(16), nullable=False)
    http_status = Column(Integer, nullable=True)
    latency_ms = Column(Integer, nullable=True)
    status_reason = Column(Text, nullable=True)
    probed_at = Column(DateTime, nullable=False)


class DagEvent(Base):
    __tablename__ = "dag_event"
    dag_event_id = Column(String(36), primary_key=True)
    event_key = Column(String(128), nullable=False)
    event_name = Column(String(64), nullable=False)
    trade_date = Column(Date, nullable=True)
    producer_task_run_id = Column(String(36), nullable=True)
    payload_json = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class DataUsageFact(Base):
    __tablename__ = "data_usage_fact"
    usage_id = Column(Text, primary_key=True)
    batch_id = Column(Text, nullable=False)
    trade_date = Column(Text, nullable=False)
    stock_code = Column(Text, nullable=False)
    source_name = Column(Text, nullable=False)
    fetch_time = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    status_reason = Column(Text, nullable=True)
    created_at = Column(Text, nullable=False)


class EventProjectionCursor(Base):
    __tablename__ = "event_projection_cursor"
    projection_cursor_id = Column(String(36), primary_key=True)
    event_type = Column(String(32), nullable=False)
    event_projection_key = Column(String(160), nullable=False)
    last_business_event_id = Column(String(36), nullable=True)
    last_sent_at = Column(DateTime, nullable=True)
    dedup_until = Column(DateTime, nullable=True)
    last_state_value = Column(String(32), nullable=True)
    recovered_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)


class ExperimentLog(Base):
    __tablename__ = "experiment_log"
    experiment_id = Column(String(36), primary_key=True)
    data_cutoff = Column(Date, nullable=False)
    test_win_rate = Column(Numeric(8, 6), nullable=True)
    baseline_win_rate = Column(Numeric(8, 6), nullable=True)
    passed = Column(Boolean, nullable=False)
    status_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    deployed_at = Column(Text, nullable=True)
    experiment_type = Column(Text, nullable=True)
    gate_passed = Column(Text, nullable=True)
    leak_check_passed = Column(Text, nullable=True)
    rolled_back_at = Column(Text, nullable=True)
    strategy_type = Column(Text, nullable=True)
    test_sample_count = Column(Text, nullable=True)
    train_sample_count = Column(Text, nullable=True)


class HotspotTop50(Base):
    __tablename__ = "hotspot_top50"
    hotspot_id = Column(Text, primary_key=True)
    trade_date = Column(Text, nullable=False)
    rank = Column(Text, nullable=False)
    topic_title = Column(Text, nullable=False)
    source_name = Column(Text, nullable=False)
    source_url = Column(Text, nullable=False)
    fetch_time = Column(Text, nullable=False)
    quality_flag = Column(Text, nullable=False)
    batch_id = Column(Text, nullable=False)
    created_at = Column(Text, nullable=False)


class InstructionCard(Base):
    __tablename__ = "instruction_card"
    instruction_card_id = Column(String(36), primary_key=True)
    report_id = Column(String(36), nullable=False)
    signal_entry_price = Column(Numeric(18, 4), nullable=False)
    atr_pct = Column(Numeric(8, 6), nullable=False)
    atr_multiplier = Column(Numeric(4, 2), nullable=False)
    stop_loss = Column(Numeric(18, 4), nullable=False)
    target_price = Column(Numeric(18, 4), nullable=False)
    stop_loss_calc_mode = Column(String(32), nullable=False)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class JtiBlacklist(Base):
    __tablename__ = "jti_blacklist"
    jti = Column(String(128), primary_key=True)
    user_id = Column(String(36), nullable=False)
    session_id = Column(String(36), nullable=True)
    source_action = Column(String(32), nullable=False)
    status_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    expires_at = Column(DateTime, nullable=False)


class LlmCircuitState(Base):
    __tablename__ = "llm_circuit_state"
    circuit_name = Column(String(32), primary_key=True)
    circuit_state = Column(String(16), nullable=False)
    consecutive_failures = Column(Integer, nullable=False, default=0)
    opened_at = Column(DateTime, nullable=True)
    cooldown_until = Column(DateTime, nullable=True)
    last_probe_at = Column(DateTime, nullable=True)
    last_failure_reason = Column(Text, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class Notification(Base):
    __tablename__ = "notification"
    notification_id = Column(String(36), primary_key=True)
    business_event_id = Column(String(36), nullable=False)
    event_type = Column(String(32), nullable=False)
    channel = Column(String(16), nullable=False)
    recipient_scope = Column(String(16), nullable=False)
    recipient_key = Column(String(64), nullable=False)
    recipient_user_id = Column(String(36), nullable=True)
    triggered_at = Column(DateTime, nullable=False)
    status = Column(String(16), nullable=False)
    payload_summary = Column(Text, nullable=False)
    status_reason = Column(Text, nullable=True)
    sent_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class OAuthIdentity(Base):
    __tablename__ = "oauth_identity"
    oauth_identity_id = Column(String(36), primary_key=True)
    user_id = Column(String(36), nullable=False)
    provider = Column(String(16), nullable=False)
    provider_user_id = Column(String(128), nullable=False)
    email_snapshot = Column(String(256), nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    last_login_at = Column(DateTime, nullable=True)
    avatar_url = Column(String(512), nullable=True)
    display_name = Column(String(128), nullable=True)
    provider_union_id = Column(String(255), nullable=True)
    updated_at = Column(DateTime, nullable=True, default=utc_now, onupdate=utc_now)


class Order(Base):
    __tablename__ = "order"
    order_id = Column(Text, primary_key=True)
    user_id = Column(Text, nullable=False)
    tier_id = Column(Text, nullable=False)
    paid_amount = Column(Text, nullable=False)
    currency = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    event_id = Column(Text, nullable=False)
    channel = Column(Text, nullable=False)
    expires_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)


class OutboxEvent(Base):
    __tablename__ = "outbox_event"
    __table_args__ = (
        Index("idx_outbox_event_dispatch", "dispatch_status", "next_retry_at"),
    )
    outbox_event_id = Column(String(36), primary_key=True)
    business_event_id = Column(String(36), nullable=False)
    dispatch_status = Column(String(32), nullable=False)
    claim_token = Column(String(64), nullable=True)
    claimed_at = Column(DateTime, nullable=True)
    claimed_by = Column(String(128), nullable=True)
    dispatch_attempt_count = Column(Integer, nullable=False, default=0)
    next_retry_at = Column(DateTime, nullable=True)
    payload_json = Column(JSON, nullable=False)
    status_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    dispatched_at = Column(DateTime, nullable=True)


class PipelineRun(Base):
    __tablename__ = "pipeline_run"
    pipeline_run_id = Column(String(36), primary_key=True)
    pipeline_name = Column(String(64), nullable=False)
    trade_date = Column(Date, nullable=False)
    pipeline_status = Column(String(32), nullable=False)
    degraded = Column(Boolean, nullable=False, default=False)
    status_reason = Column(Text, nullable=True)
    request_id = Column(String(64), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class RefreshToken(Base):
    __tablename__ = "refresh_token"
    refresh_token_id = Column(String(36), primary_key=True)
    user_id = Column(String(36), nullable=False)
    session_id = Column(String(36), nullable=False)
    token_hash = Column(String(256), nullable=False)
    rotated_from_token_id = Column(String(36), nullable=True)
    issued_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=False)
    grace_expires_at = Column(DateTime, nullable=False)
    revoked_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    revoke_reason = Column(String(64), nullable=True)


class ReportCitation(Base):
    __tablename__ = "report_citation"
    citation_id = Column(String(36), primary_key=True)
    report_id = Column(String(36), nullable=False)
    citation_order = Column(Integer, nullable=False)
    source_name = Column(String(64), nullable=False)
    source_url = Column(Text, nullable=False)
    fetch_time = Column(DateTime, nullable=False)
    title = Column(String(256), nullable=True)
    excerpt = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class ReportDataUsageLink(Base):
    __tablename__ = "report_data_usage_link"
    report_data_usage_link_id = Column(String(36), primary_key=True)
    report_id = Column(String(36), nullable=False)
    usage_id = Column(String(36), nullable=False)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class ReportGenerationTask(Base):
    __tablename__ = "report_generation_task"
    task_id = Column(String(36), primary_key=True)
    trade_date = Column(Date, nullable=False)
    stock_code = Column(String(16), nullable=False)
    idempotency_key = Column(String(64), nullable=False)
    generation_seq = Column(Integer, nullable=False)
    status = Column(String(32), nullable=False)
    retry_count = Column(Integer, nullable=False, default=0)
    quality_flag = Column(String(16), nullable=True)
    status_reason = Column(Text, nullable=True)
    llm_fallback_level = Column(String(16), nullable=True)
    risk_audit_status = Column(String(32), nullable=True)
    risk_audit_skip_reason = Column(Text, nullable=True)
    market_state_trade_date = Column(Date, nullable=False)
    refresh_task_id = Column(String(36), ForeignKey("stock_pool_refresh_task.task_id"), nullable=False)
    trigger_task_run_id = Column(String(36), nullable=True)
    request_id = Column(String(64), nullable=True)
    superseded_by_task_id = Column(String(36), nullable=True)
    superseded_at = Column(DateTime, nullable=True)
    queued_at = Column(DateTime, nullable=False)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)

    __table_args__ = (
        CheckConstraint(
            "status NOT IN ('SUCCEEDED', 'READY', 'DONE', 'OK', 'NEW')",
            name="ck_report_generation_task_status_enum",
        ),
    )


class SchedulerTask(Base):
    __tablename__ = "scheduler_task"
    task_log_id = Column(Text, primary_key=True)
    task_name = Column(Text, nullable=False)
    trade_date = Column(Text, nullable=False)
    triggered_at = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    retry_count = Column(Text, nullable=False)
    error_message = Column(Text, nullable=True)
    status_reason = Column(Text, nullable=True)
    lock_key = Column(Text, nullable=True)
    completed_at = Column(Text, nullable=True)


class SchedulerTaskRun(Base):
    __tablename__ = "scheduler_task_run"
    task_run_id = Column(String(36), primary_key=True)
    task_name = Column(String(64), nullable=False)
    trade_date = Column(Date, nullable=True)
    schedule_slot = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False)
    retry_count = Column(Integer, nullable=False, default=0)
    lock_key = Column(String(128), nullable=True)
    lock_version = Column(Integer, nullable=True)
    trigger_source = Column(String(16), nullable=False)
    status_reason = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    triggered_at = Column(DateTime, nullable=False)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class SettlementResult(Base):
    __tablename__ = "settlement_result"
    settlement_result_id = Column(String(36), primary_key=True)
    report_id = Column(String(36), nullable=False)
    stock_code = Column(String(16), nullable=False)
    signal_date = Column(Date, nullable=False)
    window_days = Column(Integer, nullable=False)
    strategy_type = Column(String(8), nullable=False)
    settlement_status = Column(String(16), nullable=False)
    quality_flag = Column(String(16), nullable=False)
    status_reason = Column(Text, nullable=True)
    entry_trade_date = Column(Date, nullable=True)
    exit_trade_date = Column(Date, nullable=True)
    shares = Column(Integer, nullable=False, default=0)
    buy_price = Column(Numeric(18, 4), nullable=True)
    sell_price = Column(Numeric(18, 4), nullable=True)
    buy_commission = Column(Numeric(18, 4), nullable=True)
    sell_commission = Column(Numeric(18, 4), nullable=True)
    stamp_duty = Column(Numeric(18, 4), nullable=True)
    buy_slippage_cost = Column(Numeric(18, 4), nullable=True)
    sell_slippage_cost = Column(Numeric(18, 4), nullable=True)
    gross_return_pct = Column(Numeric(8, 6), nullable=True)
    net_return_pct = Column(Numeric(8, 6), nullable=True)
    display_hint = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    settlement_id = Column(Text, nullable=True)
    trade_date = Column(Text, nullable=True)
    is_misclassified = Column(Integer, default=0)
    exit_reason = Column(Text, nullable=True)
    settled_at = Column(Text, nullable=True)


class SettlementTask(Base):
    __tablename__ = "settlement_task"
    task_id = Column(String(36), primary_key=True)
    task_scope_key = Column(String(128), nullable=False)
    trade_date = Column(Date, nullable=False)
    window_days = Column(Integer, nullable=False)
    target_scope = Column(String(16), nullable=False)
    target_report_id = Column(String(36), nullable=True)
    target_stock_code = Column(String(16), nullable=True)
    force = Column(Boolean, nullable=False, default=False)
    status = Column(String(32), nullable=False)
    processed_count = Column(Integer, nullable=False, default=0)
    skipped_count = Column(Integer, nullable=False, default=0)
    failed_count = Column(Integer, nullable=False, default=0)
    status_reason = Column(Text, nullable=True)
    lock_key = Column(String(128), nullable=True)
    request_id = Column(String(64), nullable=True)
    requested_by_user_id = Column(String(36), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class SimDashboardSnapshot(Base):
    __tablename__ = "sim_dashboard_snapshot"
    dashboard_snapshot_id = Column(String(36), primary_key=True)
    capital_tier = Column(String(16), nullable=False)
    snapshot_date = Column(Date, nullable=False)
    data_status = Column(String(16), nullable=False)
    status_reason = Column(Text, nullable=True)
    total_return_pct = Column(Numeric(8, 6), nullable=True)
    win_rate = Column(Numeric(8, 6), nullable=True)
    profit_loss_ratio = Column(Numeric(8, 6), nullable=True)
    alpha_annual = Column(Numeric(8, 6), nullable=True)
    max_drawdown_pct = Column(Numeric(8, 6), nullable=True)
    sample_size = Column(Integer, nullable=False, default=0)
    display_hint = Column(Text, nullable=True)
    is_simulated_only = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(Text, nullable=True)


class SimEquityCurvePoint(Base):
    __tablename__ = "sim_equity_curve_point"
    equity_curve_point_id = Column(String(36), primary_key=True)
    capital_tier = Column(String(16), nullable=False)
    trade_date = Column(Date, nullable=False)
    equity = Column(Numeric(18, 2), nullable=False)
    cash_available = Column(Numeric(18, 2), nullable=False)
    position_market_value = Column(Numeric(18, 2), nullable=False)
    drawdown_state = Column(String(16), nullable=False)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class SimTradeBatchQueueItem(Base):
    __tablename__ = "sim_trade_batch_queue_item"
    queue_item_id = Column(String(36), primary_key=True)
    trade_date = Column(Date, nullable=False)
    capital_tier = Column(String(16), nullable=False)
    report_id = Column(String(36), nullable=False)
    trade_instruction_id = Column(String(36), nullable=False)
    stock_code = Column(String(16), nullable=False)
    confidence_snapshot = Column(Numeric(6, 4), nullable=False)
    selected_for_execution = Column(Boolean, nullable=False, default=False)
    global_rank = Column(Integer, nullable=True)
    selection_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)


class SimTradeInstruction(Base):
    __tablename__ = "sim_trade_instruction"
    trade_instruction_id = Column(String(36), primary_key=True)
    report_id = Column(String(36), nullable=False)
    capital_tier = Column(String(16), nullable=False)
    status = Column(String(16), nullable=False)
    position_ratio = Column(Numeric(8, 6), nullable=False)
    skip_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class StockPool(Base):
    __tablename__ = "stock_pool"
    pool_id = Column(Text, primary_key=True)
    pool_date = Column(Text, nullable=False)
    pool_version = Column(Text, nullable=False)
    core_pool = Column(Text, nullable=False)
    standby_pool = Column(Text, nullable=False)
    evicted_stocks = Column(Text, nullable=False)
    fallback_from = Column(Text, nullable=True)
    status = Column(Text, nullable=False)
    status_reason = Column(Text, nullable=True)
    created_at = Column(Text, nullable=False)


class StockPoolSnapshot(Base):
    __tablename__ = "stock_pool_snapshot"
    pool_snapshot_id = Column(String(36), primary_key=True)
    refresh_task_id = Column(String(36), nullable=False)
    trade_date = Column(Date, nullable=False)
    pool_version = Column(Integer, nullable=False)
    stock_code = Column(String(16), nullable=False)
    pool_role = Column(String(16), nullable=False)
    rank_no = Column(Integer, nullable=True)
    score = Column(Numeric(10, 4), nullable=True)
    is_suspended = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class StockScore(Base):
    __tablename__ = "stock_score"
    score_id = Column(Text, primary_key=True)
    pool_date = Column(Text, nullable=False)
    stock_code = Column(Text, nullable=False)
    score = Column(Text, nullable=False)
    factor_momentum = Column(Text, nullable=False)
    factor_market_cap = Column(Text, nullable=False)
    factor_liquidity = Column(Text, nullable=False)
    factor_ma20_slope = Column(Text, nullable=False)
    factor_earnings = Column(Text, nullable=False)
    factor_turnover = Column(Text, nullable=False)
    factor_rsi = Column(Text, nullable=False)
    factor_52w_high = Column(Text, nullable=False)
    in_core_pool = Column(Text, nullable=False)
    in_standby_pool = Column(Text, nullable=False)
    created_at = Column(Text, nullable=False)


class StrategyMetricSnapshot(Base):
    __tablename__ = "strategy_metric_snapshot"
    metric_snapshot_id = Column(String(36), primary_key=True)
    snapshot_date = Column(Date, nullable=False)
    strategy_type = Column(String(8), nullable=False)
    window_days = Column(Integer, nullable=False)
    data_status = Column(String(16), nullable=False)
    sample_size = Column(Integer, nullable=False)
    coverage_pct = Column(Numeric(8, 6), nullable=False)
    win_rate = Column(Numeric(8, 6), nullable=True)
    profit_loss_ratio = Column(Numeric(8, 6), nullable=True)
    alpha_annual = Column(Numeric(8, 6), nullable=True)
    max_drawdown_pct = Column(Numeric(8, 6), nullable=True)
    cumulative_return_pct = Column(Numeric(8, 6), nullable=True)
    signal_validity_warning = Column(Boolean, nullable=False, default=False)
    display_hint = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)


class UserSession(Base):
    __tablename__ = "user_session"
    session_id = Column(String(36), primary_key=True)
    user_id = Column(String(36), nullable=False)
    status = Column(String(32), nullable=False)
    client_fingerprint = Column(String(128), nullable=True)
    ip_address = Column(String(64), nullable=True)
    user_agent = Column(String(512), nullable=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
    expires_at = Column(DateTime, nullable=False)
    revoked_at = Column(DateTime, nullable=True)

