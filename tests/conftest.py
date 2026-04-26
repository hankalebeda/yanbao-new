import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker


def pytest_configure(config):
    """注册自定义 markers: feature / test_kind。"""
    config.addinivalue_line("markers", "feature(id): 绑定到 feature_id，如 @pytest.mark.feature('FR10-HOME-01')")
    config.addinivalue_line("markers", "test_kind(kind): 测试类型，如 @pytest.mark.test_kind('api')")
    config.addinivalue_line("markers", "doc_driven: 文档驱动验真测试")


def pytest_sessionstart(session):
    """每次 pytest 会话使用独立时间戳目录作为 basetemp，彻底避免 Windows SQLite 文件锁（WinError 32）。

    pytest_configure 里设置 config.option.basetemp 无效（pytest 内部 TempPathFactory
    在 conftest.pytest_configure 之前就已初始化）。pytest_sessionstart 运行时
    TempPathFactory 已就绪，可直接覆写 _basetemp。
    """
    import time, shutil, subprocess
    archive = Path(__file__).resolve().parents[1] / "_archive"

    # 清理超过 2 小时的旧 pytest_tmp_* 目录（忽略权限错误）
    for old in sorted(archive.glob("pytest_tmp_*")):
        try:
            if time.time() - old.stat().st_mtime > 7200:
                shutil.rmtree(old, ignore_errors=True)
                subprocess.run(["cmd", "/c", "rd", "/s", "/q", str(old)], capture_output=True)
        except Exception:
            pass

    # 为本次会话创建唯一目录并覆写 TempPathFactory 的 _basetemp
    ts = int(time.time())
    new_basetemp = archive / f"pytest_tmp_{ts}"
    new_basetemp.mkdir(parents=True, exist_ok=True)
    try:
        session.config._tmp_path_factory._basetemp = new_basetemp
    except AttributeError:
        pass  # 极少数情况下 TempPathFactory 未注册，忽略

# 默认 mock LLM；若需用本地 Ollama 测试，运行前设置 MOCK_LLM=false
if os.environ.get("MOCK_LLM", "").lower() not in ("0", "false", "no"):
    os.environ.setdefault("MOCK_LLM", "true")
os.environ.setdefault("ENABLE_SCHEDULER", "false")
os.environ.setdefault("STRICT_REAL_DATA", "false")
os.environ.setdefault("JWT_SECRET", "test-jwt-secret-32bytes-minimum-length")
os.environ.setdefault("BILLING_WEBHOOK_SECRET", "test-billing-secret")
os.environ.setdefault("TRUSTED_HOSTS", "127.0.0.1,localhost,testserver")
os.environ.setdefault("ENABLE_MOCK_BILLING", "true")
os.environ.setdefault("DEBUG", "true")
os.environ.setdefault("SETTLEMENT_INLINE_EXECUTION", "true")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.runtime_compat import apply_runtime_compat

apply_runtime_compat()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture()
def isolated_app(tmp_path, monkeypatch):
    import app.api.routes_auth as routes_auth
    import app.core.db as core_db
    import app.main as app_main
    import app.services.settlement_ssot as settlement_ssot
    import app.services.trade_calendar as trade_calendar
    from app.models import Base

    db_path = tmp_path / "test.db"
    engine = core_db.build_engine(f"sqlite:///{db_path}")
    testing_session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    core_db.ensure_sqlite_schema_alignment(engine)

    monkeypatch.setattr(core_db, "engine", engine)
    monkeypatch.setattr(core_db, "SessionLocal", testing_session_local)
    monkeypatch.setattr(routes_auth, "SessionLocal", testing_session_local)
    monkeypatch.setattr(app_main, "engine", engine)
    monkeypatch.setattr(app_main, "SessionLocal", testing_session_local)
    monkeypatch.setattr(settlement_ssot, "SessionLocal", testing_session_local)
    monkeypatch.setattr(trade_calendar, "engine", engine)
    monkeypatch.setattr(trade_calendar, "load_tdx_day_records", lambda *args, **kwargs: [])
    trade_calendar.clear_trade_calendar_cache()

    def override_get_db():
        db = testing_session_local()
        try:
            yield db
        finally:
            db.close()

    app_main.app.dependency_overrides[core_db.get_db] = override_get_db
    try:
        yield {"app": app_main.app, "sessionmaker": testing_session_local, "engine": engine}
    finally:
        app_main.app.dependency_overrides.clear()
        trade_calendar.clear_trade_calendar_cache()
        engine.dispose()


@pytest.fixture()
def client(isolated_app):
    with TestClient(isolated_app["app"], base_url="http://localhost") as test_client:
        yield test_client


@pytest.fixture()
def db_session(isolated_app):
    db = isolated_app["sessionmaker"]()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture()
def create_user(db_session):
    from app.core.security import hash_password
    from app.models import User
    from app.services.membership import is_paid_tier

    _UNSET = object()

    def _create_user(
        *,
        email: str = "user@example.com",
        password: str = "Password123",
        tier: str = "Free",
        role: str = "user",
        email_verified: bool = True,
        tier_expires_at=_UNSET,
    ):
        resolved_tier_expires_at = tier_expires_at
        if resolved_tier_expires_at is _UNSET:
            resolved_tier_expires_at = utc_now() + timedelta(days=30) if is_paid_tier(tier) else None
        user = User(
            email=email,
            password_hash=hash_password(password),
            tier=tier,
            tier_expires_at=resolved_tier_expires_at,
            role=role,
            email_verified=email_verified,
        )
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)
        return {"user": user, "password": password}

    return _create_user


@pytest.fixture()
def internal_headers(monkeypatch):
    from app.core.config import settings

    def _internal_headers(token: str = "test-internal-token", *, use_api_key: bool = False):
        if use_api_key:
            monkeypatch.setattr(settings, "internal_cron_token", "")
            monkeypatch.setattr(settings, "internal_api_key", token)
        else:
            monkeypatch.setattr(settings, "internal_cron_token", token)
            monkeypatch.setattr(settings, "internal_api_key", "")
        return {"X-Internal-Token": token}

    return _internal_headers


@pytest.fixture()
def _legacy_seed_report_bundle(db_session):
    from app.models import Base, Report
    from app.services.trade_calendar import latest_trade_date_str

    generation_task_table = Base.metadata.tables["report_generation_task"]
    citation_table = Base.metadata.tables["report_citation"]
    instruction_card_table = Base.metadata.tables["instruction_card"]
    usage_table = Base.metadata.tables["report_data_usage"]
    usage_link_table = Base.metadata.tables["report_data_usage_link"]
    trade_instruction_table = Base.metadata.tables["sim_trade_instruction"]
    sim_position_table = Base.metadata.tables["sim_position"]
    refresh_task_table = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]

    def _seed(
        *,
        stock_code: str = "600519.SH",
        stock_name: str = "贵州茅台",
        trade_date: str | None = None,
        recommendation: str = "BUY",
        strategy_type: str = "A",
        market_state: str = "BULL",
        quality_flag: str = "ok",
        published: bool = True,
        publish_status: str | None = None,
        review_flag: str = "APPROVED",
    ):
        now = utc_now()
        actual_trade_date_str = trade_date or latest_trade_date_str()
        actual_trade_date = date.fromisoformat(actual_trade_date_str)
        task_id = str(uuid4())
        refresh_task_id = str(uuid4())
        request_id = str(uuid4())
        db_session.execute(
            refresh_task_table.insert().values(
                task_id=refresh_task_id,
                trade_date=actual_trade_date,
                status="COMPLETED",
                pool_version=1,
                fallback_from=None,
                filter_params_json={"target_pool_size": 1},
                core_pool_size=1,
                standby_pool_size=0,
                evicted_stocks_json=[],
                status_reason=None,
                request_id=str(uuid4()),
                started_at=now,
                finished_at=now,
                updated_at=now,
                created_at=now,
            )
        )
        db_session.execute(
            snapshot_table.insert().values(
                pool_snapshot_id=str(uuid4()),
                refresh_task_id=refresh_task_id,
                trade_date=actual_trade_date,
                pool_version=1,
                stock_code=stock_code,
                pool_role="core",
                rank_no=1,
                score=100.0,
                is_suspended=False,
                created_at=now,
            )
        )
        db_session.execute(
            generation_task_table.insert().values(
                task_id=task_id,
                trade_date=actual_trade_date,
                stock_code=stock_code,
                idempotency_key=f"daily:{stock_code}:{actual_trade_date_str}",
                generation_seq=1,
                status="Completed",
                retry_count=0,
                quality_flag=quality_flag,
                status_reason=None,
                llm_fallback_level="primary",
                risk_audit_status="completed",
                risk_audit_skip_reason=None,
                market_state_trade_date=actual_trade_date,
                refresh_task_id=refresh_task_id,
                trigger_task_run_id=None,
                request_id=request_id,
                superseded_by_task_id=None,
                superseded_at=None,
                queued_at=now,
                started_at=now,
                finished_at=now,
            )
        )

        report = Report(
            generation_task_id=task_id,
            trade_date=actual_trade_date,
            stock_code=stock_code,
            stock_name_snapshot=stock_name,
            pool_version=1,
            idempotency_key=f"daily:{stock_code}:{actual_trade_date_str}",
            generation_seq=1,
            published=published,
            publish_status=publish_status or ("PUBLISHED" if published else "DRAFT_GENERATED"),
            published_at=now if published else None,
            recommendation=recommendation,
            confidence=0.78,
            quality_flag=quality_flag,
            status_reason=None,
            llm_fallback_level="primary",
            strategy_type=strategy_type,
            market_state=market_state,
            market_state_reference_date=actual_trade_date,
            market_state_degraded=False,
            market_state_reason_snapshot="market ok",
            market_state_trade_date=actual_trade_date,
            conclusion_text="维持看多，等待放量确认。",
            reasoning_chain_md="第一步：确认趋势\n第二步：检查风险回报",
            prior_stats_snapshot={"recent_3m_accuracy": 0.61},
            risk_audit_status="completed",
            risk_audit_skip_reason=None,
            review_flag=review_flag,
            failure_category=None,
            negative_feedback_count=0,
            reviewed_by=None,
            reviewed_at=None,
            is_deleted=False,
            deleted_at=None,
            superseded_by_report_id=None,
        )
        db_session.add(report)
        db_session.flush()

        db_session.execute(
            citation_table.insert().values(
                citation_id=str(uuid4()),
                report_id=report.report_id,
                citation_order=1,
                source_name="eastmoney",
                source_url="https://example.com/source",
                fetch_time=now,
                title="行情快照",
                excerpt="价格与成交量保持稳健。",
            )
        )
        db_session.execute(
            instruction_card_table.insert().values(
                instruction_card_id=str(uuid4()),
                report_id=report.report_id,
                signal_entry_price=123.45,
                atr_pct=0.032,
                atr_multiplier=1.5,
                stop_loss=117.28,
                target_price=138.88,
                stop_loss_calc_mode="atr_multiplier",
            )
        )
        usage_id = str(uuid4())
        batch_table = Base.metadata.tables["data_batch"]
        batch_exists = db_session.execute(
            batch_table.select().where(batch_table.c.batch_id == "batch-001")
        ).first()
        if not batch_exists:
            db_session.execute(
                batch_table.insert().values(
                    batch_id="batch-001",
                    source_name="tdx_local",
                    trade_date=actual_trade_date,
                    batch_scope="core_pool",
                    batch_seq=1,
                    batch_status="SUCCESS",
                    quality_flag="ok",
                    covered_stock_count=1,
                    core_pool_covered_count=1,
                    records_total=1,
                    records_success=1,
                    records_failed=0,
                    status_reason=None,
                    trigger_task_run_id=None,
                    started_at=now,
                    finished_at=now,
                    updated_at=now,
                    created_at=now,
                )
            )
        db_session.execute(
            usage_table.insert().values(
                usage_id=usage_id,
                trade_date=actual_trade_date,
                stock_code=stock_code,
                dataset_name="kline_daily",
                source_name="tdx_local",
                batch_id="batch-001",
                fetch_time=now,
                status="ok",
                status_reason=None,
            )
        )
        db_session.execute(
            usage_link_table.insert().values(
                report_data_usage_link_id=str(uuid4()),
                report_id=report.report_id,
                usage_id=usage_id,
            )
        )
        for capital_tier, position_ratio in (("10k", 0.1), ("100k", 0.2), ("500k", 0.3)):
            db_session.execute(
                trade_instruction_table.insert().values(
                    trade_instruction_id=str(uuid4()),
                    report_id=report.report_id,
                    capital_tier=capital_tier,
                    status="EXECUTE",
                    position_ratio=position_ratio,
                    skip_reason=None,
                )
            )
        db_session.execute(
            sim_position_table.insert().values(
                position_id=str(uuid4()),
                report_id=report.report_id,
                stock_code=stock_code,
                capital_tier="100k",
                position_status="OPEN",
                signal_date=actual_trade_date,
                entry_date=actual_trade_date,
                actual_entry_price=123.45,
                signal_entry_price=123.45,
                position_ratio=0.2,
                shares=100,
                atr_pct_snapshot=0.032,
                atr_multiplier_snapshot=1.5,
                stop_loss_price=117.28,
                target_price=138.88,
                exit_date=None,
                exit_price=None,
                holding_days=0,
                net_return_pct=None,
                commission_total=1.2,
                stamp_duty=0,
                slippage_total=0.8,
                skip_reason=None,
                status_reason=None,
            )
        )

        db_session.commit()
        db_session.refresh(report)
        return report

    return _seed


@pytest.fixture()
def seed_report_bundle(db_session):
    from app.services.trade_calendar import latest_trade_date_str
    from tests.helpers_ssot import insert_open_position, insert_report_bundle_ssot

    def _seed(
        *,
        stock_code: str = "600519.SH",
        stock_name: str = "贵州茅台",
        trade_date: str | None = None,
        recommendation: str = "BUY",
        strategy_type: str = "A",
        market_state: str = "BULL",
        quality_flag: str = "ok",
        published: bool = True,
        publish_status: str | None = None,
        review_flag: str = "APPROVED",
    ):
        actual_trade_date_str = trade_date or latest_trade_date_str()
        report = insert_report_bundle_ssot(
            db_session,
            stock_code=stock_code,
            stock_name=stock_name,
            trade_date=actual_trade_date_str,
            recommendation=recommendation,
            strategy_type=strategy_type,
            market_state=market_state,
            quality_flag=quality_flag,
            published=published,
            review_flag=review_flag,
        )
        if publish_status and publish_status != report.publish_status:
            report.publish_status = publish_status
            report.published = publish_status == "PUBLISHED"
            report.published_at = utc_now() if report.published else None
            db_session.commit()
            db_session.refresh(report)

        insert_open_position(
            db_session,
            report_id=report.report_id,
            stock_code=stock_code,
            capital_tier="100k",
            signal_date=actual_trade_date_str,
            entry_date=actual_trade_date_str,
            actual_entry_price=123.45,
            signal_entry_price=123.45,
            position_ratio=0.2,
            shares=100,
            atr_pct_snapshot=0.032,
            atr_multiplier_snapshot=1.5,
            stop_loss_price=117.28,
            target_price=138.88,
        )
        return report

    return _seed
