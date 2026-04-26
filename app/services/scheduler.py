import asyncio
import inspect
import logging
import requests
import time
from datetime import datetime, timezone
from threading import Lock

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import select

from app.core.config import settings
from app.core.db import SessionLocal
from app.models import Base
from app.services.etf_flow_data import fetch_etf_flow_summary_global
from app.services.hotspot import fetch_douyin_hot, fetch_eastmoney_hot, fetch_weibo_hot, infer_event_type, link_topic_to_stock
from app.services.market_state import calc_and_cache_market_state
from app.services.northbound_data import fetch_northbound_summary
from app.services.report_engine import collect_topics
from app.services.report_generation_ssot import generate_report_ssot
from app.services.baseline_service import generate_ma_cross_baseline, generate_random_baseline, settle_baselines
from app.services.notification import check_and_alert_negative_feedback, send_admin_notification
from app.services.strategy_failure import check_and_update_strategy_paused
from app.services.sim_position_service import update_open_prices
from app.services.sim_settle_service import run_settle
from app.services.trade_calendar import is_trade_day, latest_trade_date_str
from app.services.stock_pool import get_daily_stock_pool, refresh_stock_pool
from app.services.ssot_read_model import sim_storage_mode

scheduler = BackgroundScheduler()
logger = logging.getLogger(__name__)

_job_lock = Lock()
_job_running = False


def _market_state_job():
    """每日 09:00：计算市场状态机 BULL/NEUTRAL/BEAR 并写入缓存。"""
    if not is_trade_day():
        logger.info("market_state_job_skipped reason=non_trade_day")
        return
    try:
        state = calc_and_cache_market_state()
        logger.info("market_state_job_done state=%s", state)
    except Exception as exc:
        logger.exception("market_state_job_failed err=%s", exc)


def _sim_open_price_job():
    """每日 09:35：更新 sim_position 的 T+1 开盘价（actual_entry_price）。"""
    if not is_trade_day():
        logger.info("sim_open_price_job_skipped reason=non_trade_day")
        return
    try:
        n = update_open_prices()
        logger.info("sim_open_price_job_done updated=%d", n)
    except Exception as exc:
        logger.exception("sim_open_price_job_failed err=%s", exc)


def _sim_settle_job():
    """每日 15:30：模拟持仓日度结算（止损/止盈/超时平仓，写入 sim_account）。"""
    if not is_trade_day():
        logger.info("sim_settle_job_skipped reason=non_trade_day")
        return
    try:
        result = run_settle()
        logger.info("sim_settle_job_done closed=%d errors=%s", result.get("closed", 0), result.get("errors", []))
        _run_baseline_settle()
        _check_strategy_failure()
        _negative_feedback_alert_job()
        send_admin_notification(
            "sim_settle",
            {"closed": result.get("closed", 0), "trade_date": latest_trade_date_str()},
        )
    except Exception as exc:
        logger.exception("sim_settle_job_failed err=%s", exc)


def _daily_stocks() -> list[str]:
    """Tier-1 股票池（24 §8 每日 15:20 全量）。"""
    return get_daily_stock_pool(name_resolver=None)


def _load_internal_exact_core_pool_codes(trade_date=None, *, allow_same_day_fallback: bool = False) -> list[str]:
    if trade_date is None:
        return get_daily_stock_pool(name_resolver=None)

    task_table = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]
    trade_day = trade_date
    db = SessionLocal()
    try:
        task_row = db.execute(
            select(
                task_table.c.task_id,
                task_table.c.status,
            )
            .where(task_table.c.trade_date == trade_day)
            .order_by(task_table.c.created_at.desc())
        ).mappings().first()
        if not task_row:
            return []

        status = str(task_row.get("status") or "").upper()
        if status == "FALLBACK" and not allow_same_day_fallback:
            return []
        if status not in {"COMPLETED", "FALLBACK"}:
            return []

        rows = db.execute(
            select(snapshot_table.c.stock_code)
            .where(
                snapshot_table.c.refresh_task_id == task_row["task_id"],
                snapshot_table.c.pool_role == "core",
            )
            .order_by(snapshot_table.c.rank_no.asc(), snapshot_table.c.stock_code.asc())
        ).scalars().all()
        return [str(stock_code) for stock_code in rows if stock_code]
    finally:
        db.close()


def _load_stock_name_map(stock_codes: list[str]) -> dict[str, str]:
    if not stock_codes:
        return {}
    db = SessionLocal()
    try:
        stock_master = Base.metadata.tables["stock_master"]
        rows = db.execute(
            select(stock_master.c.stock_code, stock_master.c.stock_name).where(stock_master.c.stock_code.in_(stock_codes))
        ).mappings().all()
        return {
            str(row.get("stock_code") or ""): str(row.get("stock_name") or "")
            for row in rows
            if row.get("stock_code")
        }
    finally:
        db.close()


def _build_scheduler_hotspot_fetcher(*, stock_codes: list[str], stock_name_map: dict[str, str]):
    def _run_hotspot_fetch(coro_factory):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro_factory())
        finally:
            loop.close()

    def fetch_hotspot_by_source(source_name: str, trade_date):
        if source_name == "weibo":
            raw_topics = _run_hotspot_fetch(lambda: fetch_weibo_hot(settings.hourly_collect_top_n))
        elif source_name == "douyin":
            raw_topics = _run_hotspot_fetch(lambda: fetch_douyin_hot(settings.hourly_collect_top_n))
        elif source_name == "eastmoney":
            raw_topics = _run_hotspot_fetch(lambda: fetch_eastmoney_hot(settings.hourly_collect_top_n))
        else:
            return []

        threshold = float(getattr(settings, "hotspot_relevance_threshold", 0.25))
        normalized: list[dict] = []
        for index, topic in enumerate(raw_topics or [], start=1):
            title = str(topic.get("title") or "").strip()
            source_url = str(topic.get("source_url") or "")
            if not title or not source_url.startswith("http"):
                continue

            matched_codes: list[str] = []
            for stock_code in stock_codes:
                link = link_topic_to_stock(
                    title,
                    stock_code,
                    stock_name=stock_name_map.get(stock_code),
                )
                if float(link.get("relevance_score") or 0.0) >= threshold:
                    matched_codes.append(stock_code)
            if not matched_codes:
                continue

            event_type = infer_event_type(title)
            normalized.append(
                {
                    "rank": int(topic.get("rank") or index),
                    "topic_title": title,
                    "source_url": source_url,
                    "fetch_time": topic.get("fetch_time"),
                    "news_event_type": None if event_type == "general" else event_type,
                    "hotspot_tags": [] if event_type == "general" else [event_type],
                    "stock_codes": matched_codes,
                }
            )
        return normalized

    return fetch_hotspot_by_source


def _build_fr04_northbound_summary_fetcher():
    try:
        first_param = next(iter(inspect.signature(fetch_northbound_summary).parameters.values()), None)
    except (TypeError, ValueError):
        first_param = None

    if first_param and any(token in first_param.name.lower() for token in ("trade", "date")):
        return fetch_northbound_summary

    def _missing_summary(trade_date):
        return {
            "status": "missing",
            "reason": "stock_level_fetcher_not_applicable_to_fr04_summary",
            "fetch_time": datetime.now(timezone.utc).isoformat(),
        }

    return _missing_summary


def _run_baseline_generation():
    """E8.2/E8.3 基线生成：每日研报生成后生成随机基线 + MA 金叉基线。"""
    try:
        db = SessionLocal()
        try:
            generate_random_baseline(db, latest_trade_date_str())
            generate_ma_cross_baseline(db, latest_trade_date_str())
        finally:
            db.close()
    except Exception as exc:
        logger.warning("baseline_generation_failed err=%s", exc)


def _run_baseline_settle():
    """E8 基线结算：结算到期的随机/MA 基线持仓。"""
    try:
        db = SessionLocal()
        try:
            settle_baselines(db, latest_trade_date_str())
        finally:
            db.close()
    except Exception as exc:
        logger.warning("baseline_settle_failed err=%s", exc)


def _check_strategy_failure():
    """E8.5 策略失效监测：滚动胜率低于 MA 基线或连续净亏时告警/暂停（12 §10.2）。"""
    try:
        db = SessionLocal()
        try:
            paused = check_and_update_strategy_paused(db)
            if paused:
                logger.info("strategy_failure_paused types=%s", paused)
        finally:
            db.close()
    except Exception as exc:
        logger.warning("strategy_failure_check_failed err=%s", exc)


def _negative_feedback_alert_job():
    """FR-07：每日检查 7 日内负反馈率，≥30% 触发 ReportHighNegativeFeedback S2 告警（01 §2.8、06 §8）。"""
    try:
        db = SessionLocal()
        try:
            check_and_alert_negative_feedback(db)
        finally:
            db.close()
    except Exception as exc:
        logger.warning("negative_feedback_alert_job_failed err=%s", exc)


async def _run_one_stock(stock_code: str):
    db = SessionLocal()
    try:
        weibo_topics = await fetch_weibo_hot(settings.hourly_collect_top_n)
        douyin_topics = await fetch_douyin_hot(settings.hourly_collect_top_n)
        collect_topics(db, stock_code=stock_code, raw_topics=weibo_topics + douyin_topics)
        # FR-04 extension: collect non-report supplemental data before report generation
        # (capital_flow, stock_profile, northbound, etf_flow → report_data_usage)
        try:
            from app.services.stock_snapshot_service import collect_non_report_usage
            await collect_non_report_usage(db, stock_code=stock_code)
        except Exception as _nr_exc:
            logger.warning("non_report_collect_failed stock=%s err=%s", stock_code, _nr_exc)
        generate_report_ssot(db, stock_code=stock_code)
    finally:
        db.close()


async def _run_daily_jobs() -> tuple[int, int]:
    """返回 (成功数, 失败数)。"""
    stock_codes = _daily_stocks()
    ok, fail = 0, 0
    for stock_code in stock_codes:
        for attempt in range(settings.scheduler_retry_count + 1):
            try:
                await _run_one_stock(stock_code)
                ok += 1
                break
            except Exception as exc:
                if attempt >= settings.scheduler_retry_count:
                    logger.exception("daily_job_failed stock=%s err=%s", stock_code, exc)
                    fail += 1
                    break
                backoff = settings.scheduler_backoff_base_seconds ** (attempt + 1)
                logger.warning(
                    "daily_job_retry stock=%s attempt=%s backoff=%ss err=%s",
                    stock_code,
                    attempt + 1,
                    backoff,
                    exc,
                )
                await asyncio.sleep(backoff)
    return ok, fail


def _run_tier2_one_stock(stock_code: str):
    """Tier-2 单只研报生成，使用 run_mode='tier2' 以走 BULK_SCREEN（13 §3.1）。"""
    db = SessionLocal()
    try:
        import asyncio
        import app.services.hotspot
        import app.services.report_engine
        weibo = asyncio.run(app.services.hotspot.fetch_weibo_hot(settings.hourly_collect_top_n))
        douyin = asyncio.run(app.services.hotspot.fetch_douyin_hot(settings.hourly_collect_top_n))
        app.services.report_engine.collect_topics(db, stock_code=stock_code, raw_topics=weibo + douyin)
        generate_report_ssot(db, stock_code=stock_code)
    finally:
        db.close()


def _tier2_report_job():
    """每日 17:00：Tier-2 轮转池研报生成，24 §8、13 §3.1。"""
    if not is_trade_day():
        logger.info("tier2_job_skipped reason=non_trade_day")
        return
    pool = get_daily_stock_pool(trade_date=latest_trade_date_str(), tier=2)
    if not pool:
        logger.info("tier2_job_skipped reason=pool_empty")
        return
    ok, fail = 0, 0
    for stock_code in pool:
        for attempt in range(settings.scheduler_retry_count + 1):
            try:
                _run_tier2_one_stock(stock_code)
                ok += 1
                break
            except Exception as exc:
                if attempt >= settings.scheduler_retry_count:
                    logger.exception("tier2_job_failed stock=%s err=%s", stock_code, exc)
                    fail += 1
                    break
                time.sleep(settings.scheduler_backoff_base_seconds ** (attempt + 1))
    logger.info(
        "tier2_job_done pool_size=%d ok=%d fail=%d trade_date=%s",
        len(pool), ok, fail, latest_trade_date_str(),
    )


def _daily_job_entry():
    global _job_running
    with _job_lock:
        if _job_running:
            logger.warning("daily_job_skipped reason=already_running")
            return
        _job_running = True

    logger.info("daily_job_started at %s", datetime.now(timezone.utc).isoformat())
    try:
        if not is_trade_day():
            logger.info("daily_job_skipped reason=non_trade_day trade_date=%s", latest_trade_date_str())
            return
        pool = _daily_stocks()
        pool_size = len(pool)
        ok, fail = asyncio.run(_run_daily_jobs())
        logger.info(
            "daily_job_done pool_size=%d ok=%d fail=%d trade_date=%s",
            pool_size,
            ok,
            fail,
            latest_trade_date_str(),
        )
        send_admin_notification(
            "report_ready",
            {"count": ok, "pool_size": pool_size, "fail": fail, "trade_date": latest_trade_date_str()},
        )
        _run_baseline_generation()
    finally:
        with _job_lock:
            _job_running = False


# ---------------------------------------------------------------------------
# Task run tracking & billing poller
# ---------------------------------------------------------------------------

def _record_task_run(task_name: str, schedule_slot: str, trade_date: str | None = None) -> str:
    """Record task execution start; return task_run_id."""
    from uuid import uuid4
    task_run_id = str(uuid4())
    db = SessionLocal()
    try:
        from app.models import Base
        table = Base.metadata.tables.get("scheduler_task_run")
        if table is not None:
            db.execute(table.insert().values(
                task_run_id=task_run_id,
                task_name=task_name,
                schedule_slot=schedule_slot,
                trade_date=trade_date,
                status="running",
                started_at=datetime.now(timezone.utc),
            ))
            db.commit()
    except Exception:
        logger.debug("_record_task_run: scheduler_task_run table may not exist", exc_info=True)
    finally:
        db.close()
    return task_run_id


def _mark_task_success(task_run_id: str, **extras) -> None:
    """Mark task run as completed successfully."""
    db = SessionLocal()
    try:
        from app.models import Base
        table = Base.metadata.tables.get("scheduler_task_run")
        if table is not None:
            db.execute(table.update().where(table.c.task_run_id == task_run_id).values(
                status="success",
                finished_at=datetime.now(timezone.utc),
            ))
            db.commit()
    except Exception:
        logger.debug("_mark_task_success failed", exc_info=True)
    finally:
        db.close()


def _mark_task_failed(task_run_id: str, **extras) -> None:
    """Mark task run as failed."""
    db = SessionLocal()
    try:
        from app.models import Base
        table = Base.metadata.tables.get("scheduler_task_run")
        if table is not None:
            db.execute(table.update().where(table.c.task_run_id == task_run_id).values(
                status="failed",
                finished_at=datetime.now(timezone.utc),
            ))
            db.commit()
    except Exception:
        logger.debug("_mark_task_failed failed", exc_info=True)
    finally:
        db.close()


def _billing_poller_job() -> None:
    """Poll stale pending billing orders and reconcile."""
    db = SessionLocal()
    try:
        from app.services.membership import reconcile_pending_orders
        provider_status_fetcher = _build_provider_status_fetcher()
        result = reconcile_pending_orders(db, provider_status_fetcher=provider_status_fetcher)
        logger.info("billing_poller_done result=%s", result)
    except Exception:
        logger.exception("billing_poller_job failed")
    finally:
        db.close()


def _build_provider_status_fetcher():
    """Build a provider status fetcher from env vars, or return None."""
    import os
    url = os.environ.get("BILLING_PROVIDER_STATUS_URL", "")
    if not url:
        return None
    token = os.environ.get("BILLING_PROVIDER_STATUS_TOKEN", "")
    timeout = float(os.environ.get("BILLING_PROVIDER_STATUS_TIMEOUT_SECONDS", "5"))

    def _fetcher(order):
        session = requests.Session()
        resp = session.get(
            url,
            params={"order_id": order.order_id, "provider": order.provider},
            headers={"Authorization": f"Bearer {token}"} if token else None,
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()

    return _fetcher


def _cookie_probe_job() -> None:
    """Periodic cookie session health probe."""
    db = SessionLocal()
    task_run_id = None
    try:
        task_run_id = _record_task_run("cookie_probe", "interval_5m")
        from app.services.cookie_session_ssot import run_all_cookie_probes
        run_all_cookie_probes(db)
        _mark_task_success(task_run_id)
    except Exception as exc:
        if task_run_id:
            _mark_task_failed(task_run_id, error=str(exc))
        logger.exception("cookie_probe_job failed")
    finally:
        db.close()


def _startup_catchup(now: datetime | None = None) -> None:
    """Run startup catch-up: if today is a trade day and daily pipeline hasn't run, trigger it."""
    import threading
    _now = now or datetime.now(timezone.utc)
    if not is_trade_day(_now):
        return
    td = latest_trade_date_str(_now)
    logger.info("startup_catchup: trade_date=%s — launching daily pipeline in background thread", td)
    t = threading.Thread(target=_daily_job_entry, daemon=True)
    t.start()


def _handler_fr01_stock_pool(trade_date=None, *, force: bool = False) -> dict:
    """DAG handler: refresh stock pool for a given trade_date."""
    from app.services.stock_pool import refresh_stock_pool
    db = SessionLocal()
    try:
        result = refresh_stock_pool(db, trade_date=trade_date, force_rebuild=force)
        return {
            "trade_date": result["trade_date"],
            "pool_size": result["core_pool_size"],
            "status": result["status"],
        }
    finally:
        db.close()


def _handler_fr06_report_gen(trade_date=None, *, force: bool = False) -> dict:
    """DAG handler: generate reports for all pool stocks with circuit breaker."""
    from datetime import datetime, timezone
    from uuid import uuid4 as _uuid4
    from app.models import Base

    td = trade_date
    if td is None:
        td = latest_trade_date_str()
    if isinstance(td, str):
        from datetime import date as _date
        td = _date.fromisoformat(td)

    pool = get_daily_stock_pool(trade_date=td.isoformat())
    db = SessionLocal()
    try:
        circuit_table = Base.metadata.tables.get("llm_circuit_state")
        task_table = Base.metadata.tables.get("report_generation_task")
        ok = 0
        fail = 0
        suspended = 0
        resumed = 0

        circuit_row = None
        if circuit_table is not None:
            circuit_row = db.execute(
                circuit_table.select().where(circuit_table.c.circuit_name == "report_generation")
            ).mappings().first()

        consecutive_failures = int((circuit_row or {}).get("consecutive_failures", 0)) if circuit_row else 0
        circuit_state = str((circuit_row or {}).get("circuit_state", "CLOSED")).upper() if circuit_row else "CLOSED"
        now = datetime.now(timezone.utc)

        # Ensure a refresh task exists for this trade_date (needed for FK)
        refresh_task_table = Base.metadata.tables.get("stock_pool_refresh_task")
        refresh_task_id = None
        if refresh_task_table is not None:
            rt = db.execute(
                refresh_task_table.select().where(refresh_task_table.c.trade_date == td)
            ).mappings().first()
            if rt:
                refresh_task_id = str(rt["task_id"])
            else:
                refresh_task_id = str(_uuid4())
                db.execute(
                    refresh_task_table.insert().values(
                        task_id=refresh_task_id,
                        trade_date=td,
                        status="COMPLETED",
                        pool_version=1,
                        fallback_from=None,
                        filter_params_json={"target_pool_size": len(pool)},
                        core_pool_size=len(pool),
                        standby_pool_size=0,
                        evicted_stocks_json=[],
                        status_reason=None,
                        request_id=str(_uuid4()),
                        started_at=now,
                        finished_at=now,
                        updated_at=now,
                        created_at=now,
                    )
                )

        # Half-open check: if cooldown expired, allow one probe before suspending
        cooldown_until = None
        if circuit_row and circuit_state == "OPEN":
            cooldown_until = circuit_row.get("cooldown_until")
        half_open = False
        if circuit_state == "OPEN" and cooldown_until is not None:
            # Normalize to offset-aware for comparison
            if hasattr(cooldown_until, 'tzinfo') and cooldown_until.tzinfo is None:
                cooldown_until = cooldown_until.replace(tzinfo=timezone.utc)
            half_open = cooldown_until <= now

        for stock_code in pool:
            if circuit_state == "OPEN" and consecutive_failures >= 5 and not half_open:
                # Suspend remaining tasks
                if task_table is not None:
                    existing = db.execute(
                        task_table.select().where(
                            task_table.c.trade_date == td,
                            task_table.c.stock_code == stock_code,
                        )
                    ).mappings().first()
                    if not existing:
                        db.execute(
                            task_table.insert().values(
                                task_id=str(_uuid4()),
                                trade_date=td,
                                stock_code=stock_code,
                                idempotency_key=f"daily:{stock_code}:{td.isoformat()}",
                                generation_seq=1,
                                status="Suspended",
                                retry_count=0,
                                quality_flag="ok",
                                status_reason="LLM_CIRCUIT_BREAKER",
                                llm_fallback_level="failed",
                                risk_audit_status="not_triggered",
                                risk_audit_skip_reason="llm_circuit_open",
                                market_state_trade_date=td,
                                refresh_task_id=refresh_task_id,
                                trigger_task_run_id=None,
                                request_id=str(_uuid4()),
                                superseded_by_task_id=None,
                                superseded_at=None,
                                queued_at=now,
                                started_at=None,
                                finished_at=None,
                                updated_at=now,
                                created_at=now,
                            )
                        )
                suspended += 1
                continue

            # Try half-open recovery for suspended tasks
            if circuit_state == "OPEN":
                existing_suspended = None
                if task_table is not None:
                    existing_suspended = db.execute(
                        task_table.select().where(
                            task_table.c.trade_date == td,
                            task_table.c.stock_code == stock_code,
                            task_table.c.status == "Suspended",
                        )
                    ).mappings().first()

            try:
                # FR-04 extension: collect supplemental non-report data before generation
                try:
                    from app.services.stock_snapshot_service import collect_non_report_usage_sync
                    collect_non_report_usage_sync(db, stock_code=stock_code, trade_date=td.isoformat())
                except Exception as _nr_exc:
                    logger.warning("non_report_collect_failed stock=%s err=%s", stock_code, _nr_exc)
                from app.services.report_generation_ssot import generate_report_ssot as _gen
                result = _gen(db, stock_code=stock_code, trade_date=td.isoformat(),
                              resume_active_task=True, skip_pool_check=True)
                is_failed = str(result.get("llm_fallback_level", "")).lower() == "failed"
                if is_failed:
                    consecutive_failures += 1
                    fail += 1
                    half_open = False  # probe failed, revert to fully OPEN
                else:
                    consecutive_failures = 0
                    ok += 1
                    if circuit_state == "OPEN":
                        circuit_state = "CLOSED"
                        half_open = False
                        resumed += 1
                        # Update the suspended task to Completed
                        if task_table is not None:
                            db.execute(
                                task_table.update().where(
                                    task_table.c.trade_date == td,
                                    task_table.c.stock_code == stock_code,
                                    task_table.c.status == "Suspended",
                                ).values(
                                    status="Completed",
                                    retry_count=task_table.c.retry_count + 1,
                                    llm_fallback_level=result.get("llm_fallback_level", "primary"),
                                    risk_audit_status=result.get("risk_audit_status", "completed"),
                                    risk_audit_skip_reason=result.get("risk_audit_skip_reason"),
                                    finished_at=now,
                                    updated_at=now,
                                )
                            )
            except Exception as _exc:
                logger.warning("fr06_handler_gen_error stock=%s err=%s", stock_code, _exc)
                consecutive_failures += 1
                fail += 1
                half_open = False

            if consecutive_failures >= 5 and circuit_state != "OPEN":
                circuit_state = "OPEN"

        # Update circuit state
        if circuit_table is not None:
            if circuit_row:
                db.execute(
                    circuit_table.update().where(
                        circuit_table.c.circuit_name == "report_generation"
                    ).values(
                        circuit_state=circuit_state,
                        consecutive_failures=consecutive_failures,
                        opened_at=now if circuit_state == "OPEN" else circuit_row.get("opened_at"),
                        updated_at=now,
                    )
                )
            else:
                db.execute(
                    circuit_table.insert().values(
                        circuit_name="report_generation",
                        circuit_state=circuit_state,
                        consecutive_failures=consecutive_failures,
                        opened_at=now if circuit_state == "OPEN" else None,
                        updated_at=now,
                        created_at=now,
                    )
                )
        db.commit()

        send_admin_notification(
            "report_ready",
            {"count": ok, "pool_size": len(pool), "fail": fail, "trade_date": td.isoformat()},
        )
        return {
            "ok": ok,
            "fail": fail,
            "suspended": suspended,
            "resumed": resumed,
            "trade_date": td.isoformat(),
        }
    finally:
        db.close()


def _handler_fr05_market_state(trade_date=None, *, force: bool = False) -> dict:
    """DAG handler: compute and persist market state for a given trade_date."""
    result_state = calc_and_cache_market_state(trade_date=trade_date)
    return {"market_state": result_state}


def _handler_fr05_non_report_truth_materialize(trade_date=None, *, force: bool = False) -> dict:
    """DAG handler: materialize non-report truth-layer usage for the latest core pool."""
    from app.services.stock_snapshot_service import materialize_non_report_usage_for_pool

    core_codes = _load_internal_exact_core_pool_codes(trade_date)
    if not core_codes:
        probe_db = SessionLocal()
        try:
            refresh_stock_pool(probe_db, trade_date=trade_date, force_rebuild=force)
        except Exception as exc:
            logger.warning("fr05_truth_materialize_pool_refresh_failed trade_date=%s err=%s", trade_date, exc)
        finally:
            probe_db.close()
        core_codes = _load_internal_exact_core_pool_codes(trade_date)
        if not core_codes:
            return {"quality_flag": "error", "status_reason": "dependency_not_ready"}

    db = SessionLocal()
    try:
        return materialize_non_report_usage_for_pool(
            db,
            stock_codes=core_codes,
            trade_date=trade_date,
        )
    finally:
        db.close()


def _handler_fr04_data_collect(trade_date=None, *, force: bool = False) -> dict:
    """FR-04: Multi-source data collection with TDX/Eastmoney fallback."""
    from app.services.multisource_ingest import ingest_market_data
    from app.services.market_data import fetch_recent_klines
    from app.services.tdx_local_data import load_tdx_day_records

    core_codes = _load_internal_exact_core_pool_codes(trade_date)
    if not core_codes:
        db = SessionLocal()
        try:
            refresh_stock_pool(db, trade_date=trade_date, force_rebuild=force)
        except Exception as exc:
            logger.warning("fr04_pool_refresh_probe_failed trade_date=%s err=%s", trade_date, exc)
        finally:
            db.close()
        core_codes = _load_internal_exact_core_pool_codes(trade_date)
        if not core_codes:
            return {"quality_flag": "error", "status_reason": "dependency_not_ready"}

    stock_name_map = _load_stock_name_map(core_codes)
    fetch_hotspot_by_source = _build_scheduler_hotspot_fetcher(
        stock_codes=core_codes,
        stock_name_map=stock_name_map,
    )

    tdx_iso = trade_date.isoformat() if hasattr(trade_date, "isoformat") else str(trade_date)
    use_eastmoney = False
    for sc in core_codes:
        recs = load_tdx_day_records(sc)
        if not recs or str(recs[-1].get("trade_date", recs[-1].get("date", "")))[:10] != tdx_iso:
            use_eastmoney = True
            break

    kline_source_name = "eastmoney" if use_eastmoney else "tdx_local"

    def fetch_kline_history(stock_code, td):
        if not use_eastmoney:
            return load_tdx_day_records(stock_code) or []
        loop = asyncio.new_event_loop()
        try:
            rows = loop.run_until_complete(fetch_recent_klines(stock_code, limit=120))
        finally:
            loop.close()
        td_str = td.isoformat() if hasattr(td, "isoformat") else str(td)
        return [
            {
                "trade_date": str(r.get("date", ""))[:10],
                "open": r.get("open"),
                "high": r.get("high"),
                "low": r.get("low"),
                "close": r.get("close"),
                "volume": r.get("volume"),
                "amount": r.get("amount"),
            }
            for r in rows
            if str(r.get("date", ""))[:10] <= td_str
        ]

    db = SessionLocal()
    try:
        return ingest_market_data(
            db,
            trade_date=trade_date,
            stock_codes=core_codes,
            core_pool_codes=core_codes,
            kline_source_name=kline_source_name,
            fetch_kline_history=fetch_kline_history,
            fetch_hotspot_by_source=fetch_hotspot_by_source,
            fetch_northbound_summary=_build_fr04_northbound_summary_fetcher(),
            fetch_etf_flow_summary=fetch_etf_flow_summary_global,
        )
    finally:
        db.close()


def _handler_fr07_settlement(trade_date=None, *, force: bool = False) -> dict:
    """DAG handler: trigger settlement pipeline for trade_date."""
    from app.services.settlement_ssot import submit_settlement_batch, wait_for_settlement_pipeline
    db = SessionLocal()
    try:
        mode = sim_storage_mode(db)
        submitted = submit_settlement_batch(db, trade_date=str(trade_date) if trade_date else None, force=force)
        pipeline = wait_for_settlement_pipeline(trade_date=str(trade_date) if trade_date else None)
        return {"accepted": True, "mode": mode, "submitted": len(submitted), "pipeline_status": pipeline.get("pipeline_status")}
    finally:
        db.close()


def start_scheduler():
    if not scheduler.running:
        scheduler.add_job(
            _daily_job_entry,
            "cron",
            hour=15,
            minute=20,
            day_of_week="mon-fri",
            timezone="Asia/Shanghai",
            id="dag_daily_chain",
            replace_existing=True,
        )
        scheduler.add_job(
            _sim_open_price_job,
            "cron",
            hour=9,
            minute=35,
            day_of_week="mon-fri",
            timezone="Asia/Shanghai",
            id="sim_open_price",
            replace_existing=True,
        )
        scheduler.add_job(
            _billing_poller_job,
            "interval",
            minutes=10,
            id="billing_poller",
            replace_existing=True,
        )
        scheduler.add_job(
            _negative_feedback_alert_job,
            "cron",
            hour=18,
            minute=0,
            day_of_week="mon-fri",
            timezone="Asia/Shanghai",
            id="tier_expiry_sweep",
            replace_existing=True,
        )
        scheduler.add_job(
            _check_strategy_failure,
            "cron",
            hour=16,
            minute=0,
            day_of_week="mon-fri",
            timezone="Asia/Shanghai",
            id="daily_cleanup",
            replace_existing=True,
        )
        scheduler.add_job(
            _cookie_probe_job,
            "interval",
            minutes=5,
            id="cookie_probe",
            replace_existing=True,
        )
        scheduler.start()
        _startup_catchup()


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)


# Re-export for backward compatibility
from app.services.cleanup_service import run_cleanup  # noqa: E402, F401


def _daily_cleanup_job() -> None:
    """Scheduled daily cleanup job."""
    db = SessionLocal()
    try:
        run_cleanup(db)
    finally:
        db.close()
