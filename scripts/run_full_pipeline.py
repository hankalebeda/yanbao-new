"""
完整端到端流水线：拉数据 → 结算 → 生研报 → 模拟交易 → 权益曲线 → 看板快照
让系统真正跑起来、达到可用发布标准
"""
import os, sys, logging, time, math
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4
from pathlib import Path

os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("MOCK_LLM", "false")
os.environ.setdefault("DATABASE_URL", "sqlite:///D:/yanbao/data/app.db")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("pipeline")

from sqlalchemy import text
from app.core.db import SessionLocal
from app.models import Base


def utcnow():
    return datetime.now(timezone.utc)


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1: 拉取最新 K 线数据 (akshare)
# ═══════════════════════════════════════════════════════════════════════════════

def step1_pull_kline(db, target_dates: list[date]):
    """从 akshare 拉取核心池股票的最新 K 线数据"""
    logger.info("=" * 70)
    logger.info("STEP 1: 拉取最新 K 线数据")
    logger.info("=" * 70)

    import akshare as ak

    # 获取核心池 + 有持仓的股票
    core_codes = [
        r[0] for r in db.execute(text(
            "SELECT stock_code FROM stock_pool_snapshot WHERE pool_role = 'core'"
        )).fetchall()
    ]
    position_codes = [
        r[0] for r in db.execute(text(
            "SELECT DISTINCT stock_code FROM sim_position WHERE position_status = 'OPEN'"
        )).fetchall()
    ]
    all_codes = list(set(core_codes + position_codes))
    logger.info("需要拉取 %d 只股票 (核心池=%d, 持仓=%d)", len(all_codes), len(core_codes), len(position_codes))

    # 确定日期范围
    min_date = min(target_dates)
    max_date = max(target_dates)
    start_str = (min_date - timedelta(days=5)).strftime("%Y%m%d")  # 多拉几天确保覆盖
    end_str = max_date.strftime("%Y%m%d")

    success = 0
    fail = 0
    new_rows = 0

    for i, code in enumerate(all_codes):
        if (i + 1) % 20 == 0:
            logger.info("  进度: %d/%d (成功=%d, 失败=%d, 新K线=%d)",
                        i + 1, len(all_codes), success, fail, new_rows)

        try:
            # 解析纯数字代码
            pure_code = code.split(".")[0]

            df = ak.stock_zh_a_hist(
                symbol=pure_code, period="daily",
                start_date=start_str, end_date=end_str, adjust="qfq"
            )
            if df is None or df.empty:
                fail += 1
                continue

            # 写入 kline_daily
            for _, row in df.iterrows():
                td_str = str(row["日期"])[:10]

                # 检查是否已存在
                existing = db.execute(text(
                    "SELECT 1 FROM kline_daily WHERE stock_code = :c AND trade_date = :d LIMIT 1"
                ), {"c": code, "d": td_str}).fetchone()
                if existing:
                    continue

                open_p = float(row.get("开盘", 0) or 0)
                close_p = float(row.get("收盘", 0) or 0)
                high_p = float(row.get("最高", 0) or 0)
                low_p = float(row.get("最低", 0) or 0)
                volume = int(row.get("成交量", 0) or 0)
                amount = float(row.get("成交额", 0) or 0)
                turnover = float(row.get("换手率", 0) or 0)
                pct_chg = float(row.get("涨跌幅", 0) or 0)

                # 判断是否停牌
                is_suspended = (volume == 0 and amount == 0)

                # 计算派生因子（简化版）
                # ATR - 需要前一日数据
                prev_close_row = db.execute(text(
                    "SELECT close FROM kline_daily WHERE stock_code = :c AND trade_date < :d ORDER BY trade_date DESC LIMIT 1"
                ), {"c": code, "d": td_str}).fetchone()
                prev_close = prev_close_row[0] if prev_close_row else close_p

                tr = max(high_p - low_p, abs(high_p - prev_close), abs(low_p - prev_close))
                atr_pct = (tr / close_p * 100) if close_p > 0 else 0

                # MA20 - 取最近20日收盘价平均
                ma_rows = db.execute(text(
                    "SELECT close FROM kline_daily WHERE stock_code = :c AND trade_date <= :d ORDER BY trade_date DESC LIMIT 20"
                ), {"c": code, "d": td_str}).fetchall()
                closes = [r[0] for r in ma_rows] + [close_p]
                ma20 = sum(closes[:20]) / min(len(closes), 20) if closes else close_p

                batch_id = str(uuid4())
                db.execute(text("""
                    INSERT INTO kline_daily (
                        kline_id, stock_code, trade_date,
                        open, high, low, close, volume, amount,
                        adjust_type, turnover_rate, is_suspended,
                        ma20, atr_pct, source_batch_id,
                        created_at
                    ) VALUES (
                        :kid, :code, :td,
                        :open, :high, :low, :close, :vol, :amt,
                        :adj, :turnover, :suspended,
                        :ma20, :atr, :batch_id,
                        :now
                    )
                """), {
                    "kid": str(uuid4()), "code": code, "td": td_str,
                    "open": open_p, "high": high_p, "low": low_p, "close": close_p,
                    "vol": volume, "amt": amount,
                    "adj": "qfq", "turnover": turnover,
                    "suspended": is_suspended,
                    "ma20": round(ma20, 4), "atr": round(atr_pct, 4),
                    "batch_id": batch_id,
                    "now": utcnow().isoformat(),
                })
                new_rows += 1

            success += 1

        except Exception as e:
            fail += 1
            if (i + 1) <= 5:  # 只记录前几个错误
                logger.warning("  %s 拉取失败: %s", code, e)

        # akshare rate limiting
        if (i + 1) % 10 == 0:
            time.sleep(0.3)

    db.commit()
    logger.info("K 线拉取完成: 成功=%d, 失败=%d, 新增行=%d", success, fail, new_rows)

    # 清除交易日历缓存，让新日期生效
    from app.services.trade_calendar import clear_trade_calendar_cache
    clear_trade_calendar_cache()

    # 验证
    for td in target_dates:
        cnt = db.execute(text(
            "SELECT COUNT(*) FROM kline_daily WHERE trade_date = :d"
        ), {"d": td.isoformat()}).fetchone()[0]
        logger.info("  %s 的 K 线数: %d", td.isoformat(), cnt)

    return {"success": success, "fail": fail, "new_rows": new_rows}


# ═══════════════════════════════════════════════════════════════════════════════
# Step 2: 刷新市场状态
# ═══════════════════════════════════════════════════════════════════════════════

def step2_market_state(db, trade_date: date | None = None):
    logger.info("=" * 70)
    logger.info("STEP 2: 刷新市场状态")
    logger.info("=" * 70)

    from app.services.scheduler import calc_and_cache_market_state
    state = calc_and_cache_market_state(trade_date=trade_date)
    logger.info("市场状态: %s", state)
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# Step 3: 运行结算链 — 平仓 + 权益曲线 (对每个新交易日)
# ═══════════════════════════════════════════════════════════════════════════════

def step3_settlement_and_sim(db, trade_date: date):
    logger.info("=" * 70)
    logger.info("STEP 3: 结算与模拟交易 trade_date=%s", trade_date)
    logger.info("=" * 70)

    td_str = trade_date.isoformat()

    # 3a: 运行 sim positioning (平仓 + 开仓 + 权益曲线)
    from app.services.sim_positioning_ssot import process_trade_date
    from app.services.runtime_materialization import (
        materialize_sim_dashboard_snapshots,
        ensure_sim_accounts,
    )

    ensure_sim_accounts(db)
    process_trade_date(db, td_str)
    logger.info("  模拟交易处理完成")

    # 3b: 运行结算 (settlement_result)
    from app.services.settlement_ssot import submit_settlement_task
    for window_days in (1, 7, 14, 30, 60):
        try:
            submit_settlement_task(
                db, trade_date=td_str, window_days=window_days,
                target_scope="all", force=False,
            )
        except Exception as e:
            logger.warning("  结算 window=%d 失败: %s", window_days, e)
    db.commit()

    # 3c: 看板快照
    materialize_sim_dashboard_snapshots(db, snapshot_date=td_str)
    db.commit()
    logger.info("  看板快照已更新")

    # 打印当前状态
    for tier in ("10k", "100k", "500k"):
        acct = db.execute(text(
            "SELECT total_asset, cash_available, max_drawdown_pct, active_position_count FROM sim_account WHERE capital_tier = :t"
        ), {"t": tier}).mappings().first()
        if acct:
            logger.info("  账户 %s: 总资产=%.2f, 现金=%.2f, 最大回撤=%.2f%%, 持仓数=%d",
                        tier, acct["total_asset"], acct["cash_available"],
                        acct["max_drawdown_pct"] * 100, acct["active_position_count"])

    # 统计持仓变化
    open_cnt = db.execute(text(
        "SELECT COUNT(*) FROM sim_position WHERE position_status = 'OPEN'"
    )).scalar()
    closed_cnt = db.execute(text(
        "SELECT COUNT(*) FROM sim_position WHERE position_status != 'OPEN'"
    )).scalar()
    logger.info("  持仓状态: OPEN=%d, 已平仓=%d", open_cnt, closed_cnt)

    # 权益曲线点数
    eq_cnt = db.execute(text(
        "SELECT COUNT(*) FROM sim_equity_curve_point WHERE trade_date = :d"
    ), {"d": td_str}).scalar()
    logger.info("  权益曲线点: %d", eq_cnt)


# ═══════════════════════════════════════════════════════════════════════════════
# Step 4: 数据采集 (report_data_usage)
# ═══════════════════════════════════════════════════════════════════════════════

def step4_ingest_data(db, trade_date: date):
    logger.info("=" * 70)
    logger.info("STEP 4: 数据采集 trade_date=%s", trade_date)
    logger.info("=" * 70)

    from app.services.multisource_ingest import ingest_market_data
    from app.services.stock_pool import get_daily_stock_pool

    core_codes = get_daily_stock_pool(trade_date=trade_date, tier=1)
    logger.info("核心池: %d 只", len(core_codes))

    # Northbound fetcher
    def fetch_nb(td):
        try:
            import akshare as ak
            df = ak.stock_hsgt_hist_em(symbol="沪股通")
            if df is not None and len(df) > 0:
                latest = df.iloc[-1]
                return {
                    "status": "ok",
                    "reason": "akshare_hsgt_hist",
                    "net_inflow_1d": float(latest.get("当日资金流入", 0) or 0),
                    "history_records": len(df),
                }
        except Exception as e:
            logger.warning("北向资金获取失败: %s", e)
        return {"status": "missing", "reason": "fetch_failed"}

    # ETF flow fetcher
    def fetch_etf(td):
        from app.services.etf_flow_data import fetch_etf_flow_summary_global
        return fetch_etf_flow_summary_global(td)

    result = ingest_market_data(
        db,
        trade_date=trade_date,
        stock_codes=core_codes,
        core_pool_codes=core_codes,
        fetch_kline_history=None,  # K 线已由 Step 1 灌入
        fetch_hotspot_by_source=None,
        fetch_northbound_summary=fetch_nb,
        fetch_etf_flow_summary=fetch_etf,
    )
    db.commit()
    logger.info("数据采集完成: quality=%s", result.get("quality_flag"))
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Step 5: 生成研报
# ═══════════════════════════════════════════════════════════════════════════════

def step5_generate_reports(db, trade_date: date, max_reports: int = 10):
    logger.info("=" * 70)
    logger.info("STEP 5: 生成研报 trade_date=%s (最多 %d 份)", trade_date, max_reports)
    logger.info("=" * 70)

    from app.services.report_generation_ssot import generate_report_ssot

    # 检查是否已有研报
    existing = db.execute(text(
        "SELECT COUNT(*) FROM report WHERE trade_date = :d"
    ), {"d": trade_date.isoformat()}).scalar()
    if existing >= max_reports:
        logger.info("已有 %d 份研报，跳过生成", existing)
        return {"skipped": True, "existing": existing}

    # 取核心池 top N
    pool_codes = [
        r[0] for r in db.execute(text(
            "SELECT stock_code FROM stock_pool_snapshot WHERE pool_role = 'core' ORDER BY rank_no ASC LIMIT :n"
        ), {"n": max_reports}).fetchall()
    ]

    ok, fail_cnt = 0, 0
    buy_cnt, hold_cnt = 0, 0
    for i, code in enumerate(pool_codes, 1):
        # 跳过已生成的
        ex = db.execute(text(
            "SELECT 1 FROM report WHERE stock_code = :c AND trade_date = :d"
        ), {"c": code, "d": trade_date.isoformat()}).fetchone()
        if ex:
            logger.info("  %d/%d %s: 已有研报，跳过", i, len(pool_codes), code)
            continue

        try:
            result = generate_report_ssot(
                db, stock_code=code, trade_date=trade_date.isoformat()
            )
            rec = result.get("recommendation", "?")
            conf = result.get("confidence", "?")
            quality = result.get("quality_flag", "?")
            logger.info("  %d/%d %s: rec=%s conf=%s quality=%s",
                        i, len(pool_codes), code, rec, conf, quality)
            ok += 1
            if rec == "BUY":
                buy_cnt += 1
            else:
                hold_cnt += 1
        except Exception as e:
            logger.error("  %d/%d %s: 生成失败 - %s", i, len(pool_codes), code, e)
            fail_cnt += 1

    logger.info("研报生成完成: 成功=%d (BUY=%d, HOLD=%d), 失败=%d", ok, buy_cnt, hold_cnt, fail_cnt)
    return {"ok": ok, "buy": buy_cnt, "hold": hold_cnt, "fail": fail_cnt}


# ═══════════════════════════════════════════════════════════════════════════════
# Step 6: 写入策略指标快照
# ═══════════════════════════════════════════════════════════════════════════════

def step6_strategy_metrics(db, trade_date: date):
    logger.info("=" * 70)
    logger.info("STEP 6: 策略指标快照 trade_date=%s", trade_date)
    logger.info("=" * 70)

    td_str = trade_date.isoformat()

    # 查询从持仓结算推导的收益数据（优先使用 sim_position 已平仓数据）
    settled_rows = db.execute(text("""
        SELECT
            COALESCE(r.strategy_type, 'B') as strategy_type,
            sp.net_return_pct
        FROM sim_position sp
        LEFT JOIN report r ON sp.report_id = r.report_id
        WHERE sp.position_status NOT IN ('OPEN', 'SKIPPED')
          AND sp.net_return_pct IS NOT NULL
    """)).fetchall()
    if not settled_rows:
        settled_rows = db.execute(text("""
            SELECT sr.strategy_type, sr.net_return_pct
            FROM settlement_result sr
            WHERE sr.settlement_status = 'settled'
        """)).fetchall()

    if not settled_rows:
        logger.info("  暂无已结算记录，生成冷启动快照")

    # 按策略类型分组
    by_type = {"A": [], "B": [], "C": []}
    for row in settled_rows:
        st = str(row[0] or "B")
        if st in by_type:
            by_type[st].append(float(row[1]) if row[1] is not None else 0.0)

    now = utcnow().isoformat()
    for window_days in (30, 60, 90):
        for strategy_type, returns in by_type.items():
            sample_size = len(returns)

            # 检查是否已存在
            existing = db.execute(text("""
                SELECT 1 FROM strategy_metric_snapshot
                WHERE strategy_type = :st AND window_days = :w AND snapshot_date = :d
            """), {"st": strategy_type, "w": window_days, "d": td_str}).fetchone()
            if existing:
                continue

            if sample_size >= 30:
                wins = [r for r in returns if r > 0]
                losses = [abs(r) for r in returns if r < 0]
                win_rate = len(wins) / sample_size
                avg_win = sum(wins) / len(wins) if wins else 0
                avg_loss = sum(losses) / len(losses) if losses else 1
                pnl_ratio = avg_win / avg_loss if avg_loss > 0 else None
                cum_return = sum(returns)
                alpha_annual = (cum_return * 365) / max(window_days, 1)
                max_dd = min(returns) if returns else 0
                display_hint = None
            else:
                win_rate = None
                pnl_ratio = None
                alpha_annual = None
                max_dd = None
                display_hint = "sample_lt_30"

            data_status = "READY" if sample_size >= 30 else "COMPUTING"
            db.execute(text("""
                INSERT INTO strategy_metric_snapshot (
                    metric_snapshot_id, strategy_type, snapshot_date, window_days,
                    data_status, sample_size, coverage_pct,
                    win_rate, profit_loss_ratio, alpha_annual,
                    max_drawdown_pct, cumulative_return_pct,
                    signal_validity_warning, display_hint,
                    created_at
                ) VALUES (
                    :sid, :st, :d, :w,
                    :ds, :ss, :cov,
                    :wr, :plr, :alpha,
                    :mdd, :cum,
                    :svw, :dh,
                    :now
                )
            """), {
                "sid": str(uuid4()), "st": strategy_type,
                "d": td_str, "w": window_days,
                "ds": data_status, "ss": sample_size, "cov": sample_size / max(200, 1),
                "wr": win_rate, "plr": pnl_ratio, "alpha": alpha_annual,
                "mdd": max_dd, "cum": sum(returns) if returns else 0,
                "svw": False, "dh": display_hint,
                "now": now,
            })

    db.commit()
    total = db.execute(text("SELECT COUNT(*) FROM strategy_metric_snapshot")).scalar()
    logger.info("  策略指标快照总数: %d", total)


# ═══════════════════════════════════════════════════════════════════════════════
# Step 7: 最终验证
# ═══════════════════════════════════════════════════════════════════════════════

def step7_final_verify(db):
    logger.info("=" * 70)
    logger.info("STEP 7: 最终验证")
    logger.info("=" * 70)

    checks = {
        "report": db.execute(text("SELECT COUNT(*) FROM report")).scalar(),
        "report_citation": db.execute(text("SELECT COUNT(*) FROM report_citation")).scalar(),
        "instruction_card": db.execute(text("SELECT COUNT(*) FROM instruction_card")).scalar(),
        "sim_trade_instruction": db.execute(text("SELECT COUNT(*) FROM sim_trade_instruction")).scalar(),
        "sim_account": db.execute(text("SELECT COUNT(*) FROM sim_account")).scalar(),
        "sim_position (OPEN)": db.execute(text("SELECT COUNT(*) FROM sim_position WHERE position_status = 'OPEN'")).scalar(),
        "sim_position (已平仓)": db.execute(text("SELECT COUNT(*) FROM sim_position WHERE position_status != 'OPEN'")).scalar(),
        "settlement_result": db.execute(text("SELECT COUNT(*) FROM settlement_result")).scalar(),
        "sim_equity_curve_point": db.execute(text("SELECT COUNT(*) FROM sim_equity_curve_point")).scalar(),
        "baseline_equity_curve_point": db.execute(text("SELECT COUNT(*) FROM baseline_equity_curve_point")).scalar(),
        "strategy_metric_snapshot": db.execute(text("SELECT COUNT(*) FROM strategy_metric_snapshot")).scalar(),
        "sim_dashboard_snapshot": db.execute(text("SELECT COUNT(*) FROM sim_dashboard_snapshot")).scalar(),
        "kline_daily": db.execute(text("SELECT COUNT(*) FROM kline_daily")).scalar(),
        "data_batch": db.execute(text("SELECT COUNT(*) FROM data_batch")).scalar(),
    }

    all_ok = True
    critical_zeros = []
    for k, v in checks.items():
        status = "✅" if v > 0 else "❌"
        if v == 0 and k in ("sim_equity_curve_point", "strategy_metric_snapshot"):
            status = "⚠️"
            critical_zeros.append(k)
        logger.info("  %s %s: %d", status, k, v)

    # 展示最新研报
    latest_reports = db.execute(text("""
        SELECT stock_code, recommendation, confidence, quality_flag, publish_status, trade_date
        FROM report ORDER BY created_at DESC LIMIT 5
    """)).fetchall()
    logger.info("\n  最新 5 份研报:")
    for r in latest_reports:
        logger.info("    %s %s conf=%.2f quality=%s publish=%s date=%s",
                     r[0], r[1], r[2] or 0, r[3], r[4], r[5])

    # 展示权益曲线
    eq_points = db.execute(text("""
        SELECT capital_tier, trade_date, equity, cash_available, position_market_value
        FROM sim_equity_curve_point ORDER BY trade_date DESC, capital_tier ASC LIMIT 9
    """)).fetchall()
    if eq_points:
        logger.info("\n  最新权益曲线点:")
        for r in eq_points:
            logger.info("    %s %s: 权益=%.2f 现金=%.2f 持仓市值=%.2f",
                         r[0], r[1], r[2] or 0, r[3] or 0, r[4] or 0)

    # 持仓分布
    pos_dist = db.execute(text("""
        SELECT position_status, COUNT(*) FROM sim_position GROUP BY position_status
    """)).fetchall()
    logger.info("\n  持仓分布:")
    for r in pos_dist:
        logger.info("    %s: %d", r[0], r[1])

    # 账户状态
    accounts = db.execute(text("""
        SELECT capital_tier, total_asset, cash_available, max_drawdown_pct, active_position_count
        FROM sim_account ORDER BY capital_tier
    """)).mappings().fetchall()
    logger.info("\n  账户状态:")
    for a in accounts:
        logger.info("    %s: 总资产=%.2f 现金=%.2f 最大回撤=%.2f%% 持仓=%d",
                     a["capital_tier"], a["total_asset"], a["cash_available"],
                     (a["max_drawdown_pct"] or 0) * 100, a["active_position_count"])

    if critical_zeros:
        logger.warning("⚠️ 部分关键表为空，但这是因为仍需积累数据")

    return checks


# ═══════════════════════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    logger.info("🚀 研报系统端到端流水线开始执行")
    logger.info("当前时间: %s", datetime.now().isoformat())

    # 确定目标日期
    # kline_daily 最新是 2026-03-10，需要拉 2026-03-11, 2026-03-12
    db = SessionLocal()
    try:
        latest_kline = db.execute(text(
            "SELECT MAX(trade_date) FROM kline_daily"
        )).scalar()
        logger.info("数据库最新 K 线日期: %s", latest_kline)
    finally:
        db.close()

    today = date.today()
    # 从最新 K 线日期的次日开始，到今天
    if latest_kline:
        last_date = date.fromisoformat(str(latest_kline)[:10])
    else:
        last_date = today - timedelta(days=3)

    target_dates = []
    d = last_date + timedelta(days=1)
    while d <= today:
        if d.weekday() < 5:  # 工作日
            target_dates.append(d)
        d += timedelta(days=1)

    if not target_dates:
        target_dates = [today]

    logger.info("目标交易日: %s", [d.isoformat() for d in target_dates])

    # ── Step 1: 拉取 K 线数据
    db = SessionLocal()
    try:
        step1_pull_kline(db, target_dates)
    except Exception as e:
        logger.error("Step 1 失败: %s", e)
        import traceback
        traceback.print_exc()
    finally:
        db.close()

    # 验证交易日历现在能识别新日期
    from app.services.trade_calendar import clear_trade_calendar_cache, is_trade_day, latest_trade_date_str
    clear_trade_calendar_cache()
    logger.info("最新交易日: %s", latest_trade_date_str())

    # ── Step 2: 刷新市场状态
    db = SessionLocal()
    try:
        step2_market_state(db, trade_date=target_dates[-1] if target_dates else None)
    except Exception as e:
        logger.error("Step 2 失败: %s", e)
        import traceback
        traceback.print_exc()
    finally:
        db.close()

    # ── Step 3-5: 按交易日处理
    for trade_date in target_dates:
        logger.info("\n" + "═" * 70)
        logger.info("处理交易日: %s", trade_date.isoformat())
        logger.info("═" * 70)

        # Step 3: 结算现有持仓 + 权益曲线
        db = SessionLocal()
        try:
            step3_settlement_and_sim(db, trade_date)
        except Exception as e:
            logger.error("Step 3 (%s) 失败: %s", trade_date, e)
            import traceback
            traceback.print_exc()
        finally:
            db.close()

    # 只对最新一天(今天)生成新研报
    latest_date = target_dates[-1]

    # Step 4: 数据采集
    db = SessionLocal()
    try:
        step4_ingest_data(db, latest_date)
    except Exception as e:
        logger.error("Step 4 失败: %s", e)
        import traceback
        traceback.print_exc()
    finally:
        db.close()

    # Step 5: 生成研报
    db = SessionLocal()
    try:
        step5_generate_reports(db, latest_date, max_reports=10)
    except Exception as e:
        logger.error("Step 5 失败: %s", e)
        import traceback
        traceback.print_exc()
    finally:
        db.close()

    # 对今天的新研报运行模拟交易（开仓+权益曲线更新）
    db = SessionLocal()
    try:
        step3_settlement_and_sim(db, latest_date)
    except Exception as e:
        logger.error("Step 3 补充 (%s) 失败: %s", latest_date, e)
        import traceback
        traceback.print_exc()
    finally:
        db.close()

    # Step 6: 策略指标快照
    db = SessionLocal()
    try:
        step6_strategy_metrics(db, latest_date)
    except Exception as e:
        logger.error("Step 6 失败: %s", e)
        import traceback
        traceback.print_exc()
    finally:
        db.close()

    # Step 7: 最终验证
    db = SessionLocal()
    try:
        step7_final_verify(db)
    except Exception as e:
        logger.error("Step 7 失败: %s", e)
        import traceback
        traceback.print_exc()
    finally:
        db.close()

    logger.info("\n" + "🏁" * 35)
    logger.info("流水线执行完成！")


if __name__ == "__main__":
    main()
