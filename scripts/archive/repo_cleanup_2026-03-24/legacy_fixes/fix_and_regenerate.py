"""
Comprehensive system fix: clean degraded data, re-ingest with all fetchers, regenerate reports.
This script fixes the broken chain:
  degraded report_data_usage → quality_flag=degraded → confidence capped → all HOLD → no positions
"""
import os, sys, asyncio, logging
from datetime import date, datetime
from uuid import uuid4

os.environ.setdefault("DATABASE_URL", "sqlite:///D:/yanbao/data/app.db")
os.environ["MOCK_LLM"] = "false"
os.environ["NO_PROXY"] = "*"

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TRADE_DATE = date(2026, 3, 10)
TRADE_DATE_STR = "2026-03-10"


def get_db():
    from app.core.db import SessionLocal
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def main():
    from sqlalchemy import text
    from app.core.db import SessionLocal

    db = SessionLocal()
    try:
        # ── Step 1: Clean old degraded data ────────────────────────────────
        logger.info("=" * 60)
        logger.info("Step 1: Cleaning old degraded data")
        logger.info("=" * 60)

        # Get report stock codes
        report_codes = [
            r[0] for r in db.execute(text("SELECT stock_code FROM report")).fetchall()
        ]
        logger.info("Report stock codes: %s", report_codes)

        # Delete old reports and all associated records
        report_ids = [
            r[0]
            for r in db.execute(
                text("SELECT report_id FROM report WHERE trade_date = :td"),
                {"td": TRADE_DATE},
            ).fetchall()
        ]
        if report_ids:
            for rid in report_ids:
                db.execute(text("DELETE FROM report_citation WHERE report_id = :rid"), {"rid": rid})
                db.execute(text("DELETE FROM report_data_usage_link WHERE report_id = :rid"), {"rid": rid})
                db.execute(text("DELETE FROM sim_trade_instruction WHERE report_id = :rid"), {"rid": rid})
                db.execute(text("DELETE FROM instruction_card WHERE report_id = :rid"), {"rid": rid})
            id_list = ",".join(f"'{rid}'" for rid in report_ids)
            db.execute(text(f"DELETE FROM report WHERE report_id IN ({id_list})"))
            db.execute(text(f"DELETE FROM report_generation_task WHERE trade_date = :td"), {"td": TRADE_DATE})
            logger.info("Deleted %d old reports and related records", len(report_ids))

        # Delete ALL degraded report_data_usage for trade_date
        cnt = db.execute(
            text("DELETE FROM report_data_usage WHERE trade_date = :td AND status IN ('degraded', 'missing')"),
            {"td": TRADE_DATE},
        ).rowcount
        logger.info("Deleted %d degraded/missing report_data_usage records", cnt)

        db.commit()

        # ── Step 2: Re-ingest data with ALL fetchers ──────────────────────
        logger.info("=" * 60)
        logger.info("Step 2: Re-ingesting data with all fetchers")
        logger.info("=" * 60)

        from app.services.multisource_ingest import ingest_market_data
        from app.services.stock_pool import get_daily_stock_pool

        core_codes = get_daily_stock_pool(trade_date=TRADE_DATE, tier=1)
        logger.info("Core pool: %d stocks", len(core_codes))

        # ── Northbound fetcher
        def fetch_northbound_global(target_date):
            try:
                from app.services.northbound_data import bypass_proxy
                bypass_proxy()
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
                logger.warning("northbound_global_fetch_err: %s", e)
            return {"status": "missing", "reason": "fetch_failed"}

        # ── Hotspot fetcher
        from app.services.hotspot import fetch_weibo_hot, fetch_douyin_hot

        def fetch_hotspot_by_source(source_name, target_date=None):
            async def _inner():
                if source_name == "weibo":
                    return await fetch_weibo_hot(50)
                if source_name == "douyin":
                    return await fetch_douyin_hot(50)
                return []
            raw = asyncio.run(_inner())
            mapped = []
            for item in raw:
                mapped.append({
                    "topic_title": item.get("title") or item.get("topic_title") or "",
                    "source_url": item.get("source_url") or "",
                    "rank": item.get("rank"),
                    "source_rank": item.get("rank"),
                    "fetch_time": item.get("fetch_time"),
                    "news_event_type": item.get("news_event_type"),
                    "hotspot_tags": item.get("hotspot_tags", []),
                    "stock_codes": item.get("stock_codes", []),
                })
            return mapped

        # ── ETF flow fetcher (real implementation via akshare)
        def fetch_etf_summary(td):
            from app.services.etf_flow_data import fetch_etf_flow_summary_global
            return fetch_etf_flow_summary_global(td)

        result = ingest_market_data(
            db,
            trade_date=TRADE_DATE,
            stock_codes=[],
            core_pool_codes=core_codes,
            fetch_kline_history=None,  # kline already in DB, skip
            fetch_hotspot_by_source=fetch_hotspot_by_source,
            fetch_northbound_summary=fetch_northbound_global,
            fetch_etf_flow_summary=fetch_etf_summary,
        )
        logger.info(
            "Ingest complete: quality=%s, northbound=%s",
            result.get("quality_flag"),
            result.get("northbound_summary", {}).get("status"),
        )

        # ── Step 3: Verify data quality for 10 stocks ─────────────────────
        logger.info("=" * 60)
        logger.info("Step 3: Verify data quality")
        logger.info("=" * 60)

        top10_codes = [
            r[0]
            for r in db.execute(
                text("""
                    SELECT s.stock_code
                    FROM stock_pool_snapshot s
                    WHERE s.pool_role = 'core'
                    ORDER BY s.rank_no ASC
                    LIMIT 10
                """)
            ).fetchall()
        ]
        logger.info("Top 10 pool stocks: %s", top10_codes)

        for code in top10_codes:
            rows = db.execute(
                text("""
                    SELECT dataset_name, status, status_reason
                    FROM report_data_usage
                    WHERE stock_code = :c AND trade_date = :td
                """),
                {"c": code, "td": TRADE_DATE},
            ).fetchall()
            statuses = set()
            for r in rows:
                statuses.add(r[1])
            degraded = [r for r in rows if r[1] == "degraded"]
            logger.info(
                "  %s: %d usage records, statuses=%s, degraded=%d",
                code, len(rows), statuses, len(degraded),
            )

        # ── Step 4: Generate 10 reports ────────────────────────────────────
        logger.info("=" * 60)
        logger.info("Step 4: Generating 10 reports")
        logger.info("=" * 60)

        from app.services.report_generation_ssot import generate_report_ssot

        success_count = 0
        buy_count = 0
        for i, code in enumerate(top10_codes, 1):
            try:
                result = generate_report_ssot(
                    db,
                    stock_code=code,
                    trade_date=TRADE_DATE_STR,
                )
                rec = result.get("recommendation", "?")
                conf = result.get("confidence", "?")
                quality = result.get("quality_flag", "?")
                status_reason = str(result.get("status_reason", ""))[:60]
                logger.info(
                    "  %d/%d %s: rec=%s conf=%s quality=%s reason=%s",
                    i, len(top10_codes), code, rec, conf, quality, status_reason,
                )
                success_count += 1
                if rec == "BUY":
                    buy_count += 1
            except Exception as e:
                logger.error("  %d/%d %s: FAILED %s", i, len(top10_codes), code, e)

        logger.info("Generated %d/%d reports, %d BUY signals", success_count, len(top10_codes), buy_count)

        # ── Step 5: Initialize sim_accounts ────────────────────────────────
        logger.info("=" * 60)
        logger.info("Step 5: Initialize sim_accounts")
        logger.info("=" * 60)

        from app.models import SimAccount
        tier_config = {"10k": 100_000, "100k": 1_000_000, "500k": 5_000_000}
        for tier, initial in tier_config.items():
            existing = db.execute(
                text("SELECT count(*) FROM sim_account WHERE capital_tier = :t"),
                {"t": tier},
            ).scalar()
            if existing == 0:
                db.execute(
                    text("""
                        INSERT INTO sim_account 
                        (account_id, capital_tier, snapshot_date, initial_capital, 
                         total_asset, cash, position_value, 
                         open_position_count, settled_count, 
                         win_rate, pnl_ratio, max_drawdown_pct, drawdown_state,
                         created_at, updated_at)
                        VALUES 
                        (:account_id, :tier, :snap_date, :initial, 
                         :initial, :initial, 0, 
                         0, 0, 
                         NULL, NULL, 0.0, 'NORMAL',
                         :now, :now)
                    """),
                    {
                        "account_id": str(uuid4()),
                        "tier": tier,
                        "snap_date": TRADE_DATE,
                        "initial": initial,
                        "now": datetime.utcnow().isoformat(),
                    },
                )
                logger.info("  Created sim_account: %s (%d)", tier, initial)
            else:
                logger.info("  sim_account %s already exists", tier)

        db.commit()

        # ── Step 6: Run settlement (for BUY reports) ──────────────────────
        logger.info("=" * 60)
        logger.info("Step 6: Settlement check")
        logger.info("=" * 60)

        buy_reports = db.execute(
            text("""
                SELECT report_id, stock_code, recommendation, confidence
                FROM report
                WHERE trade_date = :td AND recommendation = 'BUY'
            """),
            {"td": TRADE_DATE},
        ).fetchall()

        if buy_reports:
            logger.info("Found %d BUY reports for settlement chain", len(buy_reports))

            # Check sim_trade_instructions for EXECUTE status
            execute_count = db.execute(
                text("""
                    SELECT count(*) FROM sim_trade_instruction
                    WHERE status = 'EXECUTE'
                """)
            ).scalar()
            logger.info("EXECUTE instructions: %d", execute_count)

            if execute_count > 0:
                # Create sim_positions from EXECUTE instructions
                from app.services.sim_position_service import open_positions_from_instructions
                try:
                    opened = open_positions_from_instructions(db, trade_date=TRADE_DATE)
                    logger.info("Opened %d sim_positions", opened)
                except Exception as e:
                    logger.warning("open_positions error (may be expected): %s", e)
        else:
            logger.info("No BUY reports — settlement chain not applicable yet")

        # ── Step 7: Generate sim_dashboard_snapshot ────────────────────────
        logger.info("=" * 60)
        logger.info("Step 7: Generate sim_dashboard_snapshot")
        logger.info("=" * 60)

        for tier in tier_config:
            account_row = db.execute(
                text("SELECT * FROM sim_account WHERE capital_tier = :t ORDER BY snapshot_date DESC LIMIT 1"),
                {"t": tier},
            ).fetchone()
            if account_row:
                existing_snap = db.execute(
                    text("""
                        SELECT count(*) FROM sim_dashboard_snapshot 
                        WHERE capital_tier = :t AND snapshot_date = :d
                    """),
                    {"t": tier, "d": TRADE_DATE},
                ).scalar()
                if existing_snap == 0:
                    # Get position counts
                    open_count = db.execute(
                        text("SELECT count(*) FROM sim_position WHERE capital_tier = :t AND status = 'OPEN'"),
                        {"t": tier},
                    ).scalar() or 0
                    settled_count = db.execute(
                        text("SELECT count(*) FROM sim_position WHERE capital_tier = :t AND status LIKE 'CLOSED%'"),
                        {"t": tier},
                    ).scalar() or 0

                    db.execute(
                        text("""
                            INSERT INTO sim_dashboard_snapshot
                            (snapshot_id, capital_tier, snapshot_date,
                             total_asset, cash, position_value,
                             open_position_count, settled_count,
                             total_return_pct, win_rate, pnl_ratio,
                             max_drawdown_pct, drawdown_state,
                             baseline_random_return, baseline_ma_cross_return,
                             data_status, status_reason,
                             created_at)
                            VALUES
                            (:sid, :tier, :snap_date,
                             :total_asset, :cash, :pv,
                             :open, :settled,
                             0.0, NULL, NULL,
                             0.0, 'NORMAL',
                             NULL, NULL,
                             :data_status, :status_reason,
                             :now)
                        """),
                        {
                            "sid": str(uuid4()),
                            "tier": tier,
                            "snap_date": TRADE_DATE,
                            "total_asset": account_row[3] if len(account_row) > 3 else tier_config[tier],
                            "cash": account_row[4] if len(account_row) > 4 else tier_config[tier],
                            "pv": 0,
                            "open": open_count,
                            "settled": settled_count,
                            "data_status": "READY" if settled_count > 0 else "COLD_START",
                            "status_reason": "cold_start_no_settled_positions" if settled_count == 0 else None,
                            "now": datetime.utcnow().isoformat(),
                        },
                    )
                    logger.info("  Created sim_dashboard_snapshot for %s", tier)
                else:
                    logger.info("  sim_dashboard_snapshot for %s already exists", tier)

        db.commit()

        # ── Final Summary ──────────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 60)

        counts = {
            "report": db.execute(text("SELECT count(*) FROM report")).scalar(),
            "report_citation": db.execute(text("SELECT count(*) FROM report_citation")).scalar(),
            "instruction_card": db.execute(text("SELECT count(*) FROM instruction_card")).scalar(),
            "sim_trade_instruction": db.execute(text("SELECT count(*) FROM sim_trade_instruction")).scalar(),
            "sim_account": db.execute(text("SELECT count(*) FROM sim_account")).scalar(),
            "sim_position": db.execute(text("SELECT count(*) FROM sim_position")).scalar(),
            "sim_dashboard_snapshot": db.execute(text("SELECT count(*) FROM sim_dashboard_snapshot")).scalar(),
            "settlement_result": db.execute(text("SELECT count(*) FROM settlement_result")).scalar(),
        }
        for k, v in counts.items():
            logger.info("  %s: %d", k, v)

        # Report quality distribution
        rows = db.execute(
            text("SELECT quality_flag, recommendation, count(*) FROM report GROUP BY quality_flag, recommendation")
        ).fetchall()
        logger.info("Report distribution:")
        for r in rows:
            logger.info("  quality=%s rec=%s count=%d", r[0], r[1], r[2])

        # Market state
        ms = db.execute(
            text("SELECT market_state, cache_status, market_state_degraded FROM market_state_cache ORDER BY created_at DESC LIMIT 1")
        ).fetchone()
        if ms:
            logger.info("Market state: state=%s status=%s degraded=%s", ms[0], ms[1], ms[2])

    finally:
        db.close()


if __name__ == "__main__":
    main()
