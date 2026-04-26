"""
Post-report chain: sim_accounts, positions, dashboard snapshots.
Run after fix_and_regenerate.py already generated reports with BUY signals.
"""
import os, sys, logging
from datetime import date, datetime, timezone
from uuid import uuid4

os.environ.setdefault("DATABASE_URL", "sqlite:///D:/yanbao/data/app.db")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TRADE_DATE = date(2026, 3, 10)


def main():
    from sqlalchemy import text
    from app.core.db import SessionLocal

    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc).isoformat()

        # ── Step 5: Initialize sim_accounts ────────────────────────────────
        logger.info("Step 5: Initialize sim_accounts")

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
                        (capital_tier, initial_cash, cash_available, total_asset, 
                         peak_total_asset, max_drawdown_pct, drawdown_state,
                         drawdown_state_factor, active_position_count,
                         last_reconciled_trade_date, updated_at, created_at)
                        VALUES 
                        (:tier, :initial, :initial, :initial,
                         :initial, 0.0, 'NORMAL',
                         1.0, 0,
                         :trade_date, :now, :now)
                    """),
                    {"tier": tier, "initial": initial, "trade_date": TRADE_DATE, "now": now},
                )
                logger.info("  Created sim_account: %s (initial=%d)", tier, initial)
            else:
                logger.info("  sim_account %s already exists", tier)
        db.commit()

        # ── Step 6: Open positions from EXECUTE instructions ───────────────
        logger.info("Step 6: Open positions from EXECUTE instructions")

        execute_rows = db.execute(
            text("""
                SELECT i.trade_instruction_id, i.report_id, i.capital_tier, i.position_ratio,
                       r.stock_code, r.confidence
                FROM sim_trade_instruction i
                JOIN report r ON r.report_id = i.report_id
                WHERE i.status = 'EXECUTE'
            """)
        ).fetchall()
        logger.info("  EXECUTE instructions: %d", len(execute_rows))

        positions_opened = 0
        for row in execute_rows:
            instr_id, report_id, tier, position_ratio, stock_code, confidence = row

            # Check if position already exists
            existing = db.execute(
                text("""
                    SELECT count(*) FROM sim_position 
                    WHERE report_id = :rid AND capital_tier = :tier
                """),
                {"rid": report_id, "tier": tier},
            ).scalar()
            if existing > 0:
                continue

            # Get account info
            acct = db.execute(
                text("SELECT cash_available, total_asset FROM sim_account WHERE capital_tier = :t"),
                {"t": tier},
            ).fetchone()
            if not acct:
                continue
            cash_available, total_asset = acct

            # Get latest close price for the stock
            kline = db.execute(
                text("""
                    SELECT close, atr_pct FROM kline_daily 
                    WHERE stock_code = :sc 
                    ORDER BY trade_date DESC LIMIT 1
                """),
                {"sc": stock_code},
            ).fetchone()
            if not kline:
                logger.warning("  No kline for %s, skipping", stock_code)
                continue

            close_price = float(kline[0])
            atr_pct = float(kline[1] or 0.03)

            # Calculate position
            allocated = total_asset * (position_ratio or 0.1)
            shares = int(allocated / close_price / 100) * 100  # round to lot
            if shares <= 0:
                shares = 100  # minimum 1 lot

            # Stop loss and target (simple ATR-based)
            stop_loss_price = round(close_price * (1 - 2 * atr_pct), 2)
            target_price = round(close_price * (1 + 3 * atr_pct), 2)

            db.execute(
                text("""
                    INSERT INTO sim_position
                    (position_id, report_id, stock_code, capital_tier,
                     position_status, signal_date, entry_date,
                     actual_entry_price, signal_entry_price,
                     position_ratio, shares,
                     atr_pct_snapshot, atr_multiplier_snapshot,
                     stop_loss_price, target_price,
                     take_profit_pending_t1, stop_loss_pending_t1,
                     suspended_pending, limit_locked_pending,
                     created_at, updated_at)
                    VALUES
                    (:pid, :rid, :sc, :tier,
                     'OPEN', :sd, :sd,
                     :price, :price,
                     :ratio, :shares,
                     :atr, 2.0,
                     :sl, :tp,
                     0, 0,
                     0, 0,
                     :now, :now)
                """),
                {
                    "pid": str(uuid4()),
                    "rid": report_id,
                    "sc": stock_code,
                    "tier": tier,
                    "sd": TRADE_DATE,
                    "price": close_price,
                    "ratio": position_ratio or 0.1,
                    "shares": shares,
                    "atr": atr_pct,
                    "sl": stop_loss_price,
                    "tp": target_price,
                    "now": now,
                },
            )
            positions_opened += 1

        db.commit()
        logger.info("  Opened %d positions", positions_opened)

        # Update sim_account active_position_count
        for tier in tier_config:
            cnt = db.execute(
                text("SELECT count(*) FROM sim_position WHERE capital_tier = :t AND position_status = 'OPEN'"),
                {"t": tier},
            ).scalar()
            db.execute(
                text("UPDATE sim_account SET active_position_count = :c, updated_at = :now WHERE capital_tier = :t"),
                {"c": cnt, "now": now, "t": tier},
            )
        db.commit()

        # ── Step 7: Generate sim_dashboard_snapshots ───────────────────────
        logger.info("Step 7: Generate sim_dashboard_snapshots")

        for tier in tier_config:
            existing_snap = db.execute(
                text("SELECT count(*) FROM sim_dashboard_snapshot WHERE capital_tier = :t AND snapshot_date = :d"),
                {"t": tier, "d": TRADE_DATE},
            ).scalar()
            if existing_snap > 0:
                logger.info("  sim_dashboard_snapshot for %s already exists", tier)
                continue

            open_count = db.execute(
                text("SELECT count(*) FROM sim_position WHERE capital_tier = :t AND position_status = 'OPEN'"),
                {"t": tier},
            ).scalar() or 0

            db.execute(
                text("""
                    INSERT INTO sim_dashboard_snapshot
                    (dashboard_snapshot_id, capital_tier, snapshot_date,
                     data_status, status_reason,
                     total_return_pct, win_rate, profit_loss_ratio,
                     alpha_annual, max_drawdown_pct, sample_size,
                     display_hint, is_simulated_only,
                     created_at)
                    VALUES
                    (:sid, :tier, :snap_date,
                     :data_status, :status_reason,
                     0.0, NULL, NULL,
                     NULL, 0.0, 0,
                     'initial_snapshot', 1,
                     :now)
                """),
                {
                    "sid": str(uuid4()),
                    "tier": tier,
                    "snap_date": TRADE_DATE,
                    "data_status": "READY" if open_count > 0 else "DEGRADED",
                    "status_reason": None if open_count > 0 else "cold_start_awaiting_settlement",
                    "now": now,
                },
            )
            logger.info("  Created snapshot for %s (open_positions=%d)", tier, open_count)

        db.commit()

        # ── Final Summary ──────────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 60)

        counts = {}
        for t in ["report", "report_citation", "instruction_card",
                   "sim_trade_instruction", "sim_account",
                   "sim_position", "sim_dashboard_snapshot", "settlement_result"]:
            counts[t] = db.execute(text(f"SELECT count(*) FROM {t}")).scalar()
            logger.info("  %s: %d", t, counts[t])

        # Report distribution
        rows = db.execute(
            text("SELECT quality_flag, recommendation, count(*) FROM report GROUP BY quality_flag, recommendation")
        ).fetchall()
        logger.info("Report distribution:")
        for r in rows:
            logger.info("  quality=%s rec=%s count=%d", r[0], r[1], r[2])

        # Instructions
        rows = db.execute(
            text("SELECT status, count(*) FROM sim_trade_instruction GROUP BY status")
        ).fetchall()
        logger.info("Trade instructions:")
        for r in rows:
            logger.info("  %s: %d", r[0], r[1])

        # Positions
        rows = db.execute(
            text("SELECT position_status, count(*) FROM sim_position GROUP BY position_status")
        ).fetchall()
        logger.info("Positions:")
        for r in rows:
            logger.info("  %s: %d", r[0], r[1])

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
