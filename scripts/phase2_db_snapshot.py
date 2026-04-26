"""DB snapshot for Phase 2 planning (corrected table names)."""
from sqlalchemy import text
from app.core.db import SessionLocal

db = SessionLocal()
try:
    def q(sql):
        return list(db.execute(text(sql)).fetchall())

    print("=== Reports (table: report) ===")
    total = q("SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL)")[0][0]
    print(f"total not-deleted: {total}")
    print("by quality_flag:")
    for r in q("SELECT quality_flag, COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) GROUP BY quality_flag"):
        print(f"  {r[0]}: {r[1]}")
    print("by trade_date (recent 10):")
    for r in q("SELECT trade_date, COUNT(*), SUM(CASE WHEN quality_flag='ok' THEN 1 ELSE 0 END) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND trade_date >= '2026-03-20' GROUP BY trade_date ORDER BY trade_date DESC LIMIT 10"):
        print(f"  {r[0]}: total={r[1]}, ok={r[2]}")
    print("latest 5 by created_at:")
    for r in q("SELECT stock_code, trade_date, quality_flag, created_at FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) ORDER BY created_at DESC LIMIT 5"):
        print(f"  {r}")

    print("\n=== Kline Daily ===")
    print("total rows:", q("SELECT COUNT(*) FROM kline_daily")[0][0])
    print("distinct stocks:", q("SELECT COUNT(DISTINCT stock_code) FROM kline_daily")[0][0])
    print("latest date:", q("SELECT MAX(trade_date) FROM kline_daily")[0][0])
    print("by date (recent):")
    for r in q("SELECT trade_date, COUNT(*), COUNT(DISTINCT stock_code) FROM kline_daily WHERE trade_date >= '2026-03-20' GROUP BY trade_date ORDER BY trade_date DESC LIMIT 15"):
        print(f"  {r[0]}: rows={r[1]}, stocks={r[2]}")

    print("\n=== Settlement ===")
    print("total settlement_result:", q("SELECT COUNT(*) FROM settlement_result")[0][0])
    print("settlement_task:", q("SELECT COUNT(*) FROM settlement_task")[0][0])
    print("prediction_outcome:", q("SELECT COUNT(*) FROM prediction_outcome")[0][0])

    print("\n=== Stock Pool ===")
    print("stock_master alive:", q("SELECT COUNT(*) FROM stock_master WHERE (is_delisted=0 OR is_delisted IS NULL)")[0][0])
    print("stock_pool total:", q("SELECT COUNT(*) FROM stock_pool")[0][0])
    print("stock_pool_snapshot total:", q("SELECT COUNT(*) FROM stock_pool_snapshot")[0][0])
    print("latest pool snapshots:")
    for r in q("SELECT trade_date, COUNT(*) FROM stock_pool_snapshot GROUP BY trade_date ORDER BY trade_date DESC LIMIT 5"):
        print(f"  {r[0]}: {r[1]}")

    print("\n=== Cookie session ===")
    print("total:", q("SELECT COUNT(*) FROM cookie_session")[0][0])

    print("\n=== Hotspot ===")
    print("hotspot_normalized:", q("SELECT COUNT(*) FROM hotspot_normalized")[0][0])
    print("hotspot_raw:", q("SELECT COUNT(*) FROM hotspot_raw")[0][0])
    print("hotspot_top50:", q("SELECT COUNT(*) FROM hotspot_top50")[0][0])

    print("\n=== Sim positions ===")
    print("sim_position total:", q("SELECT COUNT(*) FROM sim_position")[0][0])
    print("sim_trade_instruction:", q("SELECT COUNT(*) FROM sim_trade_instruction")[0][0])
    print("sim_dashboard_snapshot:", q("SELECT COUNT(*) FROM sim_dashboard_snapshot")[0][0])
    print("sim_equity_curve_point:", q("SELECT COUNT(*) FROM sim_equity_curve_point")[0][0])
    print("baseline_equity_curve_point:", q("SELECT COUNT(*) FROM baseline_equity_curve_point")[0][0])

    print("\n=== Events ===")
    print("business_event:", q("SELECT COUNT(*) FROM business_event")[0][0])
    print("outbox_event:", q("SELECT COUNT(*) FROM outbox_event")[0][0])
    print("notification:", q("SELECT COUNT(*) FROM notification")[0][0])

    print("\n=== Users ===")
    print("app_user:", q("SELECT COUNT(*) FROM app_user")[0][0])
finally:
    db.close()
