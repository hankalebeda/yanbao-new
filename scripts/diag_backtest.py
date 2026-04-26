"""诊断 price_forecast.backtest.summary 字段"""
import sys, os, json
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root)
os.chdir(root)
from app.core.db import SessionLocal, Base, engine, ensure_report_trade_date_column
Base.metadata.create_all(bind=engine)
ensure_report_trade_date_column()
db = SessionLocal()
from app.models import Report
row = db.query(Report).filter(Report.stock_code == "600519.SH").order_by(Report.created_at.desc()).first()
db.close()
pf = row.content_json.get("price_forecast", {})
bt = pf.get("backtest", {})
summary = bt.get("summary", {})
print("=== backtest.summary ===")
print(json.dumps(summary, ensure_ascii=False, indent=2))
print("\n=== backtest.summary_recent_3m ===")
print(json.dumps(bt.get("summary_recent_3m", {}), ensure_ascii=False, indent=2))
wins = pf.get("windows", [])
print(f"\n=== windows count={len(wins)} ===")
for w in wins:
    print(f"  day={w.get('horizon_days')} conf_raw={w.get('confidence_raw')} conf_gap={w.get('confidence_gap')}")
qg = row.content_json.get("quality_gate", {})
print(f"\n=== quality_gate keys: {list(qg.keys())} ===")
