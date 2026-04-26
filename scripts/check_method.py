import sys, os
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
method = bt.get("summary", {}).get("method", "")
print("method:", repr(method))
print("contains 择优模型:", "择优模型" in method)
