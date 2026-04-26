"""找到有kline数据但没有ok研报的股票"""
import sys, json
sys.path.insert(0, 'd:/yanbao-new')
from dotenv import load_dotenv; load_dotenv('d:/yanbao-new/.env')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()
# 找有kline数据且没有ok研报的股票
rows = db.execute(text("""
    SELECT DISTINCT kd.stock_code FROM kline_daily kd
    WHERE kd.trade_date='2026-04-03'
      AND kd.stock_code NOT IN (
        SELECT DISTINCT stock_code FROM report
        WHERE trade_date='2026-04-03' AND is_deleted=0 AND published=1
          AND LOWER(COALESCE(quality_flag,'ok'))='ok'
      )
    ORDER BY kd.stock_code
    LIMIT 200
""")).fetchall()
codes = [r[0] for r in rows]
print(f"Found {len(codes)} stocks with kline data but no ok report")
print(json.dumps(codes))
db.close()
