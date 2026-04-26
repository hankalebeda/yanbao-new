"""找在pool_snapshot里但还没有ok研报的股票"""
import sys, json
sys.path.insert(0, 'd:/yanbao-new')
from dotenv import load_dotenv; load_dotenv('d:/yanbao-new/.env')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()
rows = db.execute(text("""
    SELECT DISTINCT sps.stock_code FROM stock_pool_snapshot sps
    JOIN stock_pool_refresh_task r ON r.task_id = sps.refresh_task_id
    WHERE sps.trade_date='2026-04-03'
      AND r.status IN ('COMPLETED','FALLBACK','SUCCESS')
      AND sps.stock_code NOT IN (
        SELECT DISTINCT stock_code FROM report
        WHERE trade_date='2026-04-03' AND is_deleted=0 AND published=1
          AND LOWER(COALESCE(quality_flag,'ok'))='ok'
      )
    ORDER BY sps.stock_code
""")).fetchall()
codes = [r[0] for r in rows]
print(f"Found {len(codes)} stocks in pool_snapshot without ok report")
print(json.dumps(codes))
db.close()
