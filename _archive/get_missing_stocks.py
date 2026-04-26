import sys, json
sys.path.insert(0, 'd:/yanbao-new')
from dotenv import load_dotenv; load_dotenv('d:/yanbao-new/.env')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()
rows = db.execute(text("""
    SELECT sm.stock_code FROM stock_master sm
    WHERE sm.stock_code NOT IN (
        SELECT DISTINCT stock_code FROM report
        WHERE trade_date='2026-04-03' AND is_deleted=0 AND published=1
          AND LOWER(COALESCE(quality_flag,'ok'))='ok'
    )
    ORDER BY RANDOM() LIMIT 100
""")).fetchall()
codes = [r[0] for r in rows]
print(len(codes))
print(json.dumps(codes))
db.close()
