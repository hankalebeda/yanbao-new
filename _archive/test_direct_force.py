import sys
sys.path.insert(0,'.')
from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_report_ssot, ReportGenerationServiceError

db = SessionLocal()
for code in ['000001.SZ','000100.SZ','000301.SZ']:
    try:
        r = generate_report_ssot(db, stock_code=code, trade_date='2026-04-03', skip_pool_check=True, force_same_day_rebuild=True)
        print(code, 'OK', r.get('report_id'), r.get('quality_flag'))
    except ReportGenerationServiceError as e:
        print(code, 'ERR', e.status_code, e.error_code)
    except Exception as e:
        print(code, 'EX', type(e).__name__, str(e)[:200])

db.close()
