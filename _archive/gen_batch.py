import sys, os, time
sys.path.insert(0, 'd:/yanbao-new')
os.environ.setdefault('DATABASE_URL', 'sqlite:///data/app.db')
from dotenv import load_dotenv
load_dotenv('d:/yanbao-new/.env')
from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_report_ssot, ReportGenerationServiceError

stocks = [
    '002594.SZ', '000568.SZ', '000858.SZ', '002304.SZ', '600276.SH',
    '000333.SZ', '002714.SZ', '601318.SH', '603288.SH', '600887.SH',
    '000895.SZ', '601919.SH', '002415.SZ', '600585.SH', '600036.SH'
]
ok = 0
for s in stocks:
    db = SessionLocal()
    try:
        t = time.time()
        r = generate_report_ssot(db, stock_code=s, trade_date='2026-04-03',
                                  skip_pool_check=True, force_same_day_rebuild=True)
        rec = r.get('recommendation')
        conf = r.get('confidence', 0)
        rid = str(r.get('report_id', ''))[:8]
        elapsed = time.time() - t
        print(f'OK {s}: rec={rec} conf={conf:.2f} {elapsed:.0f}s id={rid}', flush=True)
        ok += 1
    except ReportGenerationServiceError as e:
        print(f'ERR {s}: {e.error_code}({e.status_code})', flush=True)
    except Exception as e:
        import traceback
        print(f'EXC {s}: {type(e).__name__}: {str(e)[:100]}', flush=True)
        traceback.print_exc()
    finally:
        db.close()

print(f'\nDone: {ok}/{len(stocks)} succeeded')
