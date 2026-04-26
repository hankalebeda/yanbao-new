import json
import os
import urllib.request as u
import sys
sys.path.insert(0, '.')
from sqlalchemy import text
from app.core.db import SessionLocal

os.environ.pop('http_proxy', None)
os.environ.pop('HTTP_PROXY', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))

BASE = 'http://127.0.0.1:8010'

def api(path: str):
    with u.urlopen(BASE + path, timeout=20) as r:
        return json.loads(r.read().decode('utf-8'))

db = SessionLocal()
try:
    data = {}
    data['visible_total'] = db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0")).scalar()
    data['visible_ok'] = db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND quality_flag='ok' ")).scalar()
    data['visible_non_ok'] = db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND quality_flag<>'ok' ")).scalar()
    data['buy_visible_ok'] = db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND quality_flag='ok' AND recommendation='BUY' ")).scalar()
    data['settled_distinct_visible_ok'] = db.execute(text("""
        SELECT COUNT(DISTINCT sr.report_id)
        FROM settlement_result sr
        JOIN report r ON r.report_id=sr.report_id
        WHERE r.is_deleted=0 AND r.quality_flag='ok'
    """)).scalar()
    data['published_deleted_conflict'] = db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=1 AND COALESCE(published,0)=1")).scalar()
    kline_total = db.execute(text("SELECT COUNT(DISTINCT stock_code) FROM kline_daily")).scalar()
    data['kline_distinct_stock'] = kline_total
    data['kline_coverage_pct_est'] = round((kline_total or 0) * 100.0 / 5197.0, 2)
finally:
    db.close()

health = api('/health')
home = api('/api/v1/home')

out = {
    'db': data,
    'health': health.get('data', {}),
    'home_data_status': home.get('data', {}).get('data_status'),
    'home_latest_reports_count': len(home.get('data', {}).get('latest_reports') or []),
}
print(json.dumps(out, ensure_ascii=False, indent=2))
