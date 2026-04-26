import json
import os
import urllib.request as u
import urllib.error as ue
import sys
sys.path.insert(0, '.')
from sqlalchemy import text
from app.core.db import SessionLocal

os.environ.pop('http_proxy', None)
os.environ.pop('HTTP_PROXY', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))

BASE = 'http://127.0.0.1:8010'


def call(path: str, headers: dict | None = None):
    req = u.Request(BASE + path, headers=headers or {}, method='GET')
    try:
        with u.urlopen(req, timeout=25) as r:
            body = r.read().decode('utf-8')
            try:
                parsed = json.loads(body)
            except Exception:
                parsed = body[:400]
            return r.getcode(), parsed
    except ue.HTTPError as e:
        body = e.read().decode('utf-8')
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = body[:400]
        return e.code, parsed


def get_one_report_id() -> str | None:
    db = SessionLocal()
    try:
        rid = db.execute(text("SELECT report_id FROM report WHERE is_deleted=0 AND quality_flag='ok' ORDER BY created_at DESC LIMIT 1")).scalar()
        return rid
    finally:
        db.close()


def main():
    rid = get_one_report_id()
    out = {
        'report_id_sample': rid,
        'checks': []
    }

    def add(name, path, headers=None):
        code, data = call(path, headers)
        out['checks'].append({'name': name, 'path': path, 'status': code, 'sample': data if isinstance(data, dict) else str(data)[:180]})

    add('health', '/health')
    add('home', '/api/v1/home')
    add('reports_list', '/api/v1/reports?limit=5')
    add('stocks_autocomplete', '/api/v1/stocks/autocomplete?q=6005&limit=5')
    add('favorites_anon', '/api/v1/user/favorites')

    if rid:
        add('report_detail', f'/api/v1/reports/{rid}')
        add('report_advanced_anon', f'/api/v1/reports/{rid}/advanced')

    print(json.dumps(out, ensure_ascii=True, indent=2))


if __name__ == '__main__':
    main()
