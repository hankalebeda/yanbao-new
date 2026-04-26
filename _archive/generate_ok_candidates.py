import sys
import time
sys.path.insert(0, '.')

from sqlalchemy import text
from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_report_ssot, ReportGenerationServiceError

TARGET_DATE = '2026-04-03'
LIMIT = 30


def pick_stocks(db):
    rows = db.execute(
        text(
            """
            SELECT DISTINCT du.stock_code
            FROM report_data_usage du
            WHERE du.trade_date=:td
              AND du.stock_code NOT IN (
                SELECT r.stock_code
                FROM report r
                WHERE r.is_deleted=0 AND r.quality_flag='ok'
              )
            GROUP BY du.stock_code
            HAVING COUNT(DISTINCT CASE WHEN lower(COALESCE(du.status,''))='ok' THEN du.dataset_name END) >= 5
            ORDER BY du.stock_code
            LIMIT :lim
            """
        ),
        {'td': TARGET_DATE, 'lim': LIMIT},
    ).fetchall()
    return [r[0] for r in rows]


def soft_delete_if_non_ok(db, report_id: str):
    db.execute(
        text(
            """
            UPDATE report
            SET is_deleted=1, deleted_at=CURRENT_TIMESTAMP, published=0
            WHERE report_id=:rid
              AND COALESCE(lower(quality_flag),'ok') <> 'ok'
            """
        ),
        {'rid': report_id},
    )
    db.commit()


def count_visible_ok(db):
    return db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND quality_flag='ok' ")).scalar()


def main():
    db = SessionLocal()
    try:
        before_ok = count_visible_ok(db)
        stocks = pick_stocks(db)
    finally:
        db.close()

    print({'target_date': TARGET_DATE, 'stocks': len(stocks), 'before_visible_ok': before_ok})

    ok_new = 0
    fail = 0
    details = []
    for idx, code in enumerate(stocks, 1):
        dbi = SessionLocal()
        try:
            t0 = time.time()
            res = generate_report_ssot(
                dbi,
                stock_code=code,
                trade_date=TARGET_DATE,
                skip_pool_check=True,
                force_same_day_rebuild=True,
            )
            elapsed = round(time.time() - t0, 1)
            rid = str(res.get('report_id') or '')
            qf = str(res.get('quality_flag') or '')
            if qf.lower() != 'ok' and rid:
                soft_delete_if_non_ok(dbi, rid)
            if qf.lower() == 'ok':
                ok_new += 1
                status = 'ok'
            else:
                status = 'non_ok_deleted'
            details.append({'stock': code, 'status': status, 'quality_flag': qf, 'elapsed_s': elapsed, 'report_id': rid[:12]})
            print(f"[{idx}/{len(stocks)}] {code} => {status} ({qf}) {elapsed}s")
        except ReportGenerationServiceError as e:
            fail += 1
            details.append({'stock': code, 'status': 'error', 'error_code': e.error_code, 'status_code': e.status_code})
            print(f"[{idx}/{len(stocks)}] {code} => error {e.error_code}({e.status_code})")
        except Exception as e:
            fail += 1
            details.append({'stock': code, 'status': 'exception', 'error': str(e)[:120]})
            print(f"[{idx}/{len(stocks)}] {code} => exception {type(e).__name__}")
        finally:
            dbi.close()

    db2 = SessionLocal()
    try:
        after_ok = count_visible_ok(db2)
        visible_total = db2.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0")).scalar()
        visible_non_ok = db2.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND quality_flag<>'ok' ")).scalar()
    finally:
        db2.close()

    print({'ok_new': ok_new, 'fail': fail, 'after_visible_ok': after_ok, 'visible_total': visible_total, 'visible_non_ok': visible_non_ok})


if __name__ == '__main__':
    main()
