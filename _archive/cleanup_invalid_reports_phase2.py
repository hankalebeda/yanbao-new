import datetime
import pathlib
import sqlite3

conn = sqlite3.connect('data/app.db')
c = conn.cursor()

c.execute("SELECT report_id FROM report WHERE conclusion_text IS NULL OR quality_flag='degraded'")
ids = [r[0] for r in c.fetchall()]

ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
backup_path = pathlib.Path('_archive') / f'deleted_report_ids_{ts}.txt'
backup_path.write_text('\n'.join(ids), encoding='utf-8')

c.execute(
    """
    SELECT COUNT(1)
    FROM settlement_result
    WHERE report_id IN (
      SELECT report_id FROM report
      WHERE conclusion_text IS NULL OR quality_flag='degraded'
    )
    """
)
deleted_settlement = c.fetchone()[0]

c.execute(
    """
    DELETE FROM settlement_result
    WHERE report_id IN (
      SELECT report_id FROM report
      WHERE conclusion_text IS NULL OR quality_flag='degraded'
    )
    """
)

c.execute("DELETE FROM report WHERE conclusion_text IS NULL OR quality_flag='degraded'")
conn.commit()

c.execute("SELECT COUNT(1) FROM report")
report_total = c.fetchone()[0]
c.execute("SELECT COUNT(1) FROM report WHERE published=1 AND is_deleted=0")
report_published = c.fetchone()[0]
c.execute("SELECT COUNT(1) FROM settlement_result")
settlement_total = c.fetchone()[0]

print(f"deleted_reports={len(ids)}")
print(f"deleted_settlement={deleted_settlement}")
print(f"backup={backup_path.as_posix()}")
print(f"report_total={report_total}")
print(f"report_published={report_published}")
print(f"settlement_total={settlement_total}")

conn.close()
