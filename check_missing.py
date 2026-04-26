import sqlite3

db = sqlite3.connect('data/app.db')
c = db.cursor()

stocks = ['000301.SZ','000333.SZ','000400.SZ','000423.SZ',
          '000519.SZ','000538.SZ','000568.SZ','000596.SZ']

for s in stocks:
    key = 'daily:' + s + ':2026-04-16'
    c.execute("""
        SELECT task_id,generation_seq,status,status_reason,superseded_at,created_at
        FROM report_generation_task WHERE idempotency_key=?
        ORDER BY generation_seq DESC
    """, (key,))
    tasks = c.fetchall()
    c.execute("""
        SELECT report_id,published,is_deleted,quality_flag,status_reason,created_at
        FROM report WHERE stock_code=? ORDER BY created_at DESC LIMIT 5
    """, (s,))
    reports = c.fetchall()
    c.execute("SELECT COUNT(*) FROM report WHERE stock_code=? AND published=1 AND is_deleted=0", (s,))
    pub_cnt = c.fetchone()[0]
    print(f"=== {s} pub_count={pub_cnt} ===")
    for t in tasks:
        print(f"  task: {t}")
    for r in reports:
        print(f"  report: {r}")

c.execute("SELECT COUNT(*) FROM report WHERE published=1 AND is_deleted=0")
total = c.fetchone()[0]
print(f"\nTotal published: {total}")

db.close()

