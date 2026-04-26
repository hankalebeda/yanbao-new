import sqlite3, datetime
conn = sqlite3.connect('data/app.db')
c = conn.cursor()
now = datetime.datetime.utcnow().isoformat()

# fail-close: 688199.SH degraded unpublished
c.execute("UPDATE report SET is_deleted=1, updated_at=? WHERE stock_code='688199.SH' AND quality_flag='degraded' AND published=0 AND is_deleted=0", (now,))
print('688199 soft-deleted:', c.rowcount)

# fail-close: 600519.SH ok but REPORT_DATA_INCOMPLETE, unpublished
c.execute("UPDATE report SET is_deleted=1, updated_at=? WHERE stock_code='600519.SH' AND quality_flag='ok' AND published=0 AND is_deleted=0 AND (status_reason LIKE '%REPORT_DATA_INCOMPLETE%' OR status_reason LIKE '%LLM_FALLBACK%')", (now,))
print('600519 soft-deleted:', c.rowcount)

conn.commit()
# Verify remaining alive
rows = c.execute("SELECT stock_code, quality_flag, published, is_deleted FROM report WHERE is_deleted=0").fetchall()
print('Alive reports after:', rows)
conn.close()
