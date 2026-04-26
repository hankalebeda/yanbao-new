"""Final v16 DB snapshot."""
import sqlite3
c = sqlite3.connect("data/app.db")
r = c.execute(
    "SELECT count(*), "
    "sum(case when is_deleted=0 then 1 else 0 end), "
    "sum(case when is_deleted=0 and publish_status='PUBLISHED' then 1 else 0 end), "
    "sum(case when is_deleted=0 and quality_flag='ok' then 1 else 0 end) "
    "FROM report"
).fetchone()
print("total=%d alive=%d published_alive=%d quality_ok_alive=%d" % r)
r2 = c.execute(
    "SELECT count(*) FROM report WHERE is_deleted=0 AND status_reason IS NOT NULL AND status_reason<>''"
).fetchone()
print("alive_with_status_reason=%d" % r2)
r3 = c.execute(
    "SELECT substr(status_reason,1,40), count(*) FROM report WHERE is_deleted=1 "
    "GROUP BY status_reason ORDER BY 2 DESC LIMIT 10"
).fetchall()
print("soft_deleted_top_reasons:")
for row in r3:
    print(" ", row)
r4 = c.execute(
    "SELECT date(created_at), count(*), sum(case when is_deleted=0 then 1 else 0 end) "
    "FROM report GROUP BY date(created_at) ORDER BY 1 DESC LIMIT 10"
).fetchall()
print("daily_distribution:")
for row in r4:
    print(" ", row)
