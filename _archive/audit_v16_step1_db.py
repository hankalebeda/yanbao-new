"""One-off read-only DB inspection for v16 audit (阶段 A)."""
import sqlite3

c = sqlite3.connect("data/app.db")

r = c.execute(
    """
    SELECT count(*) total,
      sum(case when conclusion_text is null or conclusion_text='' then 0 else 1 end) has_concl,
      sum(case when reasoning_chain_md is null or reasoning_chain_md='' then 0 else 1 end) has_reason,
      sum(case when content_json is null then 0 else 1 end) has_cj,
      sum(case when published=1 then 1 else 0 end) pub,
      sum(case when publish_status='PUBLISHED' then 1 else 0 end) pub_status,
      sum(case when quality_flag='ok' then 1 else 0 end) qok
      FROM report WHERE is_deleted=0
    """
).fetchone()
print("alive_stats:", dict(zip(
    ["total", "has_concl", "has_reason", "has_cj", "pub", "pub_status", "qok"], r)))

print("\npublish_status_dist_alive:")
for row in c.execute(
    "SELECT publish_status, count(*) FROM report WHERE is_deleted=0 "
    "GROUP BY publish_status ORDER BY 2 DESC"
).fetchall():
    print("  ", row)

print("\nquality_flag_dist_alive:")
for row in c.execute(
    "SELECT quality_flag, count(*) FROM report WHERE is_deleted=0 "
    "GROUP BY quality_flag ORDER BY 2 DESC"
).fetchall():
    print("  ", row)

print("\nstatus_reason_dist_alive:")
for row in c.execute(
    "SELECT status_reason, count(*) FROM report WHERE is_deleted=0 "
    "GROUP BY status_reason ORDER BY 2 DESC LIMIT 15"
).fetchall():
    print("  ", row)

print("\nreport_data_usage_columns:")
usage_cols = [r[1] for r in c.execute("PRAGMA table_info(report_data_usage)").fetchall()]
print("  ", usage_cols)

# link column heuristic
link = None
for cand in ("report_id", "source_report_id", "ref_report_id", "owner_report_id"):
    if cand in usage_cols:
        link = cand
        break
print("  link_column:", link)

if link:
    print("\nusage_by_alive_reports (top 40):")
    for row in c.execute(
        f"""
        SELECT u.status, u.dataset_name, count(*)
        FROM report_data_usage u JOIN report r ON r.report_id=u.{link}
        WHERE r.is_deleted=0 GROUP BY u.status, u.dataset_name
        ORDER BY 3 DESC LIMIT 40
        """
    ).fetchall():
        print("  ", row)

    print("\nalive_reports_missing_usage:")
    r = c.execute(
        f"""
        SELECT count(*) FROM report r
        WHERE r.is_deleted=0
          AND NOT EXISTS (SELECT 1 FROM report_data_usage u WHERE u.{link}=r.report_id)
        """
    ).fetchone()
    print("  count:", r[0])

    print("\nalive_reports_with_incomplete_datasets:")
    r = c.execute(
        f"""
        SELECT count(DISTINCT r.report_id) FROM report r
        JOIN report_data_usage u ON u.{link}=r.report_id
        WHERE r.is_deleted=0 AND u.status<>'ok'
        """
    ).fetchone()
    print("  count:", r[0])
