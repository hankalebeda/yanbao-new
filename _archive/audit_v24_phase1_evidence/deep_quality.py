"""深度质量验证 - SSOT链路完整性"""
import sqlite3, json
c = sqlite3.connect('data/app.db')
c.row_factory = sqlite3.Row
cur = c.cursor()

# 1. 检查 usage_link 绑定
print("[1] report_data_usage_link 绑定统计 (2026-04-07):")
cur.execute("""SELECT count(DISTINCT l.report_id) 
    FROM report_data_usage_link l 
    JOIN report r ON r.report_id=l.report_id 
    WHERE r.is_deleted=0 AND r.trade_date='2026-04-07'""")
print(f"  reports with links: {cur.fetchone()[0]}")

cur.execute("""SELECT r.report_id, count(l.usage_id) FROM report r 
    LEFT JOIN report_data_usage_link l ON l.report_id=r.report_id
    WHERE r.is_deleted=0 AND r.trade_date>='2026-04-07'
    GROUP BY r.report_id HAVING count(l.usage_id)=0""")
orphans = cur.fetchall()
print(f"  无任何usage_link的研报: {len(orphans)}")

cur.execute("""SELECT avg(cnt), min(cnt), max(cnt) FROM (
    SELECT count(l.usage_id) cnt FROM report r 
    JOIN report_data_usage_link l ON l.report_id=r.report_id
    WHERE r.is_deleted=0 AND r.trade_date>='2026-04-07' GROUP BY r.report_id
)""")
r = cur.fetchone()
print(f"  每份研报 usage_link 数: avg={r[0]:.1f}, min={r[1]}, max={r[2]}")

# 2. prior_stats_snapshot 快照
print("\n[2] prior_stats_snapshot 填充率:")
cur.execute("""SELECT count(*) FROM report WHERE is_deleted=0 AND trade_date>='2026-04-07' 
    AND prior_stats_snapshot IS NOT NULL""")
pss = cur.fetchone()[0]
cur.execute("""SELECT count(*) FROM report WHERE is_deleted=0 AND trade_date>='2026-04-07'""")
total = cur.fetchone()[0]
print(f"  {pss}/{total}")

# sample
cur.execute("""SELECT stock_code, prior_stats_snapshot FROM report 
    WHERE is_deleted=0 AND trade_date='2026-04-07' AND prior_stats_snapshot IS NOT NULL LIMIT 1""")
r = cur.fetchone()
if r:
    print(f"  sample {r[0]}: {r[1][:300]}")

# 3. confidence / strategy_type
print("\n[3] confidence / strategy_type 分布:")
cur.execute("""SELECT strategy_type, count(*), avg(confidence), min(confidence), max(confidence) 
    FROM report WHERE is_deleted=0 AND trade_date>='2026-04-07' GROUP BY strategy_type""")
for r in cur.fetchall(): print(f"  {r[0]}: n={r[1]} conf avg={r[2]:.3f} min={r[3]} max={r[4]}")

# 4. market_state 一致性
print("\n[4] market_state 分布:")
cur.execute("""SELECT market_state, market_state_trade_date, count(*) 
    FROM report WHERE is_deleted=0 AND trade_date>='2026-04-07' 
    GROUP BY market_state, market_state_trade_date""")
for r in cur.fetchall(): print(f"  state={r[0]} ref_date={r[1]}: {r[2]}")

# 5. risk_audit_status
print("\n[5] risk_audit_status:")
cur.execute("""SELECT risk_audit_status, count(*) FROM report 
    WHERE is_deleted=0 AND trade_date>='2026-04-07' GROUP BY risk_audit_status""")
for r in cur.fetchall(): print(f"  {r[0]}: {r[1]}")

# 6. reasoning_chain_md 长度分布
print("\n[6] reasoning_chain_md 长度分布:")
cur.execute("""SELECT length(reasoning_chain_md) FROM report 
    WHERE is_deleted=0 AND trade_date>='2026-04-07'""")
lens = sorted([r[0] for r in cur.fetchall() if r[0]])
if lens:
    print(f"  n={len(lens)}, min={lens[0]}, p50={lens[len(lens)//2]}, p90={lens[int(len(lens)*0.9)]}, max={lens[-1]}")
    print(f"  <500 char: {sum(1 for l in lens if l < 500)}")

# 7. degraded 研报详情
print("\n[7] degraded 研报详情:")
cur.execute("""SELECT stock_code, trade_date, status_reason, llm_fallback_level, conclusion_text 
    FROM report WHERE is_deleted=0 AND trade_date>='2026-04-07' AND quality_flag='degraded'""")
for r in cur.fetchall():
    print(f"  {r[0]} {r[1]}: reason={r[2]}, llm_fb={r[3]}")
    print(f"    concl: {(r[4] or '')[:200]}")

c.close()
