"""分析已生成研报的质量 - 使用正确字段名"""
import sqlite3, json

c = sqlite3.connect('data/app.db')
c.row_factory = sqlite3.Row
cur = c.cursor()

print("=" * 70)
print("研报质量分析 (当前生成快照)")
print("=" * 70)

# 1. 总体分布
print("\n[1] 研报分布 (quality_flag):")
cur.execute("""SELECT trade_date, quality_flag, recommendation, llm_fallback_level, count(*) 
    FROM report WHERE is_deleted=0 AND trade_date>='2026-04-07' 
    GROUP BY trade_date, quality_flag, recommendation, llm_fallback_level ORDER BY trade_date""")
for r in cur.fetchall(): 
    print(f"  {r[0]} | qf={r[1]} rec={r[2]} llm_fb={r[3]}: {r[4]}")

# 2. 字段缺失 (5样本)
print("\n[2] content_json 结构 (2026-04-07, 5 样本):")
cur.execute("""SELECT report_id, stock_code, content_json, conclusion_text, reasoning_chain_md 
    FROM report WHERE is_deleted=0 AND trade_date='2026-04-07' LIMIT 5""")
for r in cur.fetchall():
    try:
        content = json.loads(r['content_json']) if r['content_json'] else {}
    except Exception as e:
        print(f"  {r['stock_code']}: JSON PARSE ERROR {e}")
        continue
    keys = sorted(content.keys())
    print(f"\n  {r['stock_code']} ({len(keys)} keys):")
    print(f"    keys: {keys}")
    print(f"    conclusion: {(r['conclusion_text'] or '')[:80]!r}")
    print(f"    reasoning_chain_md len: {len(r['reasoning_chain_md'] or '')}")

# 3. 降级
print("\n[3] 降级/质量标记统计 (全部):")
cur.execute("""SELECT quality_flag, llm_fallback_level, market_state_degraded, count(*) 
    FROM report WHERE is_deleted=0 AND trade_date>='2026-04-07' 
    GROUP BY quality_flag, llm_fallback_level, market_state_degraded""")
for r in cur.fetchall():
    print(f"  qf={r[0]} llm_fb={r[1]} mkt_deg={r[2]}: {r[3]}")

# 4. 问题研报
print("\n[4] 问题研报 (status_reason):")
cur.execute("""SELECT stock_code, quality_flag, status_reason, failure_category 
    FROM report WHERE is_deleted=0 AND trade_date>='2026-04-07' 
    AND (status_reason IS NOT NULL AND status_reason<>'') LIMIT 10""")
for r in cur.fetchall():
    print(f"  {r[0]} qf={r[1]} fc={r[3]}: {(r[2] or '')[:120]}")

# 5. content_json 长度
print("\n[5] content_json 长度分布:")
cur.execute("""SELECT length(content_json) FROM report 
    WHERE is_deleted=0 AND trade_date>='2026-04-07'""")
lens = [r[0] for r in cur.fetchall() if r[0]]
if lens:
    lens.sort()
    print(f"  count={len(lens)}, min={lens[0]}, p50={lens[len(lens)//2]}, p90={lens[int(len(lens)*0.9)]}, max={lens[-1]}")
    short = sum(1 for l in lens if l < 1000)
    print(f"  <1000 char (可能过短): {short}")

# 6. recommendation 分布
print("\n[6] recommendation 分布:")
cur.execute("""SELECT recommendation, count(*) FROM report 
    WHERE is_deleted=0 AND trade_date>='2026-04-07' GROUP BY recommendation""")
for r in cur.fetchall(): print(f"  {r[0]}: {r[1]}")

# 7. 绑定校验
print("\n[7] report_data_usage 绑定校验:")
cur.execute("""SELECT count(*) FROM report r WHERE r.is_deleted=0 AND r.trade_date>='2026-04-07'
    AND NOT EXISTS (SELECT 1 FROM report_data_usage rdu 
        WHERE rdu.stock_code=r.stock_code AND rdu.trade_date=r.trade_date AND rdu.status='ok')""")
orphan = cur.fetchone()[0]
print(f"  孤儿研报(无ok usage): {orphan}")

c.close()
print("\n分析完成.")
