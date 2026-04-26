"""诊断 stale_ok 报告的 trade_date 分布 vs K 线覆盖"""
import sqlite3
from pathlib import Path

ROOT = Path(__file__).parent.parent
conn = sqlite3.connect(str(ROOT / "data" / "app.db"))
c = conn.cursor()

# 1. stale_ok 报告的 trade_date 分布（前30个）
c.execute("""
    SELECT trade_date, COUNT(1) cnt
    FROM report
    WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')
    AND trade_date IS NOT NULL
    GROUP BY trade_date
    ORDER BY trade_date DESC
    LIMIT 30
""")
print("=== 最近30个 trade_date 的报告数量 ===")
for r in c.fetchall():
    print(f"  {r[0]}: {r[1]} 份报告")

# 2. K 线数据的日期范围
c.execute("SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date) FROM kline_daily")
row = c.fetchone()
print(f"\nK线日期范围: {row[0]} ~ {row[1]} ({row[2]} 个交易日)")

# 3. 有多少 stale_ok 报告的 trade_date 超出 K 线范围
c.execute("SELECT MAX(trade_date) FROM kline_daily")
kline_max = c.fetchone()[0]
c.execute("""
    SELECT COUNT(1) FROM report
    WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')
    AND trade_date IS NOT NULL AND trade_date > ?
""", (kline_max,))
outside = c.fetchone()[0]
print(f"\nK线最大日期: {kline_max}")
print(f"trade_date 超出 K线范围的报告数: {outside}")

# 4. 报告 trade_date 整体分布
c.execute("""
    SELECT 
        CASE WHEN trade_date <= ? THEN 'within_kline' ELSE 'outside_kline' END as status,
        COUNT(1)
    FROM report
    WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')
    AND trade_date IS NOT NULL
    GROUP BY status
""", (kline_max,))
print("\n报告 trade_date vs K线覆盖:")
for r in c.fetchall():
    print(f"  {r[0]}: {r[1]}")

# 5. 当前结算覆盖率
c.execute("SELECT COUNT(1) FROM settlement_result")
settled = c.fetchone()[0]
c.execute("SELECT COUNT(1) FROM report WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')")
eligible = c.fetchone()[0]
print(f"\n结算覆盖: {settled}/{eligible} = {round(settled*100/max(eligible,1),1)}%")

conn.close()
