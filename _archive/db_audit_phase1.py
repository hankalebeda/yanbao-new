"""Phase 1.1: DB snapshot audit for system availability assessment"""
import sqlite3

conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# Get all tables and row counts
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
print('=== TABLE ROW COUNTS ===')
empty_tables = []
data_tables = []
for t in tables:
    try:
        cur.execute(f'SELECT COUNT(*) FROM [{t}]')
        cnt = cur.fetchone()[0]
        if cnt == 0:
            empty_tables.append(t)
        else:
            data_tables.append((t, cnt))
        print(f'{t}: {cnt}')
    except Exception as e:
        print(f'{t}: ERROR - {e}')

print(f'\n=== SUMMARY ===')
print(f'Total tables: {len(tables)}')
print(f'Tables with data: {len(data_tables)}')
print(f'Empty tables: {len(empty_tables)}')
print(f'Empty table names: {empty_tables}')

# Key metrics
queries = [
    ('SELECT COUNT(*) FROM report', 'Reports total'),
    ("SELECT COUNT(*) FROM report WHERE status='published'", 'Reports published'),
    ('SELECT COUNT(*) FROM settlement_result', 'Settlement results'),
    ('SELECT COUNT(*) FROM prediction_outcome', 'Prediction outcomes'),
    ('SELECT COUNT(*) FROM app_user', 'Users'),
    ('SELECT COUNT(DISTINCT stock_code) FROM kline_daily', 'Kline stocks'),
    ('SELECT COUNT(*) FROM kline_daily', 'Kline rows'),
    ('SELECT COUNT(*) FROM instruction_card', 'Instruction cards'),
    ('SELECT COUNT(*) FROM report_data_usage', 'Data usage records'),
    ('SELECT COUNT(*) FROM report_data_usage_link', 'Data usage links'),
    ('SELECT COUNT(*) FROM report_citation', 'Report citations'),
    ('SELECT COUNT(*) FROM sim_account', 'Sim accounts'),
    ('SELECT COUNT(*) FROM sim_position', 'Sim positions'),
    ('SELECT COUNT(*) FROM sim_trade_instruction', 'Sim trade instructions'),
    ('SELECT COUNT(*) FROM notification', 'Notifications'),
    ('SELECT COUNT(*) FROM audit_log', 'Audit logs'),
    ('SELECT COUNT(*) FROM stock_score', 'Stock scores'),
    ('SELECT COUNT(*) FROM market_hotspot_item', 'Hotspot items'),
    ('SELECT COUNT(*) FROM market_hotspot_link', 'Hotspot links'),
    ('SELECT COUNT(*) FROM hotspot_raw', 'Hotspot raw'),
    ('SELECT COUNT(*) FROM hotspot_normalized', 'Hotspot normalized'),
    ('SELECT COUNT(*) FROM hotspot_top50', 'Hotspot top50'),
    ('SELECT COUNT(*) FROM hotspot_stock_link', 'Hotspot stock link'),
    ('SELECT COUNT(*) FROM stock_pool_snapshot', 'Stock pool snapshots'),
    ('SELECT COUNT(*) FROM stock_master', 'Stock master'),
    ('SELECT COUNT(*) FROM market_state_input', 'Market state input'),
    ('SELECT COUNT(*) FROM pipeline_run', 'Pipeline runs'),
    ('SELECT COUNT(*) FROM data_source_status', 'Data source status'),
]

print('\n=== KEY METRICS ===')
for q, label in queries:
    try:
        cur.execute(q)
        print(f'{label}: {cur.fetchone()[0]}')
    except Exception as e:
        print(f'{label}: ERROR - {e}')

# Quality distribution
print('\n=== REPORT QUALITY ===')
cur.execute('SELECT quality_flag, COUNT(*) FROM report GROUP BY quality_flag ORDER BY COUNT(*) DESC')
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]}')

# LLM fallback distribution
print('\n=== LLM FALLBACK ===')
cur.execute('SELECT llm_fallback_level, COUNT(*) FROM report GROUP BY llm_fallback_level ORDER BY COUNT(*) DESC')
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]}')

# Settlement stats
print('\n=== SETTLEMENT STATS ===')
try:
    cur.execute('SELECT COUNT(*), AVG(CASE WHEN actual_return > 0 THEN 1.0 ELSE 0.0 END), AVG(actual_return) FROM settlement_result WHERE actual_return IS NOT NULL')
    row = cur.fetchone()
    if row and row[0] > 0:
        print(f'  Count with returns: {row[0]}, Win rate: {row[1]:.2%}, Avg return: {row[2]:.4f}')
    else:
        print('  No settlement results with actual_return')
except Exception as e:
    print(f'  ERROR: {e}')

# Cookie session check
print('\n=== COOKIE SESSIONS ===')
try:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cookie_session'")
    if cur.fetchone():
        cur.execute('SELECT COUNT(*) FROM cookie_session')
        print(f'  Cookie sessions: {cur.fetchone()[0]}')
    else:
        print('  cookie_session table does not exist')
except Exception as e:
    print(f'  ERROR: {e}')

# Recent reports (last 7 days)
print('\n=== RECENT REPORTS ===')
try:
    cur.execute("SELECT trade_date, COUNT(*), AVG(CASE WHEN quality_flag='ok' THEN 1.0 WHEN quality_flag='stale_ok' THEN 0.8 WHEN quality_flag='degraded' THEN 0.5 ELSE 0.0 END) FROM report WHERE trade_date >= date('now', '-7 days') GROUP BY trade_date ORDER BY trade_date DESC")
    for row in cur.fetchall():
        print(f'  {row[0]}: {row[1]} reports, quality_score={row[2]:.2f}')
except Exception as e:
    print(f'  ERROR: {e}')

# sim_dashboard_snapshot
print('\n=== SIM DASHBOARD ===')
try:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sim_dashboard_snapshot'")
    if cur.fetchone():
        cur.execute('SELECT COUNT(*) FROM sim_dashboard_snapshot')
        print(f'  Sim dashboard snapshots: {cur.fetchone()[0]}')
    else:
        print('  sim_dashboard_snapshot table does not exist')
except Exception as e:
    print(f'  ERROR: {e}')

conn.close()
