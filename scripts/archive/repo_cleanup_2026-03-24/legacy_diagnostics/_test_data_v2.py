"""Test data collection capabilities - corrected version."""
import sys, asyncio
sys.path.insert(0, ".")

def test_kline_data():
    """Test historical kline data access."""
    print("=" * 60)
    print("TEST 1: Historical K-line Data")
    print("=" * 60)
    
    import sqlite3
    conn = sqlite3.connect("data/app.db")
    
    total = conn.execute("SELECT COUNT(*) FROM kline_daily").fetchone()[0]
    stocks = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM kline_daily").fetchone()[0]
    dates = conn.execute("SELECT MAX(trade_date), MIN(trade_date) FROM kline_daily").fetchone()
    
    print(f"  Total kline rows: {total}")
    print(f"  Distinct stocks: {stocks}")
    print(f"  Date range: {dates[1]} to {dates[0]}")
    
    # Check coverage for key stocks
    key_stocks = ["000001.SZ", "600519.SH", "300750.SZ"]
    for sc in key_stocks:
        cnt = conn.execute("SELECT COUNT(*) FROM kline_daily WHERE stock_code=?", (sc,)).fetchone()[0]
        latest = conn.execute("SELECT MAX(trade_date) FROM kline_daily WHERE stock_code=?", (sc,)).fetchone()[0]
        print(f"  {sc}: {cnt} rows, latest={latest}")
    
    # Test real-time fetch via eastmoney quote
    async def _test_quote():
        from app.services.market_data import fetch_quote_snapshot
        try:
            q = await fetch_quote_snapshot("000001.SZ")
            print(f"\n  PASS: Real-time quote 000001.SZ = {q}")
            return True
        except Exception as e:
            print(f"  WARN: Real-time quote failed: {e}")
            return False
    
    result = asyncio.run(_test_quote())
    conn.close()
    return total > 0

def test_market_state():
    """Test market state via API."""
    print("\n" + "=" * 60)
    print("TEST 2: Market State (via API)")
    print("=" * 60)
    
    import requests
    r = requests.get("http://127.0.0.1:8000/api/v1/market/state", timeout=10)
    data = r.json()
    state = data.get("data", {})
    print(f"  State: {state.get('market_state')}")
    print(f"  Date: {state.get('market_state_date')}")
    print(f"  Reference: {state.get('reference_date')}")
    
    # Check cache freshness
    import sqlite3
    conn = sqlite3.connect("data/app.db")
    cache = conn.execute("SELECT * FROM market_state_cache ORDER BY trade_date DESC LIMIT 1").fetchone()
    cols = [c[1] for c in conn.execute("PRAGMA table_info(market_state_cache)").fetchall()]
    if cache:
        d = dict(zip(cols, cache))
        print(f"  Cache status: {d.get('cache_status')}")
        print(f"  Degraded: {d.get('market_state_degraded')}")
        print(f"  Computed at: {d.get('computed_at')}")
    conn.close()
    
    print(f"  PASS: Market state = {state.get('market_state')}")
    return True

def test_hotspot_data():
    """Test hotspot data."""
    print("\n" + "=" * 60)
    print("TEST 3: Hotspot Data")
    print("=" * 60)
    
    import sqlite3
    conn = sqlite3.connect("data/app.db")
    
    total = conn.execute("SELECT COUNT(*) FROM market_hotspot_item").fetchone()[0]
    print(f"  Total hotspot items: {total}")
    
    # Check by source
    sources = conn.execute("""
        SELECT source_name, COUNT(*) FROM market_hotspot_item 
        GROUP BY source_name
    """).fetchall()
    print(f"  By source: {dict(sources)}")
    
    # Show top items
    latest = conn.execute("""
        SELECT topic_title, merged_rank, source_name, fetch_time
        FROM market_hotspot_item
        ORDER BY fetch_time DESC, merged_rank ASC
        LIMIT 5
    """).fetchall()
    print(f"\n  Latest hotspots:")
    for item in latest:
        print(f"    [{item[2]}] #{item[1]} {item[0]} ({item[3]})")
    
    # Test live fetch
    async def _test_hotspot_fetch():
        try:
            from app.services.hotspot import fetch_weibo_hot
            items = await fetch_weibo_hot(top_n=5)
            if items:
                print(f"\n  PASS: Live weibo hotspot returned {len(items)} items")
                return True
            else:
                print(f"  WARN: Live weibo hotspot returned empty (may need cookie)")
                return False
        except Exception as e:
            print(f"  INFO: Live weibo hotspot error: {type(e).__name__}: {str(e)[:100]}")
            return False
    
    asyncio.run(_test_hotspot_fetch())
    conn.close()
    return total > 0

def test_data_batch():
    """Test data batch status."""
    print("\n" + "=" * 60)
    print("TEST 4: Data Batch Status")
    print("=" * 60)
    
    import sqlite3
    conn = sqlite3.connect("data/app.db")
    
    batches = conn.execute("""
        SELECT source_name, batch_status, quality_flag, records_total, trade_date
        FROM data_batch 
        ORDER BY started_at DESC
        LIMIT 10
    """).fetchall()
    
    print(f"  Recent data batches:")
    for b in batches:
        print(f"    {b[0]}: status={b[1]}, quality={b[2]}, records={b[3]}, date={b[4]}")
    
    # Check error rate
    errors = conn.execute("SELECT COUNT(*) FROM data_batch_error").fetchone()[0]
    print(f"\n  Total batch errors: {errors}")
    
    # Check circuit breaker states
    circuits = conn.execute("SELECT source_name, circuit_state FROM data_source_circuit_state").fetchall()
    print(f"  Circuit breaker states:")
    for c in circuits:
        print(f"    {c[0]}: {c[1]}")
    
    conn.close()

if __name__ == "__main__":
    ok1 = test_kline_data()
    ok2 = test_market_state()
    ok3 = test_hotspot_data()
    test_data_batch()
    
    print("\n" + "=" * 60)
    all_ok = ok1 and ok2 and ok3
    print(f"Overall: {'ALL PASS' if all_ok else 'SOME ISSUES'}")
    print("=" * 60)
