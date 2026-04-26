"""
LEGACY SCRIPT — DO NOT USE AS GATE/VERIFICATION.
This script imports from old modules (app.services.market_data) and uses direct sqlite3 access.
For current verification, use: pytest tests/ or scripts/verify_lightweight.py

Original: Test data collection capabilities - kline, market state, hotspot.
"""
import sys
sys.path.insert(0, ".")

def test_kline_data():
    """Test historical kline data fetch via eastmoney."""
    print("=" * 60)
    print("TEST 1: Historical K-line Data")
    print("=" * 60)
    
    import sqlite3
    conn = sqlite3.connect("data/app.db")
    
    # Check existing kline data
    total = conn.execute("SELECT COUNT(*) FROM kline_daily").fetchone()[0]
    stocks = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM kline_daily").fetchone()[0]
    latest = conn.execute("SELECT MAX(trade_date), MIN(trade_date) FROM kline_daily").fetchone()
    print(f"  Existing: {total} rows, {stocks} stocks, range: {latest[1]} to {latest[0]}")
    
    # Check if data is stale (more than 1 day old)
    from datetime import date
    today = date.today().isoformat()
    print(f"  Today: {today}, Latest data: {latest[0]}")
    
    # Test fetching fresh data for one stock
    try:
        from app.services.market_data import fetch_kline_eastmoney
        data = fetch_kline_eastmoney("000001.SZ", days=5)
        if data is not None and len(data) > 0:
            print(f"  PASS: eastmoney fetch returned {len(data)} rows for 000001.SZ")
            print(f"  Latest: {data.iloc[-1].to_dict() if hasattr(data, 'iloc') else data[-1]}")
        else:
            print(f"  WARN: eastmoney fetch returned {len(data) if data is not None else None} rows")
    except Exception as e:
        print(f"  FAIL: eastmoney fetch error: {e}")
    
    # Test TDX data source
    try:
        from app.services.tdx_local_data import fetch_kline_tdx
        data = fetch_kline_tdx("000001.SZ", days=5)
        if data is not None and len(data) > 0:
            print(f"  PASS: TDX fetch returned {len(data)} rows for 000001.SZ")
        else:
            print(f"  INFO: TDX fetch returned {len(data) if data is not None else None} rows (may not be available)")
    except Exception as e:
        print(f"  INFO: TDX not available: {type(e).__name__}: {e}")
    
    conn.close()

def test_market_state():
    """Test real-time market state calculation."""
    print("\n" + "=" * 60)
    print("TEST 2: Market State")
    print("=" * 60)
    
    try:
        from app.services.market_state import compute_market_state
        from app.core.db import SessionLocal
        db = SessionLocal()
        try:
            state = compute_market_state(db)
            print(f"  Market state: {state}")
            if isinstance(state, dict):
                for k, v in state.items():
                    print(f"    {k}: {v}")
            print(f"  PASS: Market state computed successfully")
        finally:
            db.close()
    except Exception as e:
        print(f"  FAIL: Market state error: {type(e).__name__}: {e}")

    # Also check via API
    try:
        import requests
        r = requests.get("http://127.0.0.1:8000/api/v1/market/state", timeout=10)
        data = r.json()
        print(f"  API response: {data}")
        if data.get("data", {}).get("market_state"):
            print(f"  PASS: Market state API returns: {data['data']['market_state']}")
        else:
            print(f"  WARN: Market state API returned unexpected data")
    except Exception as e:
        print(f"  FAIL: Market state API error: {e}")

def test_hotspot_data():
    """Test hotspot data collection."""
    print("\n" + "=" * 60)
    print("TEST 3: Hotspot Data")
    print("=" * 60)
    
    import sqlite3
    conn = sqlite3.connect("data/app.db")
    
    hotspot_count = conn.execute("SELECT COUNT(*) FROM market_hotspot_item").fetchone()[0]
    source_count = conn.execute("SELECT COUNT(*) FROM market_hotspot_item_source").fetchone()[0]
    
    print(f"  Hotspot items: {hotspot_count}")
    print(f"  Source records: {source_count}")
    
    # Check source distribution
    sources = conn.execute("SELECT source_name, COUNT(*) FROM market_hotspot_item_source GROUP BY source_name").fetchall()
    print(f"  Sources: {dict(sources)}")
    
    # Check latest hotspot data
    latest = conn.execute("""
        SELECT h.keyword, h.score, h.fetch_time, GROUP_CONCAT(s.source_name) as sources
        FROM market_hotspot_item h
        LEFT JOIN market_hotspot_item_source s ON h.hotspot_id = s.hotspot_id
        GROUP BY h.hotspot_id
        ORDER BY h.score DESC
        LIMIT 5
    """).fetchall()
    print(f"\n  Top 5 hotspots:")
    for item in latest:
        print(f"    {item[0]} (score={item[1]}, sources={item[3]}, time={item[2]})")
    
    conn.close()
    
    # Test fetching fresh hotspot data
    try:
        from app.services.hotspot import fetch_eastmoney_hot
        items = fetch_eastmoney_hot()
        if items:
            print(f"\n  PASS: eastmoney hotspot returned {len(items)} items")
            if items:
                print(f"  Sample: {items[0] if isinstance(items[0], dict) else items[0]}")
        else:
            print(f"  WARN: eastmoney hotspot returned empty")
    except Exception as e:
        print(f"  INFO: eastmoney hotspot error: {type(e).__name__}: {e}")

if __name__ == "__main__":
    test_kline_data()
    test_market_state()
    test_hotspot_data()
    print("\n" + "=" * 60)
    print("Data collection tests complete!")
    print("=" * 60)
