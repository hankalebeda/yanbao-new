#!/usr/bin/env python3
"""
Complete System Audit - Browser & API Test Harness
按 39 角度对系统进行完整审计
"""
import requests, json, sqlite3
from datetime import datetime
import sys

BASE_URL = "http://localhost:8010"
RESULTS = {
    "audit_date": datetime.now().isoformat(),
    "tests": {},
    "problems": [],
    "system_snapshot": {}
}

def test_endpoint(name, method, url, **kwargs):
    """测试一个端点，记录结果"""
    try:
        if method.upper() == "GET":
            r = requests.get(url, timeout=5, **kwargs)
        elif method.upper() == "POST":
            r = requests.post(url, timeout=5, **kwargs)
        else:
            r = requests.request(method, url, timeout=5, **kwargs)
        
        result = {
            "status": r.status_code,
            "headers": dict(r.headers),
            "body_preview": r.text[:500] if r.text else "",
            "time_ms": int(r.elapsed.total_seconds() * 1000)
        }
        
        # 尝试解析 JSON
        try:
            result["json"] = r.json()
        except:
            pass
            
        RESULTS["tests"][name] = {
            "url": url,
            "method": method,
            "result": result,
            "status": "PASS" if r.status_code in [200, 201, 422] else "FAIL"
        }
        return r
    except Exception as e:
        RESULTS["tests"][name] = {
            "url": url,
            "method": method,
            "error": str(e),
            "status": "ERROR"
        }
        return None

def get_db_snapshot():
    """获取数据库快照"""
    conn = sqlite3.connect('data/app.db')
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    snapshot = {
        "report_total": c.execute('select count(*) from report').fetchone()[0],
        "report_visible": c.execute("select count(*) from report where published=1 and is_deleted=0").fetchone()[0],
        "report_quality_ok": c.execute("select count(*) from report where published=1 and is_deleted=0 and lower(coalesce(quality_flag,'ok'))='ok'").fetchone()[0],
        "report_quality_stale_ok": c.execute("select count(*) from report where published=1 and is_deleted=0 and lower(coalesce(quality_flag,'ok'))='stale_ok'").fetchone()[0],
        "report_quality_non_ok": c.execute("select count(*) from report where published=1 and is_deleted=0 and lower(coalesce(quality_flag,'ok'))<>'ok' and lower(coalesce(quality_flag,'ok'))<>'stale_ok'").fetchone()[0],
        "stock_master_total": c.execute('select count(*) from stock_master').fetchone()[0],
        "kline_stock_covered": c.execute('select count(distinct stock_code) from kline_daily').fetchone()[0],
        "kline_rows": c.execute('select count(*) from kline_daily').fetchone()[0],
        "settlement_records": c.execute('select count(*) from settlement_result').fetchone()[0],
        "hotspot_items": c.execute('select count(*) from market_hotspot_item').fetchone()[0],
    }
    
    conn.close()
    return snapshot

def main():
    print("[AUDIT] Starting System Full Audit at", RESULTS["audit_date"])
    print("=" * 80)
    
    # 第一步：数据库快照
    print("\n[1/5] Database Snapshot...")
    RESULTS["system_snapshot"] = get_db_snapshot()
    print(json.dumps(RESULTS["system_snapshot"], indent=2))
    
    # 第二步：基础健康检查
    print("\n[2/5] Basic Health Checks...")
    test_endpoint("GET /health", "GET", f"{BASE_URL}/health")
    test_endpoint("GET /api/v1/health", "GET", f"{BASE_URL}/api/v1/health")
    test_endpoint("GET /docs", "GET", f"{BASE_URL}/docs")
    
    # 第三步：列表接口
    print("[3/5] List API Tests...")
    test_endpoint("GET /api/v1/reports", "GET", f"{BASE_URL}/api/v1/reports")
    test_endpoint("GET /api/v1/reports?skip=0&limit=5", "GET", f"{BASE_URL}/api/v1/reports?skip=0&limit=5")
    test_endpoint("GET /api/v1/reports/featured", "GET", f"{BASE_URL}/api/v1/reports/featured")
    test_endpoint("GET /api/v1/hot-stocks", "GET", f"{BASE_URL}/api/v1/hot-stocks")
    test_endpoint("GET /api/v1/market-overview", "GET", f"{BASE_URL}/api/v1/market-overview")
    
    # 第四步：详情接口
    print("[4/5] Detail API Tests...")
    # 先获取一个真实的 report_id
    r = requests.get(f"{BASE_URL}/api/v1/reports?skip=0&limit=1")
    if r.status_code == 200:
        try:
            reports = r.json()
            if isinstance(reports, dict) and 'items' in reports:
                items = reports['items']
            else:
                items = reports if isinstance(reports, list) else []
            
            if items:
                report_id = items[0].get('id') or items[0].get('report_id')
                if report_id:
                    print(f"  Found report_id: {report_id}")
                    test_endpoint(f"GET /api/v1/reports/{report_id}", "GET", f"{BASE_URL}/api/v1/reports/{report_id}")
        except Exception as e:
            print(f"  Error extracting report_id: {e}")
    
    test_endpoint("GET /api/v1/reports/search?q=test", "GET", f"{BASE_URL}/api/v1/reports/search?q=test")
    
    # 第五步：错误态处理
    print("[5/5] Error Handling Tests...")
    test_endpoint("GET /api/v1/reports/999999", "GET", f"{BASE_URL}/api/v1/reports/999999")
    test_endpoint("GET /api/v1/nonexistent", "GET", f"{BASE_URL}/api/v1/nonexistent")
    test_endpoint("POST /api/v1/reports (no auth)", "POST", f"{BASE_URL}/api/v1/reports", json={})
    
    # 输出最终结果
    print("\n" + "=" * 80)
    print("[AUDIT] Test Summary")
    print("=" * 80)
    
    passed = sum(1 for t in RESULTS["tests"].values() if t.get("status") == "PASS")
    failed = sum(1 for t in RESULTS["tests"].values() if t.get("status") in ["FAIL", "ERROR"])
    
    print(f"Total Tests: {len(RESULTS['tests'])}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    
    # 保存结果
    with open('_archive/audit_test_results.json', 'w', encoding='utf-8') as f:
        json.dump(RESULTS, f, ensure_ascii=False, indent=2)
    
    print(f"\nResults saved to _archive/audit_test_results.json")
    return RESULTS

if __name__ == "__main__":
    main()
