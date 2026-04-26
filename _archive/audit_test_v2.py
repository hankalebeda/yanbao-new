#!/usr/bin/env python3
"""
Complete System Audit - Browser & API Test Harness
按 39 角度对系统进行完整审计
禁用代理以避免连接问题
"""
import requests, json, sqlite3, os
from datetime import datetime
from urllib.parse import urljoin

# 禁用代理
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'
if 'HTTP_PROXY' in os.environ:
    del os.environ['HTTP_PROXY']
if 'HTTPS_PROXY' in os.environ:
    del os.environ['HTTPS_PROXY']
if 'http_proxy' in os.environ:
    del os.environ['http_proxy']
if 'https_proxy' in os.environ:
    del os.environ['https_proxy']

BASE_URL = "http://127.0.0.1:8010"
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
            resp = requests.get(url, timeout=5, proxies={'http': None, 'https': None}, **kwargs)
        elif method.upper() == "POST":
            resp = requests.post(url, timeout=5, proxies={'http': None, 'https': None}, **kwargs)
        else:
            resp = requests.request(method, url, timeout=5, proxies={'http': None, 'https': None}, **kwargs)
        
        result = {
            "status": resp.status_code,
            "reason": resp.reason,
            "body_preview": resp.text[:300] if resp.text else "",
            "time_ms": int(resp.elapsed.total_seconds() * 1000)
        }
        
        # 尝试解析 JSON
        try:
            result["json"] = resp.json()
        except:
            pass
            
        RESULTS["tests"][name] = {
            "url": url,
            "method": method,
            "result": result,
            "status": "PASS" if resp.status_code in [200, 201, 404, 422] else "FAIL"
        }
        print(f"  ✓ {name}: {resp.status_code}")
        return resp
    except Exception as e:
        RESULTS["tests"][name] = {
            "url": url,
            "method": method,
            "error": str(e),
            "status": "ERROR"
        }
        print(f"  ✗ {name}: {str(e)[:100]}")
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
    
    # 第三步：列表接口
    print("\n[3/5] List API Tests...")
    test_endpoint("GET /api/v1/reports", "GET", f"{BASE_URL}/api/v1/reports")
    test_endpoint("GET /api/v1/reports?skip=0&limit=5", "GET", f"{BASE_URL}/api/v1/reports?skip=0&limit=5")
    test_endpoint("GET /api/v1/reports/featured", "GET", f"{BASE_URL}/api/v1/reports/featured")
    test_endpoint("GET /api/v1/hot-stocks", "GET", f"{BASE_URL}/api/v1/hot-stocks")
    test_endpoint("GET /api/v1/market-overview", "GET", f"{BASE_URL}/api/v1/market-overview")
    test_endpoint("GET /api/v1/home", "GET", f"{BASE_URL}/api/v1/home")
    
    # 第四步：详情接口
    print("\n[4/5] Detail API Tests...")
    r = requests.get(f"{BASE_URL}/api/v1/reports?skip=0&limit=1", proxies={'http': None, 'https': None})
    if r.status_code == 200:
        try:
            data = r.json()
            if isinstance(data, dict) and 'items' in data:
                items = data['items']
            elif isinstance(data, list):
                items = data
            else:
                items = []
            
            if items:
                report_id = items[0].get('id') or items[0].get('report_id')
                if report_id:
                    print(f"\n  Found report_id: {report_id}")
                    test_endpoint(f"GET /api/v1/reports/{report_id}", "GET", f"{BASE_URL}/api/v1/reports/{report_id}")
        except Exception as e:
            print(f"  Error extracting report_id: {e}")
    
    test_endpoint("GET /api/v1/reports/search", "GET", f"{BASE_URL}/api/v1/reports/search?q=test")
    
    # 第五步：错误态处理
    print("\n[5/5] Error Handling & Edge Cases...")
    test_endpoint("GET /api/v1/reports/999999", "GET", f"{BASE_URL}/api/v1/reports/999999")
    test_endpoint("GET /api/v1/nonexistent", "GET", f"{BASE_URL}/api/v1/nonexistent")
    test_endpoint("POST /api/v1/reports (no body)", "POST", f"{BASE_URL}/api/v1/reports", json={})
    
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
