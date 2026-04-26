"""
测试外部数据源可用性
"""
import asyncio
import httpx

_EASTMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/",
}
_HTTPX_DIRECT = {"trust_env": False}

async def test_eastmoney_kline():
    """测试东方财富K线API"""
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": "1.600519",
        "klt": "101",
        "fqt": "1",
        "lmt": "5",
        "end": "20500000",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
    }
    try:
        async with httpx.AsyncClient(timeout=10, headers=_EASTMONEY_HEADERS, **_HTTPX_DIRECT) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            klines = (data.get("data") or {}).get("klines") or []
            print(f"Eastmoney K线 OK: {len(klines)} 条")
            for k in klines[-3:]:
                print(f"  {k}")
            return True
    except Exception as e:
        print(f"Eastmoney K线 FAILED: {e}")
        return False

async def test_eastmoney_quote():
    """测试东方财富行情API"""
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": "1.600519",
        "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f170",
    }
    try:
        async with httpx.AsyncClient(timeout=10, headers=_EASTMONEY_HEADERS, **_HTTPX_DIRECT) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            print(f"Eastmoney 行情 OK: {data.get('data', {})}")
            return True
    except Exception as e:
        print(f"Eastmoney 行情 FAILED: {e}")
        return False

async def test_xueqiu_hotspot():
    """测试雪球热点API"""
    url = "https://stock.xueqiu.com/v5/stock/hot_stock/list.json"
    params = {"size": 10, "type": 10}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://xueqiu.com/",
    }
    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            print(f"雪球热点 OK: {data}")
            return True
    except Exception as e:
        print(f"雪球热点 FAILED: {e}")
        return False

async def test_tushare():
    """测试北向资金数据"""
    try:
        import tushare as ts
        print("tushare 已安装")
    except ImportError:
        print("tushare 未安装")
        return False

async def main():
    print("=== 测试外部数据源可用性 ===")
    print()
    r1 = await test_eastmoney_kline()
    print()
    r2 = await test_eastmoney_quote()
    print()
    await test_tushare()
    print()
    print(f"=== 总结 ===")
    print(f"东方财富K线: {'可用' if r1 else '不可用'}")
    print(f"东方财富行情: {'可用' if r2 else '不可用'}")

asyncio.run(main())
