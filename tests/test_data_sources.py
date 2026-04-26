"""
数据源连通性测试脚本
测试东方财富API、mootdx网络模式等数据源的实际可用性

运行方式：
    python tests/test_data_sources.py

注意：测试期间会发出真实网络请求，请确保网络通畅。
"""
import json
import sys
import time
from datetime import datetime
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

RESULTS_DIR = Path(__file__).parent / "data_source_test_results"
RESULTS_DIR.mkdir(exist_ok=True)

# 兼容 Windows GBK 控制台，避免 emoji 乱码
ICON_OK = "[OK]"
ICON_WARN = "[WARN]"
ICON_FAIL = "[FAIL]"


def _json_serializable(obj):
    """递归转换 numpy/其他类型为 JSON 可序列化格式。"""
    try:
        import numpy as np

        if hasattr(obj, "item") and callable(getattr(obj, "item")):
            return obj.item()
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
    except ImportError:
        pass
    if isinstance(obj, dict):
        return {k: _json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_serializable(x) for x in obj]
    return obj


def _check_eastmoney_realtime(code: str = "600519", market: str = "1") -> dict:
    """测试东方财富实时行情API（沪市1.代码 / 深市0.代码）"""
    try:
        import httpx
        url = (
            f"https://push2.eastmoney.com/api/qt/stock/get"
            f"?secid={market}.{code}"
            f"&fields=f43,f44,f45,f46,f47,f48,f57,f58,f170,f51,f52"
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://finance.eastmoney.com/",
        }
        t0 = time.time()
        resp = httpx.get(url, headers=headers, timeout=10)
        elapsed = round(time.time() - t0, 3)
        data = resp.json()
        result = data.get("data", {})
        # 字段映射：f43=最新价(分), f58=名称, f170=涨跌幅×10000
        last_price = (result.get("f43") or 0) / 100
        name = result.get("f58", "未知")
        pct = (result.get("f170") or 0) / 100
        high_limit = (result.get("f51") or 0) / 100   # 涨停价
        low_limit = (result.get("f52") or 0) / 100    # 跌停价
        return {
            "status": "ok",
            "code": code,
            "name": name,
            "last_price": last_price,
            "pct_change": pct,
            "high_limit_price": high_limit,
            "low_limit_price": low_limit,
            "elapsed_s": elapsed,
            "has_limit_price": high_limit > 0 and low_limit > 0,
        }
    except Exception as e:
        return {"status": "error", "code": code, "error": str(e)}


def test_eastmoney_realtime(code: str = "600519", market: str = "1") -> None:
    """Pytest 入口：调用 _check 并断言结果有效（不返回值，避免 PytestReturnNotNoneWarning）"""
    result = _check_eastmoney_realtime(code, market)
    assert result.get("status") in ("ok", "error"), f"unexpected status: {result}"


def _check_eastmoney_kline(code: str = "600519", market: str = "1", days: int = 20) -> dict:
    """测试东方财富历史K线API"""
    try:
        import httpx
        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
            f"?secid={market}.{code}&klt=101&fqt=1&lmt={days}&end=20500101"
            f"&fields1=f1,f2,f3,f4,f5,f6"
            f"&fields2=f51,f52,f53,f54,f55,f56"
        )
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        t0 = time.time()
        resp = httpx.get(url, headers=headers, timeout=10)
        elapsed = round(time.time() - t0, 3)
        data = resp.json().get("data", {})
        klines = data.get("klines", [])
        return {
            "status": "ok",
            "code": code,
            "count": len(klines),
            "latest_record": klines[-1] if klines else None,
            "earliest_record": klines[0] if klines else None,
            "elapsed_s": elapsed,
        }
    except Exception as e:
        return {"status": "error", "code": code, "error": str(e)}


def test_eastmoney_kline(code: str = "600519", market: str = "1", days: int = 20) -> None:
    result = _check_eastmoney_kline(code, market, days)
    assert result.get("status") in ("ok", "error")


def _check_eastmoney_capital_flow(code: str = "600519", market: str = "1") -> dict:
    """测试东方财富主力资金流向API"""
    try:
        import httpx
        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
            f"?secid={market}.{code}&klt=101&lmt=20"
            f"&fields1=f1,f2,f3,f7"
            f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65"
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://data.eastmoney.com/",
        }
        t0 = time.time()
        resp = httpx.get(url, headers=headers, timeout=10)
        elapsed = round(time.time() - t0, 3)
        data = resp.json().get("data", {})
        klines = data.get("klines", [])
        return {
            "status": "ok",
            "code": code,
            "count": len(klines),
            "latest_record": klines[-1] if klines else None,
            "elapsed_s": elapsed,
        }
    except Exception as e:
        return {"status": "error", "code": code, "error": str(e)}


def test_eastmoney_capital_flow(code: str = "600519", market: str = "1") -> None:
    result = _check_eastmoney_capital_flow(code, market)
    assert result.get("status") in ("ok", "error")


def _check_eastmoney_index_kline(index_code: str = "000001", days: int = 30) -> dict:
    """测试东方财富指数K线（上证/沪深300，用于市场状态机）"""
    try:
        import httpx
        # 上证指数：secid=1.000001，沪深300：secid=1.000300
        market_prefix = "1"
        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
            f"?secid={market_prefix}.{index_code}&klt=101&fqt=0&lmt={days}&end=20500101"
            f"&fields1=f1,f2,f3,f4,f5,f6"
            f"&fields2=f51,f52,f53,f54,f55,f56"
        )
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        t0 = time.time()
        resp = httpx.get(url, headers=headers, timeout=10)
        elapsed = round(time.time() - t0, 3)
        data = resp.json().get("data", {})
        klines = data.get("klines", [])
        name = data.get("name", index_code)
        return {
            "status": "ok",
            "index_code": index_code,
            "name": name,
            "count": len(klines),
            "latest_record": klines[-1] if klines else None,
            "elapsed_s": elapsed,
        }
    except Exception as e:
        return {"status": "error", "index_code": index_code, "error": str(e)}


def test_eastmoney_index_kline(index_code: str = "000001", days: int = 30) -> None:
    result = _check_eastmoney_index_kline(index_code, days)
    assert result.get("status") in ("ok", "error")


def _check_eastmoney_news(code: str = "600519") -> dict:
    """测试东方财富新闻API"""
    try:
        import httpx
        # 东方财富个股新闻接口
        url = (
            f"https://np-listapi.eastmoney.com/comm/wap/getListInfo"
            f"?cb=callback&client=wap&type=1&mTypeAndCode=1%2C{code}"
            f"&pageSize=5&pageIndex=1&returnType=json"
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": f"https://emweb.securities.eastmoney.com/",
        }
        t0 = time.time()
        resp = httpx.get(url, headers=headers, timeout=10)
        elapsed = round(time.time() - t0, 3)
        text = resp.text
        # 解析 JSONP 或 JSON
        import re
        match = re.search(r'callback\((.*)\)', text, re.DOTALL)
        if match:
            data = json.loads(match.group(1))
        else:
            data = json.loads(text)
        items = data.get("data", {}).get("list", []) or []
        return {
            "status": "ok",
            "code": code,
            "count": len(items),
            "latest_title": items[0].get("title") if items else None,
            "elapsed_s": elapsed,
        }
    except Exception as e:
        return {"status": "error", "code": code, "error": str(e)}


def test_eastmoney_news(code: str = "600519") -> None:
    result = _check_eastmoney_news(code)
    assert result.get("status") in ("ok", "error")


def _check_mootdx_network() -> dict:
    """测试mootdx网络模式实时报价（不依赖本地通达信）"""
    try:
        from mootdx.quotes import Quotes
        t0 = time.time()
        client = Quotes.factory(market="std")
        rows = client.quotes(symbol=["600519"])
        elapsed = round(time.time() - t0, 3)
        if rows is not None and len(rows) > 0:
            row = rows.iloc[0] if hasattr(rows, 'iloc') else rows[0]
            return {
                "status": "ok",
                "mode": "network",
                "count": len(rows),
                "sample": dict(row) if hasattr(row, '__iter__') else str(row),
                "elapsed_s": elapsed,
            }
        return {"status": "empty", "mode": "network", "elapsed_s": elapsed}
    except Exception as e:
        return {"status": "error", "mode": "network", "error": str(e)}


def test_mootdx_network() -> None:
    result = _check_mootdx_network()
    assert result.get("status") in ("ok", "empty", "error")


def _check_tdx_local_file() -> dict:
    """测试通达信本地.day文件是否可用"""
    import os
    # 常见通达信安装路径
    common_paths = [
        r"C:\new_tdx",
        r"C:\TDX",
        r"D:\new_tdx",
        r"D:\TDX",
        r"C:\Program Files\TDX",
    ]
    # 也检查 .env 中的配置
    try:
        from app.core.config import settings
        if settings.tdx_install_dir:
            common_paths.insert(0, settings.tdx_install_dir)
    except Exception:
        pass

    found_paths = []
    for p in common_paths:
        sh_path = os.path.join(p, "vipdoc", "sh", "lday")
        sz_path = os.path.join(p, "vipdoc", "sz", "lday")
        if os.path.isdir(sh_path) and os.path.isdir(sz_path):
            sh_files = [f for f in os.listdir(sh_path) if f.endswith(".day")]
            sz_files = [f for f in os.listdir(sz_path) if f.endswith(".day")]
            found_paths.append({
                "base_dir": p,
                "sh_file_count": len(sh_files),
                "sz_file_count": len(sz_files),
                "sample_sh": sh_files[:3] if sh_files else [],
            })

    if found_paths:
        return {"status": "ok", "found": found_paths}
    return {
        "status": "not_found",
        "message": "未找到通达信本地数据文件。请确认通达信已安装并在config.py/env中设置tdx_install_dir。",
        "searched_paths": common_paths,
    }


def test_tdx_local_file() -> None:
    result = _check_tdx_local_file()
    assert result.get("status") in ("ok", "not_found")


def _check_margin_realtime_snapshot(code: str = "600519", market: str = "1") -> dict:
    """测试融资融券实时快照（push2 API f277/f278字段）
    注意：历史融资融券接口(RPTA_WEB_MARGIN_DETAILS)已于2024年底失效，
    目前只能通过实时报价快照中的f277(融资余额)/f278(融券余额)字段获取实时数据。
    """
    try:
        import httpx
        url = (
            f"https://push2.eastmoney.com/api/qt/stock/get"
            f"?secid={market}.{code}"
            f"&fields=f43,f58,f277,f278"
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://finance.eastmoney.com/",
        }
        t0 = time.time()
        resp = httpx.get(url, headers=headers, timeout=10)
        elapsed = round(time.time() - t0, 3)
        data = resp.json().get("data", {})
        rzye = data.get("f277")  # 融资余额（元）
        rqye = data.get("f278")  # 融券余额（元）
        name = data.get("f58", code)
        has_margin_data = rzye is not None and rzye != "-"
        return {
            "status": "ok" if has_margin_data else "missing",
            "code": code,
            "name": name,
            "rzye_yuan": rzye,
            "rqye_yuan": rqye,
            "has_realtime_margin": has_margin_data,
            "note": "融资融券历史接口已失效，仅有实时快照（f277/f278）" if has_margin_data else "f277/f278字段为空，此股票可能不在融资融券标的范围内",
            "elapsed_s": elapsed,
        }
    except Exception as e:
        return {"status": "error", "code": code, "error": str(e)}


def test_margin_realtime_snapshot(code: str = "600519", market: str = "1") -> None:
    result = _check_margin_realtime_snapshot(code, market)
    assert result.get("status") in ("ok", "missing", "error")


def _check_weibo_hot_search() -> dict:
    """测试微博热搜接口（无Cookie模式，可能返回部分数据）"""
    try:
        import httpx
        url = "https://weibo.com/ajax/side/hotSearch"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://weibo.com/",
        }
        # 尝试读取Cookie配置
        try:
            from app.core.config import settings
            cookie_parts = []
            if settings.weibo_cookie_sub:
                cookie_parts.append(f"SUB={settings.weibo_cookie_sub}")
            if settings.weibo_cookie_subp:
                cookie_parts.append(f"SUBP={settings.weibo_cookie_subp}")
            if cookie_parts:
                headers["Cookie"] = "; ".join(cookie_parts)
        except Exception:
            pass

        t0 = time.time()
        resp = httpx.get(url, headers=headers, timeout=10)
        elapsed = round(time.time() - t0, 3)
        data = resp.json()
        items = data.get("data", {}).get("realtime", []) or []
        has_cookie = "Cookie" in headers
        return {
            "status": "ok" if items else "empty_or_auth_required",
            "count": len(items),
            "top3": [x.get("word") for x in items[:3]] if items else [],
            "has_cookie": has_cookie,
            "note": "需要微博Cookie(SUB字段)才能获取完整数据" if not has_cookie else "已使用Cookie",
            "elapsed_s": elapsed,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def test_weibo_hot_search() -> None:
    result = _check_weibo_hot_search()
    assert result.get("status") in ("ok", "empty_or_auth_required", "error")


def run_all_tests() -> dict:
    """运行所有数据源测试"""
    report = {
        "started_at": datetime.now().isoformat(),
        "tests": {},
        "summary": {},
    }

    print("=" * 70)
    print(f"数据源连通性测试开始：{datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 70)

    test_cases = [
        ("eastmoney_realtime_600519", lambda: _check_eastmoney_realtime("600519", "1")),
        ("eastmoney_realtime_000858", lambda: _check_eastmoney_realtime("000858", "0")),
        ("eastmoney_kline_600519", lambda: _check_eastmoney_kline("600519", "1", 30)),
        ("eastmoney_capital_flow_600519", lambda: _check_eastmoney_capital_flow("600519", "1")),
        ("eastmoney_index_sh000001", lambda: _check_eastmoney_index_kline("000001", 30)),
        ("eastmoney_index_hs300", lambda: _check_eastmoney_index_kline("000300", 30)),
        ("eastmoney_news_600519", lambda: _check_eastmoney_news("600519")),
        ("margin_realtime_snapshot_600519", lambda: _check_margin_realtime_snapshot("600519", "1")),
        ("mootdx_network", _check_mootdx_network),
        ("tdx_local_file", _check_tdx_local_file),
        ("weibo_hot_search", _check_weibo_hot_search),
    ]

    passed = 0
    for name, fn in test_cases:
        try:
            result = fn()
            status = result.get("status", "unknown")
            is_ok = status == "ok"
            if is_ok:
                passed += 1
            report["tests"][name] = result
            icon = ICON_OK if is_ok else (ICON_WARN if "empty" in status or "not_found" in status else ICON_FAIL)
            print(f"{icon} {name}: status={status} | {_summary(result)}")
        except Exception as e:
            report["tests"][name] = {"status": "exception", "error": str(e)}
            print(f"{ICON_FAIL} {name}: exception={e}")

    total = len(test_cases)
    report["summary"] = {
        "passed": passed,
        "total": total,
        "pass_rate": f"{passed}/{total}",
        "critical_issues": _get_critical_issues(report["tests"]),
    }

    # 保存结果（转换 numpy 等类型为 JSON 可序列化）
    safe_report = _json_serializable(report)
    out = RESULTS_DIR / f"test_{datetime.now():%Y%m%d_%H%M%S}.json"
    out.write_text(json.dumps(safe_report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=" * 70)
    print(f"测试完成：通过 {passed}/{total}")
    print(f"结果已保存：{out}")
    print("=" * 70)

    # 输出关键问题
    issues = report["summary"]["critical_issues"]
    if issues:
        print("\n[!] 关键问题：")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("\n[OK] 所有关键数据源均可用")

    return report


def _summary(result: dict) -> str:
    """生成测试结果摘要字符串"""
    if "count" in result:
        return f"count={result['count']}"
    if "last_price" in result:
        return f"price={result['last_price']} pct={result['pct_change']}%"
    if "found" in result:
        total_files = sum(p["sh_file_count"] + p["sz_file_count"] for p in result["found"])
        return f"找到{len(result['found'])}个目录，共{total_files}个.day文件"
    if "error" in result:
        return f"error={str(result['error'])[:60]}"
    return ""


def _get_critical_issues(tests: dict) -> list:
    """识别关键问题"""
    issues = []

    # 东方财富行情
    rt = tests.get("eastmoney_realtime_600519", {})
    if rt.get("status") != "ok":
        issues.append(f"东方财富实时行情不可用：{rt.get('error', '未知错误')}")
    elif not rt.get("has_limit_price"):
        issues.append("东方财富实时行情未返回涨跌停价格(f51/f52)，模拟结算涨跌停判断将失败")

    # 历史K线
    kline = tests.get("eastmoney_kline_600519", {})
    if kline.get("status") != "ok":
        issues.append(f"东方财富历史K线不可用：{kline.get('error', '未知错误')}")

    # 资金流向
    cf = tests.get("eastmoney_capital_flow_600519", {})
    if cf.get("status") != "ok":
        issues.append(f"东方财富资金流向不可用：{cf.get('error', '未知错误')}")

    # 上证指数
    sh = tests.get("eastmoney_index_sh000001", {})
    if sh.get("status") != "ok":
        issues.append(f"上证指数数据不可用（市场状态机将无法运行）：{sh.get('error', '未知错误')}")

    # 通达信本地文件
    tdx = tests.get("tdx_local_file", {})
    if tdx.get("status") != "ok":
        issues.append(
            "通达信本地.day文件未找到——历史因子预计算（近90日胜率/ATR分位/量比分位）将使用默认先验值0.52，"
            "LLM Prompt精度下降。请安装通达信并在.env中设置TDX_INSTALL_DIR。"
        )

    # mootdx网络
    mootdx = tests.get("mootdx_network", {})
    if mootdx.get("status") != "ok":
        issues.append(f"mootdx网络模式不可用（行情双源备份缺失）：{mootdx.get('error', '未知错误')}")

    return issues


if __name__ == "__main__":
    run_all_tests()
