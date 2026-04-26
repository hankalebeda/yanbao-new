#!/usr/bin/env python3
"""
验证北向与 ETF 数据源可用性，检查是否满足方案需求。
运行: python scripts/verify_northbound_etf.py

若本机使用代理（如 Clash），国内站点（东方财富、交易所）走代理常导致 ConnectionResetError。
脚本会绕过代理直连，与 capital_flow/market_data 一致（trust_env=False / NO_PROXY）。
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# 确保项目根目录在 path 中
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bypass_proxy() -> None:
    """绕过系统代理直连国内站点，与项目 capital_flow/market_data 一致"""
    os.environ["NO_PROXY"] = "*"
    # 临时清除代理变量，使 requests/akshare 直连
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(k, None)


def _retry(func, *args, max_attempts=2, delay=3, **kwargs):
    """简单重试，避免偶发网络问题"""
    for i in range(max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i < max_attempts - 1:
                time.sleep(delay)
            else:
                raise


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    # 绕过代理直连国内站点（与 capital_flow 一致）
    _bypass_proxy()
    print("已设置 NO_PROXY 绕过代理，直连东方财富/交易所")

    # 0. 网络可达性：项目已有的东方财富接口是否可访问
    print("--- 网络可达性：东方财富 push2 API ---")
    try:
        import httpx
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {"secid": "1.600519", "fields": "f43,f58"}
        r = httpx.get(
            url, params=params, timeout=10,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"},
            trust_env=False,  # 与 capital_flow 一致，不读系统代理
        )
        r.raise_for_status()
        data = r.json()
        if data.get("data"):
            results.append(("东方财富 push2 可达", True, "与 capital_flow 同源，可访问"))
        else:
            results.append(("东方财富 push2 可达", True, "返回无 data，但连接成功"))
    except Exception as e:
        results.append(("东方财富 push2 可达", False, str(e)))
        print(f"  注意: 若此处失败，说明当前网络无法访问东方财富，akshare 也将失败")

    # 1. 检查 akshare 是否已安装
    try:
        import akshare as ak
        results.append(("akshare 已安装", True, f"版本: {getattr(ak, '__version__', 'unknown')}"))
    except ImportError as e:
        results.append(("akshare 已安装", False, str(e)))
        _print_results(results)
        return 1

    # 2. 北向资金个股 - stock_hsgt_individual_em
    print("--- 测试北向资金个股 (stock_hsgt_individual_em) ---")
    try:
        df = _retry(ak.stock_hsgt_individual_em, symbol="600519")
        if df is None or len(df) == 0:
            results.append(("北向个股 600519 返回数据", False, "返回空 DataFrame"))
        else:
            cols = list(df.columns)
            required = ["今日增持资金", "持股日期"]
            missing = [c for c in required if c not in cols]
            if missing:
                results.append(("北向个股字段检查", False, f"缺少列: {missing}, 实际列: {cols}"))
            else:
                latest = df.iloc[-1]
                net_1d = latest.get("今日增持资金")
                date_val = latest.get("持股日期")
                results.append(("北向个股 600519", True, f"行数={len(df)}, 最新日期={date_val}, 今日增持资金={net_1d}"))
            print(f"  行数: {len(df)}, 列: {cols}")
            if len(df) >= 5:
                print(f"  最近5行摘要:")
                print(df.tail().to_string())
    except Exception as e:
        results.append(("北向个股 600519", False, str(e)))
        print(f"  异常: {e}")

    # 3. ETF 份额 - 上交所
    print("\n--- 测试 ETF 份额-上交所 (fund_etf_scale_sse) ---")
    try:
        # 使用最近交易日，避免未来日期
        today = datetime.now().strftime("%Y%m%d")
        df_sse = _retry(ak.fund_etf_scale_sse, date=today)
        if df_sse is None or len(df_sse) == 0:
            # 可能今天非交易日，尝试前几日
            for d in range(1, 8):
                prev = (datetime.now() - timedelta(days=d)).strftime("%Y%m%d")
                df_sse = _retry(ak.fund_etf_scale_sse, date=prev)
                if df_sse is not None and len(df_sse) > 0:
                    break
        if df_sse is None or len(df_sse) == 0:
            results.append(("ETF 份额-上交所", False, "返回空或今日无数据，已尝试最近7日"))
        else:
            cols = list(df_sse.columns)
            results.append(("ETF 份额-上交所", True, f"ETF 数量={len(df_sse)}"))
            print(f"  ETF 数量: {len(df_sse)}, 列: {cols}")
    except Exception as e:
        results.append(("ETF 份额-上交所", False, f"akshare 接口解析异常: {e}"))
        print(f"  异常: {e}")

    # 4. ETF 份额 - 深交所
    print("\n--- 测试 ETF 份额-深交所 (fund_etf_scale_szse) ---")
    try:
        df_szse = _retry(ak.fund_etf_scale_szse)
        if df_szse is None or len(df_szse) == 0:
            results.append(("ETF 份额-深交所", False, "返回空"))
        else:
            cols = list(df_szse.columns)
            results.append(("ETF 份额-深交所", True, f"ETF 数量={len(df_szse)}"))
            print(f"  ETF 数量: {len(df_szse)}, 列: {cols}")
    except Exception as e:
        results.append(("ETF 份额-深交所", False, f"akshare 接口解析异常: {e}"))
        print(f"  异常: {e}")

    # 5. ETF 持仓 (fund_portfolio_hold_em) - 用于个股↔ETF 映射
    print("\n--- 测试 ETF 持仓 (fund_portfolio_hold_em) ---")
    try:
        df_hold = _retry(ak.fund_portfolio_hold_em, symbol="510050", date="2024")
        if df_hold is None or len(df_hold) == 0:
            results.append(("ETF 持仓 510050", False, "返回空"))
        else:
            cols = list(df_hold.columns)
            needed = ["股票代码", "股票名称", "占净值比例", "持仓市值"]
            ok = all(c in cols for c in needed)
            results.append(("ETF 持仓 510050", ok, f"行数={len(df_hold)}, 含个股数={len(df_hold)}"))
            print(f"  510050 持仓股票数: {len(df_hold)}, 列: {cols}")
    except Exception as e:
        results.append(("ETF 持仓 510050", False, str(e)))
        print(f"  异常: {e}")

    # 汇总与输出
    print("\n" + "=" * 60)
    _print_results(results)
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)

    # 写入 JSON 供记录
    out_dir = ROOT / "docs" / "core" / "test_results"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"verify_northbound_etf_{ts}.json"
    import json
    report = {
        "timestamp": ts,
        "passed": passed,
        "total": total,
        "all_ok": passed == total,
        "results": [{"name": n, "ok": ok, "msg": str(m)} for n, ok, m in results],
        "note": "ConnectionResetError 表示当前网络无法访问东方财富/交易所，请在可访问外网的环境（如生产服务器、VPN）下重跑。",
    }
    out_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n结果已保存: {out_file}")

    return 0 if passed == total else 1


def _print_results(results: list[tuple[str, bool, str]]) -> None:
    print("验证结果汇总:")
    for name, ok, msg in results:
        status = "OK" if ok else "FAIL"
        print(f"  [{status}] {name}: {msg}")


if __name__ == "__main__":
    sys.exit(main())
