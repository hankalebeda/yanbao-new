#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据缺口分析脚本 - 识别系统中所有非研报数据缺口
"""
import sqlite3
import json
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = "data/app.db"

def run_analysis():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    results = {}

    # ==================== 1. 所有表行数 ====================
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    table_counts = {}
    for t in tables:
        cur.execute(f'SELECT COUNT(*) FROM "{t}"')
        table_counts[t] = cur.fetchone()[0]
    results["table_counts"] = table_counts
    print("\n=== 1. 表行数概览 ===")
    for t, c in table_counts.items():
        print(f"  {t}: {c}")

    # ==================== 2. stock_master 概况 ====================
    print("\n=== 2. stock_master 概况 ===")
    cur.execute("SELECT stock_code, stock_name, is_delisted, list_date FROM stock_master WHERE is_delisted=0")
    stocks = [dict(r) for r in cur.fetchall()]
    results["active_stocks"] = stocks
    print(f"  活跃股票数: {len(stocks)}")
    for s in stocks:
        print(f"  {s['stock_code']} {s['stock_name']}  list_date={s['list_date']}")

    active_codes = [s["stock_code"] for s in stocks]

    # ==================== 3. stock_profile 缺口 ====================
    print("\n=== 3. stock_profile 缺口 ===")
    cur.execute("SELECT stock_code, fetch_date, pe_ttm, pb, total_market_cap, industry FROM stock_profile ORDER BY stock_code, fetch_date DESC")
    profiles = cur.fetchall()
    profile_map = {}
    for r in profiles:
        if r["stock_code"] not in profile_map:
            profile_map[r["stock_code"]] = dict(r)
    
    profile_gaps = []
    for code in active_codes:
        if code not in profile_map:
            profile_gaps.append({"stock_code": code, "issue": "no_profile"})
        else:
            p = profile_map[code]
            missing_fields = []
            if not p["pe_ttm"]: missing_fields.append("pe_ttm")
            if not p["pb"]: missing_fields.append("pb")
            if not p["total_market_cap"]: missing_fields.append("total_market_cap")
            if not p["industry"]: missing_fields.append("industry")
            if missing_fields:
                profile_gaps.append({"stock_code": code, "issue": "missing_fields", "fields": missing_fields, "fetch_date": p["fetch_date"]})
    
    results["profile_gaps"] = profile_gaps
    if profile_gaps:
        print(f"  缺口: {len(profile_gaps)} 条")
        for g in profile_gaps:
            print(f"  {g}")
    else:
        print("  无缺口 ✓")

    # ==================== 4. kline_daily 缺口 ====================
    print("\n=== 4. kline_daily 覆盖情况 ===")
    today = datetime.today().strftime("%Y-%m-%d")
    recent_30 = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    kline_gaps = []
    for code in active_codes:
        cur.execute("SELECT COUNT(*) as cnt, MAX(trade_date) as latest FROM kline_daily WHERE stock_code=? AND trade_date >= ?", (code, recent_30))
        r = cur.fetchone()
        cnt = r["cnt"] if r else 0
        latest = r["latest"] if r else None
        if cnt == 0:
            kline_gaps.append({"stock_code": code, "issue": "no_kline_30d"})
        elif latest < "2026-04-14":
            kline_gaps.append({"stock_code": code, "issue": "kline_stale", "latest": latest})
        else:
            print(f"  {code}: {cnt} 条, 最新={latest}")
    
    results["kline_gaps"] = kline_gaps
    if kline_gaps:
        print(f"  缺口: {len(kline_gaps)} 条")
        for g in kline_gaps:
            print(f"  {g}")

    # ==================== 5. stock_pool_snapshot 缺口 ====================
    print("\n=== 5. stock_pool_snapshot 覆盖情况 ===")
    cur.execute("SELECT snapshot_date, COUNT(*) as cnt FROM stock_pool_snapshot GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 10")
    snapshots = [dict(r) for r in cur.fetchall()]
    results["recent_pool_snapshots"] = snapshots
    for s in snapshots:
        print(f"  {s['snapshot_date']}: {s['cnt']} 条")
    
    # 最新快照日期
    if snapshots:
        latest_snapshot = snapshots[0]["snapshot_date"]
        print(f"  最新快照: {latest_snapshot}")
    else:
        print("  无快照!")
        latest_snapshot = None

    # ==================== 6. report_data_usage 缺口 ====================
    print("\n=== 6. report_data_usage 覆盖情况 ===")
    if "report_data_usage" in tables:
        cur.execute("""
            SELECT stock_code, dataset, status, COUNT(*) as cnt, MAX(collect_date) as latest_date
            FROM report_data_usage
            GROUP BY stock_code, dataset, status
            ORDER BY stock_code, dataset
        """)
        usage_rows = [dict(r) for r in cur.fetchall()]
        
        # 统计各 dataset 的覆盖情况
        dataset_coverage = defaultdict(dict)
        for row in usage_rows:
            key = row["stock_code"]
            dataset = row["dataset"]
            if dataset not in dataset_coverage[key]:
                dataset_coverage[key][dataset] = {"status": row["status"], "latest": row["latest_date"]}
            elif row["status"] in ("ok", "proxy_ok", "realtime_only") and dataset_coverage[key][dataset]["status"] not in ("ok", "proxy_ok", "realtime_only"):
                dataset_coverage[key][dataset] = {"status": row["status"], "latest": row["latest_date"]}
        
        # 统计缺失
        ALL_DATASETS = [
            "kline_daily", "stock_profile", "capital_flow", "hotspot_top50",
            "northbound_flow", "etf_flow", "financial_data"
        ]
        usage_gaps = []
        for code in active_codes:
            for ds in ALL_DATASETS:
                if ds not in dataset_coverage.get(code, {}):
                    usage_gaps.append({"stock_code": code, "dataset": ds, "issue": "missing"})
                else:
                    st = dataset_coverage[code][ds]["status"]
                    if st == "missing":
                        usage_gaps.append({"stock_code": code, "dataset": ds, "issue": "status_missing", "latest": dataset_coverage[code][ds]["latest"]})
        
        results["report_data_usage_gaps"] = usage_gaps
        print(f"  总usage记录: {len(usage_rows)}")
        if usage_gaps:
            print(f"  缺口: {len(usage_gaps)}")
            for g in usage_gaps[:20]:
                print(f"  {g}")
        else:
            print("  无缺口 ✓")
        
        # 打印各股票dataset覆盖
        print("\n  各股票dataset状态:")
        for code in active_codes:
            cov = dataset_coverage.get(code, {})
            print(f"  {code}:", {ds: cov.get(ds, {}).get("status", "MISSING") for ds in ALL_DATASETS})

    # ==================== 7. capital_flow/northbound/etf 缺口 ====================
    print("\n=== 7. market_data 各类型覆盖情况 ===")
    if "market_data" in tables:
        cur.execute("""
            SELECT data_type, COUNT(*) as cnt, MAX(data_date) as latest, MIN(data_date) as earliest
            FROM market_data
            GROUP BY data_type
            ORDER BY data_type
        """)
        mdata = [dict(r) for r in cur.fetchall()]
        results["market_data_types"] = mdata
        for m in mdata:
            print(f"  {m['data_type']}: {m['cnt']} 条, 最新={m['latest']}, 最早={m['earliest']}")
        
        # 检查哪些类型最近7天没有数据
        cutoff = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        stale_types = [m for m in mdata if m["latest"] and m["latest"] < cutoff]
        results["stale_market_data_types"] = stale_types
        if stale_types:
            print(f"\n  过期数据类型 (7天内无数据):")
            for s in stale_types:
                print(f"  {s['data_type']}: 最新={s['latest']}")

    # ==================== 8. capital_usage (资金流向) ====================
    print("\n=== 8. capital_usage 覆盖情况 ===")
    if "capital_usage" in tables:
        cur.execute("""
            SELECT stock_code, COUNT(*) as cnt, MAX(trade_date) as latest
            FROM capital_usage
            GROUP BY stock_code
            ORDER BY stock_code
        """)
        cu_rows = [dict(r) for r in cur.fetchall()]
        results["capital_usage"] = cu_rows
        for r in cu_rows:
            print(f"  {r['stock_code']}: {r['cnt']} 条, 最新={r['latest']}")
        
        # 检查缺失
        cu_codes = [r["stock_code"] for r in cu_rows]
        for code in active_codes:
            if code not in cu_codes:
                print(f"  缺失: {code}")

    # ==================== 9. 最近的 data_batch 状态 ====================
    print("\n=== 9. data_batch 最近状态 ===")
    if "data_batch" in tables:
        cur.execute("""
            SELECT id, batch_seq, status, created_at, finished_at, error_msg
            FROM data_batch
            ORDER BY id DESC LIMIT 20
        """)
        batches = [dict(r) for r in cur.fetchall()]
        results["recent_batches"] = batches
        for b in batches:
            print(f"  id={b['id']} seq={b['batch_seq']} status={b['status']} created={b['created_at']}")
            if b["error_msg"]:
                print(f"    error: {b['error_msg'][:100]}")

    # ==================== 10. hotspot 热点数据 ====================
    print("\n=== 10. hotspot 数据覆盖 ===")
    if "hotspot_data" in tables:
        cur.execute("""
            SELECT source, COUNT(*) as cnt, MAX(fetch_date) as latest
            FROM hotspot_data
            GROUP BY source
        """)
        hotspot_rows = [dict(r) for r in cur.fetchall()]
        results["hotspot"] = hotspot_rows
        for r in hotspot_rows:
            print(f"  source={r['source']}: {r['cnt']} 条, 最新={r['latest']}")
    elif "hotspot" in tables:
        cur.execute("SELECT COUNT(*), MAX(fetch_date) FROM hotspot")
        r = cur.fetchone()
        print(f"  总条数={r[0]}, 最新={r[1]}")
    
    # 检查 market_data 中的热点
    if "market_data" in tables:
        cur.execute("SELECT COUNT(*), MAX(data_date) FROM market_data WHERE data_type LIKE '%hotspot%'")
        r = cur.fetchone()
        print(f"  market_data中热点: {r[0]} 条, 最新={r[1]}")

    # ==================== 11. trade_calendar 覆盖 ====================
    print("\n=== 11. trade_calendar 覆盖 ===")
    if "trade_calendar" in tables:
        cur.execute("SELECT COUNT(*), MAX(trade_date), MIN(trade_date) FROM trade_calendar WHERE is_trading_day=1")
        r = cur.fetchone()
        print(f"  交易日: {r[0]} 条, 最新={r[1]}, 最早={r[2]}")
        
        # 检查近期是否覆盖
        cur.execute("SELECT trade_date FROM trade_calendar WHERE trade_date >= '2026-04-01' ORDER BY trade_date")
        recent_cal = [r[0] for r in cur.fetchall()]
        print(f"  2026-04以来: {recent_cal}")
        results["recent_trade_calendar"] = recent_cal

    # ==================== 12. financial_data (财务数据) ====================
    print("\n=== 12. financial_data 覆盖 ===")
    if "financial_data" in tables:
        cur.execute("""
            SELECT stock_code, COUNT(*) as cnt, MAX(report_date) as latest
            FROM financial_data
            GROUP BY stock_code
            ORDER BY stock_code
        """)
        fin_rows = [dict(r) for r in cur.fetchall()]
        results["financial_data"] = fin_rows
        for r in fin_rows:
            print(f"  {r['stock_code']}: {r['cnt']} 条, 最新={r['latest']}")
        # 检查缺失
        fin_codes = [r["stock_code"] for r in fin_rows]
        for code in active_codes:
            if code not in fin_codes:
                print(f"  缺失: {code}")

    conn.close()
    
    # 保存结果
    with open("_archive/data_gaps_analysis.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print("\n\n=== 汇总缺口 ===")
    gap_summary = {}
    if results.get("profile_gaps"):
        gap_summary["stock_profile"] = results["profile_gaps"]
    if results.get("kline_gaps"):
        gap_summary["kline_daily"] = results["kline_gaps"]
    if results.get("report_data_usage_gaps"):
        gap_summary["report_data_usage"] = results["report_data_usage_gaps"]
    if results.get("stale_market_data_types"):
        gap_summary["stale_market_data"] = results["stale_market_data_types"]
    
    if not gap_summary:
        print("  未发现明显数据缺口!")
    else:
        for k, v in gap_summary.items():
            print(f"  {k}: {len(v)} 个缺口")
    
    print("\n结果已保存到 _archive/data_gaps_analysis.json")
    return results

if __name__ == "__main__":
    run_analysis()
