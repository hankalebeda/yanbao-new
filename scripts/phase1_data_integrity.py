"""Phase 1.2 data-integrity probe.

对返回200的核心端点, 检查业务数据是否真实有效 (非空/非占位).
映射到 25_系统问题分析角度清单.md 的多个角度:
- 角度 1 真实性, 2 状态语义, 8 视图自洽, 25 用户面, 29 健康指标
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import urllib.request

os.environ.setdefault("NO_PROXY", "*")
urllib.request.install_opener(
    urllib.request.build_opener(urllib.request.ProxyHandler({}))
)

BASE = "http://127.0.0.1:8000"
OUT = Path(__file__).resolve().parent.parent / "output" / "phase1_data_integrity.json"
OUT.parent.mkdir(exist_ok=True)


def _get(path: str, *, internal: bool = False) -> tuple[int, dict | None, str]:
    headers = {"User-Agent": "data-probe"}
    if internal:
        headers["X-Internal-Token"] = "phase1-audit-token-20260417"
    req = urllib.request.Request(BASE + path, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode("utf-8", errors="replace")
            try:
                return r.status, json.loads(body), body[:200]
            except Exception:
                return r.status, None, body[:200]
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        try:
            return e.code, json.loads(body), body[:200]
        except Exception:
            return e.code, None, body[:200]
    except Exception as e:
        return 0, None, f"ERR: {e}"


def check(name: str, path: str, predicate, expect_desc: str, *, internal: bool = False) -> dict:
    st, j, preview = _get(path, internal=internal)
    passed = False
    note = ""
    count_info = ""
    try:
        if st == 200 and j is not None:
            result = predicate(j)
            if isinstance(result, tuple):
                passed, count_info = result
            else:
                passed = bool(result)
    except Exception as e:
        note = f"predicate_error: {e}"
    return {
        "check": name, "path": path, "status": st, "expect": expect_desc,
        "pass": passed, "count_info": count_info, "note": note, "preview": preview[:300],
    }


def d(j):
    return (j or {}).get("data") or {}


probes: list[dict] = []

# ---- 角度1/8: 研报真实性与视图自洽 ----
def _chk_reports(j):
    items = d(j).get("items") or []
    n = len(items)
    return (n > 0, f"items={n}")

probes.append(check("研报列表非空", "/api/v1/reports?limit=20", _chk_reports, "items > 0"))

def _chk_ok(j):
    items = d(j).get("items") or []
    n = len(items)
    return (n >= 3, f"ok_items={n}, v12基线=3")

probes.append(check("ok质量研报>=3", "/api/v1/reports?quality_flag=ok&limit=100", _chk_ok, "ok items >= 3"))

def _chk_featured(j):
    items = d(j).get("items") or []
    return (len(items) > 0, f"featured={len(items)}")

probes.append(check("精选研报非空", "/api/v1/reports/featured?limit=6", _chk_featured, "items > 0"))

# ---- 角度8: 首页聚合一致性 ----
def _chk_home(j):
    data = d(j)
    today = data.get("today_report_count", 0) or 0
    pool = data.get("pool_size", 0) or 0
    hot = data.get("hot_stocks") or []
    return (today > 0 or pool > 0, f"today={today}, pool={pool}, hot={len(hot)}")

probes.append(check("首页聚合有数据", "/api/v1/home", _chk_home, "today>0 或 pool>0"))

# ---- 角度5/9: 市场状态 ----
def _chk_market(j):
    data = d(j)
    state = data.get("state") or data.get("market_state")
    trade_date = data.get("trade_date") or data.get("market_state_trade_date")
    return (state is not None, f"state={state}, trade_date={trade_date}")

probes.append(check("市场状态已计算", "/api/v1/market/state", _chk_market, "state非空"))

# ---- 角度1: 股票池 ----
def _chk_pool(j):
    items = d(j).get("items") or d(j).get("stocks") or []
    return (len(items) > 0, f"pool_stocks={len(items)}")

probes.append(check("股票池非空", "/api/v1/pool/stocks", _chk_pool, "items > 0"))

# ---- 角度1: 股票搜索 ----
def _chk_stocks(j):
    items = d(j).get("items") or []
    return (len(items) > 0, f"results={len(items)}")

probes.append(check("股票搜索可命中", "/api/v1/stocks?q=600519", _chk_stocks, "results > 0"))

# ---- 角度4: 热股 ----
def _chk_hot(j):
    items = d(j).get("items") or []
    src = d(j).get("source")
    return (len(items) > 0, f"hot={len(items)}, source={src}")

probes.append(check("热股列表非空", "/api/v1/hot-stocks", _chk_hot, "items > 0"))

# ---- 角度29: 健康指标可信 ----
def _chk_health(j):
    data = d(j)
    statuses = {k: v for k, v in data.items() if k.endswith("_status")}
    ok = all(v in ("ok", "disabled") for v in statuses.values())
    return (ok, f"statuses={statuses}")

probes.append(check("聚合健康全绿", "/health", _chk_health, "所有_status=ok或disabled"))

# ---- 角度29: 结算覆盖率 ----
def _chk_settlement(j):
    data = d(j)
    cov = data.get("settlement_coverage_pct", 0) or 0
    status = data.get("settlement_status")
    return (cov >= 50, f"coverage={cov}%, status={status}, 目标>=50%")

probes.append(check("结算覆盖率≥50%", "/health", _chk_settlement, "覆盖率>=50%"))

# ---- 角度29: K线覆盖率 ----
def _chk_kline(j):
    data = d(j)
    cov = data.get("kline_coverage_pct", 0) or 0
    return (cov >= 50, f"kline_coverage={cov}%, 目标>=50%")

probes.append(check("K线覆盖率≥50%", "/health", _chk_kline, "K线覆盖>=50%"))

# ---- FR-07: 预测统计 ----
def _chk_pred(j):
    data = d(j)
    total = data.get("total_judged") or data.get("total") or data.get("total_predictions") or data.get("judged") or 0
    win_rate = data.get("win_rate") or data.get("accuracy")
    return (total > 0, f"total={total}, win_rate={win_rate}")

probes.append(check("预测统计有样本", "/api/v1/predictions/stats", _chk_pred, "total > 0"))

# ---- FR-10: 平台汇总 ----
def _chk_platform(j):
    data = d(j)
    return (bool(data), f"keys={list(data.keys())[:8]}")

probes.append(check("平台汇总返回数据", "/api/v1/platform/summary", _chk_platform, "非空对象"))

# ---- FR-10: 统计看板 ----
def _chk_dash(j):
    data = d(j)
    # detect degraded
    status = (data.get("status") or data.get("data_status") or "").upper()
    total = data.get("total_reports") or data.get("sample_size") or 0
    return (status not in ("DEGRADED", "ERROR") and total > 0, f"status={status}, total={total}")

probes.append(check("统计看板非DEGRADED", "/api/v1/dashboard/stats?window_days=30",
                   _chk_dash, "非degraded且有样本"))

# ---- FR-13/监控: fallback-status ----
def _chk_fb(j):
    data = d(j)
    return (bool(data), f"keys={list(data.keys())[:6]}")

probes.append(check("降级状态有输出", "/api/v1/internal/source/fallback-status", _chk_fb, "非空", internal=True))

# ---- 内部指标 ----
def _chk_metrics(j):
    data = d(j)
    return (bool(data), f"keys={list(data.keys())[:10]}")

probes.append(check("内部指标汇总", "/api/v1/internal/metrics/summary", _chk_metrics, "非空", internal=True))


def main():
    passed = sum(1 for p in probes if p["pass"])
    total = len(probes)
    OUT.write_text(json.dumps({"total": total, "passed": passed, "probes": probes},
                              ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"=== 数据完整性: {passed}/{total} ({passed/total*100:.1f}%) ===\n")
    for p in probes:
        mark = "✅" if p["pass"] else "❌"
        print(f"{mark} [{p['status']}] {p['check']}")
        print(f"   expect: {p['expect']}")
        print(f"   got:    {p['count_info']}")
        if not p["pass"] and p["preview"]:
            print(f"   body:   {p['preview'][:180]}")
        if p["note"]:
            print(f"   NOTE:   {p['note']}")
    print(f"\n-> {OUT}")


if __name__ == "__main__":
    main()
