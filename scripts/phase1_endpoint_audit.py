"""Phase 1 端点全量活性测试 - 按功能分组.

对照 docs/core/22_全量功能进度总表_v12.md 的 92 个活跃功能点, 逐一 HTTP 验证.
只做 GET / 只读 POST (dry-run) 验证, 不触发状态变更.

输出: output/phase1_audit_{timestamp}.json
"""
from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import urllib.request
import urllib.error

BASE = "http://127.0.0.1:8000"
TIMEOUT = 10
OUT_DIR = Path(__file__).resolve().parent.parent / "output"
OUT_DIR.mkdir(exist_ok=True)

# ===== 端点清单 (fr_id, method, path, description, expect_status) =====
# expect_status: 期望的 HTTP 状态码 (200=正常, 401/403=需认证但活性ok, 422=参数校验ok)
ENDPOINTS: list[tuple[str, str, str, str, list[int]]] = [
    # ---- FR-10 站点/仪表盘 (公开 HTML) ----
    ("FR10-SITE-01", "GET", "/", "首页", [200]),
    ("FR10-SITE-02", "GET", "/reports", "研报列表页", [200]),
    ("FR10-SITE-03", "GET", "/login", "登录页", [200]),
    ("FR10-SITE-04", "GET", "/register", "注册页", [200]),
    ("FR10-SITE-05", "GET", "/subscribe", "订阅页", [200, 302]),
    ("FR10-SITE-06", "GET", "/forgot-password", "忘记密码页", [200]),
    ("FR10-SITE-07", "GET", "/features", "功能页", [200]),
    ("FR10-SITE-08", "GET", "/privacy", "隐私政策", [200]),
    ("FR10-SITE-09", "GET", "/terms", "服务条款", [200]),

    # ---- 健康检查 ----
    ("HEALTH-01", "GET", "/health", "聚合健康", [200]),

    # ---- FR-10 仪表盘 API ----
    ("FR10-API-01", "GET", "/api/v1/home", "首页聚合", [200]),
    ("FR10-API-02", "GET", "/api/v1/dashboard/stats?window_days=7", "统计看板", [200]),
    ("FR10-API-03", "GET", "/api/v1/dashboard/stats?window_days=30", "统计看板30d", [200]),
    ("FR10-API-04", "GET", "/api/v1/platform/summary", "平台汇总", [200]),

    # ---- FR-06 研报查询 ----
    ("FR06-RPT-01", "GET", "/api/v1/reports?limit=10", "研报列表", [200]),
    ("FR06-RPT-02", "GET", "/api/v1/reports/featured", "首页精选研报", [200]),
    ("FR06-RPT-03", "GET", "/api/v1/reports?quality_flag=ok&limit=5", "OK研报过滤", [200]),

    # ---- FR-01/FR-04 股票池 + 市场 ----
    ("FR01-POOL-01", "GET", "/api/v1/pool/stocks", "股票池查询", [200]),
    ("FR01-STK-01", "GET", "/api/v1/stocks?limit=10", "股票列表", [200]),
    ("FR01-STK-02", "GET", "/api/v1/stocks/autocomplete?q=600", "股票补全", [200]),
    ("FR04-HOT-01", "GET", "/api/v1/hot-stocks", "热股查询", [200]),
    ("FR04-MKT-01", "GET", "/api/v1/market-overview", "市场概览", [200]),
    ("FR05-MKT-01", "GET", "/api/v1/market/state", "市场状态", [200]),
    ("FR04-HOT-02", "GET", "/api/v1/market/hot-stocks", "市场热股", [200]),

    # ---- FR-08 模拟仓位 (需登录-预期401或200) ----
    ("FR08-SIM-01", "GET", "/api/v1/sim/positions", "模拟持仓", [200, 401]),
    ("FR08-SIM-02", "GET", "/api/v1/sim/account/summary", "账户汇总", [200, 401]),
    ("FR08-SIM-03", "GET", "/api/v1/sim/account/snapshots", "账户快照", [200, 401]),
    ("FR08-SIM-04", "GET", "/api/v1/portfolio/sim-dashboard?capital_tier=100k", "模拟看板", [200, 401, 402]),

    # ---- FR-09 认证 ----
    ("FR09-AUTH-01", "GET", "/api/v1/auth/me", "用户信息(未登录预期401)", [401, 200]),
    ("FR09-AUTH-02", "GET", "/api/v1/auth/oauth/providers", "OAuth providers", [200]),
    ("FR09-AUTH-03", "POST", "/api/v1/auth/login", "登录(空参预期422)", [422, 401, 400]),
    ("FR09-AUTH-04", "POST", "/api/v1/auth/register", "注册(空参预期422)", [422, 400]),

    # ---- 订阅 ----
    ("FR09-SUB-01", "GET", "/api/v1/membership/subscription/status", "订阅状态", [200, 401]),

    # ---- FR-12 管理 (未登录预期401/403) ----
    ("FR12-ADM-01", "GET", "/api/v1/admin/users", "管理-用户", [401, 403]),
    ("FR12-ADM-02", "GET", "/api/v1/admin/reports", "管理-研报", [401, 403]),
    ("FR12-ADM-03", "GET", "/api/v1/admin/scheduler/status", "管理-调度", [401, 403]),

    # ---- 治理 ----
    ("GOV-01", "GET", "/api/v1/features/catalog", "feature catalog", [200, 401, 403]),
    ("GOV-02", "GET", "/api/v1/governance/catalog", "governance catalog", [200, 401, 403]),
    ("GOV-03", "GET", "/api/v1/governance/catalog?source=live", "governance live", [200, 401, 403]),

    # ---- 内部接口 (预期401/403) ----
    ("INT-01", "GET", "/api/v1/internal/llm/health", "LLM健康", [200, 401, 403]),
    ("INT-02", "GET", "/api/v1/internal/llm/version", "LLM版本", [200, 401, 403]),
    ("INT-03", "GET", "/api/v1/internal/hotspot/health", "热点健康", [200, 401, 403]),
    ("INT-04", "GET", "/api/v1/internal/source/fallback-status", "降级状态", [200, 401, 403]),
    ("INT-05", "GET", "/api/v1/internal/runtime/gates", "runtime gates", [200, 401, 403]),
    ("INT-06", "GET", "/api/v1/internal/autonomy/state", "autonomy state", [200, 401, 403]),
    ("INT-07", "GET", "/api/v1/internal/automation/fix-loop/state", "自修复loop", [200, 401, 403]),

    # ---- 错误页 ----
    ("ERR-404", "GET", "/nonexistent-path-xyz", "404处理", [404]),
]


def probe(ep: tuple) -> dict:
    fr_id, method, path, desc, expect = ep
    url = BASE + path
    t0 = time.time()
    result: dict[str, Any] = {
        "fr_id": fr_id, "method": method, "path": path, "desc": desc,
        "expect_status": expect, "actual_status": None, "elapsed_ms": 0,
        "pass": False, "body_preview": "", "error": None,
    }
    try:
        data_bytes = b"{}" if method == "POST" else None
        req = urllib.request.Request(
            url, data=data_bytes, method=method,
            headers={"Content-Type": "application/json", "User-Agent": "phase1-audit"}
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            body = resp.read(2000).decode("utf-8", errors="replace")
            result["actual_status"] = resp.status
            result["body_preview"] = body[:400]
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read(2000).decode("utf-8", errors="replace")
        except Exception:
            pass
        result["actual_status"] = e.code
        result["body_preview"] = body[:400]
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"
    result["elapsed_ms"] = round((time.time() - t0) * 1000, 1)
    result["pass"] = result["actual_status"] in expect
    return result


def main() -> int:
    print(f"[phase1] probing {len(ENDPOINTS)} endpoints against {BASE}")
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(probe, ep): ep for ep in ENDPOINTS}
        for fut in as_completed(futures):
            results.append(fut.result())

    results.sort(key=lambda r: r["fr_id"])
    passed = sum(1 for r in results if r["pass"])
    failed = [r for r in results if not r["pass"]]

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = OUT_DIR / f"phase1_audit_{ts}.json"
    out.write_text(
        json.dumps({
            "base_url": BASE, "timestamp": ts,
            "total": len(results), "passed": passed, "failed": len(failed),
            "pass_rate": round(passed / len(results) * 100, 1),
            "results": results,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"\n=== 结果: {passed}/{len(results)} PASS ({passed/len(results)*100:.1f}%) ===")
    if failed:
        print(f"\n--- {len(failed)} 个失败项 ---")
        for r in failed:
            st = r["actual_status"] or "ERR"
            print(f"  [{r['fr_id']}] {r['method']} {r['path']} -> {st} (expect {r['expect_status']})")
            if r["error"]:
                print(f"      ERROR: {r['error']}")
            elif r["body_preview"]:
                print(f"      BODY: {r['body_preview'][:120]}")
    print(f"\nfull report -> {out}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
