"""
轻量级验证脚本（三 AI 协同用）
- 不启动 Playwright，不占用大量内存
- 检查：主服务健康、Web AI 接口可达性、数据源 HTTP 连通性

用法：
    python scripts/verify_lightweight.py [--base-url http://127.0.0.1:8010]

要求：主服务需已启动（uvicorn app.main:app --port 8010）
"""
import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import httpx
except ImportError:
    print("需要: pip install httpx")
    sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "docs" / "core" / "test_results"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def run_checks(base_url: str) -> dict:
    results = []
    base_url = base_url.rstrip("/")

    # 1. 主服务健康
    try:
        t0 = time.time()
        r = httpx.get(f"{base_url}/health", timeout=5)
        elapsed = round(time.time() - t0, 3)
        ok = r.status_code == 200
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        results.append({
            "id": "V01",
            "name": "主服务健康",
            "status": "PASS" if ok else "FAIL",
            "elapsed_s": elapsed,
            "detail": data.get("data", {}) if ok else {"status_code": r.status_code},
        })
    except Exception as e:
        results.append({
            "id": "V01",
            "name": "主服务健康",
            "status": "FAIL",
            "elapsed_s": 0,
            "detail": {"error": str(e)},
        })

    # 2. Web AI providers 列表（仅 HTTP 探测，不调用 analyze）
    try:
        t0 = time.time()
        r = httpx.get(f"{base_url}/api/v1/webai/providers", timeout=5)
        elapsed = round(time.time() - t0, 3)
        ok = r.status_code == 200
        data = r.json() if "application/json" in (r.headers.get("content-type") or "") else {}
        providers = (data.get("data") or data).get("providers", []) if isinstance(data.get("data") or data, dict) else []
        results.append({
            "id": "V02",
            "name": "Web AI providers",
            "status": "PASS" if ok and providers else "FAIL",
            "elapsed_s": elapsed,
            "detail": {"providers": providers} if providers else {"status_code": r.status_code, "body": str(data)[:200]},
        })
    except Exception as e:
        results.append({
            "id": "V02",
            "name": "Web AI providers",
            "status": "FAIL",
            "elapsed_s": 0,
            "detail": {"error": str(e)},
        })

    # 3. 东方财富实时行情（直接 HTTP，不依赖主服务）
    try:
        t0 = time.time()
        r = httpx.get(
            "https://push2.eastmoney.com/api/qt/stock/get?secid=1.600519&fields=f43,f58,f170",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        elapsed = round(time.time() - t0, 3)
        ok = r.status_code == 200
        d = r.json().get("data", {}) if ok else {}
        name = d.get("f58", "?")
        price = (d.get("f43") or 0) / 100
        results.append({
            "id": "V03",
            "name": "东方财富行情(600519)",
            "status": "PASS" if ok and d else "FAIL",
            "elapsed_s": elapsed,
            "detail": {"name": name, "price": price} if d else {"status_code": r.status_code},
        })
    except Exception as e:
        results.append({
            "id": "V03",
            "name": "东方财富行情(600519)",
            "status": "FAIL",
            "elapsed_s": 0,
            "detail": {"error": str(e)},
        })

    return {
        "timestamp": datetime.now().isoformat(),
        "base_url": base_url,
        "tests": results,
        "passed": sum(1 for t in results if t["status"] == "PASS"),
        "total": len(results),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="http://127.0.0.1:8010", help="主服务地址")
    p.add_argument("--no-save", action="store_true", help="不保存结果文件")
    args = p.parse_args()

    report = run_checks(args.base_url)

    print("\n" + "=" * 50)
    print(f"轻量级验证: {report['passed']}/{report['total']} 通过")
    print("=" * 50)
    for t in report["tests"]:
        print(f"  {t['id']} {t['name']}: {t['status']} ({t['elapsed_s']}s)")

    if not args.no_save:
        out_file = OUT_DIR / "verify_lightweight_latest.json"
        out_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n结果已保存: {out_file}")

    sys.exit(0 if report["passed"] == report["total"] else 1)


if __name__ == "__main__":
    main()
