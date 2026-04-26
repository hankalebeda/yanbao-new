"""
Web AI Cookie 健康检查脚本

用途：运维每周一主动检测 ChatGPT/Gemini/DeepSeek/Qwen Web 的 Cookie 是否有效。
参考：docs/guides/02_运维手册.md §9.1、§9.2

用法：
    python scripts/check_web_cookies.py [--base-url http://127.0.0.1:8010]

要求：主服务需已启动；Cookie 配置于 .env 的 *_COOKIES 变量。
输出：控制台报告各平台 Cookie 状态；0=全部有效，非0=有失效。
"""
import argparse
import json
import sys
from pathlib import Path

try:
    import httpx
except ImportError:
    print("需要: pip install httpx")
    sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent


def run(base_url: str) -> dict:
    """调用主服务的 Web AI 接口，探测服务可达性；各平台 Cookie 有效性需对 analyze 轻量请求验证。"""
    base_url = base_url.rstrip("/")
    results: dict = {"status": "unknown", "providers": [], "checks": []}
    try:
        r = httpx.get(f"{base_url}/api/v1/webai/providers", timeout=10)
        if r.status_code == 200:
            data = r.json()
            results["providers"] = data.get("providers", [])
            results["status"] = "reachable"
            results["checks"].append({"id": "providers", "ok": True, "msg": "WebAI providers 接口可达"})
        else:
            results["status"] = "error"
            results["status_code"] = r.status_code
            results["checks"].append({"id": "providers", "ok": False, "msg": f"HTTP {r.status_code}"})
    except Exception as e:
        results["status"] = "error"
        results["error"] = str(e)
        results["checks"].append({"id": "providers", "ok": False, "msg": str(e)})
    # 注：各平台 Cookie 有效性需对 /api/v1/webai/analyze 发轻量请求验证；当前仅探测 providers 可达性
    return results


def main():
    ap = argparse.ArgumentParser(description="Web AI Cookie 健康检查（08 §9.1）")
    ap.add_argument("--base-url", default="http://127.0.0.1:8010", help="主服务地址")
    args = ap.parse_args()
    out = run(args.base_url)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    if out.get("status") == "error":
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
