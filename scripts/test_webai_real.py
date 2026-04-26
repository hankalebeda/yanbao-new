from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path

import httpx

PROVIDERS = ("chatgpt", "deepseek", "gemini", "qwen")
TAG_RE = re.compile(r"CASE-[A-Z0-9_\-]+\|OK\|OK", re.I)


def _build_prompt(provider: str) -> tuple[str, str]:
    case_tag = f"CASE-REAL_{provider.upper()}_{int(time.time())}|OK|OK"
    prompt = (
        "请严格按要求回复。\n"
        f"你只能输出这一行：{case_tag}\n"
        "不要输出任何其他内容。"
    )
    return prompt, case_tag


def _match_expected(response_text: str, expected: str) -> bool:
    if not response_text:
        return False
    hit = TAG_RE.search(response_text.upper())
    if not hit:
        return False
    return hit.group(0).upper() == expected.upper()


def _call_single(
    client: httpx.Client,
    base_url: str,
    provider: str,
    timeout_s: int,
    retry: int,
    inter_retry_sleep_s: float,
) -> dict:
    prompt, expected = _build_prompt(provider)
    last_error = ""
    for attempt in range(1, retry + 2):
        t0 = time.time()
        try:
            resp = client.post(
                f"{base_url}/api/v1/webai/analyze",
                json={"provider": provider, "prompt": prompt, "timeout_s": timeout_s},
                timeout=max(30, timeout_s + 30),
            )
            elapsed = round(time.time() - t0, 2)
            body = resp.json()
            code = body.get("code", resp.status_code)
            data = body.get("data") or {}
            response_text = str(data.get("response") or "")
            ok = resp.status_code == 200 and code == 0 and _match_expected(response_text, expected)
            result = {
                "attempt": attempt,
                "status_code": resp.status_code,
                "code": code,
                "elapsed_wall_s": elapsed,
                "expected": expected,
                "response_preview": response_text[:240],
                "match_expected": _match_expected(response_text, expected),
                "error": body.get("error") if (resp.status_code != 200 or code != 0) else "",
                "raw": body if resp.status_code != 200 or code != 0 else None,
            }
            if ok:
                result["pass"] = True
                return result
            last_error = str(result.get("error") or f"status={resp.status_code}, code={code}")
        except Exception as exc:
            elapsed = round(time.time() - t0, 2)
            last_error = str(exc)
            result = {
                "attempt": attempt,
                "status_code": 0,
                "code": -1,
                "elapsed_wall_s": elapsed,
                "expected": expected,
                "response_preview": "",
                "match_expected": False,
                "error": str(exc),
                "raw": None,
            }
        if attempt <= retry:
            time.sleep(inter_retry_sleep_s)
    result["pass"] = False
    if not result.get("error"):
        result["error"] = last_error
    return result


def run(args: argparse.Namespace) -> dict:
    base_url = args.base_url.rstrip("/")
    providers = [p.strip() for p in args.providers.split(",") if p.strip()]
    providers = [p for p in providers if p in PROVIDERS]
    report: dict = {
        "started_at": datetime.now().isoformat(),
        "mode": "real_smoke_low_concurrency",
        "constraints": {
            "sequential": True,
            "providers": providers,
            "calls_per_provider": args.calls_per_provider,
            "inter_call_sleep_s": args.inter_call_sleep_s,
            "timeout_s": args.timeout_s,
            "retry": args.retry,
        },
        "providers": {},
    }

    with httpx.Client() as client:
        for provider in providers:
            provider_results = []
            for _ in range(args.calls_per_provider):
                one = _call_single(
                    client=client,
                    base_url=base_url,
                    provider=provider,
                    timeout_s=args.timeout_s,
                    retry=args.retry,
                    inter_retry_sleep_s=args.inter_retry_sleep_s,
                )
                provider_results.append(one)
                time.sleep(args.inter_call_sleep_s)
            report["providers"][provider] = {
                "pass_count": sum(1 for x in provider_results if x.get("pass")),
                "total": len(provider_results),
                "results": provider_results,
            }

        try:
            close_resp = client.delete(f"{base_url}/api/v1/webai/session", timeout=20)
            report["close_all"] = {
                "status_code": close_resp.status_code,
                "body": close_resp.json(),
            }
        except Exception as exc:
            report["close_all"] = {"status_code": 0, "error": str(exc)}

    total = 0
    passed = 0
    for provider in providers:
        block = report["providers"].get(provider) or {}
        total += int(block.get("total", 0))
        passed += int(block.get("pass_count", 0))

    report["finished_at"] = datetime.now().isoformat()
    report["summary"] = {"total": total, "passed": passed}
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Real WebAI smoke test with low concurrency.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--providers", default="chatgpt,deepseek,gemini,qwen")
    parser.add_argument("--calls-per-provider", type=int, default=1)
    parser.add_argument("--timeout-s", type=int, default=120)
    parser.add_argument("--retry", type=int, default=0)
    parser.add_argument("--inter-call-sleep-s", type=float, default=6.0)
    parser.add_argument("--inter-retry-sleep-s", type=float, default=8.0)
    args = parser.parse_args()

    report = run(args)
    out_dir = Path("ai-api/webai/test_results")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"real_smoke_low_concurrency_{datetime.now():%Y%m%d_%H%M%S}.json"
    out_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"report={out_file}")
    print(f"passed={report['summary']['passed']}/{report['summary']['total']}")
    return 0 if report["summary"]["passed"] == report["summary"]["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
