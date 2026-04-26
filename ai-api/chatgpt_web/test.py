import asyncio
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from chatgpt_web.client import ChatGPTWebClient  # noqa: E402

RESULTS_DIR = Path(__file__).parent / "test_results"
RESULTS_DIR.mkdir(exist_ok=True)

NOISE = ["登录", "注册", "Log in", "Sign up", "About", "订阅"]
TAG_RE = re.compile(r"CASE-([A-Z0-9_]+)\|([A-Z0-9_]+)\|OK")


def safe_text(text: str) -> str:
    return (text or "").encode("gbk", errors="ignore").decode("gbk", errors="ignore")


def assert_valid_answer(text: str, prompt: str, min_len: int = 2) -> None:
    if not text or len(text.strip()) < min_len:
        raise AssertionError("response too short")
    if text.strip() == prompt.strip():
        raise AssertionError("response equals prompt")
    if sum(1 for k in NOISE if k.lower() in text.lower()) >= 2:
        raise AssertionError("response looks like landing/login text")


def assert_case_tag(text: str, case_id: str, name_tag: str) -> None:
    m = TAG_RE.search((text or "").upper())
    if not m:
        raise AssertionError(f"missing case tag, expected CASE-{case_id}|{name_tag}|OK, got={text[:120]!r}")
    got_id, got_name = m.group(1), m.group(2)
    if got_id != case_id or got_name != name_tag:
        raise AssertionError(
            f"tag mismatch: expected CASE-{case_id}|{name_tag}|OK, got CASE-{got_id}|{got_name}|OK"
        )


async def case_model_probe() -> dict:
    print("\n[1/4] model probe (must be GPT-5.x)...")
    client = await ChatGPTWebClient.get()
    probe = client.model_probe or {}
    if not probe.get("is_5x", False):
        raise AssertionError(f"model probe failed: {probe}")
    print(f"  probe_ok raw={safe_text((probe.get('raw') or '')[:80])}")
    return probe


async def case_single() -> dict:
    print("\n[2/4] single networked query...")
    client = await ChatGPTWebClient.get()
    prompt = (
        f"今天是{datetime.now():%Y-%m-%d}。请联网给出一条今日A股重要新闻，"
        "并附上可访问来源链接。"
    )
    result = await client.analyze(prompt)
    assert_valid_answer(result["response"], prompt)
    print(
        f"  elapsed={result['elapsed_s']}s chars={len(result['response'])} cited={result['has_citation']}"
    )
    print(f"  preview: {safe_text(result['response'][:200])}")
    return result


async def case_batch() -> list:
    print("\n[3/4] concurrent batch mapping check...")
    client = await ChatGPTWebClient.get()
    items = [
        {"code": "600519", "name": "MOUTAI", "case_id": "A001"},
        {"code": "000858", "name": "WULIANGYE", "case_id": "A002"},
        {"code": "300750", "name": "CATL", "case_id": "A003"},
        {"code": "601318", "name": "PINGAN", "case_id": "A004"},
        {"code": "600036", "name": "CMB", "case_id": "A005"},
        {"code": "600276", "name": "HGRX", "case_id": "A006"},
    ]
    for item in items:
        item["prompt"] = (
            f"你只输出这一行且不要任何其它字符：CASE-{item['case_id']}|{item['name']}|OK"
        )

    t0 = time.time()
    results = await client.analyze_batch(items, timeout_ms=90_000)
    elapsed = time.time() - t0
    if len(results) != len(items):
        raise AssertionError(f"batch size mismatch: {len(results)} != {len(items)}")

    idx = {(x["code"], x["name"]): x for x in items}
    for r in results:
        if "error" in r:
            raise AssertionError(f"{r.get('name')} failed: {r.get('error')}")
        key = (r.get("code"), r.get("name"))
        src = idx.get(key)
        if not src:
            raise AssertionError(f"unexpected result key={key}")
        assert_valid_answer(r.get("response", ""), src["prompt"])
        assert_case_tag(r.get("response", ""), src["case_id"], src["name"])

    print(f"  total_elapsed={elapsed:.1f}s n={len(results)}")
    for r in results:
        preview = safe_text((r.get("response") or "")[:80]).replace(chr(10), " ")
        print(f"  {r.get('code')} {r.get('name')}: {r.get('elapsed_s')}s preview={preview}")
    return results


async def case_reconnect() -> dict:
    print("\n[4/4] close and reconnect...")
    client = await ChatGPTWebClient.get()
    await client.close()
    client2 = await ChatGPTWebClient.get()
    probe = client2.model_probe or {}
    if not probe.get("is_5x", False):
        raise AssertionError(f"reconnect model probe failed: {probe}")
    prompt = "你只输出这一行且不要任何其它字符：CASE-RECONNECT|HEALTH|OK"
    result = await client2.analyze(prompt, timeout_ms=90_000)
    assert_case_tag(result.get("response", ""), "RECONNECT", "HEALTH")
    print(f"  reconnect_ok preview={safe_text(result['response'][:80])}")
    return {"probe": probe, "result": result}


async def main() -> bool:
    print("=" * 60)
    print("ChatGPTWebClient integration test")
    print(f"time: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60)

    passed, report = 0, {}
    cases = [
        ("model_probe", case_model_probe),
        ("single", case_single),
        ("batch", case_batch),
        ("reconnect", case_reconnect),
    ]
    for name, coro in cases:
        try:
            report[name] = await coro()
            passed += 1
            print(f"  [PASS] {name}")
        except Exception as exc:
            report[name] = {"error": str(exc)}
            print(f"  [FAIL] {name}: {exc}")

    inst = ChatGPTWebClient._instance
    if inst:
        await inst.close()

    out = RESULTS_DIR / f"test_{datetime.now():%Y%m%d_%H%M%S}.json"
    out.write_text(
        json.dumps({"passed": passed, "total": len(cases), "results": report}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n{'='*60}\nresult: {passed}/{len(cases)} passed\nreport: {out}\n{'='*60}")
    return passed == len(cases)


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
