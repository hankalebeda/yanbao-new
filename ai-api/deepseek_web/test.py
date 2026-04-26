import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from deepseek_web.client import DeepSeekWebClient  # noqa: E402

RESULTS_DIR = Path(__file__).parent / "test_results"
RESULTS_DIR.mkdir(exist_ok=True)

NOISE = ["登录", "注册", "验证码", "用户协议", "隐私政策", "sign in", "log in"]


def safe_text(text: str) -> str:
    return (text or "").encode("gbk", errors="ignore").decode("gbk", errors="ignore")


def assert_valid_answer(text: str, prompt: str, min_len: int = 2) -> None:
    if not text or len(text.strip()) < min_len:
        raise AssertionError("response too short")
    if text.strip() == prompt.strip():
        raise AssertionError("response equals prompt")
    if sum(1 for k in NOISE if k.lower() in text.lower()) >= 2:
        raise AssertionError("response looks like landing/login text")


async def case_single() -> dict:
    print("\n[1/3] single networked query...")
    client = await DeepSeekWebClient.get()
    prompt = (
        f"今天是{datetime.now():%Y-%m-%d}。请联网给出一条今日A股重要新闻，"
        "并附上可访问来源链接。"
    )
    result = await client.analyze(prompt)
    assert_valid_answer(result["response"], prompt)
    print(f"  elapsed={result['elapsed_s']}s chars={len(result['response'])} cited={result['has_citation']}")
    print(f"  preview: {safe_text(result['response'][:200])}")
    return result


async def case_batch() -> list:
    print("\n[2/3] concurrent batch (3 items)...")
    client = await DeepSeekWebClient.get()
    items = [
        {"code": "600519", "name": "Kweichow Moutai", "prompt": "简述贵州茅台主营业务，50字内。"},
        {"code": "000858", "name": "Wuliangye", "prompt": "简述五粮液主营业务，50字内。"},
        {"code": "300750", "name": "CATL", "prompt": "简述宁德时代主营业务，50字内。"},
    ]
    t0 = time.time()
    results = await client.analyze_batch(items)
    elapsed = time.time() - t0
    assert len(results) == 3
    for r in results:
        assert "error" not in r, f"{r['name']} failed: {r.get('error')}"
        src_prompt = next(x["prompt"] for x in items if x["name"] == r["name"])
        assert_valid_answer(r["response"], src_prompt)
    print(f"  total_elapsed={elapsed:.1f}s")
    for r in results:
        preview = safe_text(r["response"][:80]).replace(chr(10), " ")
        print(f"  {r['name']}: {r['elapsed_s']}s preview={preview}")
    return results


async def case_reconnect() -> dict:
    print("\n[3/3] close and reconnect...")
    client = await DeepSeekWebClient.get()
    await client.close()
    client2 = await DeepSeekWebClient.get()
    result = await client2.analyze("请只回复：测试通过", timeout_ms=60_000)
    assert_valid_answer(result["response"], "请只回复：测试通过", min_len=2)
    print(f"  reconnect_ok preview={safe_text(result['response'][:80])}")
    return result


async def main() -> bool:
    print("=" * 60)
    print("DeepSeekWebClient integration test")
    print(f"time: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60)

    passed, report = 0, {}
    for name, coro in [("single", case_single), ("batch", case_batch), ("reconnect", case_reconnect)]:
        try:
            report[name] = await coro()
            passed += 1
            print(f"  [PASS] {name}")
        except Exception as exc:
            report[name] = {"error": str(exc)}
            print(f"  [FAIL] {name}: {exc}")

    inst = DeepSeekWebClient._instance
    if inst:
        await inst.close()

    out = RESULTS_DIR / f"test_{datetime.now():%Y%m%d_%H%M%S}.json"
    out.write_text(
        json.dumps({"passed": passed, "total": 3, "results": report}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n{'='*60}\nresult: {passed}/3 passed\nreport: {out}\n{'='*60}")
    return passed == 3


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)

