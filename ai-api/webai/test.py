import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from app.main import app  # noqa: E402

RESULTS_DIR = Path(__file__).parent / "test_results"
RESULTS_DIR.mkdir(exist_ok=True)

PROVIDERS = ["chatgpt", "deepseek", "gemini", "qwen"]
TAG_RE = re.compile(r"CASE-([A-Z0-9_\-]+)\|([A-Z0-9_\-]+)\|OK")


def _assert_ok(resp_json: dict) -> None:
    if resp_json.get("code") != 0:
        raise AssertionError(f"envelope code != 0: {resp_json}")


def _assert_tag(text: str, case_id: str, name: str) -> None:
    m = TAG_RE.search((text or "").upper())
    if not m:
        raise AssertionError(f"missing tag: expected CASE-{case_id}|{name}|OK got={text[:120]!r}")
    got_case, got_name = m.group(1), m.group(2)
    if got_case != case_id or got_name != name:
        raise AssertionError(
            f"tag mismatch: expected CASE-{case_id}|{name}|OK got CASE-{got_case}|{got_name}|OK"
        )


def run() -> int:
    report: dict = {"started_at": datetime.now().isoformat(), "providers": {}, "summary": {}}
    passed = 0
    total = len(PROVIDERS) * 2 + 2
    print("=" * 70)
    print(f"Unified WebAI API test start: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 70)

    with TestClient(app) as client:
        r = client.get("/api/v1/webai/providers")
        providers_payload = r.json()
        print(f"[providers] status={r.status_code} body={providers_payload}")
        _assert_ok(providers_payload)
        passed += 1

        for provider in PROVIDERS:
            provider_report: dict = {}
            report["providers"][provider] = provider_report

            try:
                case_id = f"SINGLE_{provider.upper()}"
                name = "ONE"
                prompt = f"你只输出这一行且不要其他字符：CASE-{case_id}|{name}|OK"
                t0 = time.time()
                r1 = client.post(
                    "/api/v1/webai/analyze",
                    json={"provider": provider, "prompt": prompt, "timeout_s": 120},
                    timeout=180,
                )
                d1 = r1.json()
                _assert_ok(d1)
                data = d1.get("data") or {}
                _assert_tag(data.get("response", ""), case_id, name)
                provider_report["single"] = {
                    "status_code": r1.status_code,
                    "elapsed_wall_s": round(time.time() - t0, 2),
                    "data": data,
                }
                passed += 1
                print(
                    f"[{provider}/single] ok status={r1.status_code} "
                    f"elapsed={provider_report['single']['elapsed_wall_s']}s "
                    f"resp={data.get('response', '')[:120]}"
                )
            except Exception as exc:
                provider_report["single"] = {"error": str(exc)}
                print(f"[{provider}/single] fail error={exc}")

            try:
                items = [
                    {"code": "600519", "name": "A1", "case_id": f"B1_{provider.upper()}"},
                    {"code": "000858", "name": "A2", "case_id": f"B2_{provider.upper()}"},
                    {"code": "300750", "name": "A3", "case_id": f"B3_{provider.upper()}"},
                ]
                stocks = [
                    {
                        "code": x["code"],
                        "name": x["name"],
                        "prompt": f"你只输出这一行且不要其他字符：CASE-{x['case_id']}|{x['name']}|OK",
                    }
                    for x in items
                ]
                t0 = time.time()
                r2 = client.post(
                    "/api/v1/webai/analyze/batch",
                    json={"provider": provider, "stocks": stocks, "timeout_s": 120},
                    timeout=300,
                )
                d2 = r2.json()
                _assert_ok(d2)
                data2 = d2.get("data") or {}
                results = data2.get("results") or []
                if len(results) != 3:
                    raise AssertionError(f"batch result size mismatch: {len(results)}")
                idx = {(x["code"], x["name"]): x for x in items}
                for res in results:
                    if "error" in res:
                        raise AssertionError(f"batch item error: {res}")
                    src = idx.get((res.get("code"), res.get("name")))
                    if not src:
                        raise AssertionError(f"unexpected batch key {(res.get('code'), res.get('name'))}")
                    _assert_tag(res.get("response", ""), src["case_id"], src["name"])

                provider_report["batch"] = {
                    "status_code": r2.status_code,
                    "elapsed_wall_s": round(time.time() - t0, 2),
                    "data": data2,
                }
                passed += 1
                print(
                    f"[{provider}/batch] ok status={r2.status_code} "
                    f"elapsed={provider_report['batch']['elapsed_wall_s']}s count={data2.get('count')}"
                )
                for row in results:
                    print(
                        f"  - {provider} {row.get('code')} {row.get('name')} "
                        f"-> {str(row.get('response', ''))[:80]}"
                    )
            except Exception as exc:
                provider_report["batch"] = {"error": str(exc)}
                print(f"[{provider}/batch] fail error={exc}")

        r3 = client.get("/api/v1/webai/session/status")
        session_payload = r3.json()
        print(f"[session/status] status={r3.status_code} body={session_payload}")
        _assert_ok(session_payload)
        report["session_status"] = session_payload.get("data", {})
        passed += 1

        r4 = client.delete("/api/v1/webai/session")
        close_payload = r4.json()
        print(f"[session/close-all] status={r4.status_code} body={close_payload}")
        _assert_ok(close_payload)
        report["close_all"] = close_payload.get("data", {})
        passed += 1

    report["summary"] = {"passed": passed, "total": total}
    out = RESULTS_DIR / f"test_{datetime.now():%Y%m%d_%H%M%S}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("=" * 70)
    print(f"Unified WebAI API test done: passed={passed}/{total}")
    print(f"report={out}")
    print("=" * 70)
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(run())
