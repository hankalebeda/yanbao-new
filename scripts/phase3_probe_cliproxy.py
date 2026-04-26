"""Phase 3 · Probe CLIProxyAPI (http://192.168.232.141:8317) as LLM backup.

Tests /v1/models, /v1/chat/completions, /v1/responses via urllib with explicit
no-proxy (bypass system socks5). Writes result JSON to output/.
"""
from __future__ import annotations

import json
import os
import ssl
import time
import urllib.request
from pathlib import Path

BASE = "http://192.168.232.141:8317"
API_KEY = "cpa-api-yjEgDXE2lgi4mFjH"
MGMT_KEY = "cpa-2HkQ5zx9rMmVRYeM3R_4yfr6"
OUT = Path(__file__).resolve().parent.parent / "output" / "cliproxy_probe.json"


def _opener() -> urllib.request.OpenerDirector:
    # Bypass system proxy — 192.168.232.x is on LAN
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _req(method: str, url: str, headers: dict, body: dict | None = None, timeout: int = 30) -> dict:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    t0 = time.time()
    try:
        resp = _opener().open(req, timeout=timeout)
        raw = resp.read().decode("utf-8", errors="replace")
        code = resp.status
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        code = e.code
    except Exception as e:
        return {"url": url, "error": f"{type(e).__name__}: {e}", "elapsed": round(time.time() - t0, 2)}
    out = {"url": url, "status": code, "elapsed": round(time.time() - t0, 2), "body_preview": raw[:1000]}
    try:
        out["json"] = json.loads(raw)
    except Exception:
        pass
    return out


def main() -> int:
    results: dict = {"base": BASE, "probes": []}

    # 1. /v1/models
    r1 = _req("GET", f"{BASE}/v1/models", {"Authorization": f"Bearer {API_KEY}"})
    results["probes"].append({"name": "models", **r1})

    # 2. /v1/chat/completions — try multiple likely model names
    candidate_models = ["gpt-4o-mini", "gpt-4o", "gpt-5.4", "gpt-5.3-codex",
                        "claude-3-5-sonnet-20241022", "gemini-2.5-pro", "gemini-2.0-flash"]
    # If /v1/models returned real list, prefer those
    models_seen: list[str] = []
    if isinstance(r1.get("json"), dict):
        for m in (r1["json"].get("data") or []):
            mid = m.get("id") if isinstance(m, dict) else None
            if mid:
                models_seen.append(mid)
    if models_seen:
        candidate_models = models_seen[:5]
    results["candidate_models"] = candidate_models

    chat_ok = False
    for model in candidate_models:
        r = _req("POST", f"{BASE}/v1/chat/completions",
                 {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
                 body={"model": model, "messages": [{"role": "user", "content": "ping"}],
                       "max_tokens": 5, "stream": False},
                 timeout=45)
        results["probes"].append({"name": f"chat:{model}", **r})
        if r.get("status") == 200:
            chat_ok = True
            results["working_chat_model"] = model
            break

    # 3. /v1/responses (optional)
    if chat_ok and results.get("working_chat_model"):
        r3 = _req("POST", f"{BASE}/v1/responses",
                  {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
                  body={"model": results["working_chat_model"],
                        "input": [{"role": "user", "content": "ping"}],
                        "max_output_tokens": 16},
                  timeout=45)
        results["probes"].append({"name": "responses", **r3})
        results["responses_supported"] = (r3.get("status") == 200)

    results["chat_ok"] = chat_ok
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"=== CLIProxyAPI probe: chat_ok={chat_ok} ===")
    print(f"Models seen: {len(models_seen)}")
    print(f"Working model: {results.get('working_chat_model', 'NONE')}")
    print(f"/v1/responses: {results.get('responses_supported', 'N/A')}")
    print(f"Wrote: {OUT}")
    return 0 if chat_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
