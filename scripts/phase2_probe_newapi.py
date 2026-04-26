"""Probe all NewAPI relays to find working ones."""
import json
import time
from pathlib import Path
import urllib.request

urllib.request.install_opener(
    urllib.request.build_opener(urllib.request.ProxyHandler({}))
)

ROOT = Path("ai-api/codex")
relays = []
for sub in ROOT.iterdir():
    if sub.is_dir():
        pj = sub / "provider.json"
        kj = sub / "key.txt"
        if pj.exists() and kj.exists():
            try:
                p = json.loads(pj.read_text(encoding="utf-8"))
                key = None
                for line in kj.read_text(encoding="utf-8", errors="replace").splitlines():
                    if line.startswith("sk-"):
                        key = line.strip()
                        break
                if key and p.get("endpoint") and p.get("model"):
                    relays.append({"name": sub.name, "endpoint": p["endpoint"], "model": p["model"], "key": key})
            except Exception as e:
                print(f"skip {sub.name}: {e}")

print(f"total relays: {len(relays)}")

results = []
for r in relays:
    # Try chat/completions then responses
    url_chat = r["endpoint"].rstrip("/") + "/chat/completions"
    url_resp = r["endpoint"].rstrip("/") + "/responses"
    body_chat = json.dumps({
        "model": r["model"],
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 5,
    }).encode()
    body_resp = json.dumps({
        "model": r["model"],
        "input": "ping",
        "max_output_tokens": 16,
    }).encode()

    def try_call(url, body):
        req = urllib.request.Request(url, data=body, headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {r['key']}",
        }, method="POST")
        t0 = time.time()
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                txt = resp.read().decode(errors="replace")
                return resp.status, True, round(time.time() - t0, 2), txt[:120]
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")[:120]
            return e.code, False, round(time.time() - t0, 2), body
        except Exception as e:
            return 0, False, round(time.time() - t0, 2), f"ERR: {e}"

    sc, ok_c, dt_c, sn_c = try_call(url_chat, body_chat)
    sr, ok_r, dt_r, sn_r = try_call(url_resp, body_resp)
    best = {"name": r["name"], "chat": {"status": sc, "ok": ok_c, "elapsed_s": dt_c, "snippet": sn_c},
            "responses": {"status": sr, "ok": ok_r, "elapsed_s": dt_r, "snippet": sn_r}}
    results.append(best)
    tag = "OK_CHAT" if ok_c else ("OK_RESP" if ok_r else f"FAIL(c={sc},r={sr})")
    print(f"  {r['name']:<40s} {tag:<22s} c={dt_c}s r={dt_r}s")

print("\n=== WORKING RELAYS ===")
for r in results:
    if r["chat"]["ok"] or r["responses"]["ok"]:
        print(f" {r['name']}  chat={r['chat']['status']} resp={r['responses']['status']}")

Path("output/newapi_probe.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
print("\nsaved output/newapi_probe.json")
