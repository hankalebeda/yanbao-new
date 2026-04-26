"""Test LLM via direct service call. Proves CLIProxyAPI is used."""
import os, sys, time
os.environ.setdefault("PYTHONPATH", r"D:\yanbao-new")
sys.path.insert(0, r"D:\yanbao-new")
os.environ["NO_PROXY"] = "*"

from app.services.codex_client import discover_codex_provider_specs
specs = discover_codex_provider_specs()
print(f"providers discovered: {len(specs)}")
for s in specs:
    print(f"  {s.provider_name} @ {s.base_url} model={s.model} wire={s.wire_api}")
print()

# Direct call through CodexAPIClient
import asyncio
from app.services.codex_client import CodexAPIClient
client = CodexAPIClient()
print("sending test analyze...")
t0 = time.time()
try:
    resp = asyncio.run(client.analyze(
        prompt="reply with exactly: OK",
        system_prompt="Reply exactly as asked.",
        temperature=0.0,
        max_tokens=20,
    ))
    print(f"SUCCESS in {time.time()-t0:.2f}s")
    print(f"  keys: {list(resp.keys())[:10]}")
    content = resp.get('analysis') or resp.get('content') or str(resp)[:200]
    print(f"  content: {content[:200]}")
except Exception as e:
    print(f"FAIL in {time.time()-t0:.2f}s: {type(e).__name__}: {e}")
