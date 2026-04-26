"""Batch-probe all ai-api/codex provider directories.

For each provider: try gpt-5.4 first, then gpt-5.3-codex, then gpt-5.2.
Only tests models_probe + responses_probe (skip codex exec for speed).
Outputs a JSON summary to stdout and optionally to --out file.
"""
from __future__ import annotations

import json
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# Ensure repo root is importable
ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from probe_provider_live import (
    load_provider_spec,
    probe_models,
    probe_responses,
)

CANDIDATE_MODELS = ["gpt-5.4", "gpt-5.3-codex", "gpt-5.2"]
TIMEOUT = 45.0
PROMPT = "Reply with exactly: LIVE_OK"

# Known provider directories (skip portable_*, newapi gateway, windhub placeholder)
SKIP_PREFIXES = ("portable_", "newapi-", "__pycache__")


@dataclass
class ProbeResult:
    provider: str
    model_tested: str | None = None
    model_ok: str | None = None  # the model that succeeded
    models_probe_ok: bool = False
    responses_probe_ok: bool = False
    matches_live_ok: bool = False
    latency_s: float | None = None
    error: str | None = None
    fallback_used: bool = False
    raw_steps: list[dict[str, Any]] = field(default_factory=list)


def _is_provider_dir(d: Path) -> bool:
    if not d.is_dir():
        return False
    name = d.name
    if any(name.startswith(p) for p in SKIP_PREFIXES):
        return False
    # Must have at least config.toml or provider.json
    return (d / "config.toml").exists() or (d / "provider.json").exists()


def probe_one(provider_dir: Path) -> ProbeResult:
    result = ProbeResult(provider=provider_dir.name)

    for i, model in enumerate(CANDIDATE_MODELS):
        result.model_tested = model
        try:
            spec = load_provider_spec(provider_dir, requested_model=model)
        except Exception as exc:
            result.error = f"load_spec({model}): {exc}"
            continue

        t0 = time.monotonic()
        try:
            models_check = probe_models(spec, TIMEOUT)
            result.raw_steps.append(models_check)
            result.models_probe_ok = bool(models_check.get("ok"))
        except Exception as exc:
            result.error = f"models_probe({model}): {exc}"
            result.raw_steps.append({"step": "models_probe", "ok": False, "error": str(exc)})
            continue

        try:
            resp_check = probe_responses(spec, PROMPT, TIMEOUT)
            result.raw_steps.append(resp_check)
            result.responses_probe_ok = bool(resp_check.get("ok"))
            result.matches_live_ok = bool(resp_check.get("matches_expected"))
        except Exception as exc:
            result.error = f"responses_probe({model}): {exc}"
            result.raw_steps.append({"step": "responses_probe", "ok": False, "error": str(exc)})
            continue

        result.latency_s = round(time.monotonic() - t0, 2)

        if result.responses_probe_ok and result.matches_live_ok:
            result.model_ok = model
            result.fallback_used = i > 0
            result.error = None
            break
        else:
            # Try fallback
            err_msg = resp_check.get("error", "")[:200] if not result.matches_live_ok else "output != LIVE_OK"
            result.error = f"{model}: {err_msg}"

    return result


def main() -> int:
    global TIMEOUT
    import argparse
    parser = argparse.ArgumentParser(description="Batch probe all codex providers")
    parser.add_argument("--out", default=None, help="Write JSON summary to this file")
    parser.add_argument("--timeout", type=float, default=TIMEOUT)
    args = parser.parse_args()

    TIMEOUT = args.timeout

    provider_dirs = sorted(
        [d for d in ROOT.iterdir() if _is_provider_dir(d)],
        key=lambda d: d.name,
    )

    print(f"Found {len(provider_dirs)} provider directories to probe", file=sys.stderr)

    results: list[ProbeResult] = []
    for pdir in provider_dirs:
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"Probing: {pdir.name}", file=sys.stderr)
        r = probe_one(pdir)
        results.append(r)
        status = "PASS" if r.model_ok else "FAIL"
        model_info = f" model={r.model_ok}" if r.model_ok else ""
        latency_info = f" latency={r.latency_s}s" if r.latency_s else ""
        err_info = f" error={r.error}" if r.error else ""
        print(f"  => {status}{model_info}{latency_info}{err_info}", file=sys.stderr)

    # Summary
    passed = [r for r in results if r.model_ok]
    failed = [r for r in results if not r.model_ok]

    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "candidate_models": CANDIDATE_MODELS,
        "total_providers": len(results),
        "passed_count": len(passed),
        "failed_count": len(failed),
        "passed": [
            {"provider": r.provider, "model": r.model_ok, "latency_s": r.latency_s, "fallback": r.fallback_used}
            for r in passed
        ],
        "failed": [
            {"provider": r.provider, "error": r.error}
            for r in failed
        ],
    }

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"SUMMARY: {len(passed)} passed, {len(failed)} failed out of {len(results)}", file=sys.stderr)

    output = json.dumps(summary, indent=2, ensure_ascii=False)
    print(output)

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(output, encoding="utf-8")
        print(f"Written to {args.out}", file=sys.stderr)

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
