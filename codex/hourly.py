"""Backward-compatible entry point for hourly Prompt 6 runs.

Delegates to codex.run with the 'hourly' profile. Preserves the original
CLI interface for scripts and scheduled tasks that depend on it.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from codex import mesh as codex_mesh
from codex import run as codex_run


PROMPT6_HEADING = "## Prompt 6：真实验真 + 深度修复循环控制器"
DEFAULT_PROVIDERS = list(codex_mesh.DEFAULT_PROVIDER_ALLOWLIST)
DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_ATTEMPT_TIMEOUT_SECONDS = 50 * 60

# Re-export for backward compatibility
AutomationLock = codex_run.AutomationLock
LockBusyError = codex_run.LockBusyError
extract_prompt_block = codex_run.extract_fenced_text
ensure_runtime = codex_run.ensure_runtime


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Prompt 6 through Codex with hourly-safe relay fallback."
    )
    parser.add_argument("--delegate-mode", choices=["legacy", "mesh"], default="mesh")
    parser.add_argument("--providers", nargs="+", default=DEFAULT_PROVIDERS)
    parser.add_argument("--mesh-max-workers", type=int, default=codex_mesh.DEFAULT_MAX_WORKERS)
    parser.add_argument("--mesh-max-depth", type=int, default=codex_mesh.DEFAULT_MAX_EXTERNAL_DEPTH)
    parser.add_argument("--mesh-benchmark-label", default=None)
    parser.add_argument("--mesh-disable-provider", action="append")
    parser.add_argument("--mesh-hedge-delay-seconds", type=int, default=codex_mesh.DEFAULT_HEDGE_DELAY_SECONDS)
    parser.add_argument("--prompt-doc", type=Path, default=None)
    parser.add_argument("--prompt-heading", default=PROMPT6_HEADING)
    parser.add_argument("--prompt-text", default=None)
    parser.add_argument("--prompt-prelude-file", type=Path, default=None)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--attempt-timeout-seconds", type=int, default=DEFAULT_ATTEMPT_TIMEOUT_SECONDS)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-overlay", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--ensure-runtime", dest="ensure_runtime", action="store_true", default=True)
    parser.add_argument("--no-ensure-runtime", dest="ensure_runtime", action="store_false")
    parser.add_argument("--ephemeral", dest="ephemeral", action="store_true", default=True)
    parser.add_argument("--no-ephemeral", dest="ephemeral", action="store_false")
    parser.add_argument("--no-dangerously-bypass", dest="dangerously_bypass", action="store_false", default=True)
    parser.add_argument("--sandbox", default="danger-full-access")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    profile = codex_run.load_profile("hourly")

    # Override profile settings from CLI args
    if not args.ensure_runtime:
        profile["ensure_runtime"] = False
    if args.attempt_timeout_seconds != DEFAULT_ATTEMPT_TIMEOUT_SECONDS:
        profile["timeout_minutes"] = args.attempt_timeout_seconds // 60

    # Resolve prompt override
    prompt_override = None
    if args.prompt_text:
        prompt_override = args.prompt_text
    elif args.prompt_doc:
        text = args.prompt_doc.read_text(encoding="utf-8")
        prompt_override = codex_run.extract_fenced_text(text, args.prompt_heading)
    if args.prompt_prelude_file:
        prelude = args.prompt_prelude_file.read_text(encoding="utf-8")
        prompt_override = (prelude.strip() + "\n\n" + (prompt_override or "")).strip() or None

    result = codex_run.execute(
        profile,
        prompt_override=prompt_override,
        providers=list(args.providers),
        delegate_mode=args.delegate_mode,
        max_workers=args.mesh_max_workers,
        max_depth=args.mesh_max_depth,
        hedge_delay_seconds=args.mesh_hedge_delay_seconds,
        disable_providers=args.mesh_disable_provider,
        base_url=args.base_url,
        dry_run=args.dry_run,
        benchmark_label=args.mesh_benchmark_label,
        include_overlay=not args.skip_overlay,
        dangerously_bypass=args.dangerously_bypass,
        sandbox=args.sandbox,
        ephemeral=args.ephemeral,
    )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps({
            "run_id": result.get("run_id", "N/A"),
            "success": result.get("success", False),
            "selected_provider": result.get("selected_provider"),
            "runtime_status": (result.get("runtime_preflight") or {}).get("status"),
            "output_dir": result.get("output_dir", ""),
            "delegate_mode": result.get("delegate_mode", "mesh"),
        }, ensure_ascii=False))
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
