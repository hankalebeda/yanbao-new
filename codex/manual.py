"""Backward-compatible entry point for manual analysis-only runs.

Delegates to codex.run with the 'analysis' profile. Preserves the original
CLI interface for scripts that depend on it.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from codex import mesh as codex_mesh
from codex import run as codex_run


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_PROVIDERS = list(codex_mesh.DEFAULT_PROVIDER_ALLOWLIST)
DEFAULT_TIMEOUT_MINUTES = 50

# Re-export for backward compatibility
resolve_codex_executable = codex_mesh.resolve_codex_executable


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hourly Codex runner for Prompt 6 live fix loop."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run-once", help="run Prompt 6 once with relay failover")
    run_parser.add_argument("--delegate-mode", choices=["legacy", "mesh"], default="mesh")
    run_parser.add_argument("--repo-root", type=Path, default=repo_root())
    run_parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    run_parser.add_argument("--prompt-doc", type=Path)
    run_parser.add_argument("--timeout-minutes", type=int, default=DEFAULT_TIMEOUT_MINUTES)
    run_parser.add_argument("--provider", action="append", dest="providers")
    run_parser.add_argument("--mesh-max-workers", type=int, default=codex_mesh.DEFAULT_MAX_WORKERS)
    run_parser.add_argument("--mesh-max-depth", type=int, default=codex_mesh.DEFAULT_MAX_EXTERNAL_DEPTH)
    run_parser.add_argument("--mesh-benchmark-label", default=None)
    run_parser.add_argument("--mesh-disable-provider", action="append")
    run_parser.add_argument("--mesh-hedge-delay-seconds", type=int, default=codex_mesh.DEFAULT_HEDGE_DELAY_SECONDS)
    run_parser.add_argument("--preferred-start")
    run_parser.add_argument("--prompt-text")
    run_parser.add_argument("--prompt-file", type=Path)
    run_parser.add_argument("--skip-overlay", action="store_true")
    run_parser.add_argument("--dry-run", action="store_true")
    run_parser.add_argument("--json", action="store_true")

    print_parser = subparsers.add_parser("print-prompt", help="print effective Prompt 6 payload")
    print_parser.add_argument("--repo-root", type=Path, default=repo_root())
    print_parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    print_parser.add_argument("--prompt-doc", type=Path)
    print_parser.add_argument("--provider", action="append", dest="providers")
    print_parser.add_argument("--prompt-text")
    print_parser.add_argument("--prompt-file", type=Path)
    print_parser.add_argument("--skip-overlay", action="store_true")

    return parser.parse_args(argv)


def _prompt_override_from_args(args: argparse.Namespace) -> str | None:
    if getattr(args, "prompt_text", None) and getattr(args, "prompt_file", None):
        raise ValueError("--prompt-text and --prompt-file are mutually exclusive")
    if getattr(args, "prompt_text", None):
        return args.prompt_text
    if getattr(args, "prompt_file", None):
        return args.prompt_file.read_text(encoding="utf-8")
    return None


def _providers_from_args(args: argparse.Namespace) -> list[str]:
    return list(args.providers or DEFAULT_PROVIDERS)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    profile = codex_run.load_profile("analysis")

    if args.command == "print-prompt":
        root = args.repo_root.resolve()
        prompt = codex_run.load_prompt(profile, root)
        override = _prompt_override_from_args(args)
        if override:
            prompt = override
        providers = _providers_from_args(args)
        overlay = ""
        if not args.skip_overlay:
            external_context = codex_mesh.resolve_external_execution_context(args.mesh_max_depth)
            native_subagent_settings = codex_run.profile_native_subagent_settings(profile)
            inner_codex = codex_mesh.resolve_inner_codex_options(
                allow_native_subagents=bool(native_subagent_settings["allow_native_subagents"]),
                allow_native_subagents_at_external_limit=bool(
                    native_subagent_settings["allow_native_subagents_at_external_limit"]
                ),
                depth=external_context.depth,
                max_external_depth=external_context.max_external_depth,
                agent_max_depth=int(native_subagent_settings["inner_agent_max_depth"]),
                agent_max_threads=native_subagent_settings["inner_agent_max_threads"],
            )
            overlay = codex_run.build_context_overlay(
                run_id="preview",
                task_id="preview",
                goal=profile.get("goal", ""),
                depth=external_context.depth,
                max_depth=external_context.max_external_depth,
                execution_root=root,
                provider_order=providers,
                read_scope=profile.get("read_scope", []),
                write_scope=profile.get("write_scope", []),
                subagents_enabled=bool(inner_codex["enable_multi_agent"]),
                parent_task_id=external_context.parent_task_id,
                lineage_id=external_context.lineage_id,
            )
        print(overlay + prompt)
        return 0

    if args.command == "run-once":
        if args.timeout_minutes != DEFAULT_TIMEOUT_MINUTES:
            profile["timeout_minutes"] = args.timeout_minutes

        result = codex_run.execute(
            profile,
            root=args.repo_root.resolve(),
            prompt_override=_prompt_override_from_args(args),
            providers=_providers_from_args(args),
            delegate_mode=args.delegate_mode,
            max_workers=args.mesh_max_workers,
            max_depth=args.mesh_max_depth,
            hedge_delay_seconds=args.mesh_hedge_delay_seconds,
            disable_providers=args.mesh_disable_provider,
            preferred_start=args.preferred_start,
            base_url=args.base_url,
            dry_run=args.dry_run,
            benchmark_label=args.mesh_benchmark_label,
            include_overlay=not args.skip_overlay,
        )
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"run_id={result.get('run_id', 'N/A')}")
            print(f"success={str(result.get('success', False)).lower()}")
            print(f"output_dir={result.get('output_dir', '')}")
            print(f"providers={','.join(result.get('providers', []))}")
        return 0 if result.get("success") else 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
