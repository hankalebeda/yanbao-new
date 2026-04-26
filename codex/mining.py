"""Backward-compatible entry point for 22_v7 issue mining runs.

Delegates to codex.run with the 'mining' profile. Preserves the original
CLI interface for scripts that depend on it.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from codex import mesh as codex_mesh
from codex import run as codex_run


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_PROVIDERS = list(codex_mesh.DEFAULT_PROVIDER_ALLOWLIST)
DEFAULT_TIMEOUT_MINUTES = 30


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def target_doc(root: Path) -> Path:
    matches = sorted((root / "docs" / "core").glob("22_*v7*.md"))
    if not matches:
        raise FileNotFoundError("Unable to locate docs/core/22_*v7*.md")
    return matches[0]


def collect_writeback_targets(doc: Path) -> list[dict[str, object]]:
    targets: list[dict[str, object]] = []
    for i, line in enumerate(doc.read_text(encoding="utf-8").splitlines(), 1):
        if not line.startswith("### "):
            continue
        heading = line[4:].strip()
        if heading.startswith("FR") or heading.startswith("PAGE-") or heading.startswith("NFR-"):
            targets.append({"heading": heading, "line": i})
    return targets


def build_mining_prompt(root: Path, base_url: str, providers: list[str]) -> tuple[str, Path]:
    """Build the mining analysis prompt from the target document."""
    effective_doc = target_doc(root)
    timestamp = datetime.now(timezone.utc).astimezone().isoformat()
    writeback_targets = collect_writeback_targets(effective_doc)
    writeback_preview = (
        "\n".join(f"   - `{item['heading']}`" for item in writeback_targets[:12])
        or "   - `(no writable targets found)`"
    )

    # Goal-oriented prompt: describe WHAT to achieve, not HOW to do it
    prompt = f"""You are the "22_v7 issue mining (analysis-only)" agent for this project.

Target document: `{effective_doc.as_posix()}`
SSOT sources: `docs/core/01~05` (requirements, architecture, design, data governance, API contracts)
Live base URL: `{base_url}`
Timestamp: `{timestamp}`

Your goal: find real, currently-existing problems in the system by cross-referencing
the v7 progress table against SSOT specs, current code, tests, and live state.

Rules:
- Analysis only. Do NOT modify code, tests, or scripts.
- The ONLY file you may edit is: `{effective_doc.as_posix()}`
- Write back issues that meet at least one criterion:
  - Violates a frozen contract in docs/core/01~05
  - v7 table claims something that contradicts current code/tests/live state
  - Cross-system inconsistency (page vs API vs DB vs code vs test)
  - Document claims an issue is resolved but evidence shows it still exists
  - High-value real problem missing from the document
- Each writeback must include: problem, evidence, SSOT basis, conclusion, suggested action
- Writable target sections:
{writeback_preview}

Output a brief JSON summary: status, new_issue_count, updated_doc, summary.
"""
    return prompt.strip(), effective_doc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manual Codex analysis-only issue mining for docs/core/22 v7."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run-once", help="run the 22_v7 analysis-only pass once")
    run_parser.add_argument("--delegate-mode", choices=["legacy", "mesh"], default="mesh")
    run_parser.add_argument("--repo-root", type=Path, default=repo_root())
    run_parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
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

    print_parser = subparsers.add_parser("print-prompt", help="print the effective analysis prompt")
    print_parser.add_argument("--repo-root", type=Path, default=repo_root())
    print_parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
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
    profile = codex_run.load_profile("mining")
    root = args.repo_root.resolve()

    if args.command == "print-prompt":
        override = _prompt_override_from_args(args)
        if override:
            print(override)
        else:
            prompt, _ = build_mining_prompt(root, args.base_url, _providers_from_args(args))
            print(prompt)
        return 0

    if args.command == "run-once":
        if args.timeout_minutes != DEFAULT_TIMEOUT_MINUTES:
            profile["timeout_minutes"] = args.timeout_minutes

        # Build mining-specific prompt and write scope
        override = _prompt_override_from_args(args)
        if not override:
            prompt, effective_doc = build_mining_prompt(root, args.base_url, _providers_from_args(args))
            override = prompt
            # Set write scope to the target doc
            rel_path = str(effective_doc.relative_to(root)).replace("\\", "/")
            profile["write_scope"] = [rel_path]

        result = codex_run.execute(
            profile,
            root=root,
            prompt_override=override,
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
