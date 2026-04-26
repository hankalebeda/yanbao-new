#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from scripts import codex_mesh


DEFAULT_BENCHMARK_LABEL = "queue22-recovery-rearm-round2-readonly"
DEFAULT_TIMEOUT_SECONDS = 35 * 60
DEFAULT_MAX_WORKERS = 6
PROMPT_DIR = Path("docs/_temp/problem/launch_prompts")
DOC_22 = "docs/core/22_全量功能进度总表_v7_精审.md"
DOC_25 = "docs/core/25_系统问题分析角度清单.md"
TRUTH_LAYER_FILES = [
    "docs/_temp/problem/README.md",
    "docs/_temp/problem/SLOT_OCCUPANCY.md",
    "docs/_temp/problem/ANALYSIS_MASTER_LOCK__owner-controller02__status-active.md",
]


SUPPORT_SLOT_CONFIGS: list[dict[str, object]] = [
    {
        "task_id": "slot04-runtime-readback-auditor",
        "goal": "readonly runtime evidence recheck for round 1 blocked evidence and round 2 readiness",
        "prompt_file": "runtime_readback_auditor_20260326c_r2.txt",
        "read_scope": [
            *TRUTH_LAYER_FILES,
            "docs/_temp/problem/20260326-111001__P1__runtime__runtime-rearm-executor__owner-slot02-runtime-executor__status-blocked.md",
            "docs/_temp/problem/20260326-111002__P1__runtime__runtime-rearm-watchdog__owner-slot03-runtime-watchdog__status-blocked.md",
            "docs/_temp/problem/20260326-111003__P1__runtime__runtime-rearm-executor-r2__owner-unassigned__status-todo.md",
            "docs/_temp/problem/20260326-111004__P1__runtime__runtime-rearm-watchdog-r2__owner-unassigned__status-todo.md",
            "docs/_temp/problem/20260326-100500__P1__runtime__repair-history-recovery__owner-fix-runtime-recovery-01__status-blocked.md",
            "docs/_temp/problem/20260326-103500__P1__runtime__live-runtime-recovery-exec__owner-fix-runtime-live-01__status-blocked.md",
            "output/task_runs/20260326-111001__P1__runtime__runtime-rearm-executor/",
            "output/task_runs/20260326-111002__P1__runtime__runtime-rearm-watchdog/",
        ],
    },
    {
        "task_id": "slot05-claim-resource-guard",
        "goal": "readonly claim and resource conflict audit",
        "prompt_file": "claim_resource_guard_20260326c_r2.txt",
        "read_scope": [
            "docs/_temp/problem/",
            "docs/_temp/problem/_claims/",
            "docs/_temp/problem/SLOT_OCCUPANCY.md",
        ],
    },
]

REBASELINE_SLOT_CONFIGS: list[dict[str, object]] = [
    {
        "task_id": "slot06-artifact-delta-auditor",
        "goal": "readonly artifact delta audit for 1180 vs 1168 and registry_stats.warn_features=69",
        "prompt_file": "artifact_delta_auditor_20260326c_r2.txt",
        "read_scope": [
            *TRUTH_LAYER_FILES,
            DOC_22,
            "output/junit.xml",
            "app/governance/catalog_snapshot.json",
            "output/blind_spot_audit.json",
            "github/automation/continuous_audit/latest_run.json",
        ],
    },
    {
        "task_id": "slot07-current-layer-miner-a",
        "goal": "readonly current-layer mining pass A",
        "prompt_file": "miner_current_layer_20260326c_r2.txt",
        "read_scope": [
            *TRUTH_LAYER_FILES,
            DOC_22,
            DOC_25,
            "output/junit.xml",
            "app/governance/catalog_snapshot.json",
            "output/blind_spot_audit.json",
            "github/automation/continuous_audit/latest_run.json",
        ],
    },
    {
        "task_id": "slot08-current-layer-miner-b",
        "goal": "readonly current-layer mining pass B",
        "prompt_file": "miner_current_layer_20260326c_r2.txt",
        "read_scope": [
            *TRUTH_LAYER_FILES,
            DOC_22,
            DOC_25,
            "output/junit.xml",
            "app/governance/catalog_snapshot.json",
            "output/blind_spot_audit.json",
            "github/automation/continuous_audit/latest_run.json",
        ],
    },
    {
        "task_id": "slot09-task-audit-miner",
        "goal": "readonly task-pool audit for 221000-221009 and 111000-111004",
        "prompt_file": "task_audit_miner_20260326c_r2.txt",
        "read_scope": [
            *TRUTH_LAYER_FILES,
            DOC_22,
            "docs/_temp/problem/",
        ],
    },
    {
        "task_id": "slot13-writeback-drafter",
        "goal": "readonly writeback drafting for 22 current layer",
        "prompt_file": "artifact_writeback_prep_20260326c_r2.txt",
        "read_scope": [
            *TRUTH_LAYER_FILES,
            DOC_22,
            DOC_25,
            "output/junit.xml",
            "app/governance/catalog_snapshot.json",
            "output/blind_spot_audit.json",
            "github/automation/continuous_audit/latest_run.json",
            "docs/_temp/problem/",
        ],
    },
]

FULL_SLOT_CONFIGS: list[dict[str, object]] = SUPPORT_SLOT_CONFIGS + REBASELINE_SLOT_CONFIGS[:-1] + [
    {
        "task_id": "slot10-review-precheck-a",
        "goal": "readonly review precheck for 221005 and 221006",
        "prompt_file": "review_precheck_20260326c_r2.txt",
        "read_scope": [
            "docs/_temp/problem/20260325-221005__P2__auth__fr09-oauth-callback-idempotency__owner-fix-auth-03__status-review.md",
            "docs/_temp/problem/20260325-221006__P2__billing__fr09-billing-webhook-fail-close__owner-fix-billing-04__status-review.md",
            "output/task_runs/20260325-221005__P2__auth__fr09-oauth-callback-idempotency/",
            "output/task_runs/20260325-221006__P2__billing__fr09-billing-webhook-fail-close/",
            DOC_22,
        ],
    },
    {
        "task_id": "slot11-review-precheck-b",
        "goal": "readonly review precheck for 221007 and 221008",
        "prompt_file": "review_precheck_20260326c_r2.txt",
        "read_scope": [
            "docs/_temp/problem/20260325-221007__P2__integration__fr04-hot-stocks-public-contract__owner-fix-fr04-02__status-review.md",
            "docs/_temp/problem/20260325-221008__P2__frontend__fr08-account-snapshot-e2e__owner-fix-fr08-02__status-review.md",
            "output/task_runs/20260325-221007__P2__integration__fr04-hot-stocks-public-contract/",
            "output/task_runs/20260325-221008__P2__frontend__fr08-account-snapshot-e2e/",
            DOC_22,
        ],
    },
    {
        "task_id": "slot12-review-precheck-c",
        "goal": "readonly review precheck for 221009 and 093500",
        "prompt_file": "review_precheck_20260326c_r2.txt",
        "read_scope": [
            "docs/_temp/problem/20260325-221009__P2__runtime__fr02-internal-metrics-summary-test__owner-fix-fr02-02__status-review.md",
            "docs/_temp/problem/20260326-093500__P1__frontend__report-view-feedback-and-copy__owner-fix-reportview-01__status-review.md",
            "output/task_runs/20260325-221009__P2__runtime__fr02-internal-metrics-summary-test/",
            "output/task_runs/20260326-093500__P1__frontend__report-view-feedback-and-copy/",
            DOC_22,
        ],
    },
] + [REBASELINE_SLOT_CONFIGS[-1]]
PROFILE_SLOT_CONFIGS: dict[str, list[dict[str, object]]] = {
    "support": SUPPORT_SLOT_CONFIGS,
    "rebaseline": REBASELINE_SLOT_CONFIGS,
    "full": FULL_SLOT_CONFIGS,
}


def repo_root() -> Path:
    return _repo_root


def _read_prompt(root: Path, prompt_file: str) -> str:
    path = root / PROMPT_DIR / prompt_file
    return path.read_text(encoding="utf-8")


def _merge_truth_layer(paths: list[str]) -> list[str]:
    merged: list[str] = []
    for item in [*TRUTH_LAYER_FILES, *paths]:
        if item not in merged:
            merged.append(item)
    return merged


def _resolve_max_workers(requested_max_workers: int) -> int:
    return max(1, min(requested_max_workers, DEFAULT_MAX_WORKERS))


def build_manifest(
    root: Path,
    *,
    profile: str = "rebaseline",
    providers: list[str] | None = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    benchmark_label: str = DEFAULT_BENCHMARK_LABEL,
) -> codex_mesh.MeshRunManifest:
    provider_allowlist = list(providers or codex_mesh.DEFAULT_PROVIDER_ALLOWLIST)
    slot_configs = PROFILE_SLOT_CONFIGS[profile]
    tasks: list[codex_mesh.MeshTaskManifest] = []
    for item in slot_configs:
        prompt_file = str(item["prompt_file"])
        tasks.append(
            codex_mesh.MeshTaskManifest(
                task_id=str(item["task_id"]),
                goal=str(item["goal"]),
                prompt=_read_prompt(root, prompt_file),
                task_kind="analysis",
                read_scope=_merge_truth_layer(list(item["read_scope"])),
                write_scope=[],
                provider_allowlist=provider_allowlist,
                provider_denylist=list(codex_mesh.DEFAULT_PROVIDER_DENYLIST),
                timeout_seconds=timeout_seconds,
                benchmark_label=benchmark_label,
                output_mode="text",
                working_root=str(root),
                allow_native_subagents=True,
            )
        )
    return codex_mesh.MeshRunManifest(
        tasks=tasks,
        execution_mode="mesh",
        max_workers=_resolve_max_workers(max_workers),
        benchmark_label=benchmark_label,
        provider_allowlist=provider_allowlist,
        provider_denylist=list(codex_mesh.DEFAULT_PROVIDER_DENYLIST),
    )


def manifest_payload(manifest: codex_mesh.MeshRunManifest) -> dict[str, object]:
    return {
        "execution_mode": manifest.execution_mode,
        "max_workers": manifest.max_workers,
        "benchmark_label": manifest.benchmark_label,
        "dangerously_bypass": manifest.dangerously_bypass,
        "sandbox": manifest.sandbox,
        "ephemeral": manifest.ephemeral,
        "provider_allowlist": manifest.provider_allowlist,
        "provider_denylist": manifest.provider_denylist,
        "tasks": [asdict(task) for task in manifest.tasks],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch Queue22 Round 2 readonly logical-slot waves via codex mesh."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("print-manifest", "run"):
        sub = subparsers.add_parser(command)
        sub.add_argument("--repo-root", type=Path, default=repo_root())
        sub.add_argument("--profile", choices=tuple(PROFILE_SLOT_CONFIGS), default="rebaseline")
        sub.add_argument("--provider", action="append", dest="providers")
        sub.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
        sub.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
        sub.add_argument("--benchmark-label", default=DEFAULT_BENCHMARK_LABEL)
        sub.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.repo_root.resolve()
    manifest = build_manifest(
        root,
        profile=args.profile,
        providers=args.providers,
        max_workers=args.max_workers,
        timeout_seconds=args.timeout_seconds,
        benchmark_label=args.benchmark_label,
    )
    if args.command == "print-manifest":
        print(json.dumps(manifest_payload(manifest), ensure_ascii=False, indent=2))
        return 0

    summary = codex_mesh.execute_manifest(root, manifest)
    payload = {
        "run_id": summary.run_id,
        "success": summary.success,
        "manifest_path": summary.manifest_path,
        "output_dir": summary.output_dir,
        "task_count": summary.task_count,
        "tasks": [asdict(task) for task in summary.tasks],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"run_id={summary.run_id}")
        print(f"success={summary.success}")
        print(f"manifest_path={summary.manifest_path}")
        print(f"output_dir={summary.output_dir}")
        print(f"task_count={summary.task_count}")
    return 0 if summary.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
