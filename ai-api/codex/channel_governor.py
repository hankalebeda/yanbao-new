"""
Channel Governor - the ONLY component authorized to mutate live New API channel state.

Usage:
    python -m ai-api.codex.channel_governor --mode inventory
    python -m ai-api.codex.channel_governor --mode govern
    python -m ai-api.codex.channel_governor --mode export-snapshot
    python -m ai-api.codex.channel_governor --mode generate-registry
"""
from __future__ import annotations

import argparse
import json
import os
import socket
from pathlib import Path
from typing import Any

from ai_api_codex_compat import sync_newapi_channels as sync_mod

from . import newapi_governance as gov

ROOT = Path(__file__).resolve().parent


def _holder_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def _truth_path(value: str | None) -> Path | None:
    if value:
        return Path(value).resolve()
    if sync_mod.DEFAULT_LIVE_TRUTH_PATH.exists():
        return sync_mod.DEFAULT_LIVE_TRUTH_PATH
    return None


def _registry_path(value: str | None) -> Path | None:
    if value:
        return Path(value).resolve()
    if sync_mod.DEFAULT_CHANNEL_REGISTRY_PATH.exists():
        return sync_mod.DEFAULT_CHANNEL_REGISTRY_PATH
    return None


def _load_inputs(args: argparse.Namespace) -> dict[str, Any]:
    truth_path = _truth_path(getattr(args, "truth_file", None))
    registry_path = _registry_path(getattr(args, "registry_file", None))
    truth = sync_mod._load_live_truth(truth_path)
    registry = sync_mod._load_channel_registry(registry_path)
    truth_errors = sync_mod._truth_errors(truth)
    registry_errors = sync_mod._registry_errors(registry)
    managed_sources = set(filter(None, getattr(args, "managed_source", []) or [])) or set(
        sync_mod._truth_string_list(truth, "managed_sources")
    )
    sources, skipped = sync_mod.load_provider_sources(
        Path(args.providers_root).resolve(),
        set(sync_mod.DEFAULT_EXCLUDES),
        include_root_key_sources=bool(
            getattr(args, "include_root_key_sources", False)
            or sync_mod._truth_bool(truth, "include_root_key_sources", False)
        ),
        managed_sources=managed_sources or None,
    )
    archive_unmanaged = bool(
        getattr(args, "archive_unmanaged", False)
        or getattr(args, "disable_unmanaged_candidates", False)
        or sync_mod._truth_bool(truth, "disable_unmanaged_candidates", False)
    )
    lane_policy = registry.get("lane_policy") if isinstance(registry.get("lane_policy"), dict) else {}
    freeze_lane_clones = bool(
        getattr(args, "freeze_lane_clones", False)
        or not bool(lane_policy.get("materialize_live_clones", False))
    )
    return {
        "truth_path": truth_path,
        "registry_path": registry_path,
        "truth": truth,
        "registry": registry,
        "truth_errors": truth_errors,
        "registry_errors": registry_errors,
        "managed_sources": managed_sources,
        "sources": sources,
        "skipped": skipped,
        "archive_unmanaged": archive_unmanaged,
        "freeze_lane_clones": freeze_lane_clones,
    }


def _source_by_identity(sources: list[sync_mod.ProviderSource]) -> dict[str, sync_mod.ProviderSource]:
    result: dict[str, sync_mod.ProviderSource] = {}
    for source in sources:
        for alias in sync_mod._source_aliases(source):
            result[alias] = source
    return result


def _managed_entry(
    state: dict[str, Any],
    identity: str,
    *,
    channel_id: int | None,
    mutation_source: str,
) -> gov.ChannelGovernanceEntry:
    entry = gov.get_channel_entry(state, identity)
    if entry is None:
        entry = gov.default_channel_entry(identity, channel_id=channel_id)
    if channel_id is not None:
        entry.channel_id = channel_id
    entry.inventory_class = sync_mod.INVENTORY_CLASS_MANAGED
    entry.preserve = True
    entry.allow_auto_create = True
    entry.allow_auto_enable = True
    entry.allow_auto_disable = True
    entry.archive_reason = ""
    entry.last_mutation_source = mutation_source
    gov.set_channel_entry(state, entry)
    return entry


def run_inventory(
    client: sync_mod.NewAPIClient,
    sources: list[sync_mod.ProviderSource],
    *,
    candidate_models: list[str],
    reasoning_effort: str,
    source_probe_workers: int | None = None,
    candidate_probe_workers: int | None = None,
) -> dict[str, Any]:
    """Inventory mode is strictly read-only."""
    results = sync_mod.inventory_sources(
        client,
        sources,
        candidate_models=candidate_models,
        reasoning_effort=reasoning_effort,
        source_probe_workers=source_probe_workers,
        candidate_probe_workers=candidate_probe_workers,
    )
    state = gov.load_governance_state()
    for result in results:
        identity = sync_mod._channel_identity(result.name)
        if not identity:
            continue
        _managed_entry(
            state,
            identity,
            channel_id=result.channel_id,
            mutation_source="inventory",
        )
        if result.upstream_probe_ok:
            gov.transition_channel(
                state,
                identity,
                event="probe_ok",
                channel_id=result.channel_id,
            )
        else:
            error = result.upstream_probe_error or result.error_detail or "probe_failed"
            gov.transition_channel(
                state,
                identity,
                event="probe_fail",
                error_text=error,
                channel_id=result.channel_id,
            )
        entry = gov.get_channel_entry(state, identity)
        if entry is not None:
            entry.last_mutation_source = "inventory"
            gov.set_channel_entry(state, entry)
    gov.save_governance_state(state)
    return {
        "mode": "inventory",
        "probed": len(results),
        "healthy": sum(1 for r in results if r.upstream_probe_ok),
        "source_probe_workers": source_probe_workers,
        "candidate_probe_workers": candidate_probe_workers,
        "results": [asdict_result(result) for result in results],
        "summary": gov.summary(state),
    }


def _reprobe_quarantined(
    state: dict[str, Any],
    *,
    source_by_identity: dict[str, sync_mod.ProviderSource],
    candidate_models: list[str],
    reasoning_effort: str,
    reprobe_lane: str | None = None,
    reprobe_shard: str | None = None,
    candidate_probe_workers: int | None = None,
) -> list[dict[str, Any]]:
    reprobe_results: list[dict[str, Any]] = []
    for entry in gov.channels_due_for_reprobe(state, lane=reprobe_lane, shard=reprobe_shard):
        source = source_by_identity.get(sync_mod._channel_identity(entry.channel_identity))
        if source is None:
            gov.transition_channel(
                state,
                entry.channel_identity,
                event="probe_fail",
                error_text="source_missing_for_reprobe",
                channel_id=entry.channel_id,
            )
            reprobe_results.append(
                {
                    "identity": entry.channel_identity,
                    "ok": False,
                    "error": "source_missing_for_reprobe",
                }
            )
            continue
        probe_kwargs: dict[str, Any] = {
            "candidate_models": candidate_models,
            "reasoning_effort": reasoning_effort,
        }
        if candidate_probe_workers is not None:
            probe_kwargs["candidate_probe_workers"] = candidate_probe_workers
        probe_ok, probe_error, _model, _base_url = sync_mod.probe_upstream_responses(source, **probe_kwargs)
        if probe_ok:
            gov.transition_channel(
                state,
                entry.channel_identity,
                event="probe_ok",
                channel_id=entry.channel_id,
            )
        else:
            gov.transition_channel(
                state,
                entry.channel_identity,
                event="probe_fail",
                error_text=probe_error or "reprobe_failed",
                channel_id=entry.channel_id,
            )
        updated = gov.get_channel_entry(state, entry.channel_identity)
        if updated is not None:
            updated.last_mutation_source = "govern-reprobe"
            gov.set_channel_entry(state, updated)
        reprobe_results.append(
            {
                "identity": entry.channel_identity,
                "ok": bool(probe_ok),
                "error": "" if probe_ok else (probe_error or "reprobe_failed"),
            }
        )
    return reprobe_results


def run_govern(
    client: sync_mod.NewAPIClient,
    sources: list[sync_mod.ProviderSource],
    *,
    registry: dict[str, Any],
    candidate_models: list[str],
    reasoning_effort: str,
    log_page_size: int = 100,
    archive_unmanaged: bool = False,
    freeze_lane_clones: bool = True,
    lease_ttl_seconds: int = gov.DEFAULT_LEASE_TTL_SECONDS,
    source_probe_workers: int | None = None,
    candidate_probe_workers: int | None = None,
    reprobe_lane: str | None = None,
    reprobe_shard: str | None = None,
) -> dict[str, Any]:
    """Govern mode: hold exclusive lease, then reconcile live New API state."""
    holder = _holder_id()
    with gov.governor_session(holder, ttl_seconds=lease_ttl_seconds) as lease:
        inventory = run_inventory(
            client,
            sources,
            candidate_models=candidate_models,
            reasoning_effort=reasoning_effort,
            source_probe_workers=source_probe_workers,
            candidate_probe_workers=candidate_probe_workers,
        )
        lease = gov.renew_lease(holder, ttl_seconds=lease_ttl_seconds, fencing_token=lease.fencing_token)
        state = gov.load_governance_state()
        reprobed = _reprobe_quarantined(
            state,
            source_by_identity=_source_by_identity(sources),
            candidate_models=candidate_models,
            reasoning_effort=reasoning_effort,
            reprobe_lane=reprobe_lane,
            reprobe_shard=reprobe_shard,
            candidate_probe_workers=candidate_probe_workers,
        )
        gov.save_governance_state(state)
        lease = gov.renew_lease(holder, ttl_seconds=lease_ttl_seconds, fencing_token=lease.fencing_token)
        gov.validate_fencing(lease.fencing_token)

        results_for_reconcile = sync_mod.sync_channels(
            client,
            sources,
            replace_existing=False,
            candidate_models=candidate_models,
            reasoning_effort=reasoning_effort,
            source_probe_workers=source_probe_workers,
            candidate_probe_workers=candidate_probe_workers,
        )
        lease = gov.renew_lease(holder, ttl_seconds=lease_ttl_seconds, fencing_token=lease.fencing_token)
        reconcile = sync_mod.reconcile_channel_pool(
            client,
            sources=sources,
            results=results_for_reconcile,
            registry=registry,
            candidate_models=candidate_models,
            log_page_size=log_page_size,
            archive_unmanaged=archive_unmanaged,
            freeze_lane_clones=freeze_lane_clones,
        )
        lease = gov.renew_lease(holder, ttl_seconds=lease_ttl_seconds, fencing_token=lease.fencing_token)
        return {
            "mode": "govern",
            "lease_id": lease.lease_id,
            "fencing_token": lease.fencing_token,
            "lease_ttl_seconds": lease_ttl_seconds,
            "source_probe_workers": source_probe_workers,
            "candidate_probe_workers": candidate_probe_workers,
            "reprobe_scope": {
                "lane": reprobe_lane,
                "shard": reprobe_shard,
            },
            "inventory": inventory,
            "reprobed": reprobed,
            "reconcile": reconcile,
            "summary": gov.summary(gov.load_governance_state()),
        }


def run_export_snapshot(client: sync_mod.NewAPIClient) -> dict[str, Any]:
    channels = client._list_channels()
    return {
        "mode": "export-snapshot",
        "captured_at": os.environ.get("CURRENT_DATE", ""),
        "channel_total": len(channels),
        "channels": channels,
        "governance_metrics": sync_mod.summarize_governance_metrics(channels),
    }


def run_generate_registry(
    client: sync_mod.NewAPIClient,
    sources: list[sync_mod.ProviderSource],
    *,
    candidate_models: list[str],
) -> dict[str, Any]:
    registry = sync_mod.build_manual_channel_registry(
        client._list_channels(),
        sources=sources,
        candidate_models=candidate_models,
    )
    return {
        "mode": "generate-registry",
        "registry": registry,
    }


def asdict_result(result: sync_mod.ChannelResult) -> dict[str, Any]:
    return {
        "name": result.name,
        "channel_id": result.channel_id,
        "upstream_probe_ok": result.upstream_probe_ok,
        "upstream_probe_model": result.upstream_probe_model,
        "upstream_probe_error": result.upstream_probe_error,
        "channel_test_ok": result.channel_test_ok,
        "channel_test_message": result.channel_test_message,
        "selected_model": result.selected_model,
        "channel_models": list(result.channel_models),
        "channel_test_model": result.channel_test_model,
        "message": result.message,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="New API Channel Governor")
    parser.add_argument(
        "--mode",
        choices=["inventory", "govern", "full", "export-snapshot", "generate-registry"],
        default="inventory",
    )
    parser.add_argument("--base-url", default="http://192.168.232.141:3000")
    parser.add_argument("--username", default="naadmin")
    parser.add_argument("--password", default="")
    parser.add_argument("--candidate-model", action="append", default=[])
    parser.add_argument("--reasoning-effort", default="xhigh")
    parser.add_argument("--providers-root", default=str(ROOT))
    parser.add_argument("--truth-file", default="")
    parser.add_argument("--registry-file", default="")
    parser.add_argument("--out", default="")
    parser.add_argument("--log-page-size", type=int, default=100)
    parser.add_argument("--disable-unmanaged-candidates", action="store_true", default=False)
    parser.add_argument("--archive-unmanaged", action="store_true", default=False)
    parser.add_argument("--freeze-lane-clones", action="store_true", default=False)
    parser.add_argument("--include-root-key-sources", action="store_true", default=False)
    parser.add_argument("--managed-source", action="append", default=[])
    parser.add_argument("--lease-ttl-seconds", type=int, default=gov.DEFAULT_LEASE_TTL_SECONDS)
    parser.add_argument(
        "--source-probe-workers",
        type=int,
        default=getattr(sync_mod, "DEFAULT_SOURCE_PROBE_MAX_WORKERS", 4),
    )
    parser.add_argument(
        "--candidate-probe-workers",
        type=int,
        default=getattr(sync_mod, "DEFAULT_CANDIDATE_PROBE_MAX_WORKERS", 4),
    )
    parser.add_argument("--reprobe-lane", default="")
    parser.add_argument("--reprobe-shard", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    candidate_models = args.candidate_model or list(sync_mod.DEFAULT_CANDIDATE_MODELS)
    loaded = _load_inputs(args)
    blocked_errors: list[str] = []
    if args.mode in {"govern", "generate-registry"}:
        blocked_errors.extend(loaded["truth_errors"])
    if args.mode == "govern":
        blocked_errors.extend(loaded["registry_errors"])

    client = sync_mod.NewAPIClient(args.base_url, args.username, args.password)
    try:
        client.ensure_setup()
        client.login()
        if blocked_errors:
            result: dict[str, Any] = {
                "mode": args.mode,
                "blocked_errors": blocked_errors,
                "truth_file": str(loaded["truth_path"]) if loaded["truth_path"] is not None else None,
                "registry_file": str(loaded["registry_path"]) if loaded["registry_path"] is not None else None,
                "truth_validation_errors": loaded["truth_errors"],
                "registry_validation_errors": loaded["registry_errors"],
                "managed_sources": sorted(loaded["managed_sources"]),
                "skipped": loaded["skipped"],
            }
        elif args.mode == "inventory":
            result = run_inventory(
                client,
                loaded["sources"],
                candidate_models=candidate_models,
                reasoning_effort=args.reasoning_effort,
                source_probe_workers=getattr(args, "source_probe_workers", None),
                candidate_probe_workers=getattr(args, "candidate_probe_workers", None),
            )
        elif args.mode == "govern":
            result = run_govern(
                client,
                loaded["sources"],
                registry=loaded["registry"],
                candidate_models=candidate_models,
                reasoning_effort=args.reasoning_effort,
                log_page_size=args.log_page_size,
                archive_unmanaged=loaded["archive_unmanaged"],
                freeze_lane_clones=loaded["freeze_lane_clones"],
                lease_ttl_seconds=int(getattr(args, "lease_ttl_seconds", gov.DEFAULT_LEASE_TTL_SECONDS) or gov.DEFAULT_LEASE_TTL_SECONDS),
                source_probe_workers=getattr(args, "source_probe_workers", None),
                candidate_probe_workers=getattr(args, "candidate_probe_workers", None),
                reprobe_lane=str(getattr(args, "reprobe_lane", "") or "").strip() or None,
                reprobe_shard=str(getattr(args, "reprobe_shard", "") or "").strip() or None,
            )
        elif args.mode == "export-snapshot":
            result = run_export_snapshot(client)
        elif args.mode == "generate-registry":
            result = run_generate_registry(
                client,
                loaded["sources"],
                candidate_models=candidate_models,
            )
        else:
            result = {
                "mode": "full_rehearsal",
                "inventory": run_inventory(
                    client,
                    loaded["sources"],
                    candidate_models=candidate_models,
                    reasoning_effort=args.reasoning_effort,
                ),
                "note": "no live mutations in full rehearsal",
            }
    finally:
        client.close()

    output = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    if args.out:
        Path(args.out).write_text(output, encoding="utf-8")
    # Always write registry file when in generate-registry mode
    if args.mode == "generate-registry" and "registry" in result:
        registry_path = _registry_path(args.registry_file) or sync_mod.DEFAULT_CHANNEL_REGISTRY_PATH
        registry_path.write_text(json.dumps(result["registry"], ensure_ascii=False, indent=2), encoding="utf-8")
    print(output)
    return 0 if not blocked_errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
