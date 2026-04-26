from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_BASE_URL = "http://192.168.232.141:3000"
DEFAULT_GATEWAY_PROVIDER_NAME = "newapi-192.168.232.141-3000"
DEFAULT_TOKEN_NAME = "codex-relay-xhigh"
DEFAULT_GATEWAY_PROVIDER_SHARDS = "ro-a,ro-b,ro-c,ro-d"
DEFAULT_REASONING_EFFORT = "xhigh"
DEFAULT_CANDIDATE_MODELS = ["gpt-5.4", "gpt-5.3-codex"]


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = str(os.environ.get(name, "")).strip()
        if value:
            return value
    return default


def _env_int(*names: str, default: int, minimum: int = 0) -> int:
    raw = _env_first(*names, default="")
    try:
        return max(minimum, int(raw))
    except (TypeError, ValueError):
        return default


DEFAULT_SOURCE_PROBE_WORKERS = _env_int("NEW_API_SOURCE_PROBE_MAX_WORKERS", default=4, minimum=1)
DEFAULT_CANDIDATE_PROBE_WORKERS = _env_int("NEW_API_CANDIDATE_PROBE_MAX_WORKERS", default=4, minimum=1)
DEFAULT_LEASE_TTL_SECONDS = _env_int("NEW_API_GOVERNANCE_LEASE_TTL_SECONDS", default=1800, minimum=60)
DEFAULT_PROVISION_RETRIES = _env_int("NEW_API_RELAY_PROVISION_RETRIES", default=1, minimum=0)
DEFAULT_GOVERN_RETRIES = _env_int("NEW_API_RELAY_GOVERN_RETRIES", default=2, minimum=0)
DEFAULT_RETRY_DELAY_SECONDS = _env_int("NEW_API_RELAY_RETRY_DELAY_SECONDS", default=5, minimum=0)


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        clean = str(item or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        result.append(clean)
    return result


def _resolve_path(raw: str | None, *, fallback: Path) -> Path:
    text = str(raw or "").strip()
    return Path(text).resolve() if text else fallback.resolve()


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_truth(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = _load_json(path)
    return payload if isinstance(payload, dict) else {}


def _candidate_models(cli_models: list[str], truth_path: Path) -> list[str]:
    if cli_models:
        return _dedupe(cli_models)
    truth = _load_truth(truth_path)
    models = truth.get("candidate_models")
    if isinstance(models, list):
        return _dedupe([str(item or "") for item in models]) or list(DEFAULT_CANDIDATE_MODELS)
    return list(DEFAULT_CANDIDATE_MODELS)


def _redact_command(command: list[str], password: str) -> list[str]:
    if not password:
        return list(command)
    return ["***" if item == password else item for item in command]


def _sanitize_output_payload(payload: dict[str, Any]) -> dict[str, Any]:
    clean = json.loads(json.dumps(payload, ensure_ascii=False))
    token = clean.get("token")
    if isinstance(token, dict):
        token.pop("full_key", None)
    return clean


def _summarize_output(step_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    if step_name == "provision":
        token = payload.get("token") if isinstance(payload.get("token"), dict) else {}
        shards = payload.get("gateway_provider_shards") if isinstance(payload.get("gateway_provider_shards"), dict) else {}
        return {
            "login": payload.get("login"),
            "token": {
                "id": token.get("id"),
                "name": token.get("name"),
                "group": token.get("group"),
            },
            "token_truth_drift": payload.get("token_truth_drift"),
            "gateway_provider": payload.get("gateway_provider"),
            "gateway_provider_shards": {
                "base_provider_name": shards.get("base_provider_name"),
                "suffixes": shards.get("suffixes"),
                "provider_dirs": shards.get("provider_dirs"),
                "tokens": shards.get("tokens"),
            },
        }

    inventory = payload.get("inventory") if isinstance(payload.get("inventory"), dict) else {}
    reconcile = payload.get("reconcile") if isinstance(payload.get("reconcile"), dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return {
        "mode": payload.get("mode"),
        "lease_id": payload.get("lease_id"),
        "fencing_token": payload.get("fencing_token"),
        "inventory": {
            "probed": inventory.get("probed"),
            "healthy": inventory.get("healthy"),
            "summary": inventory.get("summary"),
        },
        "reconcile": {
            "activated": len(reconcile.get("activated_channels") or []),
            "quarantined": len(reconcile.get("quarantined_channels") or []),
            "archived": len(reconcile.get("archived_channels") or []),
            "frozen": len(reconcile.get("frozen_channels") or []),
        },
        "summary": summary,
    }


@dataclass
class StepPlan:
    name: str
    command: list[str]
    output_path: Path
    retries: int = 0
    retry_delay_seconds: int = 0


def build_step_plans(
    *,
    repo_root: Path,
    base_url: str,
    username: str,
    password: str,
    providers_root: Path,
    truth_file: Path,
    registry_file: Path,
    token_name: str,
    gateway_provider_name: str,
    gateway_provider_shards: str,
    reasoning_effort: str,
    candidate_models: list[str],
    provision_output: Path,
    govern_output: Path,
    allow_token_fork: bool,
    skip_provision: bool,
    skip_govern: bool,
    lease_ttl_seconds: int,
    source_probe_workers: int,
    candidate_probe_workers: int,
    provision_retries: int,
    govern_retries: int,
    retry_delay_seconds: int,
) -> list[StepPlan]:
    python_exe = Path(sys.executable).resolve()
    sync_script = repo_root / "ai-api" / "codex" / "sync_newapi_channels.py"
    governor_script = repo_root / "ai-api" / "codex" / "channel_governor.py"
    steps: list[StepPlan] = []

    if not skip_provision:
        command = [
            str(python_exe),
            str(sync_script),
            "--base-url",
            base_url,
            "--username",
            username,
            "--password",
            password,
            "--providers-root",
            str(providers_root),
            "--token-name",
            token_name,
            "--reasoning-effort",
            reasoning_effort,
            "--truth-file",
            str(truth_file),
            "--provision-gateway-only",
            "--write-gateway-provider-dir",
            "--write-sharded-gateway-provider-dirs",
            "--gateway-provider-name",
            gateway_provider_name,
            "--gateway-provider-shards",
            gateway_provider_shards,
            "--out",
            str(provision_output),
        ]
        for model_name in candidate_models:
            command.extend(["--candidate-model", model_name])
        if allow_token_fork:
            command.append("--allow-token-fork")
        steps.append(
            StepPlan(
                name="provision",
                command=command,
                output_path=provision_output,
                retries=max(0, int(provision_retries)),
                retry_delay_seconds=max(0, int(retry_delay_seconds)),
            )
        )

    if not skip_govern:
        command = [
            str(python_exe),
            str(governor_script),
            "--mode",
            "govern",
            "--base-url",
            base_url,
            "--username",
            username,
            "--password",
            password,
            "--providers-root",
            str(providers_root),
            "--truth-file",
            str(truth_file),
            "--registry-file",
            str(registry_file),
            "--reasoning-effort",
            reasoning_effort,
            "--lease-ttl-seconds",
            str(max(60, int(lease_ttl_seconds))),
            "--source-probe-workers",
            str(max(1, int(source_probe_workers))),
            "--candidate-probe-workers",
            str(max(1, int(candidate_probe_workers))),
            "--out",
            str(govern_output),
        ]
        for model_name in candidate_models:
            command.extend(["--candidate-model", model_name])
        steps.append(
            StepPlan(
                name="govern",
                command=command,
                output_path=govern_output,
                retries=max(0, int(govern_retries)),
                retry_delay_seconds=max(0, int(retry_delay_seconds)),
            )
        )

    return steps


def run_step(step: StepPlan, *, cwd: Path, password: str) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    completed: subprocess.CompletedProcess[str] | None = None
    max_attempts = max(1, int(step.retries) + 1)
    for attempt_index in range(max_attempts):
        completed = subprocess.run(
            step.command,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
        )
        attempts.append(
            {
                "attempt": attempt_index + 1,
                "returncode": completed.returncode,
                "stdout_tail": completed.stdout[-4000:],
                "stderr_tail": completed.stderr[-4000:],
            }
        )
        if completed.returncode == 0:
            break
        if attempt_index + 1 < max_attempts and step.retry_delay_seconds > 0:
            time.sleep(step.retry_delay_seconds * (attempt_index + 1))

    assert completed is not None
    return {
        "name": step.name,
        "returncode": completed.returncode,
        "command": _redact_command(step.command, password),
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
        "attempt_count": len(attempts),
        "retried": len(attempts) > 1,
        "attempts": attempts,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provision gateway tokens and govern NewAPI channels.")
    parser.add_argument("--repo-root", default=str(_default_repo_root()))
    parser.add_argument("--base-url", default=_env_first("NEW_API_BASE_URL", default=DEFAULT_BASE_URL))
    parser.add_argument(
        "--username",
        default=_env_first("NEW_API_ADMIN_USERNAME", "NEWAPI_ADMIN_USERNAME", default="naadmin"),
    )
    parser.add_argument(
        "--password",
        default=_env_first("NEW_API_ADMIN_PASSWORD", "NEWAPI_ADMIN_PASSWORD", default=""),
    )
    parser.add_argument("--providers-root", default="")
    parser.add_argument("--truth-file", default="")
    parser.add_argument("--registry-file", default="")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--summary-out", default="")
    parser.add_argument("--provision-out", default="")
    parser.add_argument("--govern-out", default="")
    parser.add_argument("--token-name", default=DEFAULT_TOKEN_NAME)
    parser.add_argument("--gateway-provider-name", default=DEFAULT_GATEWAY_PROVIDER_NAME)
    parser.add_argument("--gateway-provider-shards", default=DEFAULT_GATEWAY_PROVIDER_SHARDS)
    parser.add_argument("--reasoning-effort", default=DEFAULT_REASONING_EFFORT)
    parser.add_argument("--candidate-model", action="append", default=[])
    parser.add_argument("--source-probe-workers", type=int, default=DEFAULT_SOURCE_PROBE_WORKERS)
    parser.add_argument("--candidate-probe-workers", type=int, default=DEFAULT_CANDIDATE_PROBE_WORKERS)
    parser.add_argument("--lease-ttl-seconds", type=int, default=DEFAULT_LEASE_TTL_SECONDS)
    parser.add_argument("--provision-retries", type=int, default=DEFAULT_PROVISION_RETRIES)
    parser.add_argument("--govern-retries", type=int, default=DEFAULT_GOVERN_RETRIES)
    parser.add_argument("--retry-delay-seconds", type=int, default=DEFAULT_RETRY_DELAY_SECONDS)
    parser.add_argument("--allow-token-fork", action="store_true", default=False)
    parser.add_argument("--skip-provision", action="store_true", default=False)
    parser.add_argument("--skip-govern", action="store_true", default=False)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = _resolve_path(args.repo_root, fallback=_default_repo_root())
    providers_root = _resolve_path(args.providers_root, fallback=repo_root / "ai-api" / "codex")
    truth_file = _resolve_path(args.truth_file, fallback=repo_root / "ai-api" / "codex" / "newapi_live_truth.json")
    registry_file = _resolve_path(args.registry_file, fallback=repo_root / "ai-api" / "codex" / "newapi_channel_registry.json")
    output_dir = _resolve_path(args.output_dir, fallback=repo_root / "output")
    summary_out = _resolve_path(args.summary_out, fallback=output_dir / "newapi_relay_governor_latest.json")
    provision_out = _resolve_path(args.provision_out, fallback=output_dir / "newapi_gateway_provision_latest.json")
    govern_out = _resolve_path(args.govern_out, fallback=output_dir / "newapi_channel_governor_latest.json")
    password = str(args.password or "").strip()
    candidate_models = _candidate_models(list(args.candidate_model), truth_file)
    # Read allow_token_fork from truth file; CLI flag overrides if explicitly set
    truth_allow_token_fork = bool(_load_truth(truth_file).get("allow_token_fork", False))
    effective_allow_token_fork = bool(args.allow_token_fork or truth_allow_token_fork)

    summary_out.parent.mkdir(parents=True, exist_ok=True)
    provision_out.parent.mkdir(parents=True, exist_ok=True)
    govern_out.parent.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base_url": str(args.base_url or "").strip(),
        "username": str(args.username or "").strip(),
        "providers_root": str(providers_root),
        "truth_file": str(truth_file),
        "registry_file": str(registry_file),
        "candidate_models": candidate_models,
        "gateway_provider_name": str(args.gateway_provider_name or "").strip(),
        "gateway_provider_shards": _dedupe(str(args.gateway_provider_shards or "").split(",")),
        "source_probe_workers": max(1, int(args.source_probe_workers or DEFAULT_SOURCE_PROBE_WORKERS)),
        "candidate_probe_workers": max(1, int(args.candidate_probe_workers or DEFAULT_CANDIDATE_PROBE_WORKERS)),
        "lease_ttl_seconds": max(60, int(args.lease_ttl_seconds or DEFAULT_LEASE_TTL_SECONDS)),
        "provision_retries": max(0, int(args.provision_retries or DEFAULT_PROVISION_RETRIES)),
        "govern_retries": max(0, int(args.govern_retries or DEFAULT_GOVERN_RETRIES)),
        "retry_delay_seconds": max(0, int(args.retry_delay_seconds or DEFAULT_RETRY_DELAY_SECONDS)),
        "steps": [],
        "success": False,
    }

    validation_errors: list[str] = []
    if not password:
        validation_errors.append("missing admin password; set NEW_API_ADMIN_PASSWORD or pass --password")
    if not truth_file.exists():
        validation_errors.append(f"truth file not found: {truth_file}")
    if not args.skip_govern and not registry_file.exists():
        validation_errors.append(f"registry file not found: {registry_file}")
    if args.skip_provision and args.skip_govern:
        validation_errors.append("nothing to do: both --skip-provision and --skip-govern were set")

    if validation_errors:
        summary["validation_errors"] = validation_errors
        summary["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        summary_out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return 2

    steps = build_step_plans(
        repo_root=repo_root,
        base_url=str(args.base_url or "").strip(),
        username=str(args.username or "").strip(),
        password=password,
        providers_root=providers_root,
        truth_file=truth_file,
        registry_file=registry_file,
        token_name=str(args.token_name or "").strip() or DEFAULT_TOKEN_NAME,
        gateway_provider_name=str(args.gateway_provider_name or "").strip() or DEFAULT_GATEWAY_PROVIDER_NAME,
        gateway_provider_shards=str(args.gateway_provider_shards or "").strip() or DEFAULT_GATEWAY_PROVIDER_SHARDS,
        reasoning_effort=str(args.reasoning_effort or "").strip() or DEFAULT_REASONING_EFFORT,
        candidate_models=candidate_models,
        provision_output=provision_out,
        govern_output=govern_out,
        allow_token_fork=effective_allow_token_fork,
        skip_provision=bool(args.skip_provision),
        skip_govern=bool(args.skip_govern),
        lease_ttl_seconds=max(60, int(args.lease_ttl_seconds or DEFAULT_LEASE_TTL_SECONDS)),
        source_probe_workers=max(1, int(args.source_probe_workers or DEFAULT_SOURCE_PROBE_WORKERS)),
        candidate_probe_workers=max(1, int(args.candidate_probe_workers or DEFAULT_CANDIDATE_PROBE_WORKERS)),
        provision_retries=max(0, int(args.provision_retries or DEFAULT_PROVISION_RETRIES)),
        govern_retries=max(0, int(args.govern_retries or DEFAULT_GOVERN_RETRIES)),
        retry_delay_seconds=max(0, int(args.retry_delay_seconds or DEFAULT_RETRY_DELAY_SECONDS)),
    )

    first_failure = 0
    for step in steps:
        result = run_step(step, cwd=repo_root, password=password)
        result["output_path"] = str(step.output_path)
        if step.output_path.exists():
            try:
                payload = _load_json(step.output_path)
                if isinstance(payload, dict):
                    sanitized = _sanitize_output_payload(payload)
                    if sanitized != payload:
                        step.output_path.write_text(
                            json.dumps(sanitized, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                    result["output_summary"] = _summarize_output(step.name, sanitized)
            except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
                result["output_summary_error"] = str(exc)
        summary["steps"].append(result)
        if result["returncode"] != 0 and first_failure == 0:
            first_failure = int(result["returncode"] or 1)
            break

    summary["success"] = first_failure == 0
    if first_failure:
        summary["failed_step"] = summary["steps"][-1]["name"]
    summary["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    summary_out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # Always attempt ubuntu.txt refresh (even on partial failure, best-effort)
    ubuntu_txt_path = _resolve_path("", fallback=repo_root / "LiteLLM" / "ubuntu.txt")
    _try_refresh_ubuntu_txt(
        repo_root=repo_root,
        ubuntu_txt_path=ubuntu_txt_path,
        provision_out=provision_out,
        govern_out=govern_out,
        base_url=str(args.base_url or "").strip(),
    )

    return first_failure


def _try_refresh_ubuntu_txt(
    *,
    repo_root: Path,
    ubuntu_txt_path: Path,
    provision_out: Path,
    govern_out: Path,
    base_url: str,
) -> None:
    """Refresh LiteLLM/ubuntu.txt from latest governance outputs.

    This is best-effort; failures are logged to stderr but never block the run.
    The update preserves the static header/footer sections of the file and only
    replaces the dynamic relay-token table, channel table, and key store section.
    """
    try:
        _do_refresh_ubuntu_txt(
            repo_root=repo_root,
            ubuntu_txt_path=ubuntu_txt_path,
            provision_out=provision_out,
            govern_out=govern_out,
            base_url=base_url,
        )
    except Exception as exc:
        print(f"[ubuntu.txt refresh] WARNING: {exc}", file=sys.stderr)


def _do_refresh_ubuntu_txt(
    *,
    repo_root: Path,
    ubuntu_txt_path: Path,
    provision_out: Path,
    govern_out: Path,
    base_url: str,
) -> None:
    """Core logic for ubuntu.txt refresh."""
    now_str = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())

    # Load provision output for relay token info
    provision_data: dict[str, Any] = {}
    if provision_out.exists():
        try:
            provision_data = json.loads(provision_out.read_text(encoding="utf-8"))
        except Exception:
            pass

    # Load govern output for channel health info
    govern_data: dict[str, Any] = {}
    if govern_out.exists():
        try:
            govern_data = json.loads(govern_out.read_text(encoding="utf-8"))
        except Exception:
            pass

    # Load local token key store
    key_store_path = repo_root / "ai-api" / "codex" / ".token_key_store.json"
    key_store: dict[str, str] = {}
    if key_store_path.exists():
        try:
            key_store = json.loads(key_store_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    # Read current ubuntu.txt content
    current_text = ""
    if ubuntu_txt_path.exists():
        current_text = ubuntu_txt_path.read_text(encoding="utf-8")

    # Build the dynamic section
    dynamic_lines: list[str] = []
    dynamic_lines.append(f"=== 最后自动更新 (Last Auto-Updated) ===")
    dynamic_lines.append(f"  {now_str}")
    dynamic_lines.append("")

    # Token key store section
    if key_store:
        dynamic_lines.append("=== 网关 Token 密钥存储 (已固定，不再自动变更) ===")
        dynamic_lines.append("")
        for token_name, token_key in sorted(key_store.items()):
            dynamic_lines.append(f"  {token_name:<30}  key={token_key}")
        dynamic_lines.append("")

    # Active channels from govern output
    govern_reconcile = govern_data.get("reconcile") if isinstance(govern_data.get("reconcile"), dict) else {}
    activated = govern_reconcile.get("activated_channels") or []
    quarantined = govern_reconcile.get("quarantined_channels") or []
    if activated or quarantined:
        dynamic_lines.append("=== 最近治理结果 (Latest Governance Run) ===")
        dynamic_lines.append("")
        if activated:
            dynamic_lines.append(f"  已激活渠道 ({len(activated)} 个):")
            for ch in activated:
                ch_name = ch.get("name") or ch.get("id") or "?"
                dynamic_lines.append(f"    + {ch_name}")
        if quarantined:
            dynamic_lines.append(f"  已隔离渠道 ({len(quarantined)} 个):")
            for ch in quarantined:
                ch_name = ch.get("name") or ch.get("id") or "?"
                logs = ch.get("blocked_by_logs") or []
                suffix = f"  [{', '.join(logs)}]" if logs else ""
                dynamic_lines.append(f"    - {ch_name}{suffix}")
        dynamic_lines.append("")

    dynamic_section = "\n".join(dynamic_lines)

    # Replace the dynamic section if it already exists, otherwise append
    marker_start = "=== 最后自动更新 (Last Auto-Updated) ==="
    marker_end = "=== END AUTO-UPDATED ==="

    if marker_start in current_text:
        # Find the end marker; if not present, replace to end
        if marker_end in current_text:
            pattern = re.compile(
                re.escape(marker_start) + r".*?" + re.escape(marker_end),
                re.DOTALL,
            )
            new_text = pattern.sub(dynamic_section + "\n" + marker_end, current_text)
        else:
            idx = current_text.index(marker_start)
            new_text = current_text[:idx] + dynamic_section + "\n" + marker_end + "\n"
    else:
        # Append at the end
        new_text = current_text.rstrip("\n") + "\n\n" + dynamic_section + "\n" + marker_end + "\n"

    ubuntu_txt_path.parent.mkdir(parents=True, exist_ok=True)
    ubuntu_txt_path.write_text(new_text, encoding="utf-8")
    print(f"[ubuntu.txt refresh] Updated: {ubuntu_txt_path}")


if __name__ == "__main__":
    raise SystemExit(main())


