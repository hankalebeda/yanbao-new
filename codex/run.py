"""Unified Codex task runner.

Loads task profiles from codex/profiles/*.yaml, builds a minimal structural
context overlay, and delegates execution to the mesh engine. This replaces
the three separate runners (hourly, manual, mining) with one generic entrypoint.

Design principle: pass the AI structural boundaries (depth, scopes, topology)
but never tactical instructions. Let the AI decide *how* to accomplish goals.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

from codex import mesh as codex_mesh


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def profiles_dir() -> Path:
    return Path(__file__).resolve().parent / "profiles"


def _runtime_dir(root: Path, profile_name: str) -> Path:
    return root / "runtime" / f"codex_{profile_name}"


def _runs_dir(root: Path, profile_name: str) -> Path:
    return _runtime_dir(root, profile_name) / "runs"


def _state_file(root: Path, profile_name: str) -> Path:
    return _runtime_dir(root, profile_name) / "state.json"


def _lock_file(root: Path, profile_name: str) -> Path:
    return _runtime_dir(root, profile_name) / "lock.json"


# ---------------------------------------------------------------------------
# JSON helpers (reuse pattern from mesh.py)
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Profile loading
# ---------------------------------------------------------------------------

def load_profile(name_or_path: str) -> dict[str, Any]:
    """Load a task profile by name (from codex/profiles/) or by file path."""
    if yaml is None:
        raise ImportError("PyYAML is required for profile loading. Install with: pip install pyyaml")

    path = Path(name_or_path)
    if not path.exists():
        # Try as a profile name
        candidate = profiles_dir() / f"{name_or_path}.yaml"
        if candidate.exists():
            path = candidate
        else:
            candidate = profiles_dir() / f"{name_or_path}.yml"
            if candidate.exists():
                path = candidate
            else:
                raise FileNotFoundError(f"Profile not found: {name_or_path} (searched {profiles_dir()})")

    return yaml.safe_load(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Prompt loading
# ---------------------------------------------------------------------------

def extract_fenced_text(text: str, heading: str) -> str:
    """Extract text from a ```text code fence under a heading."""
    heading_idx = text.find(heading)
    if heading_idx < 0:
        raise ValueError(f"heading not found: {heading}")
    block_start = text.find("```text", heading_idx)
    if block_start < 0:
        raise ValueError(f"text code block not found after heading: {heading}")
    content_start = block_start + len("```text")
    block_end = text.find("```", content_start)
    if block_end < 0:
        raise ValueError(f"code fence not closed after heading: {heading}")
    return text[content_start:block_end].strip()


def load_prompt(profile: dict[str, Any], root: Path) -> str:
    """Load prompt text according to the profile's prompt_source config."""
    source = profile.get("prompt_source", {})

    # Direct text override
    if source.get("text"):
        return str(source["text"]).strip()

    # Builder function (for special cases like mining)
    if source.get("builder"):
        return ""  # Builder prompts are handled by the caller

    # File-based prompt loading
    file_path = source.get("file")
    if not file_path:
        raise ValueError("prompt_source must have 'file', 'text', or 'builder'")

    full_path = root / file_path
    if not full_path.exists():
        raise FileNotFoundError(f"Prompt source file not found: {full_path}")

    text = full_path.read_text(encoding="utf-8")
    fmt = source.get("format", "raw")

    if fmt == "fenced_text":
        heading = source.get("heading", "")
        return extract_fenced_text(text, heading)
    elif fmt == "section":
        heading = source.get("heading", "")
        heading_idx = text.find(heading)
        if heading_idx < 0:
            raise ValueError(f"heading not found: {heading}")
        next_heading = text.find("\n## ", heading_idx + len(heading))
        section = text[heading_idx : next_heading if next_heading != -1 else len(text)]
        return section.strip()
    else:
        return text.strip()


# ---------------------------------------------------------------------------
# Context overlay -- structural only, no tactical instructions
# ---------------------------------------------------------------------------

def build_context_overlay(
    *,
    run_id: str,
    task_id: str,
    goal: str,
    depth: int,
    max_depth: int,
    execution_root: Path,
    provider_order: list[str],
    read_scope: list[str],
    write_scope: list[str],
    subagents_enabled: bool = True,
    parent_task_id: str | None = None,
    lineage_id: str | None = None,
    runtime_status: str | None = None,
) -> str:
    """Minimal structural context. No tactical instructions.

    The AI decides *how* to use subagents, what to parallelize, etc.
    We only tell it the structural boundaries it must respect.
    """
    lines = [
        "--- Orchestrator Context ---",
        f"run_id: {run_id}",
        f"task_id: {task_id}",
        f"goal: {goal}",
        f"depth: {depth}/{max_depth}",
        f"execution_root: {execution_root}",
        f"providers: {', '.join(provider_order)}",
        f"read_scope: {json.dumps(read_scope, ensure_ascii=False)}",
        f"write_scope: {json.dumps(write_scope, ensure_ascii=False)}",
        f"subagents: {'enabled' if subagents_enabled else 'disabled'}",
        f"nested_codex: {'allowed' if depth < max_depth else 'not_allowed'}",
    ]
    if parent_task_id:
        lines.append(f"parent_task_id: {parent_task_id}")
    if lineage_id:
        lines.append(f"lineage_id: {lineage_id}")
    if runtime_status:
        lines.append(f"runtime_status: {runtime_status}")
    lines.extend(["---", ""])
    return "\n".join(lines)


def profile_native_subagent_settings(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "allow_native_subagents": bool(profile.get("allow_native_subagents", True)),
        "allow_native_subagents_at_external_limit": bool(
            profile.get(
                "allow_native_subagents_at_external_limit",
                codex_mesh.DEFAULT_ALLOW_NATIVE_SUBAGENTS_AT_EXTERNAL_LIMIT,
            )
        ),
        "inner_agent_max_depth": max(
            1,
            int(profile.get("inner_agent_max_depth", codex_mesh.DEFAULT_INNER_AGENT_MAX_DEPTH)),
        ),
        "inner_agent_max_threads": (
            None
            if profile.get("inner_agent_max_threads") is None
            else max(
                1,
                int(profile.get("inner_agent_max_threads", codex_mesh.DEFAULT_INNER_AGENT_MAX_THREADS)),
            )
        ),
    }


# ---------------------------------------------------------------------------
# Automation lock (moved from hourly.py for shared use)
# ---------------------------------------------------------------------------

class LockBusyError(RuntimeError):
    pass


class AutomationLock:
    """File-based lock with stale detection."""

    def __init__(self, path: Path, stale_seconds: int = 70 * 60) -> None:
        self.path = path
        self.stale_seconds = stale_seconds
        self.acquired = False

    def __enter__(self) -> "AutomationLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            payload = _load_json(self.path)
            created_at = str(payload.get("created_at") or "")
            if created_at:
                try:
                    created_ts = datetime.fromisoformat(created_at).timestamp()
                except ValueError:
                    created_ts = 0.0
                if created_ts and (time.time() - created_ts) > self.stale_seconds:
                    self.path.unlink(missing_ok=True)
            if self.path.exists():
                raise LockBusyError(f"lock file already exists: {self.path}")
        fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        payload = {
            "pid": os.getpid(),
            "created_at": datetime.now().astimezone().isoformat(),
        }
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        self.acquired = True
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.acquired:
            self.path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Runtime health check and startup
# ---------------------------------------------------------------------------

def _health_probe(base_url: str, timeout_seconds: int = 8) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/health"
    request = urllib.request.Request(url, headers={"User-Agent": "codex-runner/2.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            status_code = getattr(response, "status", 200)
    except urllib.error.HTTPError as exc:
        return {"ok": False, "status_code": exc.code, "reason": f"http_error:{exc.code}", "url": url}
    except Exception as exc:
        return {"ok": False, "status_code": None, "reason": f"request_failed:{exc}", "url": url}

    parsed: Any = None
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        parsed = body

    ok = False
    if isinstance(parsed, dict):
        if parsed.get("success") is True or parsed.get("status") == "ok":
            ok = True
        elif isinstance(parsed.get("data"), dict) and parsed["data"].get("status") == "ok":
            ok = True
    return {"ok": ok and status_code == 200, "status_code": status_code, "body": parsed, "url": url}


def ensure_runtime(
    *,
    root: Path,
    base_url: str = "http://127.0.0.1:8000",
    output_dir: Path,
    startup_timeout_seconds: int = 90,
) -> dict[str, Any]:
    """Ensure the application runtime is reachable, starting it if needed."""
    probe = _health_probe(base_url)
    if probe.get("ok"):
        return {"status": "healthy", "health": probe, "started_process": False}

    host, port = "127.0.0.1", 8000
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return {"status": "port_busy_health_failed", "health": probe, "started_process": False}
    except OSError:
        pass

    # Try to start the runtime
    venv_python = root / ".venv" / "Scripts" / "python.exe"
    python_cmd = str(venv_python) if venv_python.exists() else sys.executable
    stdout_path = output_dir / "runtime_stdout.log"
    stderr_path = output_dir / "runtime_stderr.log"
    stdout_path.parent.mkdir(parents=True, exist_ok=True)

    command = [python_cmd, "-m", "uvicorn", "app.main:app", "--host", host, "--port", str(port)]
    creationflags = 0
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "DETACHED_PROCESS", 0)

    stdout_handle = open(stdout_path, "a", encoding="utf-8")
    stderr_handle = open(stderr_path, "a", encoding="utf-8")
    try:
        process = subprocess.Popen(
            command, cwd=str(root), stdout=stdout_handle, stderr=stderr_handle, creationflags=creationflags,
        )
        deadline = time.time() + startup_timeout_seconds
        while time.time() < deadline:
            time.sleep(2)
            probe = _health_probe(base_url)
            if probe.get("ok"):
                return {"status": "started_by_wrapper", "health": probe, "started_process": True, "pid": process.pid}
        return {"status": "startup_timeout", "health": probe, "started_process": True, "pid": process.pid}
    finally:
        stdout_handle.close()
        stderr_handle.close()


# ---------------------------------------------------------------------------
# Provider rotation (simple, reusable)
# ---------------------------------------------------------------------------

def rotate_providers(providers: list[str], state_file: Path) -> list[str]:
    """Round-robin provider rotation with persistent state."""
    providers = [p.strip() for p in providers if p.strip()]
    if not providers:
        raise ValueError("providers must not be empty")
    state = _load_json(state_file)
    last_start = int(state.get("last_start_index", -1))
    start = (last_start + 1) % len(providers)
    ordered = providers[start:] + providers[:start]
    _write_json(state_file, {
        "last_start_index": start,
        "last_provider_order": ordered,
        "updated_at": datetime.now().astimezone().isoformat(),
    })
    return ordered


def order_providers_for_run(
    providers: list[str],
    state_file: Path,
    *,
    preferred_start: str | None = None,
) -> list[str]:
    providers = [p.strip() for p in providers if p.strip()]
    if not providers:
        raise ValueError("providers must not be empty")
    start = str(preferred_start or "").strip()
    if not start:
        return rotate_providers(providers, state_file)
    if start not in providers:
        start = providers[0]
    start_idx = providers.index(start)
    ordered = providers[start_idx:] + providers[:start_idx]
    _write_json(state_file, {
        "last_start_index": start_idx,
        "last_provider_order": ordered,
        "updated_at": datetime.now().astimezone().isoformat(),
    })
    return ordered


# ---------------------------------------------------------------------------
# Execute: the core workflow
# ---------------------------------------------------------------------------

def execute(
    profile: dict[str, Any],
    *,
    root: Path | None = None,
    prompt_override: str | None = None,
    providers: list[str] | None = None,
    delegate_mode: str = "mesh",
    max_workers: int = codex_mesh.DEFAULT_MAX_WORKERS,
    max_depth: int = codex_mesh.DEFAULT_MAX_EXTERNAL_DEPTH,
    hedge_delay_seconds: int = codex_mesh.DEFAULT_HEDGE_DELAY_SECONDS,
    disable_providers: list[str] | None = None,
    preferred_start: str | None = None,
    base_url: str = "http://127.0.0.1:8000",
    dry_run: bool = False,
    benchmark_label: str | None = None,
    include_overlay: bool = True,
    dangerously_bypass: bool = True,
    sandbox: str = "danger-full-access",
    ephemeral: bool = True,
) -> dict[str, Any]:
    """Execute a task defined by a profile config."""
    root = (root or repo_root()).resolve()
    profile_name = profile.get("task_id_prefix", "task")
    native_subagent_settings = profile_native_subagent_settings(profile)
    try:
        external_context = codex_mesh.resolve_external_execution_context(max_depth)
    except codex_mesh.ExternalDepthLimitError as exc:
        return {
            "success": False,
            "error": "external_depth_limit",
            "message": str(exc),
            "profile": profile_name,
        }
    disable_list = disable_providers or list(codex_mesh.DEFAULT_PROVIDER_DENYLIST)
    providers = list(providers) if providers else codex_mesh.resolve_provider_allowlist(
        root,
        denylist=disable_list,
    )

    # Setup output directory
    started_at = datetime.now().astimezone().isoformat()
    run_ts = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S")
    output_dir = _runs_dir(root, profile_name) / run_ts
    output_dir.mkdir(parents=True, exist_ok=True)

    # Optionally acquire lock
    lock = None
    if profile.get("lock"):
        lock = AutomationLock(_lock_file(root, profile_name))
        lock.__enter__()

    try:
        # Optionally ensure runtime
        runtime_preflight: dict[str, Any] = {"status": "skipped"}
        if profile.get("ensure_runtime"):
            runtime_preflight = ensure_runtime(
                root=root, base_url=base_url, output_dir=output_dir,
            )

        # Rotate providers
        ordered_providers = order_providers_for_run(
            providers,
            _state_file(root, profile_name),
            preferred_start=preferred_start,
        )

        # Load prompt
        if prompt_override:
            prompt = prompt_override.strip()
        else:
            prompt = load_prompt(profile, root)
            if not prompt:
                raise ValueError(f"No prompt could be loaded for profile: {profile_name}")

        # Build overlay and compose final prompt
        overlay = ""
        if include_overlay:
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
            overlay = build_context_overlay(
                run_id=run_ts,
                task_id=f"{profile_name}-{run_ts}",
                goal=profile.get("goal", ""),
                depth=external_context.depth,
                max_depth=external_context.max_external_depth,
                execution_root=root,
                provider_order=ordered_providers,
                read_scope=profile.get("read_scope", []),
                write_scope=profile.get("write_scope", []),
                subagents_enabled=bool(inner_codex["enable_multi_agent"]),
                parent_task_id=external_context.parent_task_id,
                lineage_id=external_context.lineage_id,
                runtime_status=runtime_preflight.get("status"),
            )
        final_prompt = overlay + prompt + "\n"
        (output_dir / "prompt.txt").write_text(final_prompt, encoding="utf-8")

        if dry_run:
            payload = {
                "run_id": run_ts,
                "success": True,
                "dry_run": True,
                "profile": profile_name,
                "providers": ordered_providers,
                "output_dir": str(output_dir),
                "runtime_preflight": runtime_preflight,
            }
            _write_json(output_dir / "summary.json", payload)
            return payload

        # Build manifest and execute
        task_id = f"{profile_name}-{run_ts}"
        manifest = codex_mesh.MeshRunManifest(
            tasks=[
                codex_mesh.MeshTaskManifest(
                    task_id=task_id,
                    goal=profile.get("goal", ""),
                    prompt=final_prompt,
                    task_kind=profile.get("task_kind", "mixed"),
                    read_scope=profile.get("read_scope", []),
                    write_scope=profile.get("write_scope", []),
                    max_external_depth=external_context.max_external_depth,
                    allow_native_subagents=bool(native_subagent_settings["allow_native_subagents"]),
                    allow_native_subagents_at_external_limit=bool(
                        native_subagent_settings["allow_native_subagents_at_external_limit"]
                    ),
                    inner_agent_max_depth=int(native_subagent_settings["inner_agent_max_depth"]),
                    inner_agent_max_threads=native_subagent_settings["inner_agent_max_threads"],
                    provider_allowlist=list(ordered_providers),
                    provider_denylist=disable_list,
                    timeout_seconds=int(profile.get("timeout_minutes", 50)) * 60,
                    benchmark_label=benchmark_label,
                    output_mode="text",
                    working_root=str(root),
                    parent_task_id=external_context.parent_task_id,
                    lineage_id=external_context.lineage_id or task_id,
                    depth=external_context.depth,
                    hedge_delay_seconds=hedge_delay_seconds,
                )
            ],
            execution_mode=delegate_mode,
            max_workers=max_workers,
            benchmark_label=benchmark_label,
            dangerously_bypass=dangerously_bypass,
            sandbox=sandbox,
            ephemeral=ephemeral,
            provider_allowlist=list(ordered_providers),
            provider_denylist=disable_list,
        )

        mesh_summary = codex_mesh.execute_manifest(root, manifest)
        task_result = mesh_summary.tasks[0]

        payload = {
            "run_id": mesh_summary.run_id,
            "success": mesh_summary.success,
            "profile": profile_name,
            "delegate_mode": delegate_mode,
            "selected_provider": task_result.selected_provider,
            "providers": task_result.provider_order,
            "output_dir": mesh_summary.output_dir,
            "runtime_preflight": runtime_preflight,
            "started_at": started_at,
            "finished_at": mesh_summary.finished_at,
            "attempts": [asdict(a) for a in task_result.attempts],
        }
        _write_json(output_dir / "summary.json", payload)
        return payload

    except LockBusyError:
        return {"success": False, "error": "lock_busy", "profile": profile_name}
    finally:
        if lock is not None:
            lock.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified Codex task runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # execute
    run_parser = subparsers.add_parser("execute", help="Execute a task profile")
    run_parser.add_argument("--profile", required=True, help="Profile name or path")
    run_parser.add_argument("--delegate-mode", choices=["legacy", "mesh"], default="mesh")
    run_parser.add_argument("--providers", nargs="+", default=None)
    run_parser.add_argument("--disable-provider", action="append")
    run_parser.add_argument("--max-workers", type=int, default=codex_mesh.DEFAULT_MAX_WORKERS)
    run_parser.add_argument("--max-depth", type=int, default=codex_mesh.DEFAULT_MAX_EXTERNAL_DEPTH)
    run_parser.add_argument("--hedge-delay-seconds", type=int, default=codex_mesh.DEFAULT_HEDGE_DELAY_SECONDS)
    run_parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    run_parser.add_argument("--prompt-text", default=None)
    run_parser.add_argument("--prompt-file", type=Path, default=None)
    run_parser.add_argument("--benchmark-label", default=None)
    run_parser.add_argument("--dry-run", action="store_true")
    run_parser.add_argument("--json", action="store_true")

    # show-profile
    show_parser = subparsers.add_parser("show-profile", help="Display a resolved profile")
    show_parser.add_argument("--profile", required=True, help="Profile name or path")

    # list-profiles
    subparsers.add_parser("list-profiles", help="List available profiles")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.command == "list-profiles":
        pdir = profiles_dir()
        if pdir.exists():
            for f in sorted(pdir.glob("*.yaml")) + sorted(pdir.glob("*.yml")):
                print(f.stem)
        return 0

    if args.command == "show-profile":
        profile = load_profile(args.profile)
        print(json.dumps(profile, ensure_ascii=False, indent=2))
        return 0

    if args.command == "execute":
        profile = load_profile(args.profile)
        prompt_override = None
        if args.prompt_text:
            prompt_override = args.prompt_text
        elif args.prompt_file:
            prompt_override = args.prompt_file.read_text(encoding="utf-8")

        result = execute(
            profile,
            prompt_override=prompt_override,
            providers=args.providers,
            delegate_mode=args.delegate_mode,
            max_workers=args.max_workers,
            max_depth=args.max_depth,
            hedge_delay_seconds=args.hedge_delay_seconds,
            disable_providers=args.disable_provider,
            base_url=args.base_url,
            dry_run=args.dry_run,
            benchmark_label=args.benchmark_label,
        )
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"run_id={result.get('run_id', 'N/A')}")
            print(f"success={str(result.get('success', False)).lower()}")
            print(f"profile={result.get('profile', 'N/A')}")
            if result.get("selected_provider"):
                print(f"selected_provider={result['selected_provider']}")
            if result.get("output_dir"):
                print(f"output_dir={result['output_dir']}")
        return 0 if result.get("success") else 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
