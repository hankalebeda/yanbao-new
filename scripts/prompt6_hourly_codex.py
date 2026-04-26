#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import textwrap
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from scripts import codex_mesh


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_PROVIDERS = list(codex_mesh.DEFAULT_PROVIDER_ALLOWLIST)
DEFAULT_TIMEOUT_MINUTES = 50
PROMPT6_HEADING = "## Prompt 6：真实验真 + 深度修复循环控制器"


@dataclass
class AttemptResult:
    provider: str
    portable_home: str
    command: list[str]
    returncode: int | None
    success: bool
    duration_seconds: float
    stdout_path: str
    stderr_path: str
    last_message_path: str
    error: str | None = None


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_codex_executable() -> str:
    candidates = [
        shutil.which("codex.exe"),
        shutil.which("codex.cmd"),
        shutil.which("codex"),
    ]
    for candidate in candidates:
        if candidate:
            return candidate
    raise FileNotFoundError(
        "Unable to locate Codex CLI. Expected codex.exe or codex.cmd on PATH."
    )


def default_prompt_doc(root: Path) -> Path:
    matches = sorted((root / "docs").glob("*/18_*.md"))
    if not matches:
        raise FileNotFoundError("Unable to locate docs/*/18_*.md for Prompt 6 source")
    return matches[0]


def state_dir(root: Path) -> Path:
    return root / "runtime" / "prompt6_hourly"


def state_path(root: Path) -> Path:
    return state_dir(root) / "state.json"


def runs_dir(root: Path) -> Path:
    return state_dir(root) / "runs"


def provider_root(root: Path) -> Path:
    return root / "ai-api" / "codex"


def safe_provider_name(provider: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in provider)


def prepare_provider_home(root: Path, provider: str) -> Path:
    source_dir = provider_root(root) / provider
    if not source_dir.exists():
        raise FileNotFoundError(f"Provider directory not found: {source_dir}")
    config_path = source_dir / "config.toml"
    auth_path = source_dir / "auth.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Missing provider config.toml: {config_path}")
    if not auth_path.exists():
        raise FileNotFoundError(f"Missing provider auth.json: {auth_path}")

    portable_home = provider_root(root) / f"portable_{safe_provider_name(provider)}"
    portable_codex = portable_home / ".codex"
    portable_codex.mkdir(parents=True, exist_ok=True)
    shutil.copy2(config_path, portable_codex / "config.toml")
    shutil.copy2(auth_path, portable_codex / "auth.json")
    return portable_home


def provider_env(portable_home: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["HOME"] = str(portable_home)
    env["USERPROFILE"] = str(portable_home)
    return env


def extract_prompt_section(prompt_doc: Path, heading: str = PROMPT6_HEADING) -> str:
    text = prompt_doc.read_text(encoding="utf-8")
    heading_idx = text.find(heading)
    if heading_idx == -1:
        raise ValueError(f"heading not found: {heading}")

    next_heading_idx = text.find("\n## ", heading_idx + len(heading))
    section = text[heading_idx : next_heading_idx if next_heading_idx != -1 else len(text)]

    fence_start = section.find("```text")
    if fence_start == -1:
        raise ValueError(f"missing ```text block under {heading}")
    fence_start += len("```text")
    fence_end = section.find("\n```", fence_start)
    if fence_end == -1:
        raise ValueError(f"missing closing ``` fence under {heading}")
    return section[fence_start:fence_end].strip() + "\n"


def load_state(root: Path) -> dict[str, object]:
    path = state_path(root)
    if not path.exists():
        return {
            "next_start_provider": DEFAULT_PROVIDERS[0],
            "last_success_provider": None,
            "last_run_id": None,
            "last_success_at": None,
            "provider_history": [],
        }
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid state file: {path}: {exc}") from exc


def save_state(root: Path, payload: dict[str, object]) -> None:
    path = state_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def emit_progress(message: str, *, json_mode: bool) -> None:
    stream = sys.stderr if json_mode else sys.stdout
    print(message, file=stream, flush=True)


def provider_order(providers: list[str], state: dict[str, object], preferred_start: str | None = None) -> list[str]:
    if not providers:
        raise ValueError("providers must not be empty")
    start = preferred_start or str(state.get("next_start_provider") or providers[0])
    if start not in providers:
        start = providers[0]
    start_idx = providers.index(start)
    return providers[start_idx:] + providers[:start_idx]


def next_provider_after(providers: list[str], current: str) -> str:
    idx = providers.index(current)
    return providers[(idx + 1) % len(providers)]


def build_prompt(
    *,
    root: Path,
    prompt_doc: Path,
    base_url: str,
    providers: list[str],
    prompt_override: str | None = None,
    include_overlay: bool = True,
) -> str:
    prompt_body = prompt_override if prompt_override is not None else extract_prompt_section(prompt_doc)
    if not include_overlay:
        return prompt_body.strip() + "\n"
    timestamp = datetime.now(timezone.utc).astimezone().isoformat()
    overlay = textwrap.dedent(
        f"""
        【自动化调度上下文】
        - 当前触发方式：Windows 每小时定时执行的新会话；本次时间戳：`{timestamp}`。
        - 当前仓库根目录：`{root}`。
        - 当前 live base URL：`{base_url}`。
        - 中转站冗余顺序：`{" -> ".join(providers)}`。单站失败不是完成条件，必须自动切到下一个 relay 重试。
        - 这是续跑，不是一次性扫描。开始前先读取 `github/automation/live_fix_loop/issue_register.md` 与 `github/automation/live_fix_loop/review_log.md`；若缺失则初始化，若已存在则沿用，不得无故清零既有 open issues。
        - 当前执行器支持持续多轮工具调用，也支持子代理；不要再报告“当前执行器不支持自动多轮闭环”。
        - 必须显式使用子代理功能加速：Round 0 完成后，以及每轮扫描/修复期间，至少并行启动 2 个子代理，一个负责 SSOT / 风险地图定位，另一个负责非阻塞的代码 / 日志 / 测试探索；关键阻塞动作如站点启动、真实浏览器预检、最终修复集成必须本地执行。
        - 若 `127.0.0.1:8000` 预检失败，但你拥有本地 shell 与文件权限，必须先执行一次“运行时自愈”：定位当前仓库正式启动命令，尝试启动当前代码实例到 `127.0.0.1:8000`，记录 PID / 命令 / 时间，再重新执行 Round 0 预检。只有在启动失败、实例仍不可信，或确认运行实例不是当前代码时，才允许按阻塞停止。
        - 若本轮形成新的更稳健方法论，除了更新台账，也要同步回写 `docs/提示词/18_全量自动化提示词.md` 的 Prompt 6。
        - 最终回答必须包含每轮的 `Round N / focus / open_issue_count / new_high_value_issue_count / fixed_count / reopened_issue_count / regression_failures / next_focus`。

        【以下为必须执行的 Prompt 6 正文】
        """
    ).strip()
    return overlay + "\n\n" + prompt_body.strip() + "\n"


def build_attempt_command(
    *,
    root: Path,
    last_message_path: Path,
    enable_multi_agent: bool = True,
    agent_max_depth: int = codex_mesh.DEFAULT_INNER_AGENT_MAX_DEPTH,
    agent_max_threads: int | None = codex_mesh.DEFAULT_INNER_AGENT_MAX_THREADS,
) -> list[str]:
    command = [
        resolve_codex_executable(),
        "exec",
        "--skip-git-repo-check",
    ]
    command.extend(codex_mesh.inner_codex_feature_toggle_args(enable_multi_agent))
    for override in codex_mesh.inner_codex_config_overrides(
        enable_multi_agent,
        agent_max_depth=agent_max_depth,
        agent_max_threads=agent_max_threads,
    ):
        command.extend(["--config", override])
    command.extend([
        "--dangerously-bypass-approvals-and-sandbox",
        "--ephemeral",
        "--color",
        "never",
        "--cd",
        str(root),
        "--output-last-message",
        str(last_message_path),
        "--json",
        "-",
    ])
    return command


def invoke_codex_attempt(
    *,
    command: list[str],
    env: dict[str, str],
    prompt: str,
    stdout_path: Path,
    stderr_path: Path,
    timeout_seconds: int,
) -> tuple[int | None, float, str | None]:
    started = time.perf_counter()
    result_holder: dict[str, subprocess.CompletedProcess[str]] = {}
    error_holder: dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            result_holder["completed"] = subprocess.run(
                command,
                input=prompt,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout_seconds,
                check=False,
                env=env,
            )
        except BaseException as exc:  # pragma: no cover - forwarded below
            error_holder["exc"] = exc

    worker = threading.Thread(target=_runner, daemon=True)
    worker.start()

    last_heartbeat = started
    while worker.is_alive():
        worker.join(timeout=1)
        now = time.perf_counter()
        if worker.is_alive() and now - last_heartbeat >= 30:
            elapsed = int(now - started)
            print(f"[codex] still running... elapsed={elapsed}s", file=sys.stderr, flush=True)
            last_heartbeat = now

    if "exc" in error_holder:
        exc = error_holder["exc"]
        if isinstance(exc, subprocess.TimeoutExpired):
            stdout_path.write_text((exc.stdout or "") if isinstance(exc.stdout, str) else "", encoding="utf-8")
            stderr_path.write_text((exc.stderr or "") if isinstance(exc.stderr, str) else "", encoding="utf-8")
            return None, time.perf_counter() - started, f"timeout_after_{timeout_seconds}s"
        raise exc

    completed = result_holder["completed"]
    stdout_path.write_text(completed.stdout or "", encoding="utf-8")
    stderr_path.write_text(completed.stderr or "", encoding="utf-8")
    return completed.returncode, time.perf_counter() - started, None


def run_once(
    *,
    root: Path,
    prompt_doc: Path,
    base_url: str,
    providers: list[str],
    timeout_minutes: int,
    max_external_depth: int = codex_mesh.DEFAULT_MAX_EXTERNAL_DEPTH,
    prompt_override: str | None = None,
    preferred_start: str | None = None,
    dry_run: bool = False,
    include_overlay: bool = True,
    json_mode: bool = False,
) -> dict[str, object]:
    state = load_state(root)
    ordered_providers = provider_order(providers, state, preferred_start=preferred_start)
    run_id = datetime.now(timezone.utc).astimezone().strftime("%Y%m%dT%H%M%S")
    run_dir = runs_dir(root) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    task_id = f"prompt6-run-once-{run_id}"
    try:
        external_context = codex_mesh.resolve_external_execution_context(max_external_depth)
    except codex_mesh.ExternalDepthLimitError as exc:
        payload = {
            "run_id": run_id,
            "base_url": base_url,
            "providers": ordered_providers,
            "prompt_doc": str(prompt_doc),
            "prompt_path": str(run_dir / "prompt.txt"),
            "run_dir": str(run_dir),
            "success": False,
            "error": "external_depth_limit",
            "message": str(exc),
            "attempts": [],
            "next_start_provider": state.get("next_start_provider"),
        }
        (run_dir / "result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        emit_progress(f"[prompt6] blocked error=external_depth_limit detail={exc}", json_mode=json_mode)
        return payload
    emit_progress(
        f"[prompt6] run_id={run_id} base_url={base_url} providers={','.join(ordered_providers)}",
        json_mode=json_mode,
    )
    emit_progress(
        f"[prompt6] prompt_doc={prompt_doc} run_dir={run_dir}",
        json_mode=json_mode,
    )

    prompt = build_prompt(
        root=root,
        prompt_doc=prompt_doc,
        base_url=base_url,
        providers=ordered_providers,
        prompt_override=prompt_override,
        include_overlay=include_overlay,
    )
    (run_dir / "prompt.txt").write_text(prompt, encoding="utf-8")

    attempts: list[AttemptResult] = []
    timeout_seconds = max(60, int(timeout_minutes * 60))
    inner_codex = codex_mesh.resolve_inner_codex_options(
        allow_native_subagents=True,
        depth=external_context.depth,
        max_external_depth=external_context.max_external_depth,
    )

    for provider in ordered_providers:
        emit_progress(f"[prompt6] starting provider={provider}", json_mode=json_mode)
        stdout_path = run_dir / f"{provider}.stdout.jsonl"
        stderr_path = run_dir / f"{provider}.stderr.log"
        last_message_path = run_dir / f"{provider}.last_message.txt"
        portable_home = prepare_provider_home(root, provider)
        env = provider_env(portable_home)
        codex_mesh.apply_external_execution_context_env(
            env,
            run_id=run_id,
            task_id=task_id,
            context=external_context,
            lineage_id=external_context.lineage_id or task_id,
            parent_task_id=external_context.parent_task_id,
        )
        command = build_attempt_command(
            root=root,
            last_message_path=last_message_path,
            enable_multi_agent=bool(inner_codex["enable_multi_agent"]),
            agent_max_depth=int(inner_codex["agent_max_depth"]),
            agent_max_threads=inner_codex["agent_max_threads"],
        )

        if dry_run:
            attempts.append(
                AttemptResult(
                    provider=provider,
                    portable_home=str(portable_home),
                    command=command,
                    returncode=0,
                    success=True,
                    duration_seconds=0.0,
                    stdout_path=str(stdout_path),
                    stderr_path=str(stderr_path),
                    last_message_path=str(last_message_path),
                )
            )
            emit_progress(f"[prompt6] dry_run provider={provider} prepared", json_mode=json_mode)
            break

        returncode, duration_seconds, error = invoke_codex_attempt(
            command=command,
            env=env,
            prompt=prompt,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            timeout_seconds=timeout_seconds,
        )
        last_message = last_message_path.read_text(encoding="utf-8") if last_message_path.exists() else ""
        success = returncode == 0 and bool(last_message.strip())
        if not success and error is None:
            error = f"returncode={returncode}; last_message_present={bool(last_message.strip())}"
        attempt = AttemptResult(
            provider=provider,
            portable_home=str(portable_home),
            command=command,
            returncode=returncode,
            success=success,
            duration_seconds=duration_seconds,
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            last_message_path=str(last_message_path),
            error=error,
        )
        attempts.append(attempt)
        emit_progress(
            f"[prompt6] provider={provider} rc={returncode} success={str(success).lower()} duration={duration_seconds:.1f}s",
            json_mode=json_mode,
        )
        if success:
            state["next_start_provider"] = next_provider_after(providers, provider)
            state["last_success_provider"] = provider
            state["last_run_id"] = run_id
            state["last_success_at"] = datetime.now(timezone.utc).isoformat()
            history = list(state.get("provider_history") or [])
            history.append({"run_id": run_id, "provider": provider, "success": True})
            state["provider_history"] = history[-20:]
            save_state(root, state)
            emit_progress(
                f"[prompt6] completed provider={provider} next_start_provider={state['next_start_provider']}",
                json_mode=json_mode,
            )
            break
        emit_progress(
            f"[prompt6] provider={provider} failed; trying next relay if available",
            json_mode=json_mode,
        )

    payload = {
        "run_id": run_id,
        "base_url": base_url,
        "providers": ordered_providers,
        "prompt_doc": str(prompt_doc),
        "prompt_path": str(run_dir / "prompt.txt"),
        "run_dir": str(run_dir),
        "success": any(item.success for item in attempts),
        "attempts": [asdict(item) for item in attempts],
        "next_start_provider": state.get("next_start_provider"),
    }
    (run_dir / "result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def run_once_mesh(
    *,
    root: Path,
    prompt_doc: Path,
    base_url: str,
    providers: list[str],
    prompt_override: str | None = None,
    json_mode: bool = False,
    mesh_max_workers: int = codex_mesh.DEFAULT_MAX_WORKERS,
    mesh_max_depth: int = codex_mesh.DEFAULT_MAX_EXTERNAL_DEPTH,
    mesh_benchmark_label: str | None = None,
    mesh_disable_provider: list[str] | None = None,
    mesh_hedge_delay_seconds: int = codex_mesh.DEFAULT_HEDGE_DELAY_SECONDS,
) -> dict[str, object]:
    try:
        external_context = codex_mesh.resolve_external_execution_context(mesh_max_depth)
    except codex_mesh.ExternalDepthLimitError as exc:
        return {
            "run_id": None,
            "base_url": base_url,
            "providers": list(providers),
            "prompt_doc": str(prompt_doc),
            "prompt_path": None,
            "run_dir": None,
            "success": False,
            "error": "external_depth_limit",
            "message": str(exc),
            "attempts": [],
            "next_start_provider": None,
            "delegate_mode": "mesh",
        }
    prompt = build_prompt(
        root=root,
        prompt_doc=prompt_doc,
        base_url=base_url,
        providers=providers,
        prompt_override=prompt_override,
        include_overlay=True,
    )
    manifest = codex_mesh.MeshRunManifest(
        tasks=[
            codex_mesh.MeshTaskManifest(
                task_id="prompt6-run-once",
                goal="Prompt 6 manual run-once",
                prompt=prompt,
                task_kind="mixed",
                read_scope=["app", "tests", "scripts", "docs/core", "github/automation/live_fix_loop"],
                write_scope=["app", "tests", "scripts", "docs/core", "docs/提示词", "github/automation/live_fix_loop"],
                max_external_depth=mesh_max_depth,
                allow_native_subagents=True,
                provider_allowlist=list(providers),
                provider_denylist=list(mesh_disable_provider or codex_mesh.DEFAULT_PROVIDER_DENYLIST),
                timeout_seconds=DEFAULT_TIMEOUT_MINUTES * 60,
                benchmark_label=mesh_benchmark_label,
                output_mode="text",
                working_root=str(root),
                parent_task_id=external_context.parent_task_id,
                lineage_id=external_context.lineage_id or "prompt6-run-once",
                depth=external_context.depth,
                hedge_delay_seconds=mesh_hedge_delay_seconds,
            )
        ],
        execution_mode="mesh",
        max_workers=mesh_max_workers,
        benchmark_label=mesh_benchmark_label,
        dangerously_bypass=True,
        sandbox="danger-full-access",
        ephemeral=True,
        provider_allowlist=list(providers),
        provider_denylist=list(mesh_disable_provider or codex_mesh.DEFAULT_PROVIDER_DENYLIST),
    )
    summary = codex_mesh.execute_manifest(root, manifest)
    task = summary.tasks[0]
    payload = {
        "run_id": summary.run_id,
        "base_url": base_url,
        "providers": task.provider_order,
        "prompt_doc": str(prompt_doc),
        "prompt_path": str(Path(summary.output_dir) / "tasks" / task.task_id / "prompt.txt"),
        "run_dir": summary.output_dir,
        "success": summary.success,
        "attempts": [asdict(item) for item in task.attempts],
        "next_start_provider": task.provider_order[1] if len(task.provider_order) > 1 else (task.provider_order[0] if task.provider_order else None),
        "delegate_mode": "mesh",
    }
    if json_mode:
        emit_progress(f"[prompt6] mesh run_id={summary.run_id} selected_provider={task.selected_provider}", json_mode=json_mode)
    return payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hourly Codex runner for Prompt 6 live fix loop.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run-once", help="run Prompt 6 once with relay failover")
    run_parser.add_argument("--delegate-mode", choices=["legacy", "mesh"], default="legacy")
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
    if args.prompt_text and args.prompt_file:
        raise ValueError("--prompt-text and --prompt-file are mutually exclusive")
    if args.prompt_text:
        return args.prompt_text
    if args.prompt_file:
        return args.prompt_file.read_text(encoding="utf-8")
    return None


def _providers_from_args(args: argparse.Namespace) -> list[str]:
    return list(args.providers or DEFAULT_PROVIDERS)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.command == "run-once":
        root = args.repo_root.resolve()
        prompt_doc = args.prompt_doc.resolve() if args.prompt_doc else default_prompt_doc(root)
        if args.delegate_mode == "mesh":
            payload = run_once_mesh(
                root=root,
                prompt_doc=prompt_doc,
                base_url=args.base_url,
                providers=_providers_from_args(args),
                prompt_override=_prompt_override_from_args(args),
                json_mode=args.json,
                mesh_max_workers=args.mesh_max_workers,
                mesh_max_depth=args.mesh_max_depth,
                mesh_benchmark_label=args.mesh_benchmark_label,
                mesh_disable_provider=args.mesh_disable_provider,
                mesh_hedge_delay_seconds=args.mesh_hedge_delay_seconds,
            )
        else:
            payload = run_once(
                root=root,
                prompt_doc=prompt_doc,
                base_url=args.base_url,
                providers=_providers_from_args(args),
                timeout_minutes=args.timeout_minutes,
                max_external_depth=args.mesh_max_depth,
                prompt_override=_prompt_override_from_args(args),
                preferred_start=args.preferred_start,
                dry_run=args.dry_run,
                include_overlay=not args.skip_overlay,
                json_mode=args.json,
            )
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(f"run_id={payload['run_id']}")
            print(f"success={str(payload['success']).lower()}")
            print(f"run_dir={payload['run_dir']}")
            print(f"providers={','.join(payload['providers'])}")
        return 0 if payload["success"] else 1

    if args.command == "print-prompt":
        root = args.repo_root.resolve()
        prompt_doc = args.prompt_doc.resolve() if args.prompt_doc else default_prompt_doc(root)
        print(
            build_prompt(
                root=root,
                prompt_doc=prompt_doc,
                base_url=args.base_url,
                providers=_providers_from_args(args),
                prompt_override=_prompt_override_from_args(args),
                include_overlay=not args.skip_overlay,
            )
        )
        return 0

    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
