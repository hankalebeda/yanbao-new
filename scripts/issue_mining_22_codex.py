#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    import codex_mesh
    import prompt6_hourly_codex as codex_common
except ImportError:  # pragma: no cover - package import fallback
    from scripts import codex_mesh
    from scripts import prompt6_hourly_codex as codex_common


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_PROVIDERS = list(codex_mesh.DEFAULT_PROVIDER_ALLOWLIST)
DEFAULT_TIMEOUT_MINUTES = 30


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


def state_dir(root: Path) -> Path:
    return root / "runtime" / "issue_mining_22"


def state_path(root: Path) -> Path:
    return state_dir(root) / "state.json"


def runs_dir(root: Path) -> Path:
    return state_dir(root) / "runs"


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
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(root: Path, payload: dict[str, object]) -> None:
    path = state_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_escort_doc22_prompt(
    *,
    root: Path,
    effective_doc: Path,
    base_url: str,
    providers: list[str],
    include_overlay: bool,
) -> str:
    timestamp = datetime.now(timezone.utc).astimezone().isoformat()
    writeback_targets = collect_writeback_targets(effective_doc)
    writeback_preview = "\n".join(
        f"   - `{item['heading']}`" for item in writeback_targets[:12]
    ) or "   - `(未找到可直接写回的小节，需人工确认位置)`"

    body = f"""
[角色]
你是本项目的 `22_v7` 精审问题挖掘代理，属于 `Escort Team` 的 analysis-only 审查轮。
`{effective_doc.as_posix()}` 只是审查结果写回目标，不是业务规格来源。
业务规格 SSOT 只认：
- `docs/core/01_需求基线.md`
- `docs/core/02_系统架构.md`
- `docs/core/03_详细设计.md`
- `docs/core/04_数据治理与血缘.md`
- `docs/core/05_API与数据契约.md`

[核心目标]
围绕“让 Escort Team 自身进入彻底完成态”做一轮高价值审查，但本轮只做分析与文档写回，不改代码。
执行原则：只分析，不改代码。
你必须真实使用以下审查路径：
1. 以 `docs/core/25_系统问题分析角度清单.md` 作为固定分析视角库。
2. 以 `22_v7` 作为当前问题台账与写回目标。
3. 交叉核对 SSOT、当前代码、当前测试、当前运行工件。
4. 优先发现 Escort Team 自身还未闭环的缺口：
   - 自发现
   - 自分析
   - 自修复
   - 自验证
   - 自写回
   - 自发布（Promote）

[多 Agent 协调要求]
你必须显式使用多 Agent 协调模式，至少并行启动 2 个子代理。
- 子代理 A：按 `docs/core/25` 审查 `22_v7`，找出仍存活、表述过时、或伪清零的问题。
- 子代理 B：检查当前实现与测试，确认这些问题在代码层是否真的闭环。
- 如有必要，可增加子代理 C：专门检查 Escort Team 自身的 orchestration / writeback / promote / codex 调用链是否仍有冲突。
- 主代理负责最终裁决、去重、证据整合、写回 `22_v7`，不得把最终判断外包给子代理。

[LiteLLM 参考边界]
你可以参考以下目录中的多 Agent 协调、路由、并发、防冲突、权限桥接、状态同步模式：
- `LiteLLM/issue_mesh/**`
- `LiteLLM/claude-code-fixed-main/**`
- `LiteLLM/claude-code-sourcemap-main/**`
但这些内容只能作为实现参考，不能替代 SSOT，也不能直接当成“本仓当前已完成”的证据。

[只允许做的事]
1. 阅读仓库文件、运行只读检查、调用子代理做分析。
2. 将确认后的问题写回 `{effective_doc.as_posix()}`。
3. 允许写运行产物到 `runtime/issue_mining_22/**`。

[禁止做的事]
1. 不得修改 `app/**`、`tests/**`、`scripts/**`、`docs/core/01~05`。
2. 不得修改 `{effective_doc.as_posix()}` 之外的正式业务文档。
3. 唯一允许改动的文件是 `{effective_doc.as_posix()}`。
4. 不得把 `docs/old/**`、旧测试、旧方案文档当成规格来源。
5. 不得把“pytest 通过”直接当作“问题不存在”的唯一证据。
6. 不得伪造 live 证据、运行结果、问题已清零状态。

[问题准入标准]
只有满足以下至少一条，才允许写回：
- 与 `01~05` 任一 SSOT 定义冲突。
- `22_v7` 当前表述与代码 / 测试 / 工件 / live 事实不一致。
- 文档把历史问题写成已收口，但当前仍可证实存在。
- 文档漏记了高价值、可追溯、可证明的真实问题。
- Escort Team 自身的自治闭环仍存在断点、伪闭环或人工兜底依赖。

[写回规则]
1. 优先写回最具体的 `### FRxx-* / PAGE-* / NFR-*` 子节。
2. 当前可直接写回的小节预览：
{writeback_preview}
3. 若是跨 FR / 全局治理 / Escort Team 自治问题，可写到当前全局问题汇总或当前轮次分析小节。
4. 每条新写回内容至少包含：
   - `问题`
   - `证据`
   - `SSOT 依据`
   - `当前结论`
   - `建议动作（仅记录，不实施）`
5. 如果原文写着 `**差距**: 无`、`已清零` 或等价表述，但你确认问题仍存活，必须改写为当前真实状态，禁止保留伪清零口径。

[优先级]
本轮优先顺序不是“扫得更广”，而是“先把 Escort Team 自身做完”：
1. Escort Team 自治闭环断点
2. 文档与当前实现冲突
3. Promote / writeback / verify / runtime gate 语义失真
4. 其它高价值残余问题

[证据原则]
- 若 live 站点 `{base_url}` 可用，可以补充 live 证据。
- 若 live 不可用，必须明确说明，并改用 SSOT + 代码 + 测试 + 工件交叉取证。
- 所有结论都必须最小可追溯，不得写成空泛判断。

[最终输出]
除写回文档外，最后输出一段简短 JSON，总结本轮：
```json
{{
  "status": "completed or blocked",
  "provider_used": "provider name",
  "used_subagents": 2,
  "new_issue_count": 0,
  "updated_doc": "{effective_doc.as_posix()}",
  "summary": "一句话总结"
}}
```

[运行上下文]
- 当前时间：`{timestamp}`
- 仓库根目录：`{root.as_posix()}`
- 目标文档：`{effective_doc.as_posix()}`
- live base URL：`{base_url}`
- relay providers：`{' -> '.join(providers)}`
- 单一问题台账位置：`## 单一问题台账`
"""

    if not include_overlay:
        return body.strip() + "\n"

    overlay = """
[自动化调度上下文]
- 这是手动触发的 analysis-only 运行，不是代码修复任务。
- 当前执行器支持多轮工具调用与子代理协作，禁止谎报“执行器不支持多 Agent / 多轮闭环”。
- 本轮唯一允许修改的正式文件是 `22_v7` 目标文档；其余代码和 SSOT 文档只读。
- 如果发现提示词内部存在冲突，以“SSOT 优先、只分析不改代码、Escort Team 自身优先闭环、证据可追溯”四条规则为最高优先级。
"""
    return overlay.strip() + "\n\n" + body.strip() + "\n"


def build_prompt(
    *,
    root: Path,
    total_doc: Path | None = None,
    target_doc: Path | None = None,
    base_url: str,
    providers: list[str],
    prompt_override: str | None = None,
    include_overlay: bool = True,
) -> str:
    effective_doc = target_doc or total_doc
    if effective_doc is None:
        raise ValueError("target_doc or total_doc is required")
    if prompt_override is not None:
        return prompt_override.strip() + "\n"
    return _build_escort_doc22_prompt(
        root=root,
        effective_doc=effective_doc,
        base_url=base_url,
        providers=providers,
        include_overlay=include_overlay,
    )

    timestamp = datetime.now(timezone.utc).astimezone().isoformat()
    writeback_targets = collect_writeback_targets(effective_doc)
    writeback_preview = "\n".join(f"   - `{item['heading']}`" for item in writeback_targets[:12]) or "   - `(未找到可写回子节，需人工确认)`"
    body = f"""
【角色】
你是本项目的“22_v7 精审问题挖掘（analysis-only）”代理。`{effective_doc.as_posix()}` 只是排查地图与问题写回目标；真正的规格来源只认 `docs/core/01_需求基线.md`、`docs/core/02_系统架构.md`、`docs/core/03_详细设计.md`、`docs/core/04_数据治理与血缘.md`、`docs/core/05_API与数据契约.md`。

【任务】
你的目标是在同一会话内循环挖掘系统仍然存在的真实问题，但只分析，不改代码。你必须：
1. 明确使用子代理功能加速，至少并行启动 2 个子代理。
   - 子代理 A：把 `22_v7` 的高风险域映射到 `01~05` 的 SSOT 章节，并指出最应该优先复核的 2~3 个 FR / NFR 域。
   - 子代理 B：阅读对应代码、测试、运行辅助脚本，寻找“22_v7 当前表述可能过时、过宽或遗漏”的候选问题。
   - 主代理负责最终取证、判题、写回 `22_v7`，不得把最终结论外包给子代理。
2. 以 `22_v7` 为排查地图，优先复核高风险域；若 30 分钟预算内还有时间，继续下一轮，不得只看一个点就结束。
3. 只把满足以下至少一条的问题写回：
   - 违反 `01~05` 任一冻结契约或验收标准；
   - `22_v7` 当前口径与 SSOT / 当前代码 / 当前测试 / 当前 live 事实不一致；
   - 页面、接口、数据库、代码、测试之间存在矛盾；
   - 当前文档把历史问题写成已收口，但当前仍可证实存在；
   - 当前文档漏记了高价值真实问题。
4. 严格遵守“只分析，不改实现”：
   - 禁止修改 `app/**`、`tests/**`、`scripts/**`、`docs/core/01~05`。
   - 唯一允许改动的文件是 `{effective_doc.as_posix()}`。
   - 允许写运行产物到 `runtime/issue_mining_22/**`。
5. 把问题写回 `{effective_doc.as_posix()}` 对应部分：
   - 优先写回最具体的 `### FRxx-* / PAGE-* / NFR-*` 子节。
   - 当前文档可直接写回的目标小节预览：
{writeback_preview}
   - 若问题属于单一 FR 子功能，写入最具体的 `### FRxx-...` 小节，在该小节现有 `**差距**` 附近补一段 `**🆕 分析补记（{timestamp}）**`。
   - 若问题跨多个 FR 或属于全局治理 / 测试 / 工具问题，写到 `## 交叉验证与全局问题汇总（v7.3复核版）` 或已有“本轮最新 / 问题 / 结果”结构附近，新增当日本轮分析小节。
   - 每条新写回问题至少包含：`问题`、`证据`、`SSOT 依据`、`当前结论`、`建议动作（仅记录，不修）`。
   - 如果原文写着 `**差距**: 无` 或 `已清零`，但你确认问题仍在，必须把该口径改写成当前真实状态，禁止保留假清零。
6. 若 live 站点 `{base_url}` 可用，可将其作为补充证据；若不可用，不得伪造 live 证据，转而使用 SSOT + 代码 + 测试 + 文档交叉验证。
7. 最终输出一段简短 JSON，总结本轮：
   - `status`: `completed` 或 `blocked`
   - `provider_used`
   - `used_subagents`
   - `new_issue_count`
   - `updated_doc`
   - `summary`

【边界约束】
- `docs/old/**`、旧代码、旧测试都不能作为规格来源。
- 禁止把“pytest 通过”直接当成问题不存在的证据；必须结合 SSOT 和当前实现判断。
- 禁止为了保持文档好看而回避问题；凡确认仍存在，就要写回到对应部分。
- 禁止修改 `{effective_doc.name}` 之外的业务文档。

【运行上下文】
- 当前时间：`{timestamp}`
- 仓库根目录：`{root.as_posix()}`
- 目标文档：`{effective_doc.as_posix()}`
- live base URL：`{base_url}`
- relay providers：`{' -> '.join(providers)}`
- 单一问题台账位置：`## 单一问题台账`
"""
    if not include_overlay:
        return body.strip() + "\n"
    overlay = """
【自动化调度上下文】
- 这是手动触发的 30 分钟 analysis-only 运行，不是修复任务。
- 当前执行器支持多轮工具调用与子代理，禁止谎报“执行器不支持多轮闭环”。
- 本轮只允许分析并写回 22_v7，对代码与其他 SSOT 不做改动。
"""
    return overlay.strip() + "\n\n" + body.strip() + "\n"

build_attempt_command = codex_common.build_attempt_command


def run_once(
    *,
    root: Path,
    target_doc: Path | None = None,
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
    effective_doc = target_doc or globals()["target_doc"](root)
    state = load_state(root)
    ordered_providers = codex_common.provider_order(providers, state, preferred_start=preferred_start)
    run_id = datetime.now(timezone.utc).astimezone().strftime("%Y%m%dT%H%M%S")
    run_dir = runs_dir(root) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    task_id = f"issue-mining-22-{run_id}"
    try:
        external_context = codex_mesh.resolve_external_execution_context(max_external_depth)
    except codex_mesh.ExternalDepthLimitError as exc:
        payload = {
            "run_id": run_id,
            "base_url": base_url,
            "target_doc": str(effective_doc),
            "providers": ordered_providers,
            "prompt_path": str(run_dir / "prompt.txt"),
            "run_dir": str(run_dir),
            "success": False,
            "error": "external_depth_limit",
            "message": str(exc),
            "attempts": [],
            "next_start_provider": state.get("next_start_provider"),
        }
        (run_dir / "result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        codex_common.emit_progress(f"[issue22] blocked error=external_depth_limit detail={exc}", json_mode=json_mode)
        return payload
    codex_common.emit_progress(
        f"[issue22] run_id={run_id} base_url={base_url} providers={','.join(ordered_providers)}",
        json_mode=json_mode,
    )
    codex_common.emit_progress(f"[issue22] target_doc={effective_doc} run_dir={run_dir}", json_mode=json_mode)

    prompt = build_prompt(
        root=root,
        target_doc=effective_doc,
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
        codex_common.emit_progress(f"[issue22] starting provider={provider}", json_mode=json_mode)
        stdout_path = run_dir / f"{provider}.stdout.jsonl"
        stderr_path = run_dir / f"{provider}.stderr.log"
        last_message_path = run_dir / f"{provider}.last_message.txt"
        portable_home = codex_common.prepare_provider_home(root, provider)
        env = codex_common.provider_env(portable_home)
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
            codex_common.emit_progress(f"[issue22] dry_run provider={provider} prepared", json_mode=json_mode)
            break

        returncode, duration_seconds, error = codex_common.invoke_codex_attempt(
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

        attempts.append(
            AttemptResult(
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
        )
        codex_common.emit_progress(
            f"[issue22] provider={provider} rc={returncode} success={str(success).lower()} duration={duration_seconds:.1f}s",
            json_mode=json_mode,
        )
        if success:
            state["next_start_provider"] = ordered_providers[(ordered_providers.index(provider) + 1) % len(ordered_providers)]
            state["last_success_provider"] = provider
            state["last_run_id"] = run_id
            state["last_success_at"] = datetime.now(timezone.utc).isoformat()
            history = list(state.get("provider_history") or [])
            history.append({"run_id": run_id, "provider": provider, "success": True})
            state["provider_history"] = history[-20:]
            save_state(root, state)
            codex_common.emit_progress(
                f"[issue22] completed provider={provider} next_start_provider={state['next_start_provider']}",
                json_mode=json_mode,
            )
            break
        codex_common.emit_progress(
            f"[issue22] provider={provider} failed; trying next relay if available",
            json_mode=json_mode,
        )

    payload = {
        "run_id": run_id,
        "base_url": base_url,
        "target_doc": str(effective_doc),
        "providers": ordered_providers,
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
    target_doc: Path | None = None,
    base_url: str,
    providers: list[str],
    prompt_override: str | None = None,
    mesh_max_workers: int = codex_mesh.DEFAULT_MAX_WORKERS,
    mesh_max_depth: int = codex_mesh.DEFAULT_MAX_EXTERNAL_DEPTH,
    mesh_benchmark_label: str | None = None,
    mesh_disable_provider: list[str] | None = None,
    mesh_hedge_delay_seconds: int = codex_mesh.DEFAULT_HEDGE_DELAY_SECONDS,
) -> dict[str, object]:
    effective_doc = target_doc or globals()["target_doc"](root)
    try:
        external_context = codex_mesh.resolve_external_execution_context(mesh_max_depth)
    except codex_mesh.ExternalDepthLimitError as exc:
        return {
            "run_id": None,
            "base_url": base_url,
            "target_doc": str(effective_doc),
            "providers": list(providers),
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
        target_doc=effective_doc,
        base_url=base_url,
        providers=providers,
        prompt_override=prompt_override,
        include_overlay=True,
    )
    manifest = codex_mesh.MeshRunManifest(
        tasks=[
            codex_mesh.MeshTaskManifest(
                task_id="issue-mining-22",
                goal="22_v7 analysis and writeback",
                prompt=prompt,
                task_kind="write",
                read_scope=["docs/core/22_全量功能进度总表_v7_精审.md", "docs/core/01_需求基线.md", "docs/core/02_系统架构.md", "docs/core/03_详细设计.md", "docs/core/04_数据治理与血缘.md", "docs/core/05_API与数据契约.md", "app", "tests", "scripts"],
                write_scope=[str(effective_doc.relative_to(root)).replace("\\", "/")],
                max_external_depth=mesh_max_depth,
                allow_native_subagents=True,
                provider_allowlist=list(providers),
                provider_denylist=list(mesh_disable_provider or codex_mesh.DEFAULT_PROVIDER_DENYLIST),
                timeout_seconds=DEFAULT_TIMEOUT_MINUTES * 60,
                benchmark_label=mesh_benchmark_label,
                output_mode="text",
                working_root=str(root),
                parent_task_id=external_context.parent_task_id,
                lineage_id=external_context.lineage_id or "issue-mining-22",
                depth=external_context.depth,
                hedge_delay_seconds=mesh_hedge_delay_seconds,
            )
        ],
        execution_mode="mesh",
        max_workers=mesh_max_workers,
        benchmark_label=mesh_benchmark_label,
        provider_allowlist=list(providers),
        provider_denylist=list(mesh_disable_provider or codex_mesh.DEFAULT_PROVIDER_DENYLIST),
    )
    summary = codex_mesh.execute_manifest(root, manifest)
    task = summary.tasks[0]
    return {
        "run_id": summary.run_id,
        "base_url": base_url,
        "target_doc": str(effective_doc),
        "providers": task.provider_order,
        "prompt_path": str(Path(summary.output_dir) / "tasks" / task.task_id / "prompt.txt"),
        "run_dir": summary.output_dir,
        "success": summary.success,
        "attempts": [asdict(item) for item in task.attempts],
        "next_start_provider": task.provider_order[1] if len(task.provider_order) > 1 else (task.provider_order[0] if task.provider_order else None),
        "delegate_mode": "mesh",
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual Codex analysis-only issue mining for docs/core/22 v7.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run-once", help="run the 22_v7 analysis-only pass once with relay failover")
    run_parser.add_argument("--delegate-mode", choices=["legacy", "mesh"], default="legacy")
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
        if args.delegate_mode == "mesh":
            payload = run_once_mesh(
                root=root,
                base_url=args.base_url,
                providers=_providers_from_args(args),
                prompt_override=_prompt_override_from_args(args),
                mesh_max_workers=args.mesh_max_workers,
                mesh_max_depth=args.mesh_max_depth,
                mesh_benchmark_label=args.mesh_benchmark_label,
                mesh_disable_provider=args.mesh_disable_provider,
                mesh_hedge_delay_seconds=args.mesh_hedge_delay_seconds,
            )
        else:
            payload = run_once(
                root=root,
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
            print(f"target_doc={payload['target_doc']}")
            print(f"run_dir={payload['run_dir']}")
            print(f"providers={','.join(payload['providers'])}")
        return 0 if payload["success"] else 1

    if args.command == "print-prompt":
        root = args.repo_root.resolve()
        print(
            build_prompt(
                root=root,
                target_doc=target_doc(root),
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
