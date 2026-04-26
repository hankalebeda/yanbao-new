from __future__ import annotations

import argparse
import base64
import fnmatch
import json
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.github_automation_paths import (
        continuous_audit_dir,
        live_fix_loop_dir,
        live_fix_loop_runs_dir,
    )
except ModuleNotFoundError:
    from github_automation_paths import (
        continuous_audit_dir,
        live_fix_loop_dir,
        live_fix_loop_runs_dir,
    )


DEFAULT_REMOTE = "origin"
DEFAULT_PROVIDERS = list(("sub.jlypx.de", "snew.145678.xyz", "ai.qaq.al", "infiniteai.cc"))
DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_BRANCH_PREFIX = "auto-fix"

MANAGED_SKILLS: dict[str, dict[str, Any]] = {
    "security-best-practices": {
        "value": "Verify security guidance is actually grounded in this FastAPI + web frontend repo.",
        "required_files": (
            "SKILL.md",
            "references/python-fastapi-web-server-security.md",
            "references/javascript-general-web-frontend-security.md",
        ),
        "hard_dependencies": ("python",),
        "soft_dependencies": (),
    },
    "playwright": {
        "value": "Validate browser-facing fixes against the repo's real Playwright/browser gates.",
        "required_files": (
            "SKILL.md",
            "scripts/playwright_cli.sh",
            "references/cli.md",
            "references/workflows.md",
        ),
        "hard_dependencies": ("node", "npx"),
        "soft_dependencies": ("bash",),
    },
    "gh-fix-ci": {
        "value": "Turn post-push CI failures into actionable GitHub diagnostics instead of fake green assumptions.",
        "required_files": (
            "SKILL.md",
            "scripts/inspect_pr_checks.py",
        ),
        "hard_dependencies": ("python", "gh", "gh_auth"),
        "soft_dependencies": (),
    },
    "gh-address-comments": {
        "value": "Convert real GitHub PR review comments into tracked follow-up work after open_pr flows.",
        "required_files": (
            "SKILL.md",
            "scripts/fetch_comments.py",
        ),
        "hard_dependencies": ("python", "gh", "gh_auth"),
        "soft_dependencies": (),
    },
    "babysit-pr": {
        "value": "Keep a created PR under watch until CI/review state is truly clean, not just locally committed.",
        "required_files": (
            "SKILL.md",
            "scripts/gh_pr_watch.py",
            "references/heuristics.md",
        ),
        "hard_dependencies": ("python", "gh", "gh_auth"),
        "soft_dependencies": (),
    },
}

ARTIFACT_PATTERNS = (
    "github/automation/**",
    "docs/_temp/**",
    "docs/old/**",
    "_archive/**",
    "runtime/**",
    "data/**",
    "output/**",
    "artifacts/**",
    "logs/**",
    ".venv/**",
    "venv/**",
    ".playwright-cli/**",
    ".pytest-tmp/**",
    ".pytest_tmp/**",
    "basetemp_audit/**",
    "tmp/**",
    "_tmp_*",
    "tmp_*.js",
    "tmp_*_debug.db",
    "tmp_url_list.txt",
    "tests/data_source_test_results/**",
    "tests/gemini_results/**",
    "ai-api/*/test_results/**",
    "ai-api/*/*/test_results/**",
    "app/governance/feature_registry.json",
    "app/governance/catalog_snapshot.json",
    "app/governance/mismatch_report.json",
)

REFERENCE_ONLY_PATTERNS = (
    "tests/legacy_*.py",
    "scripts/archive/**",
    "docs/core/tmp/**",
)

PUBLISHABLE_PATTERNS = (
    "app/**",
    "tests/test_*.py",
    "scripts/**",
    "app/web/**",
    "ai-api/codex/**",
    ".cursor/rules/**",
    ".cursor/skills/**",
    "docs/core/**",
    "docs/提示词/**",
    "github/README.md",
    "github/docs/**",
    ".gitignore",
    "package.json",
    "package-lock.json",
    "pytest.ini",
    "open-with-*.ps1",
    "open-with-*.cmd",
    "probe-with-*.ps1",
    "probe-with-*.cmd",
)

COMMON_WORKTREE_SUPPORT_PATTERNS = (
    "scripts/continuous_repo_audit.py",
    "scripts/live_fix_loop.py",
    "scripts/github_automation_paths.py",
    "scripts/doc_driven/__init__.py",
    "scripts/doc_driven/audit_blind_spots.py",
    "scripts/doc_driven/audit_test_quality.py",
    "scripts/doc_driven/page_expectations.py",
    "tests/conftest.py",
    "tests/helpers*.py",
    "tests/test_*.py",
    "app/**/*.py",
    "app/**/*.json",
    "app/web/**/*.html",
    "app/web/**/*.js",
    "app/web/**/*.css",
    "docs/core/*.md",
    "github/README.md",
    "github/docs/**",
    "pytest.ini",
)

FIX_WORKTREE_SUPPORT_PATTERNS = (
    "scripts/codex_prompt6_hourly.py",
    "docs/提示词/*.md",
    ".cursor/rules/**",
    ".cursor/skills/**",
)

WORKTREE_SUPPORT_SKIP_PATTERNS = (
    *tuple(
        pattern
        for pattern in ARTIFACT_PATTERNS
        if pattern
        not in {
            "app/governance/feature_registry.json",
            "app/governance/catalog_snapshot.json",
            "app/governance/mismatch_report.json",
        }
    ),
    *REFERENCE_ONLY_PATTERNS,
    "docs/core/test_results/**",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_token_path(root: Path) -> Path:
    return root / "github" / "token.txt"


def default_artifact_root() -> Path:
    return Path("github") / "automation" / "runs"


def default_worktree_root(root: Path) -> Path:
    return root / "runtime" / "github_guardian" / "worktrees"


def default_local_mirror_root(root: Path) -> Path:
    return root / "github" / "automation" / "_local" / "runs"


def default_local_latest_path(root: Path) -> Path:
    return root / "github" / "automation" / "_local" / "latest.json"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_run_id() -> str:
    return now_utc().strftime("%Y%m%dT%H%M%SZ")


def resolve_python(root: Path) -> str:
    venv_python = root / ".venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def probe_command(name: str, *, verify_args: list[str] | None = None) -> dict[str, Any]:
    path = command_path(name)
    if not path:
        return {"available": False, "verified": False, "path": None, "detail": "command_missing"}

    probe = {"available": True, "verified": True, "path": path, "detail": "ok"}
    if verify_args:
        try:
            completed = subprocess.run(
                [path, *verify_args],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
        except OSError as exc:
            probe["verified"] = False
            probe["detail"] = f"launch_failed:{exc}"
            return probe
        probe["verified"] = completed.returncode == 0
        probe["detail"] = "ok" if completed.returncode == 0 else (completed.stderr.strip() or completed.stdout.strip() or "verification_failed")
    return probe


def collect_dependency_probes() -> dict[str, dict[str, Any]]:
    gh_probe = probe_command("gh")
    return {
        "python": probe_command("python", verify_args=["--version"]),
        "codex": probe_command("codex", verify_args=["--version"]),
        "node": probe_command("node", verify_args=["--version"]),
        "npx": probe_command("npx", verify_args=["--version"]),
        "bash": probe_command("bash", verify_args=["--version"]),
        "gh": gh_probe,
        "gh_auth": (
            probe_command("gh", verify_args=["auth", "status"])
            if gh_probe["available"]
            else {"available": False, "verified": False, "path": None, "detail": "gh_missing"}
        ),
    }


def probe_is_ready(probe: dict[str, Any] | None) -> bool:
    if not isinstance(probe, dict):
        return False
    return bool(probe.get("available")) and bool(probe.get("verified", True))


def normalize_repo_path(path: str) -> str:
    normalized = path.replace("\\", "/").strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def codex_home_root(root: Path) -> Path:
    override = os.environ.get("CODEX_HOME_ROOT")
    if override:
        return Path(override)
    return root / "ai-api" / "codex"


def command_path(name: str) -> str | None:
    resolved = shutil.which(name)
    return resolved


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def build_repo_fit(root: Path, *, has_github_remote: bool) -> dict[str, bool]:
    return {
        "has_fastapi": (root / "app" / "main.py").exists(),
        "has_web_frontend": (root / "app" / "web").exists() or (root / "package.json").exists(),
        "has_browser_gate": (root / "tests" / "test_gate_browser_playwright.py").exists(),
        "has_github_remote": has_github_remote,
    }


def skill_dir(root: Path, provider: str, skill_name: str) -> Path:
    return codex_home_root(root) / provider / "skills" / skill_name


def select_relevant_skills(*, mode: str, push: bool, open_pr: bool, repo_fit: dict[str, bool]) -> list[str]:
    relevant: set[str] = set()
    if mode == "audit-and-fix":
        if repo_fit.get("has_browser_gate") or repo_fit.get("has_web_frontend"):
            relevant.add("playwright")
        if push or open_pr:
            relevant.add("gh-fix-ci")
        if open_pr:
            relevant.update({"gh-address-comments", "babysit-pr"})
    return [skill for skill in MANAGED_SKILLS if skill in relevant]


def assess_skill_readiness(
    *,
    root: Path,
    providers: list[str],
    mode: str,
    push: bool,
    open_pr: bool,
    dependency_probes: dict[str, dict[str, Any]] | None = None,
    has_github_remote: bool = True,
) -> dict[str, Any]:
    dependency_probes = dependency_probes or collect_dependency_probes()
    repo_fit = build_repo_fit(root, has_github_remote=has_github_remote)
    relevant_skills = select_relevant_skills(mode=mode, push=push, open_pr=open_pr, repo_fit=repo_fit)
    skills: dict[str, Any] = {}
    ready = partial = unavailable = 0

    for skill_name, spec in MANAGED_SKILLS.items():
        provider_states: dict[str, Any] = {}
        structure_ok = True
        reasons: list[str] = []
        for provider in providers:
            base = skill_dir(root, provider, skill_name)
            missing_files = [item for item in spec["required_files"] if not (base / item).exists()]
            provider_states[provider] = {
                "path": str(base),
                "exists": base.exists(),
                "skill_md": (base / "SKILL.md").exists(),
                "missing_files": missing_files,
                "status": "ready" if base.exists() and not missing_files else "unavailable",
            }
            if not base.exists():
                structure_ok = False
                reasons.append(f"{provider}:skill_missing")
            elif missing_files:
                structure_ok = False
                reasons.append(f"{provider}:missing:{','.join(missing_files)}")

        hard_failures = []
        soft_failures = []
        for dep in spec["hard_dependencies"]:
            probe = dependency_probes.get(dep, {"available": False, "verified": False, "detail": "probe_missing"})
            if not probe.get("available") or not probe.get("verified", True):
                hard_failures.append(f"{dep}:{probe.get('detail')}")
        for dep in spec["soft_dependencies"]:
            probe = dependency_probes.get(dep, {"available": False, "verified": False, "detail": "probe_missing"})
            if not probe.get("available") or not probe.get("verified", True):
                soft_failures.append(f"{dep}:{probe.get('detail')}")

        relevant = skill_name in relevant_skills
        status = "ready"
        if not structure_ok or hard_failures:
            status = "unavailable"
        elif soft_failures:
            status = "partial"

        if skill_name.startswith("gh-") or skill_name == "babysit-pr":
            if not repo_fit["has_github_remote"]:
                status = "partial" if status == "ready" else status
                reasons.append("repo:no_github_remote")
        bash_probe = dependency_probes.get("bash", {"available": False, "verified": False, "detail": "probe_missing"})
        if skill_name == "playwright" and os.name == "nt" and not bash_probe.get("available"):
            if status == "ready":
                status = "partial"
            reasons.append("windows:no_bash_wrapper")

        reasons.extend(hard_failures)
        reasons.extend(soft_failures)

        if relevant:
            if status == "ready":
                ready += 1
            elif status == "partial":
                partial += 1
            else:
                unavailable += 1

        skills[skill_name] = {
            "relevant": relevant,
            "status": status,
            "value": spec["value"],
            "provider_states": provider_states,
            "dependencies": {dep: dependency_probes.get(dep) for dep in (*spec["hard_dependencies"], *spec["soft_dependencies"])},
            "reasons": reasons,
            "repo_fit": repo_fit,
        }

    gate = "ready"
    if relevant_skills and ready == 0:
        gate = "blocked"
    elif partial or unavailable:
        gate = "degraded"

    return {
        "summary": {
            "gate": gate,
            "relevant_skills": relevant_skills,
            "ready_count": ready,
            "partial_count": partial,
            "unavailable_count": unavailable,
        },
        "repo_fit": repo_fit,
        "dependency_probes": dependency_probes,
        "skills": skills,
    }


def relative_to_root(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)


def load_github_token(token_path: Path | None = None) -> str:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        return token.strip()
    if token_path is None:
        token_path = default_token_path(repo_root())
    if not token_path.exists():
        raise FileNotFoundError(f"GitHub token file not found: {token_path}")
    return token_path.read_text(encoding="utf-8").strip()


def parse_repo_slug(remote_url: str) -> str | None:
    url = remote_url.strip()
    if not url:
        return None
    if url.startswith("git@github.com:"):
        slug = url.split("git@github.com:", 1)[1]
    elif "github.com/" in url:
        slug = url.split("github.com/", 1)[1]
    else:
        return None
    if slug.endswith(".git"):
        slug = slug[:-4]
    return slug.strip("/") or None


def resolve_push_url(remote_url: str, repo_slug: str | None = None) -> str:
    slug = repo_slug or parse_repo_slug(remote_url)
    if slug:
        return f"https://github.com/{slug}.git"
    raise RuntimeError(f"Unsupported remote for token-based GitHub push: {remote_url}")


def provider_skill_dir(root: Path, provider: str, skill_name: str) -> Path:
    return codex_home_root(root) / provider / "skills" / skill_name


def collect_skill_readiness(
    root: Path,
    providers: list[str],
    *,
    mode: str = "audit-and-fix",
    push: bool = False,
    open_pr: bool = False,
    remote: str = DEFAULT_REMOTE,
    dependency_probes: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    remote_slug = parse_repo_slug(git_output(root, "remote", "get-url", remote, check=False))
    assessed = assess_skill_readiness(
        root=root,
        providers=providers,
        mode=mode,
        push=push,
        open_pr=open_pr,
        dependency_probes=dependency_probes,
        has_github_remote=bool(remote_slug),
    )

    skills: dict[str, Any] = {}
    ready_skills: list[str] = []
    partial_skills: list[str] = []
    unavailable_skills: list[str] = []
    not_selected_skills: list[str] = []
    fake_available_skills: list[str] = []

    for skill_name, assessed_skill in assessed["skills"].items():
        provider_statuses: dict[str, Any] = {}
        provider_missing: list[str] = []
        provider_file_gaps: dict[str, list[str]] = {}
        hard_dependency_gaps: list[str] = []
        soft_dependency_gaps: list[str] = []

        for provider, state in assessed_skill["provider_states"].items():
            missing_files = list(state.get("missing_files") or [])
            provider_statuses[provider] = {
                "skill_dir": state["path"],
                "exists": state["exists"],
                "missing_files": missing_files,
            }
            if not state["exists"]:
                provider_missing.append(provider)
            elif missing_files:
                provider_file_gaps[provider] = missing_files

        spec = MANAGED_SKILLS[skill_name]
        for dep in spec["hard_dependencies"]:
            if not probe_is_ready(assessed_skill["dependencies"].get(dep)):
                hard_dependency_gaps.append(dep)
        for dep in spec["soft_dependencies"]:
            if not probe_is_ready(assessed_skill["dependencies"].get(dep)):
                soft_dependency_gaps.append(dep)

        fully_installed = not provider_missing and not provider_file_gaps
        if fully_installed and hard_dependency_gaps:
            fake_available_skills.append(skill_name)

        status = assessed_skill["status"]
        if assessed_skill["relevant"]:
            if status == "ready":
                ready_skills.append(skill_name)
            elif status == "partial":
                partial_skills.append(skill_name)
            else:
                unavailable_skills.append(skill_name)
        else:
            not_selected_skills.append(skill_name)

        skills[skill_name] = {
            "status": status,
            "relevant": assessed_skill["relevant"],
            "value": assessed_skill["value"],
            "providers": provider_statuses,
            "providers_missing": provider_missing,
            "provider_file_gaps": provider_file_gaps,
            "dependencies_required": [*spec["hard_dependencies"], *spec["soft_dependencies"]],
            "dependencies_missing": hard_dependency_gaps,
            "soft_dependencies_missing": soft_dependency_gaps,
            "value_kind": skill_name,
            "reasons": assessed_skill["reasons"],
            "repo_fit": assessed_skill["repo_fit"],
        }

    degraded_skills = [*partial_skills, *unavailable_skills]
    return {
        "providers": providers,
        "repo_fit": assessed["repo_fit"],
        "dependency_probes": assessed["dependency_probes"],
        "skills": skills,
        "summary": {
            "relevant_skills": assessed["summary"]["relevant_skills"],
            "not_selected_skills": not_selected_skills,
            "ready_skills": ready_skills,
            "partial_skills": partial_skills,
            "unavailable_skills": unavailable_skills,
            "degraded_skills": degraded_skills,
            "ready_count": len(ready_skills),
            "partial_count": len(partial_skills),
            "degraded_count": len(degraded_skills),
            "unavailable_count": len(unavailable_skills),
            "fake_available_skills": fake_available_skills,
            "overall_status": assessed["summary"]["gate"],
        },
    }


def build_skill_readiness_prompt(readiness: dict[str, Any]) -> str:
    summary = readiness["summary"]
    partial_skills = summary.get("partial_skills", [])
    unavailable_skills = summary.get("unavailable_skills", summary.get("degraded_skills", []))
    attention_skills = list(dict.fromkeys([*partial_skills, *unavailable_skills, *summary.get("degraded_skills", [])]))
    lines = [
        "【Skills Readiness】",
        f"- Selected providers: {', '.join(readiness['providers'])}",
        f"- Relevant skills: {', '.join(summary.get('relevant_skills', [])) if summary.get('relevant_skills') else '(none)'}",
        f"- Ready skills: {', '.join(summary['ready_skills']) if summary['ready_skills'] else '(none)'}",
        f"- Partial skills: {', '.join(partial_skills) if partial_skills else '(none)'}",
        f"- Unavailable skills: {', '.join(unavailable_skills) if unavailable_skills else '(none)'}",
        f"- Gate: {summary.get('overall_status', 'unknown')}",
    ]
    not_selected = summary.get("not_selected_skills", [])
    if not_selected:
        lines.append(f"- Not selected for this run: {', '.join(not_selected)}")
    for skill_name in attention_skills:
        skill = readiness["skills"].get(
            skill_name,
            {"providers_missing": [], "provider_file_gaps": {}, "dependencies_missing": [], "soft_dependencies_missing": []},
        )
        reasons: list[str] = []
        if skill["providers_missing"]:
            reasons.append("missing providers=" + ",".join(skill["providers_missing"]))
        if skill["provider_file_gaps"]:
            gap_parts = []
            for provider, files in skill["provider_file_gaps"].items():
                gap_parts.append(f"{provider}:{'|'.join(files)}")
            reasons.append("missing files=" + ";".join(gap_parts))
        if skill["dependencies_missing"]:
            reasons.append("missing deps=" + ",".join(skill["dependencies_missing"]))
        if skill.get("soft_dependencies_missing"):
            reasons.append("soft deps=" + ",".join(skill["soft_dependencies_missing"]))
        lines.append(f"- {skill_name}: {'; '.join(reasons) if reasons else 'degraded'}")
    lines.extend(
        [
            "- Only claim to use a skill when it is listed as ready and relevant for this run.",
            "- If a skill is partial or unavailable, do not pretend to use it. Fall back to general repository analysis and state the downgrade explicitly.",
        ]
    )
    return "\n".join(lines) + "\n"


def git_output(root: Path, *args: str, check: bool = True) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if check and completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or f"git {' '.join(args)} failed")
    return completed.stdout.strip()


def current_branch(root: Path) -> str:
    return git_output(root, "branch", "--show-current", check=False)


def resolve_base_ref(root: Path, *, remote: str, explicit_base_ref: str | None = None) -> str:
    if explicit_base_ref:
        return explicit_base_ref

    branch = current_branch(root)
    if branch:
        return branch

    remote_head = git_output(root, "symbolic-ref", f"refs/remotes/{remote}/HEAD", check=False)
    remote_prefix = f"refs/remotes/{remote}/"
    if remote_head.startswith(remote_prefix):
        return remote_head[len(remote_prefix) :]

    head_sha = git_output(root, "rev-parse", "HEAD", check=False)
    if head_sha:
        return head_sha

    raise RuntimeError("Unable to resolve base ref. Pass --base-ref explicitly.")


def remote_url(root: Path, remote: str) -> str:
    return git_output(root, "remote", "get-url", remote)


def basic_auth_header(token: str) -> str:
    encoded = base64.b64encode(f"x-access-token:{token}".encode("utf-8")).decode("ascii")
    return f"AUTHORIZATION: basic {encoded}"


def build_audit_command(python: str) -> list[str]:
    return [python, "scripts/continuous_repo_audit.py"]


def build_live_fix_init_command(python: str, worktree: Path) -> list[str]:
    return [python, "scripts/live_fix_loop.py", "init", "--root", str(worktree)]


def build_fix_command(
    python: str,
    *,
    providers: list[str],
    base_url: str,
    dry_run: bool,
    ensure_runtime: bool,
    dangerously_bypass: bool,
    sandbox: str,
    delegate_mode: str,
    mesh_max_workers: int,
    mesh_max_depth: int,
    mesh_benchmark_label: str | None,
    mesh_disable_provider: list[str],
    prompt_prelude_file: Path | None = None,
) -> list[str]:
    command = [
        python,
        "scripts/codex_prompt6_hourly.py",
        "--json",
        "--delegate-mode",
        delegate_mode,
        "--base-url",
        base_url,
        "--sandbox",
        sandbox,
        "--providers",
        *providers,
    ]
    command.extend(["--mesh-max-workers", str(mesh_max_workers)])
    command.extend(["--mesh-max-depth", str(mesh_max_depth)])
    if mesh_benchmark_label:
        command.extend(["--mesh-benchmark-label", mesh_benchmark_label])
    for provider in mesh_disable_provider:
        command.extend(["--mesh-disable-provider", provider])
    if prompt_prelude_file is not None:
        command.extend(["--prompt-prelude-file", str(prompt_prelude_file)])
    if dry_run:
        command.append("--dry-run")
    if not ensure_runtime:
        command.append("--no-ensure-runtime")
    if not dangerously_bypass:
        command.append("--no-dangerously-bypass")
    return command


def build_guardian_runtime_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("MOCK_LLM", "true")
    env.setdefault("ENABLE_SCHEDULER", "false")
    env.setdefault("STRICT_REAL_DATA", "false")
    env.setdefault("JWT_SECRET", "github-guardian-local-jwt-secret")
    env.setdefault("BILLING_WEBHOOK_SECRET", "github-guardian-local-billing-secret")
    return env


@dataclass(slots=True)
class StepResult:
    name: str
    command: list[str]
    cwd: str
    returncode: int
    stdout_path: str
    stderr_path: str
    started_at: str
    finished_at: str


def run_logged_command(
    *,
    name: str,
    command: list[str],
    cwd: Path,
    logs_dir: Path,
    env: dict[str, str] | None = None,
) -> StepResult:
    logs_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = logs_dir / f"{name}.stdout.log"
    stderr_path = logs_dir / f"{name}.stderr.log"
    started_at = now_utc().isoformat()
    completed = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        check=False,
    )
    finished_at = now_utc().isoformat()
    stdout_path.write_text(completed.stdout or "", encoding="utf-8")
    stderr_path.write_text(completed.stderr or "", encoding="utf-8")
    return StepResult(
        name=name,
        command=command,
        cwd=str(cwd),
        returncode=completed.returncode,
        stdout_path=str(stdout_path),
        stderr_path=str(stderr_path),
        started_at=started_at,
        finished_at=finished_at,
    )


def copy_file_if_exists(src: Path, dest: Path, copied: list[str], root: Path) -> None:
    if not src.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    copied.append(relative_to_root(dest, root))


def copy_tree_if_exists(src: Path, dest: Path, copied: list[str], root: Path) -> None:
    if not src.exists():
        return
    shutil.copytree(src, dest, dirs_exist_ok=True)
    copied.append(relative_to_root(dest, root))


def collect_run_artifacts(worktree: Path, run_dir: Path) -> list[str]:
    copied: list[str] = []
    audit_root = continuous_audit_dir(worktree)
    live_root = live_fix_loop_dir(worktree)
    copy_file_if_exists(
        audit_root / "latest_run.json",
        run_dir / "continuous_audit" / "latest_run.json",
        copied,
        worktree,
    )
    copy_file_if_exists(
        audit_root / "continuous_audit_issue_ledger.md",
        run_dir / "continuous_audit" / "continuous_audit_issue_ledger.md",
        copied,
        worktree,
    )
    copy_file_if_exists(
        live_root / "issue_register.md",
        run_dir / "live_fix_loop" / "issue_register.md",
        copied,
        worktree,
    )
    copy_file_if_exists(
        live_root / "review_log.md",
        run_dir / "live_fix_loop" / "review_log.md",
        copied,
        worktree,
    )
    copy_file_if_exists(
        worktree / "docs" / "_temp" / "stage123_loop" / "issue_register.md",
        run_dir / "stage123_loop" / "issue_register.md",
        copied,
        worktree,
    )
    copy_file_if_exists(
        worktree / "docs" / "_temp" / "stage123_loop" / "review_log.md",
        run_dir / "stage123_loop" / "review_log.md",
        copied,
        worktree,
    )
    copy_tree_if_exists(
        worktree / "docs" / "_temp" / "skill_readiness",
        run_dir / "skill_readiness",
        copied,
        worktree,
    )
    latest_summary_src = live_fix_loop_runs_dir(worktree) / "latest_summary.json"
    copy_file_if_exists(
        latest_summary_src,
        run_dir / "live_fix_loop" / "latest_summary.json",
        copied,
        worktree,
    )
    latest_summary = load_json(latest_summary_src)
    canonical_run = latest_summary.get("canonical_output_dir")
    if isinstance(canonical_run, str) and canonical_run.strip():
        copy_tree_if_exists(
            Path(canonical_run),
            run_dir / "live_fix_loop" / "canonical_run",
            copied,
            worktree,
        )
    raw_child_run = latest_summary.get("raw_child_output_dir")
    if isinstance(raw_child_run, str) and raw_child_run.strip():
        copy_tree_if_exists(
            Path(raw_child_run),
            run_dir / "live_fix_loop" / "raw_child_run",
            copied,
            worktree,
        )
    selected_run = latest_summary.get("output_dir")
    if isinstance(selected_run, str) and selected_run.strip():
        copy_tree_if_exists(
            Path(selected_run),
            run_dir / "live_fix_loop" / "selected_run",
            copied,
            worktree,
        )
    return copied


def summarize_audit_payload(payload: dict[str, Any]) -> dict[str, Any]:
    findings = payload.get("findings")
    if not isinstance(findings, list):
        findings = []
    severity_counts: dict[str, int] = {}
    finding_ids: list[str] = []
    for item in findings:
        if not isinstance(item, dict):
            continue
        severity = str(item.get("severity") or "unknown")
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        issue_id = item.get("issue_id")
        if isinstance(issue_id, str) and issue_id:
            finding_ids.append(issue_id)
    return {
        "available": bool(payload),
        "findings_count": len(findings),
        "severity_counts": severity_counts,
        "finding_ids": finding_ids,
        "registry_stats": payload.get("registry_stats", {}),
        "blind_spot_summary": payload.get("blind_spot_summary", {}),
    }


def summarize_fix_payload(payload: dict[str, Any], *, attempted: bool) -> dict[str, Any]:
    attempts = payload.get("attempts")
    if not isinstance(attempts, list):
        attempts = []
    runtime_preflight = payload.get("runtime_preflight")
    runtime_status = runtime_preflight.get("status") if isinstance(runtime_preflight, dict) else None
    return {
        "attempted": attempted,
        "available": bool(payload),
        "success": bool(payload.get("success")),
        "selected_provider": payload.get("selected_provider"),
        "attempt_count": len(attempts),
        "provider_order": payload.get("provider_order", []),
        "runtime_status": runtime_status,
        "run_status": payload.get("run_status"),
        "promotion_status": payload.get("promotion_status"),
        "canonical_state_updated": bool(payload.get("canonical_state_updated")),
        "canonical_output_dir": payload.get("canonical_output_dir"),
        "raw_child_output_dir": payload.get("raw_child_output_dir"),
    }


def git_status_paths(root: Path) -> list[str]:
    output = git_output(root, "status", "--porcelain", "--untracked-files=all")
    if not output:
        return []
    paths: list[str] = []
    for raw_line in output.splitlines():
        line = raw_line.rstrip()
        if len(line) < 4:
            continue
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.append(path.strip().strip('"'))
    return paths


def _matches_any(path: str, patterns: tuple[str, ...]) -> bool:
    normalized = normalize_repo_path(path)
    return any(fnmatch.fnmatch(normalized, pattern) for pattern in patterns)


def classify_paths_detailed(paths: list[str]) -> dict[str, Any]:
    artifact_changes: list[str] = []
    reference_changes: list[str] = []
    publishable_changes: list[str] = []
    other_changes: list[str] = []

    for path in paths:
        normalized = normalize_repo_path(path)
        if _matches_any(normalized, ARTIFACT_PATTERNS):
            artifact_changes.append(normalized)
        elif _matches_any(normalized, REFERENCE_ONLY_PATTERNS):
            reference_changes.append(normalized)
        elif _matches_any(normalized, PUBLISHABLE_PATTERNS):
            publishable_changes.append(normalized)
        else:
            other_changes.append(normalized)

    candidate_pr_changes = list(publishable_changes)
    return {
        "artifact_changes": artifact_changes,
        "reference_changes": reference_changes,
        "publishable_changes": publishable_changes,
        "other_changes": other_changes,
        "candidate_pr_changes": candidate_pr_changes,
        "artifact_only": bool(paths) and not candidate_pr_changes and not reference_changes and not other_changes,
        "pr_blocked_on_unclassified": bool(other_changes),
        "pr_publishable": bool(candidate_pr_changes) and not other_changes,
    }


def classify_paths(paths: list[str]) -> tuple[list[str], list[str]]:
    detailed = classify_paths_detailed(paths)
    return detailed["artifact_changes"], detailed["candidate_pr_changes"]


def worktree_support_patterns(*, mode: str) -> tuple[str, ...]:
    patterns = list(COMMON_WORKTREE_SUPPORT_PATTERNS)
    if mode == "audit-and-fix":
        patterns.extend(FIX_WORKTREE_SUPPORT_PATTERNS)
    return tuple(patterns)


def iter_support_files(root: Path, *, mode: str = "audit-and-fix") -> list[tuple[Path, str]]:
    discovered: dict[str, Path] = {}

    def _record(candidate: Path) -> None:
        if not candidate.is_file():
            return
        rel = normalize_repo_path(str(candidate.relative_to(root)))
        if _matches_any(rel, WORKTREE_SUPPORT_SKIP_PATTERNS):
            return
        discovered.setdefault(rel, candidate)

    for pattern in worktree_support_patterns(mode=mode):
        has_glob = any(char in pattern for char in "*?[]")
        candidate = root / pattern
        if not has_glob:
            if candidate.is_file():
                _record(candidate)
            elif candidate.is_dir():
                for nested in candidate.rglob("*"):
                    _record(nested)
            continue
        for matched in root.glob(pattern):
            if matched.is_dir():
                for nested in matched.rglob("*"):
                    _record(nested)
            else:
                _record(matched)

    return [(path, rel) for rel, path in sorted(discovered.items())]


def sync_worktree_support_files(
    source_root: Path,
    worktree: Path,
    *,
    mode: str = "audit-and-fix",
    overwrite_existing: bool = False,
) -> list[str]:
    copied: list[str] = []
    for src, rel in iter_support_files(source_root, mode=mode):
        dest = worktree / rel
        if dest.exists() and not overwrite_existing:
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        copied.append(rel)
    return copied


def ensure_worktree_runtime_dirs(worktree: Path) -> list[str]:
    created: list[str] = []
    for rel in ("data", "output", "docs/_temp", "github/automation", "runtime"):
        path = worktree / rel
        path.mkdir(parents=True, exist_ok=True)
        created.append(rel)
    return created


def commit_all_changes(root: Path, message: str) -> str:
    env = os.environ.copy()
    env.setdefault("GIT_AUTHOR_NAME", "Codex Automation")
    env.setdefault("GIT_AUTHOR_EMAIL", "codex-automation@local")
    env.setdefault("GIT_COMMITTER_NAME", env["GIT_AUTHOR_NAME"])
    env.setdefault("GIT_COMMITTER_EMAIL", env["GIT_AUTHOR_EMAIL"])
    add_result = subprocess.run(
        ["git", "add", "--all"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if add_result.returncode != 0:
        raise RuntimeError(add_result.stderr.strip() or add_result.stdout.strip() or "git add failed")
    commit_result = subprocess.run(
        ["git", "commit", "-m", message],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if commit_result.returncode != 0:
        raise RuntimeError(commit_result.stderr.strip() or commit_result.stdout.strip() or "git commit failed")
    return git_output(root, "rev-parse", "HEAD")


def push_branch(root: Path, *, remote: str, branch: str, token: str) -> None:
    header = basic_auth_header(token)
    push_url = resolve_push_url(remote_url(root, remote))
    result = subprocess.run(
        ["git", "-c", f"http.extraHeader={header}", "push", push_url, f"HEAD:refs/heads/{branch}"],
        cwd=root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "git push failed")


def create_worktree(repo: Path, *, branch: str, base_ref: str, worktree: Path) -> None:
    worktree.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["git", "worktree", "add", "--force", "-b", branch, str(worktree), base_ref],
        cwd=repo,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "git worktree add failed")


def remove_worktree(repo: Path, worktree: Path) -> None:
    subprocess.run(
        ["git", "worktree", "remove", "--force", str(worktree)],
        cwd=repo,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_latest_pointer(root: Path, payload: dict[str, Any]) -> None:
    write_json(root / "github" / "automation" / "latest.json", payload)


def persist_run_state(
    *,
    worktree: Path,
    artifact_dir: Path,
    manifest: dict[str, Any],
    latest_payload: dict[str, Any],
) -> None:
    write_json(artifact_dir / "manifest.json", manifest)
    write_latest_pointer(worktree, latest_payload)


def mirror_local_artifacts(
    *,
    repo_root: Path,
    artifact_dir: Path,
    run_id: str,
    manifest: dict[str, Any],
    latest_payload: dict[str, Any],
) -> Path:
    mirror_root = default_local_mirror_root(repo_root)
    mirror_dir = mirror_root / run_id
    if mirror_dir.exists():
        shutil.rmtree(mirror_dir)
    mirror_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(artifact_dir, mirror_dir)
    write_json(mirror_dir / "manifest.json", manifest)
    latest_path = default_local_latest_path(repo_root)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return mirror_dir


def build_pr_body(
    *,
    run_id: str,
    branch: str,
    base_ref: str,
    findings_count: int,
    code_changes: list[str],
    artifact_dir: str,
) -> str:
    lines = [
        f"Automated repair run `{run_id}`.",
        "",
        f"- Base ref: `{base_ref}`",
        f"- Head branch: `{branch}`",
        f"- Findings detected: `{findings_count}`",
        f"- Artifact dir: `{artifact_dir}`",
        "",
        "Changed non-artifact paths:",
    ]
    if code_changes:
        for path in code_changes[:20]:
            lines.append(f"- `{path}`")
    else:
        lines.append("- `(none)`")
    lines.append("")
    lines.append("This PR was created by `scripts/github_guardian.py`.")
    return "\n".join(lines)


def github_api_request(
    *,
    token: str,
    url: str,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "codex-github-guardian",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {method} {url} failed: {exc.code} {detail}") from exc


def create_pull_request(
    *,
    token: str,
    repo_slug: str,
    head_branch: str,
    base_ref: str,
    title: str,
    body: str,
) -> dict[str, Any]:
    owner = repo_slug.split("/", 1)[0]
    return github_api_request(
        token=token,
        url=f"https://api.github.com/repos/{repo_slug}/pulls",
        method="POST",
        payload={
            "title": title,
            "head": f"{owner}:{head_branch}",
            "base": base_ref,
            "body": body,
            "maintainer_can_modify": True,
        },
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run repository audit/fix automation in an isolated worktree and push artifacts to GitHub.")
    parser.add_argument("--mode", choices=["audit-only", "audit-and-fix"], default="audit-and-fix")
    parser.add_argument("--remote", default=DEFAULT_REMOTE)
    parser.add_argument("--base-ref", default=None)
    parser.add_argument("--branch-prefix", default=DEFAULT_BRANCH_PREFIX)
    parser.add_argument("--artifact-root", type=Path, default=default_artifact_root())
    parser.add_argument("--worktree-root", type=Path, default=default_worktree_root(repo_root()))
    parser.add_argument("--token-path", type=Path, default=default_token_path(repo_root()))
    parser.add_argument("--providers", nargs="+", default=DEFAULT_PROVIDERS)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--sandbox", default="danger-full-access")
    parser.add_argument("--delegate-mode", choices=["legacy", "mesh"], default="legacy")
    parser.add_argument("--mesh-max-workers", type=int, default=4)
    parser.add_argument("--mesh-max-depth", type=int, default=2)
    parser.add_argument("--mesh-benchmark-label", default=None)
    parser.add_argument("--mesh-disable-provider", action="append", default=["api.925214.xyz"])
    parser.add_argument("--push", action="store_true")
    parser.add_argument("--open-pr", action="store_true")
    parser.add_argument("--dry-run-fix", action="store_true")
    parser.add_argument("--no-ensure-runtime", dest="ensure_runtime", action="store_false", default=True)
    parser.add_argument("--no-dangerously-bypass", dest="dangerously_bypass", action="store_false", default=True)
    parser.add_argument("--keep-worktree", action="store_true")
    args = parser.parse_args(argv)
    if args.open_pr and not args.push:
        parser.error("--open-pr requires --push")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = repo_root()
    base_ref = resolve_base_ref(root, remote=args.remote, explicit_base_ref=args.base_ref)
    run_id = utc_run_id()
    branch = f"{args.branch_prefix}/{run_id.lower()}"
    worktree = args.worktree_root / run_id
    started_at = now_utc().isoformat()
    token = None
    repo_remote_url = None
    repo_slug = None
    if args.push or args.open_pr:
        token = load_github_token(args.token_path)
        repo_remote_url = remote_url(root, args.remote)
        repo_slug = parse_repo_slug(repo_remote_url)
        if args.push and not repo_slug:
            raise RuntimeError(f"Remote does not point to a GitHub repository: {repo_remote_url}")

    create_worktree(root, branch=branch, base_ref=base_ref, worktree=worktree)

    push_result = {"enabled": args.push, "status": "skipped", "branch": branch, "remote": args.remote}
    pr_result: dict[str, Any] = {"enabled": args.open_pr, "status": "skipped"}
    commit_sha: str | None = None
    head_commit_sha: str | None = None
    overall_success = True
    local_mirror_dir: Path | None = None
    synced_support_files: list[str] = []
    ensured_runtime_dirs: list[str] = []
    try:
        synced_support_files = sync_worktree_support_files(
            root,
            worktree,
            mode=args.mode,
            overwrite_existing=not (args.push or args.open_pr),
        )
        ensured_runtime_dirs = ensure_worktree_runtime_dirs(worktree)
        python = resolve_python(root)
        artifact_root = args.artifact_root if args.artifact_root.is_absolute() else worktree / args.artifact_root
        artifact_dir = artifact_root / run_id
        logs_dir = artifact_dir / "logs"
        skill_readiness = collect_skill_readiness(
            root,
            list(args.providers),
            mode=args.mode,
            push=args.push,
            open_pr=args.open_pr,
            remote=args.remote,
        )
        write_json(artifact_dir / "skills_readiness.json", skill_readiness)
        skills_prompt_file = artifact_dir / "skills_readiness_prompt.txt"
        skills_prompt_file.write_text(build_skill_readiness_prompt(skill_readiness), encoding="utf-8")
        steps: list[StepResult] = []

        steps.append(
            run_logged_command(
                name="continuous_repo_audit",
                command=build_audit_command(python),
                cwd=worktree,
                logs_dir=logs_dir,
                env=build_guardian_runtime_env(),
            )
        )
        if steps[-1].returncode != 0:
            overall_success = False

        steps.append(
            run_logged_command(
                name="live_fix_loop_init",
                command=build_live_fix_init_command(python, worktree),
                cwd=worktree,
                logs_dir=logs_dir,
            )
        )
        if steps[-1].returncode != 0:
            overall_success = False

        fix_attempted = args.mode == "audit-and-fix"
        fix_allowed = skill_readiness["summary"]["ready_count"] > 0
        if fix_attempted and fix_allowed:
            fix_env = build_guardian_runtime_env()
            fix_env["CODEX_HOME_ROOT"] = str(root / "ai-api" / "codex")
            steps.append(
                run_logged_command(
                    name="codex_prompt6_hourly",
                    command=build_fix_command(
                        python,
                        providers=list(args.providers),
                        base_url=args.base_url,
                        dry_run=args.dry_run_fix,
                        ensure_runtime=args.ensure_runtime,
                        dangerously_bypass=args.dangerously_bypass,
                        sandbox=args.sandbox,
                        delegate_mode=args.delegate_mode,
                        mesh_max_workers=args.mesh_max_workers,
                        mesh_max_depth=args.mesh_max_depth,
                        mesh_benchmark_label=args.mesh_benchmark_label,
                        mesh_disable_provider=list(args.mesh_disable_provider),
                        prompt_prelude_file=skills_prompt_file,
                    ),
                    cwd=worktree,
                    logs_dir=logs_dir,
                    env=fix_env,
                )
            )
            if steps[-1].returncode != 0:
                overall_success = False
        elif fix_attempted:
            overall_success = False

        copied_artifacts = collect_run_artifacts(worktree, artifact_dir)
        audit_payload = load_json(continuous_audit_dir(worktree) / "latest_run.json")
        fix_payload = load_json(live_fix_loop_runs_dir(worktree) / "latest_summary.json")
        audit_summary = summarize_audit_payload(audit_payload)
        fix_summary = summarize_fix_payload(fix_payload, attempted=fix_attempted and fix_allowed)
        if fix_attempted and not fix_allowed:
            fix_summary["blocked_reason"] = "no_ready_skills"

        changed_paths = git_status_paths(worktree)
        path_summary = classify_paths_detailed(changed_paths)

        latest_payload = {
            "run_id": run_id,
            "branch": branch,
            "base_ref": base_ref,
            "artifact_dir": relative_to_root(artifact_dir, worktree),
            "manifest_path": relative_to_root(artifact_dir / "manifest.json", worktree),
            "finished_at": now_utc().isoformat(),
            "audit_findings_count": audit_summary["findings_count"],
            "fix_success": fix_summary["success"],
            "ready_skills_count": skill_readiness["summary"]["ready_count"],
            "fake_available_skills": skill_readiness["summary"]["fake_available_skills"],
        }

        manifest = {
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": latest_payload["finished_at"],
            "mode": args.mode,
            "branch": branch,
            "base_ref": base_ref,
            "worktree": str(worktree),
            "artifact_dir": relative_to_root(artifact_dir, worktree),
            "steps": [asdict(step) for step in steps],
            "audit": audit_summary,
            "fix": fix_summary,
            "skills_readiness": skill_readiness,
            "workspace_support": {
                "synced_file_count": len(synced_support_files),
                "synced_files": synced_support_files,
                "ensured_runtime_dirs": ensured_runtime_dirs,
                "overwrite_existing": not (args.push or args.open_pr),
            },
            "git": {
                "changed_paths": changed_paths,
                "artifact_changes": path_summary["artifact_changes"],
                "reference_changes": path_summary["reference_changes"],
                "publishable_changes": path_summary["publishable_changes"],
                "other_changes": path_summary["other_changes"],
                "candidate_pr_changes": path_summary["candidate_pr_changes"],
                "artifact_only": path_summary["artifact_only"],
                "pr_blocked_on_unclassified": path_summary["pr_blocked_on_unclassified"],
            },
            "copied_artifacts": copied_artifacts,
            "push": push_result,
            "pull_request": pr_result,
        }
        persist_run_state(worktree=worktree, artifact_dir=artifact_dir, manifest=manifest, latest_payload=latest_payload)

        changed_paths = git_status_paths(worktree)
        path_summary = classify_paths_detailed(changed_paths)
        manifest["git"] = {
            "changed_paths": changed_paths,
            "artifact_changes": path_summary["artifact_changes"],
            "reference_changes": path_summary["reference_changes"],
            "publishable_changes": path_summary["publishable_changes"],
            "other_changes": path_summary["other_changes"],
            "candidate_pr_changes": path_summary["candidate_pr_changes"],
            "artifact_only": path_summary["artifact_only"],
            "pr_blocked_on_unclassified": path_summary["pr_blocked_on_unclassified"],
        }
        persist_run_state(worktree=worktree, artifact_dir=artifact_dir, manifest=manifest, latest_payload=latest_payload)

        if changed_paths:
            commit_sha = commit_all_changes(worktree, f"automation: {args.mode} {run_id}")
        else:
            overall_success = False

        if args.push and commit_sha and token:
            push_branch(worktree, remote=args.remote, branch=branch, token=token)
            push_result["status"] = "success"
            push_result["commit_sha"] = commit_sha
        elif args.push:
            push_result["status"] = "failed"
            overall_success = False

        if args.open_pr and push_result["status"] != "success":
            pr_result = {
                "enabled": True,
                "status": "failed",
                "reason": "push_not_successful",
            }
            overall_success = False
        elif args.open_pr and path_summary["pr_blocked_on_unclassified"]:
            pr_result = {
                "enabled": True,
                "status": "skipped",
                "reason": "unclassified_changes_present",
            }
        elif args.open_pr and not path_summary["candidate_pr_changes"]:
            pr_result = {
                "enabled": True,
                "status": "skipped",
                "reason": "artifact_or_reference_only_changes",
            }
        elif args.open_pr and commit_sha and token and repo_slug:
            if path_summary["candidate_pr_changes"]:
                pr = create_pull_request(
                    token=token,
                    repo_slug=repo_slug,
                    head_branch=branch,
                    base_ref=base_ref,
                    title=f"automation: repair findings {run_id}",
                    body=build_pr_body(
                        run_id=run_id,
                        branch=branch,
                        base_ref=base_ref,
                        findings_count=audit_summary["findings_count"],
                        code_changes=path_summary["candidate_pr_changes"],
                        artifact_dir=relative_to_root(artifact_dir, worktree),
                    ),
                )
                pr_result = {
                    "enabled": True,
                    "status": "success",
                    "number": pr.get("number"),
                    "html_url": pr.get("html_url"),
                }
            else:
                pr_result = {
                    "enabled": True,
                    "status": "skipped",
                    "reason": "artifact_or_reference_only_changes",
                }
        elif args.open_pr:
            pr_result = {
                "enabled": True,
                "status": "failed",
                "reason": "missing_commit_or_repo_or_token",
            }
            overall_success = False

        if args.push or args.open_pr:
            manifest["push"] = push_result
            manifest["pull_request"] = pr_result
            latest_payload["push_status"] = push_result["status"]
            latest_payload["pull_request_status"] = pr_result["status"]
            latest_payload["finished_at"] = now_utc().isoformat()
            manifest["finished_at"] = latest_payload["finished_at"]
            persist_run_state(worktree=worktree, artifact_dir=artifact_dir, manifest=manifest, latest_payload=latest_payload)

            post_status_changes = git_status_paths(worktree)
            if post_status_changes:
                commit_sha = commit_all_changes(worktree, f"automation: finalize {run_id}")
                if args.push and token:
                    push_branch(worktree, remote=args.remote, branch=branch, token=token)
                head_commit_sha = commit_sha
            elif commit_sha:
                head_commit_sha = commit_sha

        if head_commit_sha:
            latest_payload["head_commit_sha"] = head_commit_sha
            if args.push:
                push_result["commit_sha"] = head_commit_sha

        local_mirror_dir = mirror_local_artifacts(
            repo_root=root,
            artifact_dir=artifact_dir,
            run_id=run_id,
            manifest=manifest,
            latest_payload=latest_payload,
        )

        print(
            json.dumps(
                {
                    "run_id": run_id,
                    "success": overall_success,
                    "branch": branch,
                    "artifact_dir": relative_to_root(artifact_dir, worktree),
                    "findings_count": audit_summary["findings_count"],
                    "fix_success": fix_summary["success"],
                    "push_status": push_result["status"],
                    "pull_request_status": pr_result["status"],
                    "head_commit_sha": head_commit_sha,
                    "ready_skills_count": skill_readiness["summary"]["ready_count"],
                    "fake_available_skills": skill_readiness["summary"]["fake_available_skills"],
                    "local_mirror_dir": relative_to_root(local_mirror_dir, root) if local_mirror_dir else None,
                },
                ensure_ascii=False,
            )
        )
        return 0 if overall_success else 1
    finally:
        if not args.keep_worktree:
            remove_worktree(root, worktree)


if __name__ == "__main__":
    raise SystemExit(main())
