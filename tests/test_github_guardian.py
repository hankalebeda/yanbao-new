from __future__ import annotations

import json
import subprocess
from pathlib import Path

from scripts import github_guardian


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or f"git {' '.join(args)} failed")
    return completed.stdout.strip()


def _init_git_repo(repo: Path, *, default_branch: str = "main") -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-b", default_branch)
    _git(repo, "config", "user.name", "Test User")
    _git(repo, "config", "user.email", "test@example.com")
    (repo / "README.md").write_text("# temp\n", encoding="utf-8")
    (repo / ".gitignore").write_text(
        "github/automation/_local/\ngithub/automation/continuous_audit/\ngithub/automation/live_fix_loop/\nruntime/\n",
        encoding="utf-8",
    )
    _git(repo, "add", "README.md", ".gitignore")
    _git(repo, "commit", "-m", "init")


def _fake_run_logged_command(*, name, command, cwd, logs_dir, env=None):
    logs_dir.mkdir(parents=True, exist_ok=True)
    if name == "continuous_repo_audit":
        target = cwd / "github" / "automation" / "continuous_audit"
        target.mkdir(parents=True, exist_ok=True)
        (target / "latest_run.json").write_text(
            json.dumps({"findings": [{"issue_id": "A-1", "severity": "high"}]}),
            encoding="utf-8",
        )
        (target / "continuous_audit_issue_ledger.md").write_text("# ledger\n", encoding="utf-8")
    elif name == "live_fix_loop_init":
        target = cwd / "github" / "automation" / "live_fix_loop"
        target.mkdir(parents=True, exist_ok=True)
        (target / "issue_register.md").write_text("# issues\n", encoding="utf-8")
        (target / "review_log.md").write_text("# review\n", encoding="utf-8")

    stdout_path = logs_dir / f"{name}.stdout.log"
    stderr_path = logs_dir / f"{name}.stderr.log"
    stdout_path.write_text("", encoding="utf-8")
    stderr_path.write_text("", encoding="utf-8")
    return github_guardian.StepResult(
        name=name,
        command=command,
        cwd=str(cwd),
        returncode=0,
        stdout_path=str(stdout_path),
        stderr_path=str(stderr_path),
        started_at="2026-03-22T00:00:00+00:00",
        finished_at="2026-03-22T00:00:01+00:00",
    )


def _fake_run_logged_command_with_fix_outputs(*, name, command, cwd, logs_dir, env=None):
    result = _fake_run_logged_command(name=name, command=command, cwd=cwd, logs_dir=logs_dir, env=env)
    if name == "codex_prompt6_hourly":
        target = cwd / "github" / "automation" / "live_fix_loop" / "automation_runs"
        target.mkdir(parents=True, exist_ok=True)
        canonical_dir = target / "canonical-run"
        raw_dir = target / "raw-child-run"
        canonical_dir.mkdir(parents=True, exist_ok=True)
        raw_dir.mkdir(parents=True, exist_ok=True)
        (canonical_dir / "summary.json").write_text("{}", encoding="utf-8")
        (raw_dir / "summary.json").write_text("{}", encoding="utf-8")
        (target / "latest_summary.json").write_text(
            json.dumps(
                {
                    "success": True,
                    "selected_provider": "infiniteai.cc",
                    "attempts": [],
                    "output_dir": str(canonical_dir),
                    "canonical_output_dir": str(canonical_dir),
                    "raw_child_output_dir": str(raw_dir),
                    "run_status": "completed",
                    "promotion_status": "promoted",
                    "canonical_state_updated": True,
                }
            ),
            encoding="utf-8",
        )
    return result


def _fake_run_logged_command_with_unclassified_change(*, name, command, cwd, logs_dir, env=None):
    result = _fake_run_logged_command(name=name, command=command, cwd=cwd, logs_dir=logs_dir, env=env)
    if name == "continuous_repo_audit":
        target = cwd / "notes"
        target.mkdir(parents=True, exist_ok=True)
        (target / "random.txt").write_text("unclassified\n", encoding="utf-8")
    return result


def _probe(*, available: bool = True, verified: bool = True, path: str | None = "/bin/tool", detail: str = "ok") -> dict[str, object]:
    return {
        "available": available,
        "verified": verified,
        "path": path if available else None,
        "detail": detail,
    }


def _install_skill_files(repo: Path, providers: tuple[str, ...], skill_name: str, *, missing: tuple[str, ...] = ()) -> None:
    for provider in providers:
        base = repo / "ai-api" / "codex" / provider / "skills" / skill_name
        for rel in github_guardian.MANAGED_SKILLS[skill_name]["required_files"]:
            if rel in missing:
                continue
            path = base / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("x", encoding="utf-8")


def test_probe_command_verifies_with_resolved_path(monkeypatch):
    recorded: list[list[str]] = []

    class DummyCompleted:
        returncode = 0
        stdout = "ok"
        stderr = ""

    monkeypatch.setattr(github_guardian, "command_path", lambda name: r"C:\tools\npx.CMD")
    monkeypatch.setattr(
        github_guardian.subprocess,
        "run",
        lambda command, **kwargs: recorded.append(command) or DummyCompleted(),
    )

    probe = github_guardian.probe_command("npx", verify_args=["--version"])

    assert probe["verified"] is True
    assert recorded == [[r"C:\tools\npx.CMD", "--version"]]


def test_parse_repo_slug_supports_https_and_ssh():
    assert github_guardian.parse_repo_slug("https://github.com/openai/skills.git") == "openai/skills"
    assert github_guardian.parse_repo_slug("git@github.com:openai/codex.git") == "openai/codex"
    assert github_guardian.parse_repo_slug("https://example.com/not-github.git") is None


def test_resolve_push_url_normalizes_github_ssh_and_https():
    assert github_guardian.resolve_push_url("git@github.com:openai/codex.git") == "https://github.com/openai/codex.git"
    assert github_guardian.resolve_push_url("https://github.com/openai/codex.git") == "https://github.com/openai/codex.git"


def test_basic_auth_header_uses_github_token_format():
    header = github_guardian.basic_auth_header("secret-token")

    assert header.startswith("AUTHORIZATION: basic ")
    assert "secret-token" not in header


def test_parse_args_rejects_open_pr_without_push():
    try:
        github_guardian.parse_args(["--open-pr"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected parse_args to reject --open-pr without --push")


def test_build_fix_command_includes_providers_and_flags():
    command = github_guardian.build_fix_command(
        "python",
        providers=["sub.jlypx.de", "infiniteai.cc"],
        base_url="http://127.0.0.1:8000",
        dry_run=True,
        ensure_runtime=False,
        dangerously_bypass=False,
        sandbox="danger-full-access",
        delegate_mode="mesh",
        mesh_max_workers=3,
        mesh_max_depth=2,
        mesh_benchmark_label="analysis-review",
        mesh_disable_provider=["api.925214.xyz"],
        prompt_prelude_file=Path("prompt.txt"),
    )

    assert command[:2] == ["python", "scripts/codex_prompt6_hourly.py"]
    assert "--delegate-mode" in command
    assert "mesh" in command
    assert "--providers" in command
    assert "--mesh-max-workers" in command
    assert "--mesh-max-depth" in command
    assert "--mesh-disable-provider" in command
    assert "--prompt-prelude-file" in command
    assert "--dry-run" in command
    assert "--no-ensure-runtime" in command
    assert "--no-dangerously-bypass" in command


def test_build_guardian_runtime_env_sets_local_runtime_defaults(monkeypatch):
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.delenv("BILLING_WEBHOOK_SECRET", raising=False)

    env = github_guardian.build_guardian_runtime_env()

    assert env["MOCK_LLM"] == "true"
    assert env["ENABLE_SCHEDULER"] == "false"
    assert env["STRICT_REAL_DATA"] == "false"
    assert env["JWT_SECRET"] == "github-guardian-local-jwt-secret"
    assert env["BILLING_WEBHOOK_SECRET"] == "github-guardian-local-billing-secret"


def test_resolve_base_ref_prefers_remote_head_when_detached(monkeypatch, tmp_path):
    repo = tmp_path / "repo"

    def fake_git_output(root, *args, check=True):
        if args == ("branch", "--show-current"):
            return ""
        if args == ("symbolic-ref", "refs/remotes/origin/HEAD"):
            return "refs/remotes/origin/trunk"
        if args == ("rev-parse", "HEAD"):
            return "deadbeef"
        return ""

    monkeypatch.setattr(github_guardian, "git_output", fake_git_output)

    result = github_guardian.resolve_base_ref(repo, remote="origin")

    assert result == "trunk"


def test_resolve_base_ref_falls_back_to_head_sha(monkeypatch, tmp_path):
    repo = tmp_path / "repo"

    def fake_git_output(root, *args, check=True):
        if args == ("branch", "--show-current"):
            return ""
        if args == ("symbolic-ref", "refs/remotes/origin/HEAD"):
            return ""
        if args == ("rev-parse", "HEAD"):
            return "deadbeef"
        return ""

    class DummyCompleted:
        returncode = 1

    monkeypatch.setattr(github_guardian, "git_output", fake_git_output)
    monkeypatch.setattr(github_guardian.subprocess, "run", lambda *args, **kwargs: DummyCompleted())

    result = github_guardian.resolve_base_ref(repo, remote="origin")

    assert result == "deadbeef"


def test_resolve_base_ref_detached_local_main_still_uses_head_sha(tmp_path):
    repo = tmp_path / "repo"
    _init_git_repo(repo, default_branch="main")
    expected = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "--detach", "HEAD")

    result = github_guardian.resolve_base_ref(repo, remote="origin")

    assert result == expected


def test_select_relevant_skills_excludes_security_for_general_fix_flow():
    relevant = github_guardian.select_relevant_skills(
        mode="audit-and-fix",
        push=False,
        open_pr=False,
        repo_fit={
            "has_fastapi": True,
            "has_web_frontend": True,
            "has_browser_gate": True,
            "has_github_remote": True,
        },
    )

    assert relevant == ["playwright"]


def test_collect_skill_readiness_detects_fake_github_skills(monkeypatch, tmp_path):
    repo = tmp_path / "repo"
    providers = ("sub.jlypx.de", "infiniteai.cc")
    _install_skill_files(repo, providers, "gh-fix-ci")
    _install_skill_files(repo, providers, "security-best-practices")

    readiness = github_guardian.collect_skill_readiness(
        repo,
        list(providers),
        push=True,
        dependency_probes={
            "python": _probe(path="/bin/python"),
            "gh": _probe(available=False, verified=False, path=None, detail="command_missing"),
            "gh_auth": _probe(available=False, verified=False, path=None, detail="gh_missing"),
        },
    )

    assert readiness["skills"]["gh-fix-ci"]["status"] == "unavailable"
    assert readiness["skills"]["gh-fix-ci"]["dependencies_missing"] == ["gh", "gh_auth"]
    assert "gh-fix-ci" in readiness["summary"]["fake_available_skills"]
    assert readiness["skills"]["security-best-practices"]["status"] == "ready"


def test_collect_skill_readiness_does_not_count_security_for_general_fix_gate(tmp_path):
    repo = tmp_path / "repo"
    providers = ("sub.jlypx.de", "infiniteai.cc")
    _install_skill_files(repo, providers, "security-best-practices")
    (repo / "app" / "web").mkdir(parents=True, exist_ok=True)

    readiness = github_guardian.collect_skill_readiness(
        repo,
        list(providers),
        mode="audit-and-fix",
        dependency_probes={
            "python": _probe(path="/bin/python"),
            "node": _probe(path="/bin/node"),
            "npx": _probe(path="/bin/npx"),
            "bash": _probe(path="/bin/bash"),
        },
    )

    assert readiness["summary"]["relevant_skills"] == ["playwright"]
    assert readiness["summary"]["ready_count"] == 0
    assert readiness["summary"]["not_selected_skills"] == ["security-best-practices", "gh-fix-ci", "gh-address-comments", "babysit-pr"]
    assert readiness["skills"]["security-best-practices"]["status"] == "ready"
    assert readiness["skills"]["security-best-practices"]["relevant"] is False


def test_collect_skill_readiness_marks_playwright_partial_without_bash_on_windows(monkeypatch, tmp_path):
    repo = tmp_path / "repo"
    providers = ("sub.jlypx.de", "infiniteai.cc")
    _install_skill_files(repo, providers, "playwright")
    _install_skill_files(repo, providers, "security-best-practices")
    (repo / "app" / "web").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(github_guardian.os, "name", "nt")

    readiness = github_guardian.collect_skill_readiness(
        repo,
        list(providers),
        dependency_probes={
            "python": _probe(path="/bin/python"),
            "node": _probe(path="/bin/node"),
            "npx": _probe(path="/bin/npx"),
            "bash": _probe(available=False, verified=False, path=None, detail="command_missing"),
        },
    )

    assert readiness["skills"]["playwright"]["status"] == "partial"
    assert readiness["skills"]["playwright"]["soft_dependencies_missing"] == ["bash"]
    assert "playwright" in readiness["summary"]["partial_skills"]


def test_collect_skill_readiness_marks_github_skills_unavailable_when_auth_fails(tmp_path):
    repo = tmp_path / "repo"
    providers = ("sub.jlypx.de", "infiniteai.cc")
    _install_skill_files(repo, providers, "gh-fix-ci")
    _install_skill_files(repo, providers, "security-best-practices")

    readiness = github_guardian.collect_skill_readiness(
        repo,
        list(providers),
        push=True,
        dependency_probes={
            "python": _probe(path="/bin/python"),
            "gh": _probe(path="/bin/gh"),
            "gh_auth": _probe(available=True, verified=False, path="/bin/gh", detail="auth_failed"),
        },
    )

    assert readiness["skills"]["gh-fix-ci"]["status"] == "unavailable"
    assert readiness["skills"]["gh-fix-ci"]["dependencies_missing"] == ["gh_auth"]
    assert "gh-fix-ci" in readiness["summary"]["fake_available_skills"]


def test_collect_skill_readiness_requires_fetch_comments_script(tmp_path):
    repo = tmp_path / "repo"
    providers = ("sub.jlypx.de", "infiniteai.cc")
    _install_skill_files(repo, providers, "gh-address-comments", missing=("scripts/fetch_comments.py",))
    _install_skill_files(repo, providers, "security-best-practices")

    readiness = github_guardian.collect_skill_readiness(
        repo,
        list(providers),
        open_pr=True,
        dependency_probes={
            "python": _probe(path="/bin/python"),
            "gh": _probe(path="/bin/gh"),
            "gh_auth": _probe(path="/bin/gh"),
        },
    )

    assert readiness["skills"]["gh-address-comments"]["status"] == "unavailable"
    assert readiness["skills"]["gh-address-comments"]["provider_file_gaps"] == {
        "sub.jlypx.de": ["scripts/fetch_comments.py"],
        "infiniteai.cc": ["scripts/fetch_comments.py"],
    }


def test_build_skill_readiness_prompt_flags_degraded_skills():
    prompt = github_guardian.build_skill_readiness_prompt(
        {
            "providers": ["sub.jlypx.de", "infiniteai.cc"],
            "summary": {
                "relevant_skills": ["playwright", "gh-fix-ci"],
                "ready_skills": ["playwright"],
                "partial_skills": [],
                "unavailable_skills": ["gh-fix-ci"],
                "degraded_skills": ["gh-fix-ci"],
                "overall_status": "degraded",
            },
            "skills": {
                "gh-fix-ci": {
                    "providers_missing": [],
                    "provider_file_gaps": {},
                    "dependencies_missing": ["gh"],
                    "soft_dependencies_missing": [],
                }
            },
        }
    )

    assert "Ready skills: playwright" in prompt
    assert "Unavailable skills: gh-fix-ci" in prompt
    assert "gh-fix-ci: missing deps=gh" in prompt
    assert "do not pretend to use it" in prompt


def test_collect_run_artifacts_copies_expected_files(tmp_path):
    worktree = tmp_path / "repo"
    run_dir = worktree / "github" / "automation" / "runs" / "20260322t000000z"
    (worktree / "github" / "automation" / "continuous_audit").mkdir(parents=True, exist_ok=True)
    (worktree / "github" / "automation" / "live_fix_loop" / "automation_runs" / "canonical-run").mkdir(parents=True, exist_ok=True)
    (worktree / "github" / "automation" / "live_fix_loop" / "automation_runs" / "raw-child-run").mkdir(parents=True, exist_ok=True)
    (worktree / "docs" / "_temp" / "stage123_loop").mkdir(parents=True, exist_ok=True)
    (worktree / "docs" / "_temp" / "skill_readiness").mkdir(parents=True, exist_ok=True)
    (worktree / "github" / "automation" / "continuous_audit" / "latest_run.json").write_text('{"findings":[]}', encoding="utf-8")
    (worktree / "github" / "automation" / "continuous_audit" / "continuous_audit_issue_ledger.md").write_text("# audit\n", encoding="utf-8")
    (worktree / "github" / "automation" / "live_fix_loop" / "issue_register.md").write_text("# issues\n", encoding="utf-8")
    (worktree / "github" / "automation" / "live_fix_loop" / "review_log.md").write_text("# review\n", encoding="utf-8")
    (worktree / "docs" / "_temp" / "stage123_loop" / "issue_register.md").write_text("# stage123\n", encoding="utf-8")
    (worktree / "docs" / "_temp" / "stage123_loop" / "review_log.md").write_text("# stage123 review\n", encoding="utf-8")
    (worktree / "docs" / "_temp" / "skill_readiness" / "latest.json").write_text("{}", encoding="utf-8")
    (worktree / "github" / "automation" / "live_fix_loop" / "automation_runs" / "canonical-run" / "summary.json").write_text("{}", encoding="utf-8")
    (worktree / "github" / "automation" / "live_fix_loop" / "automation_runs" / "raw-child-run" / "summary.json").write_text("{}", encoding="utf-8")
    (worktree / "github" / "automation" / "live_fix_loop" / "automation_runs" / "latest_summary.json").write_text(
        json.dumps(
            {
                "output_dir": str(worktree / "github" / "automation" / "live_fix_loop" / "automation_runs" / "canonical-run"),
                "canonical_output_dir": str(worktree / "github" / "automation" / "live_fix_loop" / "automation_runs" / "canonical-run"),
                "raw_child_output_dir": str(worktree / "github" / "automation" / "live_fix_loop" / "automation_runs" / "raw-child-run"),
            }
        ),
        encoding="utf-8",
    )

    copied = github_guardian.collect_run_artifacts(worktree, run_dir)

    assert "github/automation/runs/20260322t000000z/continuous_audit/latest_run.json" in copied
    assert "github/automation/runs/20260322t000000z/stage123_loop/issue_register.md" in copied
    assert "github/automation/runs/20260322t000000z/skill_readiness" in copied
    assert (run_dir / "live_fix_loop" / "canonical_run" / "summary.json").exists()
    assert (run_dir / "live_fix_loop" / "raw_child_run" / "summary.json").exists()
    assert (run_dir / "live_fix_loop" / "selected_run" / "summary.json").exists()


def test_classify_paths_separates_artifacts_publishable_and_reference_changes():
    summary = github_guardian.classify_paths_detailed(
        [
            "github/automation/runs/20260322/manifest.json",
            "github/automation/continuous_audit/latest_run.json",
            "tests/legacy_api.py",
            "app/main.py",
            "docs/core/24_repo_index.md",
        ]
    )

    assert summary["artifact_changes"] == [
        "github/automation/runs/20260322/manifest.json",
        "github/automation/continuous_audit/latest_run.json",
    ]
    assert summary["reference_changes"] == ["tests/legacy_api.py"]
    assert summary["publishable_changes"] == ["app/main.py", "docs/core/24_repo_index.md"]
    assert summary["candidate_pr_changes"] == ["app/main.py", "docs/core/24_repo_index.md"]
    assert summary["artifact_only"] is False


def test_classify_paths_blocks_pr_on_unclassified_changes():
    summary = github_guardian.classify_paths_detailed(
        [
            "ai-api/codex/infiniteai.cc/skills/playwright/SKILL.md",
            ".cursor/skills/sc-test/SKILL.md",
            "docs/提示词/18_全量自动化提示词.md",
            "notes/random.txt",
            "data/app.db",
        ]
    )

    assert summary["artifact_changes"] == ["data/app.db"]
    assert summary["publishable_changes"] == [
        "ai-api/codex/infiniteai.cc/skills/playwright/SKILL.md",
        ".cursor/skills/sc-test/SKILL.md",
        "docs/提示词/18_全量自动化提示词.md",
    ]
    assert summary["other_changes"] == ["notes/random.txt"]
    assert summary["candidate_pr_changes"] == [
        "ai-api/codex/infiniteai.cc/skills/playwright/SKILL.md",
        ".cursor/skills/sc-test/SKILL.md",
        "docs/提示词/18_全量自动化提示词.md",
    ]
    assert summary["pr_blocked_on_unclassified"] is True
    assert summary["pr_publishable"] is False


def test_classify_paths_treats_github_docs_as_publishable():
    summary = github_guardian.classify_paths_detailed(
        [
            "github/README.md",
            "github/docs/02_运行矩阵与目录约定.md",
        ]
    )

    assert summary["artifact_changes"] == []
    assert summary["publishable_changes"] == [
        "github/README.md",
        "github/docs/02_运行矩阵与目录约定.md",
    ]
    assert summary["pr_publishable"] is True


def test_iter_support_files_collects_runtime_automation_scope(tmp_path):
    root = tmp_path / "repo"
    (root / "scripts" / "doc_driven").mkdir(parents=True, exist_ok=True)
    (root / "tests" / "data_source_test_results").mkdir(parents=True, exist_ok=True)
    (root / "app" / "governance").mkdir(parents=True, exist_ok=True)
    (root / "docs" / "core" / "test_results").mkdir(parents=True, exist_ok=True)
    (root / "docs" / "提示词").mkdir(parents=True, exist_ok=True)
    (root / "ai-api" / "codex" / "infiniteai.cc" / "skills" / "playwright").mkdir(parents=True, exist_ok=True)

    (root / "scripts" / "continuous_repo_audit.py").write_text("x", encoding="utf-8")
    (root / "scripts" / "doc_driven" / "page_expectations.py").write_text("x", encoding="utf-8")
    (root / "tests" / "conftest.py").write_text("x", encoding="utf-8")
    (root / "tests" / "test_alpha.py").write_text("x", encoding="utf-8")
    (root / "tests" / "data_source_test_results" / "result.json").write_text("x", encoding="utf-8")
    (root / "app" / "governance" / "build_feature_catalog.py").write_text("x", encoding="utf-8")
    (root / "app" / "governance" / "feature_registry.json").write_text("{}", encoding="utf-8")
    (root / "docs" / "core" / "01_a.md").write_text("x", encoding="utf-8")
    (root / "docs" / "core" / "test_results" / "ignore.json").write_text("x", encoding="utf-8")
    (root / "docs" / "提示词" / "18_全量自动化提示词.md").write_text("x", encoding="utf-8")
    (root / "ai-api" / "codex" / "infiniteai.cc" / "skills" / "playwright" / "SKILL.md").write_text("x", encoding="utf-8")

    files = github_guardian.iter_support_files(root)
    rels = [rel for _, rel in files]

    assert "scripts/continuous_repo_audit.py" in rels
    assert "scripts/doc_driven/page_expectations.py" in rels
    assert "tests/test_alpha.py" in rels
    assert "app/governance/build_feature_catalog.py" in rels
    assert "app/governance/feature_registry.json" in rels
    assert "docs/core/01_a.md" in rels
    assert "docs/提示词/18_全量自动化提示词.md" in rels
    assert "ai-api/codex/infiniteai.cc/skills/playwright/SKILL.md" not in rels
    assert "tests/data_source_test_results/result.json" not in rels
    assert "docs/core/test_results/ignore.json" not in rels


def test_iter_support_files_audit_only_excludes_fix_only_assets(tmp_path):
    root = tmp_path / "repo"
    (root / ".cursor" / "rules").mkdir(parents=True, exist_ok=True)
    (root / "docs" / "提示词").mkdir(parents=True, exist_ok=True)
    (root / "scripts").mkdir(parents=True, exist_ok=True)
    (root / ".cursor" / "rules" / "rule.mdc").write_text("x", encoding="utf-8")
    (root / "docs" / "提示词" / "18_全量自动化提示词.md").write_text("x", encoding="utf-8")
    (root / "scripts" / "codex_prompt6_hourly.py").write_text("x", encoding="utf-8")
    (root / "scripts" / "continuous_repo_audit.py").write_text("x", encoding="utf-8")
    (root / "scripts" / "live_fix_loop.py").write_text("x", encoding="utf-8")

    files = github_guardian.iter_support_files(root, mode="audit-only")
    rels = [rel for _, rel in files]

    assert "scripts/continuous_repo_audit.py" in rels
    assert "scripts/live_fix_loop.py" in rels
    assert "scripts/codex_prompt6_hourly.py" not in rels
    assert ".cursor/rules/rule.mdc" not in rels
    assert "docs/提示词/18_全量自动化提示词.md" not in rels


def test_sync_worktree_support_files_copies_required_runtime_assets(tmp_path):
    source = tmp_path / "source"
    worktree = tmp_path / "worktree"
    (source / "scripts").mkdir(parents=True, exist_ok=True)
    (source / "docs" / "提示词").mkdir(parents=True, exist_ok=True)
    (source / "scripts" / "continuous_repo_audit.py").write_text("audit", encoding="utf-8")
    (source / "docs" / "提示词" / "18_全量自动化提示词.md").write_text("prompt", encoding="utf-8")

    copied = github_guardian.sync_worktree_support_files(source, worktree)

    assert "scripts/continuous_repo_audit.py" in copied
    assert "docs/提示词/18_全量自动化提示词.md" in copied
    assert (worktree / "scripts" / "continuous_repo_audit.py").read_text(encoding="utf-8") == "audit"
    assert (worktree / "docs" / "提示词" / "18_全量自动化提示词.md").read_text(encoding="utf-8") == "prompt"


def test_sync_worktree_support_files_audit_only_skips_fix_only_assets(tmp_path):
    source = tmp_path / "source"
    worktree = tmp_path / "worktree"
    (source / "scripts").mkdir(parents=True, exist_ok=True)
    (source / "docs" / "提示词").mkdir(parents=True, exist_ok=True)
    (source / "scripts" / "continuous_repo_audit.py").write_text("audit", encoding="utf-8")
    (source / "scripts" / "codex_prompt6_hourly.py").write_text("fix", encoding="utf-8")
    (source / "docs" / "提示词" / "18_全量自动化提示词.md").write_text("prompt", encoding="utf-8")

    copied = github_guardian.sync_worktree_support_files(source, worktree, mode="audit-only")

    assert "scripts/continuous_repo_audit.py" in copied
    assert "scripts/codex_prompt6_hourly.py" not in copied
    assert "docs/提示词/18_全量自动化提示词.md" not in copied


def test_sync_worktree_support_files_does_not_override_existing_worktree_files(tmp_path):
    source = tmp_path / "source"
    worktree = tmp_path / "worktree"
    (source / "scripts").mkdir(parents=True, exist_ok=True)
    (worktree / "scripts").mkdir(parents=True, exist_ok=True)
    (source / "scripts" / "continuous_repo_audit.py").write_text("source", encoding="utf-8")
    (worktree / "scripts" / "continuous_repo_audit.py").write_text("existing", encoding="utf-8")

    copied = github_guardian.sync_worktree_support_files(source, worktree)

    assert "scripts/continuous_repo_audit.py" not in copied
    assert (worktree / "scripts" / "continuous_repo_audit.py").read_text(encoding="utf-8") == "existing"


def test_sync_worktree_support_files_can_override_existing_when_requested(tmp_path):
    source = tmp_path / "source"
    worktree = tmp_path / "worktree"
    (source / "scripts").mkdir(parents=True, exist_ok=True)
    (worktree / "scripts").mkdir(parents=True, exist_ok=True)
    (source / "scripts" / "continuous_repo_audit.py").write_text("source", encoding="utf-8")
    (worktree / "scripts" / "continuous_repo_audit.py").write_text("existing", encoding="utf-8")

    copied = github_guardian.sync_worktree_support_files(source, worktree, overwrite_existing=True)

    assert "scripts/continuous_repo_audit.py" in copied
    assert (worktree / "scripts" / "continuous_repo_audit.py").read_text(encoding="utf-8") == "source"


def test_ensure_worktree_runtime_dirs_creates_expected_directories(tmp_path):
    worktree = tmp_path / "worktree"

    created = github_guardian.ensure_worktree_runtime_dirs(worktree)

    assert created == ["data", "output", "docs/_temp", "github/automation", "runtime"]
    for rel in created:
        assert (worktree / rel).exists()


def test_summarize_payloads_extract_counts():
    audit = github_guardian.summarize_audit_payload(
        {
            "findings": [
                {"issue_id": "A-1", "severity": "high"},
                {"issue_id": "A-2", "severity": "medium"},
            ]
        }
    )
    fix = github_guardian.summarize_fix_payload(
        {
            "success": True,
            "selected_provider": "infiniteai.cc",
            "attempts": [{"provider": "sub.jlypx.de"}, {"provider": "infiniteai.cc"}],
            "provider_order": ["sub.jlypx.de", "infiniteai.cc"],
            "runtime_preflight": {"status": "healthy"},
            "run_status": "completed",
            "promotion_status": "promoted",
            "canonical_state_updated": True,
            "canonical_output_dir": "canonical-run",
            "raw_child_output_dir": "raw-child-run",
        },
        attempted=True,
    )

    assert audit["findings_count"] == 2
    assert audit["severity_counts"] == {"high": 1, "medium": 1}
    assert fix["success"] is True
    assert fix["attempt_count"] == 2
    assert fix["runtime_status"] == "healthy"
    assert fix["run_status"] == "completed"
    assert fix["promotion_status"] == "promoted"
    assert fix["canonical_state_updated"] is True


def test_main_audit_only_persists_local_artifacts_after_cleanup(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    worktrees = tmp_path / "worktrees"
    _init_git_repo(repo)

    monkeypatch.setattr(github_guardian, "repo_root", lambda: repo)
    monkeypatch.setattr(github_guardian, "utc_run_id", lambda: "20260322T000000Z")
    monkeypatch.setattr(github_guardian, "run_logged_command", _fake_run_logged_command)

    rc = github_guardian.main(
        [
            "--mode",
            "audit-only",
            "--worktree-root",
            str(worktrees),
        ]
    )

    assert rc == 0
    worktree_dir = worktrees / "20260322T000000Z"
    worktree_list = _git(repo, "worktree", "list")
    assert str(worktree_dir) not in worktree_list
    local_manifest = repo / "github" / "automation" / "_local" / "runs" / "20260322T000000Z" / "manifest.json"
    assert local_manifest.exists()
    payload = json.loads(local_manifest.read_text(encoding="utf-8"))
    assert payload["audit"]["findings_count"] == 1
    assert payload["fix"]["attempted"] is False


def test_main_syncs_support_files_into_worktree_before_running(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    worktrees = tmp_path / "worktrees"
    _init_git_repo(repo)
    (repo / "scripts").mkdir(parents=True, exist_ok=True)
    (repo / "scripts" / "continuous_repo_audit.py").write_text("print('audit')\n", encoding="utf-8")
    (repo / "scripts" / "live_fix_loop.py").write_text("print('loop')\n", encoding="utf-8")

    def assert_support_present(*, name, command, cwd, logs_dir, env=None):
        if name == "continuous_repo_audit":
            assert (cwd / "scripts" / "continuous_repo_audit.py").exists()
            assert env is not None
            assert env["JWT_SECRET"] == "github-guardian-local-jwt-secret"
            assert env["BILLING_WEBHOOK_SECRET"] == "github-guardian-local-billing-secret"
        if name == "live_fix_loop_init":
            assert (cwd / "scripts" / "live_fix_loop.py").exists()
        return _fake_run_logged_command(name=name, command=command, cwd=cwd, logs_dir=logs_dir, env=env)

    monkeypatch.setattr(github_guardian, "repo_root", lambda: repo)
    monkeypatch.setattr(github_guardian, "utc_run_id", lambda: "20260322T000500Z")
    monkeypatch.setattr(github_guardian, "run_logged_command", assert_support_present)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.delenv("BILLING_WEBHOOK_SECRET", raising=False)

    rc = github_guardian.main(
        [
            "--mode",
            "audit-only",
            "--worktree-root",
            str(worktrees),
            "--keep-worktree",
        ]
    )

    assert rc == 0
    manifest_path = worktrees / "20260322T000500Z" / "github" / "automation" / "runs" / "20260322T000500Z" / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["workspace_support"]["synced_file_count"] >= 2
    assert "scripts/continuous_repo_audit.py" in payload["workspace_support"]["synced_files"]
    assert payload["workspace_support"]["ensured_runtime_dirs"] == ["data", "output", "docs/_temp", "github/automation", "runtime"]
    assert payload["workspace_support"]["overwrite_existing"] is True


def test_main_audit_and_fix_blocks_when_no_ready_skills(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    worktrees = tmp_path / "worktrees"
    _init_git_repo(repo)

    monkeypatch.setattr(github_guardian, "repo_root", lambda: repo)
    monkeypatch.setattr(github_guardian, "utc_run_id", lambda: "20260322T005500Z")
    monkeypatch.setattr(github_guardian, "run_logged_command", _fake_run_logged_command)
    monkeypatch.setattr(
        github_guardian,
        "collect_skill_readiness",
        lambda root, providers, **kwargs: {
            "providers": providers,
            "skills": {},
            "summary": {
                "ready_skills": [],
                "degraded_skills": ["gh-fix-ci"],
                "ready_count": 0,
                "degraded_count": 1,
                "fake_available_skills": ["gh-fix-ci"],
                "overall_status": "degraded",
            },
        },
    )

    rc = github_guardian.main(
        [
            "--mode",
            "audit-and-fix",
            "--worktree-root",
            str(worktrees),
        ]
    )

    assert rc == 1
    local_manifest = repo / "github" / "automation" / "_local" / "runs" / "20260322T005500Z" / "manifest.json"
    payload = json.loads(local_manifest.read_text(encoding="utf-8"))
    assert payload["fix"]["attempted"] is False
    assert payload["fix"]["blocked_reason"] == "no_ready_skills"
    assert payload["skills_readiness"]["summary"]["ready_count"] == 0


def test_main_push_path_finalizes_manifest_and_leaves_clean_worktree(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    worktrees = tmp_path / "worktrees"
    _init_git_repo(repo)
    _git(repo, "remote", "add", "origin", "https://github.com/example/repo.git")

    monkeypatch.setattr(github_guardian, "repo_root", lambda: repo)
    monkeypatch.setattr(github_guardian, "utc_run_id", lambda: "20260322T010000Z")
    monkeypatch.setattr(github_guardian, "run_logged_command", _fake_run_logged_command)
    monkeypatch.setattr(github_guardian, "load_github_token", lambda token_path=None: "token")
    monkeypatch.setattr(github_guardian, "push_branch", lambda root, **kwargs: None)

    rc = github_guardian.main(
        [
            "--mode",
            "audit-only",
            "--worktree-root",
            str(worktrees),
            "--push",
            "--keep-worktree",
        ]
    )

    worktree = worktrees / "20260322T010000Z"
    assert rc == 0
    assert worktree.exists()
    assert _git(worktree, "status", "--short") == ""

    manifest_path = worktree / "github" / "automation" / "runs" / "20260322T010000Z" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["push"]["status"] == "success"
    assert manifest["pull_request"]["status"] == "skipped"

    log_lines = _git(worktree, "log", "--oneline", "--decorate=no").splitlines()
    assert len(log_lines) >= 3


def test_main_artifact_only_changes_skip_pr(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    worktrees = tmp_path / "worktrees"
    _init_git_repo(repo)
    _git(repo, "remote", "add", "origin", "https://github.com/example/repo.git")

    pr_called = {"value": False}

    def fake_create_pull_request(**kwargs):
        pr_called["value"] = True
        return {"number": 1, "html_url": "https://github.com/example/repo/pull/1"}

    monkeypatch.setattr(github_guardian, "repo_root", lambda: repo)
    monkeypatch.setattr(github_guardian, "utc_run_id", lambda: "20260322T020000Z")
    monkeypatch.setattr(github_guardian, "run_logged_command", _fake_run_logged_command)
    monkeypatch.setattr(github_guardian, "load_github_token", lambda token_path=None: "token")
    monkeypatch.setattr(github_guardian, "push_branch", lambda root, **kwargs: None)
    monkeypatch.setattr(github_guardian, "create_pull_request", fake_create_pull_request)

    rc = github_guardian.main(
        [
            "--mode",
            "audit-only",
            "--worktree-root",
            str(worktrees),
            "--push",
            "--open-pr",
            "--keep-worktree",
        ]
    )

    assert rc == 0
    assert pr_called["value"] is False
    manifest_path = worktrees / "20260322T020000Z" / "github" / "automation" / "runs" / "20260322T020000Z" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["git"]["artifact_only"] is True
    assert manifest["git"]["candidate_pr_changes"] == []
    assert manifest["pull_request"]["status"] == "skipped"
    assert manifest["pull_request"]["reason"] == "artifact_or_reference_only_changes"


def test_main_unclassified_changes_skip_pr(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    worktrees = tmp_path / "worktrees"
    _init_git_repo(repo)
    _git(repo, "remote", "add", "origin", "https://github.com/example/repo.git")

    pr_called = {"value": False}

    def fake_create_pull_request(**kwargs):
        pr_called["value"] = True
        return {"number": 1, "html_url": "https://github.com/example/repo/pull/1"}

    monkeypatch.setattr(github_guardian, "repo_root", lambda: repo)
    monkeypatch.setattr(github_guardian, "utc_run_id", lambda: "20260322T021500Z")
    monkeypatch.setattr(github_guardian, "run_logged_command", _fake_run_logged_command_with_unclassified_change)
    monkeypatch.setattr(github_guardian, "load_github_token", lambda token_path=None: "token")
    monkeypatch.setattr(github_guardian, "push_branch", lambda root, **kwargs: None)
    monkeypatch.setattr(github_guardian, "create_pull_request", fake_create_pull_request)

    rc = github_guardian.main(
        [
            "--mode",
            "audit-only",
            "--worktree-root",
            str(worktrees),
            "--push",
            "--open-pr",
            "--keep-worktree",
        ]
    )

    assert rc == 0
    assert pr_called["value"] is False
    manifest_path = worktrees / "20260322T021500Z" / "github" / "automation" / "runs" / "20260322T021500Z" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["git"]["pr_blocked_on_unclassified"] is True
    assert manifest["pull_request"]["status"] == "skipped"
    assert manifest["pull_request"]["reason"] == "unclassified_changes_present"


def test_main_fix_path_passes_skill_readiness_prelude_file(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    worktrees = tmp_path / "worktrees"
    _init_git_repo(repo)

    seen_fix_command: list[str] = []

    def capturing_run_logged_command(**kwargs):
        if kwargs["name"] == "codex_prompt6_hourly":
            seen_fix_command.extend(kwargs["command"])
        return _fake_run_logged_command_with_fix_outputs(**kwargs)

    monkeypatch.setattr(github_guardian, "repo_root", lambda: repo)
    monkeypatch.setattr(github_guardian, "utc_run_id", lambda: "20260322T030000Z")
    monkeypatch.setattr(github_guardian, "run_logged_command", capturing_run_logged_command)
    monkeypatch.setattr(
        github_guardian,
        "collect_skill_readiness",
        lambda root, providers, **kwargs: {
            "providers": providers,
            "skills": {"playwright": {"providers_missing": [], "provider_file_gaps": {}, "dependencies_missing": []}},
            "summary": {
                "ready_skills": ["playwright"],
                "degraded_skills": [],
                "ready_count": 1,
                "degraded_count": 0,
                "fake_available_skills": [],
                "overall_status": "ready",
            },
        },
    )

    rc = github_guardian.main(
        [
            "--mode",
            "audit-and-fix",
            "--worktree-root",
            str(worktrees),
        ]
    )

    assert rc == 0
    assert "--prompt-prelude-file" in seen_fix_command
