from __future__ import annotations

from codex import ralph_cycle


class _Summary:
    def __init__(self, **payload):
        self._payload = payload
        for key, value in payload.items():
            setattr(self, key, value)

    def to_dict(self):
        return dict(self._payload)


def _story_summary(*, stories_total: int = 2, stories_passed: int = 1, stories_failed: int = 1, story_set_hash: str = "hash-a"):
    return _Summary(
        mode="rebuild",
        changed_docs=[],
        changed_prd=[],
        stories_total=stories_total,
        stories_passed=stories_passed,
        stories_failed=stories_failed,
        new_story_ids=[],
        regressed_story_ids=[],
        blocked_external_ids=[],
        story_set_hash=story_set_hash,
        baseline_commit_created=False,
        baseline_commit=None,
    )


def _branch_state(
    *,
    expected_branch: str = "ralph/ashare-research-platform",
    last_branch: str = "ralph/ashare-research-platform",
    current_branch: str = "ralph/ashare-research-platform",
    head_only_count: int = 0,
    expected_branch_only_count: int = 0,
    tracked_changes: list[str] | None = None,
    verify_summary: dict | None = None,
):
    payload = {
        "expected_branch": expected_branch,
        "last_branch": last_branch,
        "current_branch": current_branch,
        "branch_distance": {
            "head_only_count": head_only_count,
            "expected_branch_only_count": expected_branch_only_count,
        },
        "tracked_changes": list(tracked_changes or []),
    }
    if verify_summary is not None:
        payload["verify_summary"] = verify_summary
    return payload


def test_tracked_changes_preserves_unicode_paths(monkeypatch, tmp_path):
    calls = []

    class _Proc:
        def __init__(self, stdout: str):
            self.returncode = 0
            self.stdout = stdout
            self.stderr = ""

    def fake_git_run(repo_root, *args):
        calls.append(args)
        if args[1] == "--cached":
            return _Proc("")
        return _Proc("docs/core/30_Ralph双步自举运行手册.md\0codex/ralph_cycle.py\0")

    monkeypatch.setattr(ralph_cycle, "_git_run", fake_git_run)

    assert ralph_cycle._tracked_changes(tmp_path) == [
        "codex/ralph_cycle.py",
        "docs/core/30_Ralph双步自举运行手册.md",
    ]
    assert calls == [
        ("diff", "--name-only", "-z"),
        ("diff", "--cached", "--name-only", "-z"),
    ]


def test_run_cycles_core_completes_when_second_rebuild_is_green(monkeypatch, tmp_path):
    rebuilds = iter(
        [
            _story_summary(),
            _story_summary(stories_passed=2, stories_failed=0),
        ]
    )
    verify_calls = iter([RuntimeError("needs rebuild"), _Summary(mode="verify")])

    def fake_verify(**kwargs):
        result = next(verify_calls)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(ralph_cycle.ralph_compile, "verify_repo", fake_verify)
    monkeypatch.setattr(ralph_cycle.ralph_compile, "rebuild_repo", lambda **kwargs: next(rebuilds))
    monkeypatch.setattr(ralph_cycle, "run_ralph_step2", lambda **kwargs: {"status": "complete", "returncode": 0, "output": ""})

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1, enforce_preflight=False)

    assert summary.final_status == "complete"
    assert summary.stories_remaining == 0


def test_run_cycles_core_blocks_when_step2_blocks_and_story_set_stable(monkeypatch, tmp_path):
    rebuilds = iter(
        [
            _story_summary(),
            _story_summary(),
        ]
    )
    monkeypatch.setattr(ralph_cycle.ralph_compile, "verify_repo", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("needs rebuild")))
    monkeypatch.setattr(ralph_cycle.ralph_compile, "rebuild_repo", lambda **kwargs: next(rebuilds))
    monkeypatch.setattr(ralph_cycle, "run_ralph_step2", lambda **kwargs: {"status": "blocked", "returncode": 2, "output": ""})

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1, enforce_preflight=False)

    assert summary.final_status == "blocked"
    assert summary.stories_remaining == 1


def test_run_cycles_core_short_circuits_with_verify_and_adjudicate(monkeypatch, tmp_path):
    monkeypatch.setattr(
        ralph_cycle.ralph_compile,
        "verify_repo",
        lambda **kwargs: _Summary(
            mode="verify",
            stories_total=108,
            stories_passed=108,
            stories_failed=0,
            new_story_ids=[],
            regressed_story_ids=[],
            blocked_external_ids=[],
            story_set_hash="hash-complete",
            changed_docs=[],
            changed_prd=[],
            baseline_commit_created=False,
            baseline_commit=None,
        ),
    )
    monkeypatch.setattr(
        ralph_cycle.ralph_compile,
        "adjudicate_repo",
        lambda **kwargs: _Summary(
            mode="adjudicate",
            stories_total=108,
            stories_passed=108,
            stories_failed=0,
            new_story_ids=[],
            regressed_story_ids=[],
            blocked_external_ids=[],
            story_set_hash="hash-complete",
            changed_docs=[],
            changed_prd=[],
            baseline_commit_created=False,
            baseline_commit=None,
        ),
    )

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1, enforce_preflight=False)

    assert summary.final_status == "complete"
    assert summary.stories_remaining == 0


def test_run_cycles_stops_on_branch_drift_before_core_runner(monkeypatch, tmp_path):
    branch_state = _branch_state(current_branch="main", expected_branch_only_count=3)
    preflight = [
        ralph_cycle.PreflightCheck(
            "branch_policy",
            "fail",
            "branch drift detected; expected=ralph/ashare-research-platform; current=main",
            data=branch_state,
        )
    ]

    monkeypatch.setattr(
        ralph_cycle,
        "_run_preflight_checks",
        lambda **kwargs: (branch_state, preflight, "branch_drift", preflight[0].detail),
    )
    monkeypatch.setattr(
        ralph_cycle,
        "_run_cycles_core",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("core cycle runner must not execute")),
    )

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=5)

    assert summary.final_status == "branch_drift"
    assert summary.cycles_run == 0
    assert summary.status_reason == preflight[0].detail
    assert summary.expected_branch == "ralph/ashare-research-platform"
    assert summary.last_branch == "ralph/ashare-research-platform"
    assert summary.current_branch == "main"
    assert summary.branch_distance == {"head_only_count": 0, "expected_branch_only_count": 3}
    assert summary.preflight == preflight


def test_run_cycles_stops_when_workspace_is_dirty(monkeypatch, tmp_path):
    tracked_changes = ["docs/core/plan.md", ".claude/ralph/loop/prd.json"]
    branch_state = _branch_state(tracked_changes=tracked_changes)
    preflight = [
        ralph_cycle.PreflightCheck("branch_policy", "pass", "branch aligned", data=branch_state),
        ralph_cycle.PreflightCheck(
            "workspace_clean",
            "fail",
            "tracked changes present: docs/core/plan.md, .claude/ralph/loop/prd.json",
            data={"tracked_changes": tracked_changes},
        ),
    ]

    monkeypatch.setattr(
        ralph_cycle,
        "_run_preflight_checks",
        lambda **kwargs: (branch_state, preflight, "workspace_dirty", preflight[1].detail),
    )
    monkeypatch.setattr(
        ralph_cycle,
        "_run_cycles_core",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("core cycle runner must not execute")),
    )

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=5)

    assert summary.final_status == "workspace_dirty"
    assert summary.cycles_run == 0
    assert summary.status_reason == preflight[1].detail
    assert summary.tracked_changes == tracked_changes
    assert [item.name for item in summary.preflight] == ["branch_policy", "workspace_clean"]


def test_run_cycles_successful_preflight_delegates_to_core_and_preserves_metadata(monkeypatch, tmp_path):
    verify_summary = {
        "stories_total": 108,
        "stories_passed": 108,
        "stories_failed": 0,
    }
    branch_state = _branch_state(verify_summary=verify_summary)
    preflight = [
        ralph_cycle.PreflightCheck("branch_policy", "pass", "branch aligned", data=branch_state),
        ralph_cycle.PreflightCheck("workspace_clean", "pass", "tracked git diff is clean"),
        ralph_cycle.PreflightCheck("check_state", "pass", "Active tasks: 0; Total published: 9", returncode=0),
        ralph_cycle.PreflightCheck("verify", "pass", "stories_total=108; stories_passed=108; stories_failed=0", data=verify_summary),
        ralph_cycle.PreflightCheck("runner_dry_run", "pass", "runner dry-run passed", returncode=0),
        ralph_cycle.PreflightCheck("targeted_pytest", "pass", "targeted Ralph pytest passed", returncode=0),
    ]
    core_summary = ralph_cycle.CycleSummary(
        cycles_run=1,
        final_status="complete",
        stories_total=108,
        stories_passed=108,
        stories_remaining=0,
        new_story_ids_last_cycle=[],
        regressed_story_ids_last_cycle=[],
        history=[],
    )
    captured = {}

    monkeypatch.setattr(
        ralph_cycle,
        "_run_preflight_checks",
        lambda **kwargs: (branch_state, preflight, None, None),
    )

    def fake_run_cycles_core(**kwargs):
        captured.update(kwargs)
        return core_summary

    monkeypatch.setattr(ralph_cycle, "_run_cycles_core", fake_run_cycles_core)

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, tool="claude", max_cycles=7)

    assert summary is core_summary
    assert captured == {"repo_root": tmp_path, "tool": "claude", "max_cycles": 7}
    assert summary.expected_branch == "ralph/ashare-research-platform"
    assert summary.last_branch == "ralph/ashare-research-platform"
    assert summary.current_branch == "ralph/ashare-research-platform"
    assert summary.branch_distance == {"head_only_count": 0, "expected_branch_only_count": 0}
    assert summary.tracked_changes == []
    assert summary.preflight == preflight
    assert summary.to_dict()["preflight"][3]["data"] == verify_summary
