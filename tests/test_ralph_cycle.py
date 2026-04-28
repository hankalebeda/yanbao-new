from __future__ import annotations

from types import SimpleNamespace

import pytest

from codex import ralph_cycle


class _Summary:
    def __init__(self, **payload):
        self._payload = payload
        for key, value in payload.items():
            setattr(self, key, value)

    def to_dict(self):
        return dict(self._payload)


def _story_summary(
    *,
    stories_total: int = 2,
    stories_passed: int = 1,
    stories_failed: int = 1,
    story_set_hash: str = "hash-a",
    new_story_ids: list[str] | None = None,
):
    return _Summary(
        mode="rebuild",
        changed_docs=[],
        changed_prd=[],
        stories_total=stories_total,
        stories_passed=stories_passed,
        stories_failed=stories_failed,
        new_story_ids=list(new_story_ids or []),
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


def test_tool_policy_check_accepts_claude():
    check = ralph_cycle._tool_policy_check("claude")
    assert check.status == "pass"
    assert check.data == {"tool": "claude", "supported_tool": "claude"}


def test_tool_policy_check_rejects_non_claude():
    check = ralph_cycle._tool_policy_check("amp")
    assert check.status == "fail"
    assert "unsupported tool" in check.detail


def test_branch_check_fails_when_last_branch_missing():
    check = ralph_cycle._branch_check(_branch_state(last_branch=""))
    assert check.status == "fail"
    assert check.detail.startswith(".last-branch missing")


def test_branch_check_fails_when_last_branch_mismatches_expected():
    check = ralph_cycle._branch_check(_branch_state(last_branch="main"))
    assert check.status == "fail"
    assert check.detail.startswith(".last-branch mismatch")


def test_branch_check_fails_when_branch_tip_diverges():
    check = ralph_cycle._branch_check(_branch_state(expected_branch_only_count=1))
    assert check.status == "fail"
    assert check.detail.startswith("branch tip drift detected")


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
        return _Proc("docs/core/30_Ralph????????.md\0codex/ralph_cycle.py\0")

    monkeypatch.setattr(ralph_cycle, "_git_run", fake_git_run)

    assert ralph_cycle._tracked_changes(tmp_path) == [
        "codex/ralph_cycle.py",
        "docs/core/30_Ralph????????.md",
    ]
    assert calls == [
        ("diff", "--name-only", "-z"),
        ("diff", "--cached", "--name-only", "-z"),
    ]


def test_run_preflight_checks_rejects_non_claude_before_branch_probe(monkeypatch, tmp_path):
    monkeypatch.setattr(ralph_cycle, "_collect_branch_state", lambda repo_root: (_ for _ in ()).throw(AssertionError("branch state should not be collected")))
    monkeypatch.setattr(ralph_cycle, "_expected_branch", lambda repo_root: "ralph/ashare-research-platform")
    monkeypatch.setattr(ralph_cycle, "_read_last_branch", lambda repo_root: "ralph/ashare-research-platform")

    branch_state, checks, failure_status, status_reason = ralph_cycle._run_preflight_checks(repo_root=tmp_path, tool="amp")

    assert failure_status == "preflight_failed"
    assert status_reason == checks[0].detail
    assert checks[0].name == "tool_policy"
    assert branch_state["expected_branch"] == "ralph/ashare-research-platform"


def test_run_preflight_checks_returns_branch_drift_when_branch_state_collection_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(ralph_cycle, "_collect_branch_state", lambda repo_root: (_ for _ in ()).throw(RuntimeError("git exploded")))
    monkeypatch.setattr(ralph_cycle, "_expected_branch", lambda repo_root: "ralph/ashare-research-platform")
    monkeypatch.setattr(ralph_cycle, "_read_last_branch", lambda repo_root: "ralph/ashare-research-platform")

    branch_state, checks, failure_status, status_reason = ralph_cycle._run_preflight_checks(repo_root=tmp_path, tool="claude")

    assert failure_status == "branch_drift"
    assert checks[0].name == "tool_policy"
    assert checks[1].name == "branch_policy"
    assert "git exploded" in status_reason
    assert branch_state["current_branch"] is None


def test_run_preflight_checks_returns_preflight_failed_when_workspace_probe_raises(monkeypatch, tmp_path):
    branch_state = _branch_state()
    monkeypatch.setattr(ralph_cycle, "_collect_branch_state", lambda repo_root: branch_state.copy())
    monkeypatch.setattr(ralph_cycle, "_tracked_changes", lambda repo_root: (_ for _ in ()).throw(RuntimeError("diff failed")))

    _, checks, failure_status, status_reason = ralph_cycle._run_preflight_checks(repo_root=tmp_path, tool="claude")

    assert failure_status == "preflight_failed"
    assert checks[-1].name == "workspace_clean"
    assert "diff failed" in status_reason


@pytest.mark.parametrize(
    ("failing_stage", "expected_name", "expected_status"),
    [
        ("check_state", "check_state", "preflight_failed"),
        ("verify", "verify", "preflight_failed"),
        ("runner", "runner_dry_run", "preflight_failed"),
        ("pytest", "targeted_pytest", "preflight_failed"),
    ],
)
def test_run_preflight_checks_stops_at_first_runtime_failure(monkeypatch, tmp_path, failing_stage, expected_name, expected_status):
    branch_state = _branch_state()
    verify_payload = {"stories_total": 108, "stories_passed": 108, "stories_failed": 0}

    monkeypatch.setattr(ralph_cycle, "_collect_branch_state", lambda repo_root: branch_state.copy())
    monkeypatch.setattr(ralph_cycle, "_tracked_changes", lambda repo_root: [])
    monkeypatch.setattr(ralph_cycle, "_run_check_state", lambda repo_root: ralph_cycle.PreflightCheck("check_state", "pass", "ok", 0))
    monkeypatch.setattr(ralph_cycle, "_run_verify", lambda repo_root: (verify_payload, ralph_cycle.PreflightCheck("verify", "pass", "ok", data=verify_payload)))
    monkeypatch.setattr(ralph_cycle, "_run_runner_dry_run", lambda repo_root, tool: ralph_cycle.PreflightCheck("runner_dry_run", "pass", "ok", 0))
    monkeypatch.setattr(ralph_cycle, "_run_targeted_pytest", lambda repo_root: ralph_cycle.PreflightCheck("targeted_pytest", "pass", "ok", 0))

    if failing_stage == "check_state":
        monkeypatch.setattr(ralph_cycle, "_run_check_state", lambda repo_root: ralph_cycle.PreflightCheck("check_state", "fail", "bad state", 1))
    elif failing_stage == "verify":
        monkeypatch.setattr(ralph_cycle, "_run_verify", lambda repo_root: (None, ralph_cycle.PreflightCheck("verify", "fail", "verify blew up")))
    elif failing_stage == "runner":
        monkeypatch.setattr(ralph_cycle, "_run_runner_dry_run", lambda repo_root, tool: ralph_cycle.PreflightCheck("runner_dry_run", "fail", "dry run bad", 1))
    else:
        monkeypatch.setattr(ralph_cycle, "_run_targeted_pytest", lambda repo_root: ralph_cycle.PreflightCheck("targeted_pytest", "fail", "pytest bad", 1))

    _, checks, failure_status, status_reason = ralph_cycle._run_preflight_checks(repo_root=tmp_path, tool="claude")

    assert failure_status == expected_status
    assert checks[-1].name == expected_name
    assert status_reason == checks[-1].detail


def test_run_cycles_stops_on_branch_drift_before_core_runner(monkeypatch, tmp_path):
    branch_state = _branch_state(current_branch="main", expected_branch_only_count=3)
    preflight = [
        ralph_cycle.PreflightCheck("tool_policy", "pass", "tool=claude; supported=claude", data={"tool": "claude", "supported_tool": "claude"}),
        ralph_cycle.PreflightCheck(
            "branch_policy",
            "fail",
            "branch drift detected; expected=ralph/ashare-research-platform; current=main",
            data=branch_state,
        ),
    ]

    monkeypatch.setattr(
        ralph_cycle,
        "_run_preflight_checks",
        lambda **kwargs: (branch_state, preflight, "branch_drift", preflight[1].detail),
    )
    monkeypatch.setattr(
        ralph_cycle,
        "_run_cycles_core",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("core cycle runner must not execute")),
    )

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=5)

    assert summary.final_status == "branch_drift"
    assert summary.cycles_run == 0
    assert summary.status_reason == preflight[1].detail
    assert summary.expected_branch == "ralph/ashare-research-platform"
    assert summary.last_branch == "ralph/ashare-research-platform"
    assert summary.current_branch == "main"
    assert summary.branch_distance == {"head_only_count": 0, "expected_branch_only_count": 3}
    assert summary.preflight == preflight


def test_run_cycles_stops_when_workspace_is_dirty(monkeypatch, tmp_path):
    tracked_changes = ["docs/core/plan.md", ".claude/ralph/loop/prd.json"]
    branch_state = _branch_state(tracked_changes=tracked_changes)
    preflight = [
        ralph_cycle.PreflightCheck("tool_policy", "pass", "tool=claude; supported=claude", data={"tool": "claude", "supported_tool": "claude"}),
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
        lambda **kwargs: (branch_state, preflight, "workspace_dirty", preflight[2].detail),
    )
    monkeypatch.setattr(
        ralph_cycle,
        "_run_cycles_core",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("core cycle runner must not execute")),
    )

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=5)

    assert summary.final_status == "workspace_dirty"
    assert summary.cycles_run == 0
    assert summary.status_reason == preflight[2].detail
    assert summary.tracked_changes == tracked_changes
    assert [item.name for item in summary.preflight] == ["tool_policy", "branch_policy", "workspace_clean"]


def test_run_cycles_successful_preflight_delegates_to_core_and_preserves_metadata(monkeypatch, tmp_path):
    verify_summary = {
        "stories_total": 108,
        "stories_passed": 108,
        "stories_failed": 0,
    }
    branch_state = _branch_state(verify_summary=verify_summary)
    preflight = [
        ralph_cycle.PreflightCheck("tool_policy", "pass", "tool=claude; supported=claude", data={"tool": "claude", "supported_tool": "claude"}),
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
    assert summary.to_dict()["preflight"][4]["data"] == verify_summary


def test_run_cycles_core_completes_when_initial_rebuild_is_green(monkeypatch, tmp_path):
    verify_calls = []
    monkeypatch.setattr(ralph_cycle.ralph_compile, "rebuild_repo", lambda **kwargs: _story_summary(stories_passed=2, stories_failed=0))
    monkeypatch.setattr(ralph_cycle.ralph_compile, "verify_repo", lambda **kwargs: verify_calls.append(kwargs) or _Summary(mode="verify"))
    monkeypatch.setattr(ralph_cycle, "run_ralph_step2", lambda **kwargs: (_ for _ in ()).throw(AssertionError("step2 should not run")))

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1, enforce_preflight=False)

    assert summary.final_status == "complete"
    assert summary.stories_remaining == 0
    assert len(summary.history) == 1
    assert summary.history[0].status == "complete"
    assert len(verify_calls) == 1


def test_run_cycles_core_completes_when_second_rebuild_is_green(monkeypatch, tmp_path):
    rebuilds = iter([
        _story_summary(),
        _story_summary(stories_passed=2, stories_failed=0),
    ])

    monkeypatch.setattr(ralph_cycle.ralph_compile, "rebuild_repo", lambda **kwargs: next(rebuilds))
    monkeypatch.setattr(ralph_cycle.ralph_compile, "verify_repo", lambda **kwargs: _Summary(mode="verify"))
    monkeypatch.setattr(ralph_cycle, "run_ralph_step2", lambda **kwargs: {"status": "complete", "returncode": 0, "output": ""})

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1, enforce_preflight=False)

    assert summary.final_status == "complete"
    assert summary.stories_remaining == 0
    assert summary.history[0].status == "complete"


def test_run_cycles_core_blocks_when_step2_returns_nonzero(monkeypatch, tmp_path):
    rebuilds = iter([
        _story_summary(story_set_hash="before"),
        _story_summary(story_set_hash="after", new_story_ids=["US-109"]),
    ])

    monkeypatch.setattr(ralph_cycle.ralph_compile, "rebuild_repo", lambda **kwargs: next(rebuilds))
    monkeypatch.setattr(ralph_cycle, "run_ralph_step2", lambda **kwargs: {"status": "blocked", "returncode": 1, "output": "boom"})

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1, enforce_preflight=False)

    assert summary.final_status == "blocked"
    assert summary.status_reason == "step2_failed:returncode=1"
    assert summary.history[0].status == "blocked"


def test_run_cycles_core_blocks_when_post_step2_rebuild_raises(monkeypatch, tmp_path):
    rebuilds = iter([
        _story_summary(),
        RuntimeError("post rebuild broke"),
    ])

    def fake_rebuild(**kwargs):
        result = next(rebuilds)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(ralph_cycle.ralph_compile, "rebuild_repo", fake_rebuild)
    monkeypatch.setattr(ralph_cycle, "run_ralph_step2", lambda **kwargs: {"status": "complete", "returncode": 0, "output": ""})

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1, enforce_preflight=False)

    assert summary.final_status == "blocked"
    assert summary.status_reason == "post_rebuild_failed:post rebuild broke"
    assert summary.history[0].rebuild_after["mode"] == "error"


def test_run_cycles_core_blocks_when_verify_after_green_rebuild_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(ralph_cycle.ralph_compile, "rebuild_repo", lambda **kwargs: _story_summary(stories_passed=2, stories_failed=0))
    monkeypatch.setattr(ralph_cycle.ralph_compile, "verify_repo", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("verify boom")))

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1, enforce_preflight=False)

    assert summary.final_status == "blocked"
    assert summary.status_reason == "verify_after_rebuild_failed:verify boom"


def test_run_cycles_core_becomes_incomplete_when_max_cycles_exhausted(monkeypatch, tmp_path):
    rebuilds = iter([
        _story_summary(story_set_hash="hash-a"),
        _story_summary(story_set_hash="hash-b"),
        _story_summary(story_set_hash="hash-c"),
        _story_summary(story_set_hash="hash-d"),
    ])

    monkeypatch.setattr(ralph_cycle.ralph_compile, "rebuild_repo", lambda **kwargs: next(rebuilds))
    monkeypatch.setattr(ralph_cycle, "run_ralph_step2", lambda **kwargs: {"status": "complete", "returncode": 0, "output": ""})

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=2, enforce_preflight=False)

    assert summary.final_status == "incomplete"
    assert summary.cycles_run == 2
    assert [item.status for item in summary.history] == ["continue", "continue"]
