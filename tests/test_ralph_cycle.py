from __future__ import annotations

from codex import ralph_cycle


class _Summary:
    def __init__(self, **payload):
        self._payload = payload
        for key, value in payload.items():
            setattr(self, key, value)

    def to_dict(self):
        return dict(self._payload)


def test_run_cycles_completes_when_second_rebuild_is_green(monkeypatch, tmp_path):
    rebuilds = iter(
        [
            _Summary(
                mode="rebuild",
                changed_docs=[],
                changed_prd=[],
                stories_total=2,
                stories_passed=1,
                stories_failed=1,
                new_story_ids=[],
                regressed_story_ids=[],
                blocked_external_ids=[],
                story_set_hash="hash-a",
                baseline_commit_created=False,
                baseline_commit=None,
            ),
            _Summary(
                mode="rebuild",
                changed_docs=[],
                changed_prd=[],
                stories_total=2,
                stories_passed=2,
                stories_failed=0,
                new_story_ids=[],
                regressed_story_ids=[],
                blocked_external_ids=[],
                story_set_hash="hash-a",
                baseline_commit_created=False,
                baseline_commit=None,
            ),
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

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1)

    assert summary.final_status == "complete"
    assert summary.stories_remaining == 0


def test_run_cycles_blocks_when_step2_blocks_and_story_set_stable(monkeypatch, tmp_path):
    rebuilds = iter(
        [
            _Summary(
                mode="rebuild",
                changed_docs=[],
                changed_prd=[],
                stories_total=2,
                stories_passed=1,
                stories_failed=1,
                new_story_ids=[],
                regressed_story_ids=[],
                blocked_external_ids=[],
                story_set_hash="hash-a",
                baseline_commit_created=False,
                baseline_commit=None,
            ),
            _Summary(
                mode="rebuild",
                changed_docs=[],
                changed_prd=[],
                stories_total=2,
                stories_passed=1,
                stories_failed=1,
                new_story_ids=[],
                regressed_story_ids=[],
                blocked_external_ids=[],
                story_set_hash="hash-a",
                baseline_commit_created=False,
                baseline_commit=None,
            ),
        ]
    )
    monkeypatch.setattr(ralph_cycle.ralph_compile, "verify_repo", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("needs rebuild")))
    monkeypatch.setattr(ralph_cycle.ralph_compile, "rebuild_repo", lambda **kwargs: next(rebuilds))
    monkeypatch.setattr(ralph_cycle, "run_ralph_step2", lambda **kwargs: {"status": "blocked", "returncode": 2, "output": ""})

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1)

    assert summary.final_status == "blocked"
    assert summary.stories_remaining == 1


def test_run_cycles_short_circuits_with_verify_and_adjudicate(monkeypatch, tmp_path):
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

    summary = ralph_cycle.run_cycles(repo_root=tmp_path, max_cycles=1)

    assert summary.final_status == "complete"
    assert summary.stories_remaining == 0
