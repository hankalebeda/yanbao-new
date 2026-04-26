from __future__ import annotations

import json
import sys
import types
from pathlib import Path

from scripts import live_fix_loop


def _issue_register(root: Path) -> Path:
    return root / "github" / "automation" / "live_fix_loop" / "issue_register.md"


def _review_log(root: Path) -> Path:
    return root / "github" / "automation" / "live_fix_loop" / "review_log.md"


def test_live_fix_loop_init_creates_ledgers(tmp_path):
    rc = live_fix_loop.main(["init", "--root", str(tmp_path)])

    assert rc == 0
    assert _issue_register(tmp_path).exists()
    assert _review_log(tmp_path).exists()
    assert "zero_open_streak: 0" in _issue_register(tmp_path).read_text(encoding="utf-8")
    assert "rounds_recorded: 0" in _review_log(tmp_path).read_text(encoding="utf-8")


def test_live_fix_loop_init_seeds_from_legacy_temp_dir(tmp_path):
    legacy_dir = tmp_path / "docs" / "_temp" / "live_fix_loop"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / "issue_register.md").write_text(live_fix_loop.issue_register_template(), encoding="utf-8")
    (legacy_dir / "review_log.md").write_text(live_fix_loop.review_log_template(), encoding="utf-8")

    rc = live_fix_loop.main(["init", "--root", str(tmp_path)])

    assert rc == 0
    assert _issue_register(tmp_path).exists()
    assert _review_log(tmp_path).exists()


def test_live_fix_loop_append_round_updates_streak_and_stop_allowed(tmp_path):
    assert live_fix_loop.main(["init", "--root", str(tmp_path)]) == 0

    rc1 = live_fix_loop.main(
        [
            "append-round",
            "--root",
            str(tmp_path),
            "--round-id",
            "round-001",
            "--focus",
            "FR-09 auth",
            "--new-high-value-issue-count",
            "0",
            "--fixed-count",
            "1",
            "--reopened-issue-count",
            "0",
            "--regression-failures",
            "0",
            "--open-issue-count",
            "0",
            "--closed-issue-count",
            "1",
            "--next-focus",
            "FR-10 reports",
            "--summary",
            "first clean round after auth repair",
        ]
    )
    assert rc1 == 0
    state1 = live_fix_loop._read_state(tmp_path)
    assert state1["zero_open_streak"] == 1
    assert state1["stop_allowed"] is False

    rc2 = live_fix_loop.main(
        [
            "append-round",
            "--root",
            str(tmp_path),
            "--round-id",
            "round-002",
            "--focus",
            "FR-10 reports",
            "--new-high-value-issue-count",
            "0",
            "--fixed-count",
            "0",
            "--reopened-issue-count",
            "0",
            "--regression-failures",
            "0",
            "--open-issue-count",
            "0",
            "--closed-issue-count",
            "1",
            "--next-focus",
            "FR-12 admin",
            "--summary",
            "second clean round confirmed",
        ]
    )
    assert rc2 == 0
    state2 = live_fix_loop._read_state(tmp_path)
    assert state2["zero_open_streak"] == 2
    assert state2["stop_allowed"] is True
    assert state2["recent_open_issue_counts"][:2] == [0, 0]


def test_live_fix_loop_append_round_resets_streak_when_open_issues_return(tmp_path):
    assert live_fix_loop.main(["init", "--root", str(tmp_path)]) == 0
    assert (
        live_fix_loop.main(
            [
                "append-round",
                "--root",
                str(tmp_path),
                "--round-id",
                "round-001",
                "--focus",
                "FR-09 auth",
                "--new-high-value-issue-count",
                "0",
                "--fixed-count",
                "1",
                "--reopened-issue-count",
                "0",
                "--regression-failures",
                "0",
                "--open-issue-count",
                "0",
                "--closed-issue-count",
                "1",
                "--next-focus",
                "FR-10 reports",
                "--summary",
                "clean round one",
            ]
        )
        == 0
    )
    assert (
        live_fix_loop.main(
            [
                "append-round",
                "--root",
                str(tmp_path),
                "--round-id",
                "round-002",
                "--focus",
                "FR-10 reports",
                "--new-high-value-issue-count",
                "2",
                "--fixed-count",
                "0",
                "--reopened-issue-count",
                "1",
                "--regression-failures",
                "0",
                "--open-issue-count",
                "3",
                "--closed-issue-count",
                "1",
                "--next-focus",
                "FR-12 admin",
                "--summary",
                "new runtime drift discovered",
            ]
        )
        == 0
    )

    state = live_fix_loop._read_state(tmp_path)
    assert state["zero_open_streak"] == 0
    assert state["stop_allowed"] is False
    assert state["recent_open_issue_counts"][:2] == [3, 0]


def test_live_fix_loop_status_require_stop_allowed_returns_nonzero_before_two_clean_rounds(tmp_path):
    assert live_fix_loop.main(["init", "--root", str(tmp_path)]) == 0

    rc = live_fix_loop.main(["status", "--root", str(tmp_path), "--require-stop-allowed"])

    assert rc == 1


def test_live_fix_loop_precheck_reports_success_with_monkeypatched_probes(monkeypatch):
    monkeypatch.setattr(
        live_fix_loop,
        "_health_probe",
        lambda base_url, timeout: {"ok": True, "url": f"{base_url}/health", "status_code": 200, "body": {"status": "ok"}},
    )
    monkeypatch.setattr(
        live_fix_loop,
        "_browser_probe",
        lambda base_url, page_paths, timeout_ms: {
            "ok": True,
            "path": "/login",
            "url": f"{base_url}/login",
            "status": 200,
            "title": "登录",
            "checked_pages": [{"path": "/login", "status": 200}],
        },
    )

    result = live_fix_loop.run_precheck("http://127.0.0.1:8000", ["/", "/login"], 10)

    assert result["allowed_to_continue"] is True
    assert result["blocked_reason"] is None
    assert result["browser"]["path"] == "/login"


def test_live_fix_loop_precheck_reports_browser_blocker(monkeypatch):
    monkeypatch.setattr(
        live_fix_loop,
        "_health_probe",
        lambda base_url, timeout: {"ok": True, "url": f"{base_url}/health", "status_code": 200, "body": {"status": "ok"}},
    )
    monkeypatch.setattr(
        live_fix_loop,
        "_browser_probe",
        lambda base_url, page_paths, timeout_ms: {
            "ok": False,
            "reason": "playwright_unavailable:test",
            "checked_paths": page_paths,
        },
    )

    result = live_fix_loop.run_precheck("http://127.0.0.1:8000", ["/", "/login"], 10)

    assert result["allowed_to_continue"] is False
    assert result["blocked_reason"] == "browser_precheck_failed:playwright_unavailable:test"


def test_live_fix_loop_browser_probe_returns_success_without_touching_closed_page(monkeypatch):
    class FakePlaywrightError(Exception):
        ...

    class FakeResponse:
        status = 200

    class FakePage:
        def __init__(self):
            self.url = ""

        def goto(self, target, wait_until, timeout):
            self.url = target
            return FakeResponse()

        def wait_for_timeout(self, ms):
            return None

        def title(self):
            return "A股研报平台"

    class FakeBrowser:
        def __init__(self):
            self.page = FakePage()

        def new_page(self):
            return self.page

        def close(self):
            return None

    class FakeChromium:
        def launch(self, headless):
            return FakeBrowser()

    class FakePlaywright:
        chromium = FakeChromium()

    class FakeContextManager:
        def __enter__(self):
            return FakePlaywright()

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_sync_api = types.SimpleNamespace(
        Error=FakePlaywrightError,
        sync_playwright=lambda: FakeContextManager(),
    )
    monkeypatch.setitem(sys.modules, "playwright", types.SimpleNamespace(sync_api=fake_sync_api))
    monkeypatch.setitem(sys.modules, "playwright.sync_api", fake_sync_api)

    result = live_fix_loop._browser_probe("http://127.0.0.1:8000", ["/"], 1000)

    assert result["ok"] is True
    assert result["status"] == 200
    assert result["title"] == "A股研报平台"


def test_live_fix_loop_status_json_contains_machine_readable_fields(tmp_path, capsys):
    assert live_fix_loop.main(["init", "--root", str(tmp_path)]) == 0
    assert (
        live_fix_loop.main(
            [
                "append-round",
                "--root",
                str(tmp_path),
                "--round-id",
                "round-001",
                "--focus",
                "FR-09 auth",
                "--new-high-value-issue-count",
                "0",
                "--fixed-count",
                "0",
                "--reopened-issue-count",
                "0",
                "--regression-failures",
                "0",
                "--open-issue-count",
                "0",
                "--closed-issue-count",
                "0",
                "--next-focus",
                "FR-10 reports",
                "--summary",
                "clean status snapshot",
            ]
        )
        == 0
    )
    capsys.readouterr()

    rc = live_fix_loop.main(["status", "--root", str(tmp_path), "--json"])
    captured = capsys.readouterr().out

    assert rc == 0
    payload = json.loads(captured)
    assert payload["current_round_id"] == "round-001"
    assert payload["zero_open_streak"] == 1
    assert payload["stop_allowed"] is False
