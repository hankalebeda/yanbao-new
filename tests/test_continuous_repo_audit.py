from __future__ import annotations

import json
import os
from pathlib import Path

import scripts.doc_driven.audit_blind_spots as blind_spots
from scripts import continuous_repo_audit as audit


def test_continuous_repo_audit_output_root_is_github_automation():
    assert audit.OUTPUT_DIR.as_posix().endswith("github/automation/continuous_audit")
    assert audit.HISTORY_DIR.as_posix().endswith("github/automation/continuous_audit/history")


def test_build_markdown_points_history_note_to_github_automation():
    text = audit.build_markdown(
        started_at="2026-03-23T00:00:00+00:00",
        finished_at="2026-03-23T00:01:00+00:00",
        command_results=[],
        findings=[],
        registry_stats={
            "registry_total": 0,
            "warn_features": 0,
            "catalog_status_summary": {},
            "mismatch_count": 0,
        },
        blind_spot_summary={
            "fake_count": 0,
            "hollow_count": 0,
            "weak_count": 0,
            "guarded_assertions": 0,
            "missing_expectations": 0,
            "pages_without_dom": 0,
            "pages_without_browser": 0,
        },
        history_json_name="sample.json",
    )

    assert "github/automation/continuous_audit/history/" in text


def _configure_audit_outputs(monkeypatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "github" / "automation" / "continuous_audit"
    history_dir = output_dir / "history"
    monkeypatch.setattr(audit, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(audit, "LEGACY_OUTPUT_DIR", tmp_path / "legacy")
    monkeypatch.setattr(audit, "HISTORY_DIR", history_dir)
    monkeypatch.setattr(audit, "ISSUE_LEDGER", output_dir / "continuous_audit_issue_ledger.md")
    monkeypatch.setattr(audit, "LATEST_JSON", output_dir / "latest_run.json")
    monkeypatch.setattr(audit, "LOCK_FILE", output_dir / ".audit.lock")


def _stub_audit_dependencies(monkeypatch) -> None:
    monkeypatch.setattr(
        audit,
        "run_command",
        lambda name, command: audit.CommandResult(
            name=name,
            command=command,
            returncode=0,
            duration_seconds=0.1,
        ),
    )
    monkeypatch.setattr(
        audit,
        "load_registry_stats",
        lambda: {
            "registry_total": 119,
            "warn_features": 0,
            "warn_by_fr": {},
            "catalog_status_summary": {"READY": 119},
            "mismatch_count": 0,
            "mismatch_titles": [],
        },
    )
    monkeypatch.setattr(
        blind_spots,
        "audit_blind_spots",
        lambda: {
            "summary": {
                "fake_count": 0,
                "hollow_count": 0,
                "weak_count": 0,
                "guarded_assertions": 0,
                "missing_expectations": 0,
                "pages_without_dom": 0,
                "pages_without_browser": 0,
            }
        },
    )
    monkeypatch.setattr(audit, "detect_detail_chain_issue", lambda command_results: None)
    monkeypatch.setattr(audit, "detect_page_alignment_issue", lambda: None)
    monkeypatch.setattr(audit, "detect_uncovered_templates_issue", lambda: None)
    monkeypatch.setattr(audit, "detect_features_gate_issue", lambda: None)
    monkeypatch.setattr(audit, "probe_report_list_cutoff", lambda: None)
    monkeypatch.setattr(audit, "probe_features_css", lambda: None)
    monkeypatch.setattr(audit, "probe_html_500_behavior", lambda: None)
    monkeypatch.setattr(audit, "probe_report_status_contract", lambda: None)
    monkeypatch.setattr(audit, "detect_static_registry_issues", lambda registry_stats: [])
    monkeypatch.setattr(audit, "detect_blind_spot_findings", lambda report: [])


def test_continuous_repo_audit_reclaims_stale_lock_and_refreshes_latest_run(monkeypatch, tmp_path):
    _configure_audit_outputs(monkeypatch, tmp_path)
    _stub_audit_dependencies(monkeypatch)
    audit.LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    audit.LOCK_FILE.write_text("pid=999999\nstarted_at=2026-03-24T01:26:33+00:00\n", encoding="utf-8")

    rc = audit.main()

    assert rc == 0
    assert not audit.LOCK_FILE.exists()
    payload = json.loads(audit.LATEST_JSON.read_text(encoding="utf-8"))
    assert payload["status"] == "completed"
    assert len(payload["command_results"]) == 4
    ledger = audit.ISSUE_LEDGER.read_text(encoding="utf-8")
    assert "skipped because a previous audit run" not in ledger
    assert "Registry total features" in ledger
    assert list(tmp_path.rglob("*.tmp")) == []


def test_continuous_repo_audit_writes_skip_payload_when_live_lock_is_held(monkeypatch, tmp_path):
    _configure_audit_outputs(monkeypatch, tmp_path)
    audit.LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    audit.LOCK_FILE.write_text(
        f"pid={os.getpid()}\nstarted_at=2026-03-24T01:26:33+00:00\n",
        encoding="utf-8",
    )

    rc = audit.main()

    assert rc == 0
    assert audit.LOCK_FILE.exists()
    payload = json.loads(audit.LATEST_JSON.read_text(encoding="utf-8"))
    assert payload["status"] == "skipped_locked"
    assert payload["command_results"] == []
    assert payload["findings"] == []
    assert payload["lock_info"]["pid"] == str(os.getpid())
    history_files = list(audit.HISTORY_DIR.glob("*.json"))
    assert len(history_files) == 1
    ledger = audit.ISSUE_LEDGER.read_text(encoding="utf-8")
    assert "skipped because a previous audit run is still holding the lock" in ledger
    assert f"Lock PID: `{os.getpid()}`" in ledger


def test_continuous_repo_audit_preserves_completed_latest_when_skip_locked(monkeypatch, tmp_path):
    _configure_audit_outputs(monkeypatch, tmp_path)
    audit.LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    audit.LATEST_JSON.parent.mkdir(parents=True, exist_ok=True)
    audit.LATEST_JSON.write_text(
        json.dumps(
            {
                "status": "completed",
                "started_at": "2026-03-24T00:00:00+00:00",
                "finished_at": "2026-03-24T00:01:00+00:00",
                "registry_stats": {"warn_features": 3},
                "findings": [{"issue_id": "ISSUE-REGISTRY"}],
            }
        ),
        encoding="utf-8",
    )
    audit.LOCK_FILE.write_text(
        f"pid={os.getpid()}\nstarted_at=2026-03-24T01:26:33+00:00\n",
        encoding="utf-8",
    )

    rc = audit.main()

    assert rc == 0
    payload = json.loads(audit.LATEST_JSON.read_text(encoding="utf-8"))
    assert payload["status"] == "completed"
    assert payload["registry_stats"]["warn_features"] == 3
    history_files = list(audit.HISTORY_DIR.glob("*.json"))
    assert len(history_files) == 1
    history_payload = json.loads(history_files[0].read_text(encoding="utf-8"))
    assert history_payload["status"] == "skipped_locked"
    ledger = audit.ISSUE_LEDGER.read_text(encoding="utf-8")
    assert "last completed run" in ledger


def test_page_alignment_stats_use_page_contract_feature_set():
    stats = audit.load_page_alignment_stats()
    assert "FR03-COOKIE-02" not in stats["registry_page_ids"]
    assert "FR09-BILLING-01" in stats["registry_page_ids"]
    assert "FR09-BILLING-03" in stats["registry_page_ids"]


def test_detect_uncovered_templates_issue_ignores_non_formal_partial_templates(monkeypatch):
    monkeypatch.setattr(
        audit,
        "load_page_alignment_stats",
        lambda: {
            "registry_page_ids": [],
            "expectation_page_ids": [],
            "only_in_registry": [],
            "only_in_expectations": [],
            "missing_templates": ["400.html", "_nav_links.html"],
            "features_selectors_empty": False,
        },
    )
    assert audit.detect_uncovered_templates_issue() is None


def test_page_alignment_stats_cover_formal_templates_and_report_transition_templates():
    stats = audit.load_page_alignment_stats()
    for template in (
        "privacy.html",
        "terms.html",
        "403.html",
        "404.html",
        "500.html",
        "report_error.html",
        "report_loading.html",
        "report_not_ready.html",
    ):
        assert template not in stats["missing_templates"]
