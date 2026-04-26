from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.doc_driven.audit_blind_spots import _route_coverage_stats, _scan_guarded_assertions, _write_report_artifacts
from scripts.doc_driven.parse_progress_doc import build_claim_registry
from scripts.doc_driven.page_expectations import get_expectation_by_page
from scripts.doc_driven.audit_test_quality import audit_test_quality
from scripts.doc_driven.render_v8_doc import render_v8_doc

ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.feature("FR10-FEATURE-01")]


def _fingerprints(report: list[dict]) -> set[tuple[str, str, str]]:
    fingerprints: set[tuple[str, str, str]] = set()
    for item in report:
        raw_path = str(item["test_file"]).replace("\\", "/")
        if "/tests/" in raw_path:
            raw_path = "tests/" + raw_path.split("/tests/", 1)[1]
        elif not raw_path.startswith("tests/"):
            raw_path = f"tests/{Path(raw_path).name}"
        fingerprints.add((raw_path, str(item["test_func"]), str(item["pattern"])))
    return fingerprints


def test_doc_driven_audit_quality_is_path_invariant_and_gates_new_weak_assertions():
    expected: set[tuple[str, str, str]] = {
        ("tests/test_api.py", "test_demo_report_cached_fallback_plain_blocks", "html_substring_check"),
        ("tests/test_api.py", "test_page_routes_return_200", "html_substring_check"),
    }

    relative_report = audit_test_quality(Path("tests"))
    absolute_report = audit_test_quality(ROOT / "tests")

    assert _fingerprints(relative_report) == expected
    assert _fingerprints(absolute_report) == expected


def test_blind_spot_route_inventory_tracks_mock_pay_expectation():
    stats = _route_coverage_stats()
    assert "/billing/mock-pay/{order_id}" in stats["html_routes"]
    assert "/billing/mock-pay/{order_id}" not in stats["missing_expectations"]
    assert "mock_pay_retired" in stats["retired_compat_pages"]


def test_mock_pay_expectation_is_explicitly_modeled_as_retired_compat():
    expectation = get_expectation_by_page("mock_pay_retired")
    assert expectation is not None
    assert expectation.contract_kind == "compat_json"
    assert expectation.retention_mode == "retired_compat"
    assert expectation.expect_dom_reference is False
    assert expectation.expect_browser_reference is False


def test_formal_info_error_templates_are_modeled_in_page_expectations():
    expected_templates = {
        "terms": "terms.html",
        "privacy": "privacy.html",
        "403": "403.html",
        "404": "404.html",
        "500": "500.html",
        "report_error": "report_error.html",
        "report_loading": "report_loading.html",
        "report_not_ready": "report_not_ready.html",
    }
    for page_id, template in expected_templates.items():
        expectation = get_expectation_by_page(page_id)
        assert expectation is not None, f"missing page expectation: {page_id}"
        assert expectation.template == template


def test_formal_error_templates_are_tracked_without_forced_route_level_browser_evidence():
    for page_id in ("403", "404", "500"):
        expectation = get_expectation_by_page(page_id)
        assert expectation is not None
        assert expectation.expect_dom_reference is False
        assert expectation.expect_browser_reference is False


def test_blind_spot_route_inventory_supports_attribute_selectors():
    stats = _route_coverage_stats()
    assert 'report_error:a[href="/reports"]' not in stats["selectors_missing_in_template"]


def test_claim_registry_defaults_to_registry_main_chain():
    claims = build_claim_registry()
    by_id = {item["feature_id"]: item for item in claims}

    assert "FR10-HOME-01" in by_id
    assert by_id["FR10-HOME-01"]["claim_source"] == "feature_registry"
    assert by_id["FR03-COOKIE-02"]["claim_source"] == "feature_registry"


def test_render_v8_doc_marks_registry_chain_as_default_input():
    text = render_v8_doc(
        claim_registry=[
            {
                "fr_id": "FR-10",
                "feature_id": "FR10-HOME-01",
                "title": "首页",
                "claimed_gap": "无",
                "code_rating": "ok",
                "test_rating": "ok",
            }
        ],
        gap_report={"summary": {"total_features": 1, "by_verdict": {"CONFIRMED": 1}, "confirmed_rate": 100.0}, "details": []},
        quality_report=[],
        verification_plan=[],
    )

    assert "SSOT 01~05 + feature_registry.json + page_expectations.py" in text
    assert "legacy/compat 22_v7" not in text


def test_guarded_assertion_scan_ignores_nested_helper_passes():
    findings = _scan_guarded_assertions()
    assert not any(
        item.test_file == "tests/test_live_fix_loop.py"
        and item.test_func == "test_live_fix_loop_browser_probe_returns_success_without_touching_closed_page"
        and item.pattern == "pass_in_test"
        for item in findings
    )


def test_blind_spot_cli_artifacts_are_written_atomically(tmp_path):
    report = {
        "generated_at": "2026-03-24T02:05:00+00:00",
        "summary": {
            "fake_count": 0,
            "hollow_count": 0,
            "weak_count": 1,
            "guarded_assertions": 0,
            "time_coupled_seed_defaults": 0,
            "missing_expectations": 0,
            "pages_without_dom": 0,
            "pages_without_browser": 0,
            "mapping_drift": 0,
            "selector_drift": 0,
        },
        "weak_tests": [],
        "guarded_assertions": [],
        "seed_default_issues": [],
        "route_stats": {},
        "feature_stats": {},
        "quality_report": [],
    }
    output_json = tmp_path / "blind_spot_audit.json"
    output_md = tmp_path / "blind_spot_audit.md"

    _write_report_artifacts(report, output_json=output_json, output_md=output_md)

    assert json.loads(output_json.read_text(encoding="utf-8")) == report
    assert output_md.read_text(encoding="utf-8").startswith("# Blind Spot Audit")
    assert list(tmp_path.glob("*.tmp")) == []
