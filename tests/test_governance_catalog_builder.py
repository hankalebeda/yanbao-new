from __future__ import annotations

import json
import os
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace

import pytest

from app.governance.build_feature_catalog import (
    DEFAULT_PROGRESS_DOC_REL_PATH,
    _atomic_write_json,
    _collect_feature_marker_index_for_path,
    build_catalog,
    build_catalog_snapshot_payload,
    collect_pytest_nodes,
    derive_feature_verification_metadata,
    get_feature_test_nodes,
    infer_fr_id_from_nodeid,
    load_junit_node_verified_at,
    load_latest_junit_results_bundle,
    resolve_progress_doc_path,
    split_test_collection_map,
    summarize_catalog_audit_scope,
    summarize_catalog_features,
)


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "app" / "governance" / "feature_registry.json"
CATALOG_PATH = ROOT / "app" / "governance" / "catalog_snapshot.json"
pytestmark = [pytest.mark.feature("FR10-FEATURE-01")]


def test_resolve_progress_doc_path_defaults_to_generated_temp_doc(monkeypatch):
    monkeypatch.delenv("FEATURE_CATALOG_DOC_PATH", raising=False)
    resolved = resolve_progress_doc_path()
    assert resolved == ROOT / DEFAULT_PROGRESS_DOC_REL_PATH
    assert "docs/_temp/" in resolved.as_posix()
    assert resolved.name == "22_governance_feature_catalog.generated.md"


def test_resolve_progress_doc_path_honors_override(monkeypatch):
    monkeypatch.setenv("FEATURE_CATALOG_DOC_PATH", "docs/core/custom-governance.md")
    resolved = resolve_progress_doc_path()
    assert resolved == ROOT / "docs" / "core" / "custom-governance.md"


def test_collect_feature_marker_index_for_path_reads_module_class_and_function_markers(tmp_path):
    tests_root = tmp_path / "tests"
    tests_root.mkdir()
    sample = tests_root / "test_feature_markers.py"
    sample.write_text(
        dedent(
            """
            import pytest

            pytestmark = pytest.mark.feature("FR10-FEATURE-01")

            @pytest.mark.feature("FR12-ADMIN-08")
            def test_function_level():
                pass

            @pytest.mark.feature("FR12-ADMIN-06")
            class TestAdmin:
                @pytest.mark.feature("FR12-ADMIN-07")
                def test_method_level(self):
                    pass
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    index = _collect_feature_marker_index_for_path(sample, root=tmp_path)
    assert index["tests/test_feature_markers.py::test_function_level"] == [
        "FR10-FEATURE-01",
        "FR12-ADMIN-08",
    ]
    assert index["tests/test_feature_markers.py::TestAdmin::test_method_level"] == [
        "FR10-FEATURE-01",
        "FR12-ADMIN-06",
        "FR12-ADMIN-07",
    ]


def test_collect_feature_marker_index_for_path_handles_utf8_bom(tmp_path):
    tests_root = tmp_path / "tests"
    tests_root.mkdir()
    sample = tests_root / "test_bom_feature.py"
    sample.write_bytes(
        (
            "\ufeffimport pytest\n\n"
            "@pytest.mark.feature('FR13-EVT-01')\n"
            "def test_bom_feature():\n"
            "    pass\n"
        ).encode("utf-8")
    )

    index = _collect_feature_marker_index_for_path(sample, root=tmp_path)
    assert index["tests/test_bom_feature.py::test_bom_feature"] == ["FR13-EVT-01"]


def test_get_feature_test_nodes_prefers_explicit_feature_mapping_over_fr_fallback():
    feature = {"feature_id": "FR12-ADMIN-08", "fr_id": "FR-12"}
    mapping = {
        "FR12-ADMIN-08": ["tests/test_fr12_admin.py::test_fr12_billing_reconcile_idempotent"],
        "FR-12": ["tests/test_fr12_admin.py::test_fr12_overview_min_fields"],
    }
    assert get_feature_test_nodes(feature, mapping) == [
        "tests/test_fr12_admin.py::test_fr12_billing_reconcile_idempotent"
    ]


def test_get_feature_test_nodes_skips_out_of_ssot_features():
    feature = {"feature_id": "OOS-MOCK-PAY-01", "fr_id": "FR-09", "visibility": "out_of_ssot"}
    mapping = {
        "OOS-MOCK-PAY-01": ["tests/test_fr09_auth.py::test_fr09_mock_pay_routes_retired"],
        "FR-09": ["tests/test_fr09_auth.py::test_fr09_register_returns_201"],
    }
    assert get_feature_test_nodes(feature, mapping) == []


def test_split_test_collection_map_normalizes_duplicate_nodes_and_meta_drift():
    groups, summary = split_test_collection_map(
        {
            "FR10-HOME-01": [
                "tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields",
                "tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields",
            ],
            "FR-10": [
                "tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields",
                "tests/test_fr10_site_dashboard.py::test_fr10_home_term_context",
            ],
            "_other": ["tests/test_misc.py::test_unmapped"],
            "__meta__": {
                "total_collected": 999,
                "mapped_by_feature_marker": 1,
                "mapped_by_fr_inference": 1,
                "unmapped": 0,
            },
        }
    )

    assert groups["FR10-HOME-01"] == ["tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields"]
    assert groups["FR-10"] == [
        "tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields",
        "tests/test_fr10_site_dashboard.py::test_fr10_home_term_context",
    ]
    assert groups["_other"] == ["tests/test_misc.py::test_unmapped"]
    assert summary == {
        "total_collected": 3,
        "mapped_by_feature_marker": 1,
        "mapped_by_fr_inference": 1,
        "unmapped": 1,
    }


def test_atomic_write_json_replaces_catalog_payload_without_leaking_temp_files(tmp_path):
    target = tmp_path / "catalog_snapshot.json"
    target.write_text('{"stale": true}', encoding="utf-8")

    _atomic_write_json(target, {"generated_at": "2026-03-24T02:00:00+00:00", "features": []})

    assert json.loads(target.read_text(encoding="utf-8")) == {
        "generated_at": "2026-03-24T02:00:00+00:00",
        "features": [],
    }
    assert list(tmp_path.glob("*.tmp")) == []


def test_summarize_catalog_features_tracks_negative_bucket_and_latest_feature_timestamp():
    summary = summarize_catalog_features(
        [
            {
                "feature_id": "FR10-HOME-01",
                "structural_status": "READY",
                "visibility": "public",
                "mismatch_flags": [],
                "last_verified_at": "2026-03-23T08:00:00+00:00",
            },
            {
                "feature_id": "FR02-SCHED-02",
                "structural_status": "OUT_OF_SSOT",
                "visibility": "admin",
                "mismatch_flags": ["RETIRED_ROUTE"],
                "last_verified_at": None,
            },
            {
                "feature_id": "FR09-BILLING-03",
                "structural_status": "OUT_OF_SSOT",
                "visibility": "public",
                "mismatch_flags": ["FAIL_CLOSE_ROUTE"],
                "last_verified_at": None,
            },
            {
                "feature_id": "LEGACY-REPORT-01",
                "structural_status": "OUT_OF_SSOT",
                "visibility": "deprecated",
                "mismatch_flags": ["out_of_ssot"],
                "last_verified_at": "2026-03-23T09:15:00+00:00",
            },
        ]
    )

    assert summary["status_summary"] == {"READY": 1, "OUT_OF_SSOT": 3}
    assert summary["negative_status_summary"] == {
        "negative_total": 3,
        "retired_route": 1,
        "fail_close_route": 1,
        "deprecated": 1,
        "out_of_ssot": 0,
    }
    assert summary["latest_feature_verified_at"] == "2026-03-23T09:15:00+00:00"


def test_summarize_catalog_audit_scope_freezes_doc22_denominator_buckets_from_registry():
    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))["features"]

    summary = summarize_catalog_audit_scope(registry)

    assert summary["catalog_total"] == 119
    assert summary["governance_eligible_total"] == 107
    assert summary["governance_negative_total"] == 12
    assert summary["doc22_code_presence_total"] == 113
    assert summary["doc22_active_total"] == 92
    assert summary["doc22_excluded_total"] == 27
    assert summary["legacy_compat_total"] == 4
    assert summary["mock_pay_oos_total"] == 2
    assert summary["provider_policy_oos_total"] == 17
    assert summary["external_blocker_total"] == 4
    assert summary["governance_terminal_total"] == 6
    assert summary["bucket_feature_ids"]["legacy_compat"] == [
        "LEGACY-REPORT-01",
        "LEGACY-REPORT-02",
        "LEGACY-REPORT-03",
        "LEGACY-REPORT-04",
    ]
    assert summary["bucket_feature_ids"]["mock_pay_oos"] == [
        "OOS-MOCK-PAY-01",
        "OOS-MOCK-PAY-02",
    ]
    assert summary["bucket_feature_ids"]["external_blocker"] == [
        "FR09-AUTH-05",
        "FR09-BILLING-01",
        "FR09-BILLING-02",
        "FR12-ADMIN-08",
    ]
    assert summary["bucket_feature_ids"]["governance_terminal"] == [
        "FR02-SCHED-02",
        "FR09-BILLING-03",
        "FR09B-CLEAN-01",
        "FR09B-CLEAN-02",
        "FR09B-CLEAN-03",
        "FR09B-CLEAN-04",
    ]


def test_feature_freshness_is_feature_scoped_not_junit_batch_scoped():
    test_status, last_verified_at = derive_feature_verification_metadata(
        ["tests/test_demo.py::test_case"],
        {"tests/test_demo.py::test_case": "PASS"},
        "2026-03-23T00:00:00+00:00",
    )

    assert test_status == "PASS"
    assert last_verified_at is None


def test_feature_last_verified_at_uses_latest_matching_testcase_timestamp(tmp_path):
    junit_path = tmp_path / "feature_times.xml"
    junit_path.write_text(
        (
            "<testsuites>"
            "<testsuite timestamp='2026-03-23T09:30:00+00:00'>"
            "<testcase classname='tests.test_demo' name='test_case_a' time='5' timestamp='2026-03-23T09:30:10+00:00' />"
            "<testcase classname='tests.test_demo' name='test_case_b' time='7' timestamp='2026-03-23T09:30:12+00:00' />"
            "</testsuite>"
            "</testsuites>"
        ),
        encoding="utf-8",
    )
    node_verified_at = load_junit_node_verified_at(junit_path)
    test_status, last_verified_at = derive_feature_verification_metadata(
        ["tests/test_demo.py::test_case_a", "tests/test_demo.py::test_case_b"],
        {
            "tests/test_demo.py::test_case_a": "PASS",
            "tests/test_demo.py::test_case_b": "PASS",
        },
        "2026-03-23T00:00:00+00:00",
        node_verified_at=node_verified_at,
    )

    assert test_status == "PASS"
    assert last_verified_at == "2026-03-23T09:30:12+00:00"


def test_collect_pytest_nodes_keeps_explicit_feature_markers_out_of_fr_fallback(monkeypatch):
    import app.governance.build_feature_catalog as builder

    monkeypatch.setattr(
        builder,
        "collect_feature_marker_index",
        lambda: {
            "tests/test_doc_driven_verify.py::test_FR10_SITE_01_home_dom": ["FR10-HOME-01"],
            "tests/test_features_page.py::test_features_page_contains_fr_groups": ["FR10-FEATURE-01"],
        },
    )
    monkeypatch.setattr(
        builder.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            stdout="\n".join(
                [
                    "tests/test_doc_driven_verify.py::test_FR10_SITE_01_home_dom",
                    "tests/test_features_page.py::test_features_page_contains_fr_groups",
                    "tests/test_fr10_site_dashboard.py::test_fr10_list_contract",
                ]
            ),
            returncode=0,
        ),
    )

    collected = collect_pytest_nodes()

    assert collected["FR10-HOME-01"] == ["tests/test_doc_driven_verify.py::test_FR10_SITE_01_home_dom"]
    assert collected["FR10-FEATURE-01"] == ["tests/test_features_page.py::test_features_page_contains_fr_groups"]
    assert collected["FR-10"] == ["tests/test_fr10_site_dashboard.py::test_fr10_list_contract"]
    assert "tests/test_doc_driven_verify.py::test_FR10_SITE_01_home_dom" not in collected["FR-10"]
    assert "tests/test_features_page.py::test_features_page_contains_fr_groups" not in collected["FR-10"]
    assert collected["__meta__"] == {
        "total_collected": 3,
        "mapped_by_feature_marker": 2,
        "mapped_by_fr_inference": 1,
        "unmapped": 0,
    }


def test_infer_fr_id_from_nodeid_supports_windows_paths_and_function_fallback():
    assert infer_fr_id_from_nodeid(r"tests\test_fr09b_cleanup.py::test_anything") == "FR-09-b"
    assert infer_fr_id_from_nodeid("tests/test_other.py::test_fr11_review_gate") == "FR-11"
    assert infer_fr_id_from_nodeid("tests/test_other.py::test_anything") is None


def test_registry_fr12_entries_are_not_rotated():
    import json

    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))["features"]
    by_id = {item["feature_id"]: item for item in registry}

    assert "admin/users" in by_id["FR12-ADMIN-01"]["spec_requirement"]
    assert "admin/reports" in by_id["FR12-ADMIN-02"]["spec_requirement"]
    assert "admin/reports/{report_id}" in by_id["FR12-ADMIN-03"]["spec_requirement"]
    assert "force-regenerate" in by_id["FR12-ADMIN-04"]["spec_requirement"]
    assert "admin/overview" in by_id["FR12-ADMIN-05"]["spec_requirement"]
    assert "admin/system-status" in by_id["FR12-ADMIN-06"]["spec_requirement"]
    assert "admin/users/{user_id}" in by_id["FR12-ADMIN-07"]["spec_requirement"]
    assert "billing/orders/{order_id}/reconcile" in by_id["FR12-ADMIN-08"]["spec_requirement"]


def test_catalog_uses_full_test_nodeids_for_last_test_status():
    registry = {
        "generated_at": "2026-03-23T00:00:00+00:00",
        "features": [
            {
                "feature_id": "FR10-HOME-01",
                "fr_id": "FR-10",
                "title": "首页概览",
                "visibility": "public",
                "primary_api": {"method": "GET", "path": "/api/v1/home"},
                "required_test_kinds": ["api"],
                "gaps": [],
            }
        ],
    }
    test_nodes = [f"tests/test_fr10_site_dashboard.py::test_case_{idx}" for idx in range(1, 7)]
    snapshot, _ = build_catalog_snapshot_payload(
        registry=registry,
        code_routes={"GET /api/v1/home"},
        html_templates=set(),
        test_nodes_raw={
            "FR10-HOME-01": test_nodes,
            "__meta__": {
                "total_collected": len(test_nodes),
                "mapped_by_feature_marker": len(test_nodes),
                "mapped_by_fr_inference": 0,
                "unmapped": 0,
            },
        },
        generated_at="2026-03-23T09:00:00+00:00",
        junit_bundle=(
            {**{nodeid: "PASS" for nodeid in test_nodes[:5]}, test_nodes[5]: "FAIL"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
    )

    feature = snapshot["features"][0]
    assert feature["test_nodeids"] == test_nodes
    assert feature["last_test_status"] == "FAIL"
    assert feature["last_verified_at"] is None


@pytest.mark.feature("FR10-FEATURE-01")
def test_build_catalog_snapshot_payload_marks_api_only_features_without_pages():
    snapshot, _ = build_catalog_snapshot_payload(
        registry={
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR00-AUTH-01",
                    "fr_id": "FR-00",
                    "title": "published report readonly",
                    "visibility": "public",
                    "runtime_page_path": None,
                    "primary_api": {"method": "GET", "path": "/api/v1/reports/{report_id}"},
                    "required_test_kinds": ["api"],
                    "gaps": [],
                }
            ],
        },
        code_routes={"GET /api/v1/reports/{report_id}"},
        html_templates=set(),
        test_nodes_raw={
            "FR00-AUTH-01": ["tests/test_fr00_authenticity_guard.py::test_fr00_published_report_readonly"],
            "__meta__": {
                "total_collected": 1,
                "mapped_by_feature_marker": 1,
                "mapped_by_fr_inference": 0,
                "unmapped": 0,
            },
        },
        generated_at="2026-03-23T09:00:00+00:00",
        junit_bundle=(
            {"tests/test_fr00_authenticity_guard.py::test_fr00_published_report_readonly": "PASS"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
    )

    feature = snapshot["features"][0]
    assert feature["structural_status"] == "READY"
    assert feature["route_exists"] is True
    assert feature["page_exists"] is False


@pytest.mark.feature("FR10-FEATURE-01")
def test_build_catalog_snapshot_payload_keeps_page_only_features_without_routes():
    snapshot, _ = build_catalog_snapshot_payload(
        registry={
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR10-LEGAL-01",
                    "fr_id": "FR-10",
                    "title": "terms page",
                    "visibility": "public",
                    "runtime_page_path": "/terms",
                    "primary_api": {},
                    "required_test_kinds": ["page"],
                    "gaps": [],
                }
            ],
        },
        code_routes=set(),
        html_templates={"terms"},
        test_nodes_raw={
            "FR10-LEGAL-01": ["tests/test_doc_driven_verify.py::TestDOMVerification::test_FR10_terms_dom"],
            "__meta__": {
                "total_collected": 1,
                "mapped_by_feature_marker": 1,
                "mapped_by_fr_inference": 0,
                "unmapped": 0,
            },
        },
        generated_at="2026-03-23T09:00:00+00:00",
        junit_bundle=(
            {"tests/test_doc_driven_verify.py::TestDOMVerification::test_FR10_terms_dom": "PASS"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
    )

    feature = snapshot["features"][0]
    assert feature["structural_status"] == "READY"
    assert feature["route_exists"] is False
    assert feature["page_exists"] is True


@pytest.mark.feature("FR09-BILLING-03")
def test_build_catalog_snapshot_payload_marks_fail_close_routes_out_of_ssot():
    snapshot, _ = build_catalog_snapshot_payload(
        registry={
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR09-BILLING-03",
                    "fr_id": "FR-09",
                    "title": "订阅状态查询",
                    "visibility": "public",
                    "governance_flags": ["FAIL_CLOSE_ROUTE"],
                    "primary_api": {"method": "GET", "path": "/api/v1/membership/subscription/status"},
                    "required_test_kinds": ["api"],
                    "gaps": [],
                }
            ],
        },
        code_routes={"GET /api/v1/membership/subscription/status"},
        html_templates=set(),
        test_nodes_raw={
            "FR09-BILLING-03": ["tests/test_fr09_auth.py::test_fr09_subscription_status_route_is_retired"],
            "__meta__": {
                "total_collected": 1,
                "mapped_by_feature_marker": 1,
                "mapped_by_fr_inference": 0,
                "unmapped": 0,
            },
        },
        generated_at="2026-03-23T09:00:00+00:00",
        junit_bundle=(
            {"tests/test_fr09_auth.py::test_fr09_subscription_status_route_is_retired": "PASS"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
    )

    feature = snapshot["features"][0]
    assert feature["structural_status"] == "OUT_OF_SSOT"
    assert feature["mismatch_flags"] == ["FAIL_CLOSE_ROUTE"]
    assert feature["route_exists"] is False
    assert feature["test_nodeids"] == []
    assert feature["last_test_status"] == "UNKNOWN"
    assert snapshot["negative_status_summary"]["fail_close_route"] == 1


@pytest.mark.feature("FR02-SCHED-02")
def test_build_catalog_snapshot_payload_marks_retired_routes_out_of_ssot():
    snapshot, _ = build_catalog_snapshot_payload(
        registry={
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR02-SCHED-02",
                    "fr_id": "FR-02",
                    "title": "DAG 重触发",
                    "visibility": "admin",
                    "governance_flags": ["RETIRED_ROUTE"],
                    "primary_api": {"method": "POST", "path": "/api/v1/admin/dag/retrigger"},
                    "required_test_kinds": ["api"],
                    "gaps": [],
                }
            ],
        },
        code_routes={"POST /api/v1/admin/dag/retrigger"},
        html_templates=set(),
        test_nodes_raw={
            "FR02-SCHED-02": ["tests/test_fr02_scheduler_ops.py::test_fr02_retired_retrigger_route"],
            "__meta__": {
                "total_collected": 1,
                "mapped_by_feature_marker": 1,
                "mapped_by_fr_inference": 0,
                "unmapped": 0,
            },
        },
        generated_at="2026-03-23T09:00:00+00:00",
        junit_bundle=(
            {"tests/test_fr02_scheduler_ops.py::test_fr02_retired_retrigger_route": "PASS"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
    )

    feature = snapshot["features"][0]
    assert feature["structural_status"] == "OUT_OF_SSOT"
    assert feature["mismatch_flags"] == ["RETIRED_ROUTE"]
    assert feature["route_exists"] is False
    assert feature["test_nodeids"] == []
    assert feature["last_test_status"] == "UNKNOWN"
    assert snapshot["negative_status_summary"]["retired_route"] == 1


@pytest.mark.feature("FR09B-CLEAN-01")
@pytest.mark.feature("FR09B-CLEAN-02")
@pytest.mark.parametrize(
    ("feature_id", "path"),
    [
        ("FR09B-CLEAN-01", "/api/v1/internal/reports/clear"),
        ("FR09B-CLEAN-02", "/api/v1/internal/stats/clear"),
    ],
)
def test_build_catalog_snapshot_payload_marks_internal_clear_routes_out_of_ssot(feature_id: str, path: str):
    snapshot, _ = build_catalog_snapshot_payload(
        registry={
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": feature_id,
                    "fr_id": "FR-09-b",
                    "title": "internal clear retired route",
                    "visibility": "internal",
                    "primary_api": {"method": "POST", "path": path},
                    "required_test_kinds": ["api"],
                    "gaps": [],
                }
            ],
        },
        code_routes={f"POST {path}"},
        html_templates=set(),
        test_nodes_raw={
            feature_id: [f"tests/test_fr09b_cleanup.py::test_{feature_id.lower()}_retired"],
            "__meta__": {
                "total_collected": 1,
                "mapped_by_feature_marker": 1,
                "mapped_by_fr_inference": 0,
                "unmapped": 0,
            },
        },
        generated_at="2026-03-23T09:00:00+00:00",
        junit_bundle=(
            {f"tests/test_fr09b_cleanup.py::test_{feature_id.lower()}_retired": "PASS"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
    )

    feature = snapshot["features"][0]
    assert feature["structural_status"] == "OUT_OF_SSOT"
    assert feature["mismatch_flags"] == ["RETIRED_ROUTE"]
    assert feature["route_exists"] is False
    assert feature["test_nodeids"] == []
    assert feature["last_test_status"] == "UNKNOWN"
    assert snapshot["negative_status_summary"]["retired_route"] == 1


@pytest.mark.feature("FR10-FEATURE-01")
def test_build_catalog_snapshot_payload_marks_zero_feature_marker_coverage_as_stale(monkeypatch):
    snapshot, _ = build_catalog_snapshot_payload(
        registry={
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR10-HOME-01",
                    "fr_id": "FR-10",
                    "title": "首页概览",
                    "visibility": "public",
                    "primary_api": {"method": "GET", "path": "/api/v1/home"},
                    "required_test_kinds": ["api"],
                    "gaps": [],
                }
            ],
        },
        code_routes={"GET /api/v1/home"},
        html_templates=set(),
        test_nodes_raw={
            "FR-10": ["tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields"],
            "__meta__": {
                "total_collected": 1,
                "mapped_by_feature_marker": 0,
                "mapped_by_fr_inference": 1,
                "unmapped": 0,
            },
        },
        generated_at="2026-03-23T09:00:00+00:00",
        junit_bundle=(
            {"tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields": "PASS"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "fresh",
            0,
            None,
        ),
        node_verified_at={
            "tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields": "2026-03-23T08:00:00+00:00"
        },
    )

    feature = snapshot["features"][0]
    assert feature["last_test_status"] == "PASS"
    assert feature["last_verified_at"] == "2026-03-23T08:00:00+00:00"
    assert snapshot["test_result_freshness"] == "stale"
    assert snapshot["test_result_stale_reason"] == "feature_marker_coverage_zero"


def test_load_latest_junit_results_bundle_marks_stale_when_newest_candidate_mismatches_total(tmp_path, monkeypatch):
    import app.governance.build_feature_catalog as builder

    older_valid = tmp_path / "older_valid.xml"
    older_valid.write_text(
        "<testsuite><testcase classname='tests.test_demo' name='test_ok' /></testsuite>",
        encoding="utf-8",
    )
    newer_mismatch = tmp_path / "newer_mismatch.xml"
    newer_mismatch.write_text(
        "<testsuite><testcase classname='tests.test_demo' name='test_ok' /><testcase classname='tests.test_demo' name='test_extra' /></testsuite>",
        encoding="utf-8",
    )
    older_valid.touch()
    newer_mismatch.touch()
    monkeypatch.setattr(builder, "JUNIT_CANDIDATES", (newer_mismatch, older_valid))

    parsed, source, generated_at, freshness, age_seconds, stale_reason = load_latest_junit_results_bundle(expected_total=1)

    assert parsed
    # _path_display returns relative path with forward slashes
    expected_display = str(older_valid.relative_to(Path.cwd())).replace("\\", "/") if older_valid.is_relative_to(Path.cwd()) else str(older_valid)
    assert source == expected_display
    assert generated_at is not None
    assert isinstance(age_seconds, int)
    assert freshness == "stale"
    assert stale_reason == "testcase_count_mismatch:2!=1"


@pytest.mark.feature("FR10-FEATURE-01")
def test_load_latest_junit_results_bundle_marks_stale_without_fallback_when_all_candidates_mismatch(tmp_path, monkeypatch):
    import app.governance.build_feature_catalog as builder

    newest_mismatch = tmp_path / "newest_mismatch.xml"
    newest_mismatch.write_text(
        "<testsuite><testcase classname='tests.test_demo' name='test_ok' /><testcase classname='tests.test_demo' name='test_extra' /></testsuite>",
        encoding="utf-8",
    )
    older_mismatch = tmp_path / "older_mismatch.xml"
    older_mismatch.write_text(
        "<testsuite><testcase classname='tests.test_demo' name='test_ok' /><testcase classname='tests.test_demo' name='test_extra' /></testsuite>",
        encoding="utf-8",
    )
    os.utime(older_mismatch, (1_700_000_000, 1_700_000_000))
    os.utime(newest_mismatch, (1_700_000_010, 1_700_000_010))
    monkeypatch.setattr(builder, "JUNIT_CANDIDATES", (newest_mismatch, older_mismatch))

    parsed, source, generated_at, freshness, age_seconds, stale_reason = load_latest_junit_results_bundle(expected_total=1)

    assert parsed == {}
    assert source is None
    assert generated_at is None
    assert age_seconds is None
    assert freshness == "stale"
    assert stale_reason == "testcase_count_mismatch:2!=1"


def test_catalog_snapshot_test_collection_summary_stays_well_formed_without_refreshing_shared_artifacts():
    snapshot = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    test_nodes_raw = collect_pytest_nodes()
    _, current_summary = split_test_collection_map(test_nodes_raw)
    snapshot_summary = snapshot["test_collection_summary"]

    for summary in (snapshot_summary, current_summary):
        assert isinstance(summary, dict)
        assert summary["mapped_by_feature_marker"] >= 1
        assert summary["mapped_by_feature_marker"] + summary["mapped_by_fr_inference"] + summary["unmapped"] == summary["total_collected"]

    expected_total = int(current_summary.get("total_collected") or 0)
    _, source, generated_at, freshness, age_seconds, stale_reason = load_latest_junit_results_bundle(expected_total=expected_total)
    assert freshness in {"fresh", "stale", "missing"}
    assert snapshot["test_result_freshness"] in {"fresh", "stale", "missing"}
    if source is None:
        assert snapshot["test_result_source"] is None or isinstance(snapshot["test_result_source"], str)
    else:
        assert isinstance(snapshot["test_result_source"], str)
    if generated_at is None:
        assert snapshot["test_result_generated_at"] is None or isinstance(snapshot["test_result_generated_at"], str)
    else:
        assert isinstance(snapshot["test_result_generated_at"], str)
    snapshot_age_seconds = snapshot["test_result_age_seconds"]
    if age_seconds is None:
        assert snapshot_age_seconds is None or isinstance(snapshot_age_seconds, int)
    else:
        assert isinstance(snapshot_age_seconds, int)
        assert isinstance(age_seconds, int)
        assert snapshot_age_seconds >= 0
    assert snapshot["test_result_stale_reason"] is None or isinstance(snapshot["test_result_stale_reason"], str)


def test_catalog_counts_come_from_single_generation_chain(monkeypatch):
    import app.governance.build_feature_catalog as builder

    captured: dict[str, int | Path | None] = {}

    def _fake_bundle(*, expected_total=None):
        captured["bundle_expected_total"] = expected_total
        return {}, "output/junit.xml", None, "stale", None, "testcase_count_mismatch:4!=2"

    def _fake_verified(path):
        captured["verified_path"] = path
        return {}

    monkeypatch.setattr(builder, "load_latest_junit_results_bundle", _fake_bundle)
    monkeypatch.setattr(builder, "load_junit_node_verified_at", _fake_verified)

    snapshot, _ = builder.build_catalog_snapshot_payload(
        registry={
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR10-HOME-01",
                    "fr_id": "FR-10",
                    "title": "首页概览",
                    "visibility": "public",
                    "primary_api": {"method": "GET", "path": "/api/v1/home"},
                    "required_test_kinds": ["api"],
                    "gaps": [],
                }
            ],
        },
        code_routes={"GET /api/v1/home"},
        html_templates=set(),
        test_nodes_raw={
            "FR10-HOME-01": [
                "tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields",
                "tests/test_fr10_site_dashboard.py::test_fr10_home_term_context",
            ],
            "FR-10": [
                "tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields",
                "tests/test_fr10_site_dashboard.py::test_fr10_home_term_context",
            ],
            "__meta__": {},
        },
        generated_at="2026-03-23T09:00:00+00:00",
        junit_bundle=None,
    )

    assert captured["bundle_expected_total"] == 2
    assert captured["verified_path"] == ROOT / "output" / "junit.xml"
    assert snapshot["total"] == 1
    assert snapshot["status_summary"] == {"READY": 1}
    assert snapshot["negative_status_summary"]["negative_total"] == 0
    assert snapshot["audit_scope_summary"]["doc22_active_total"] == 1
    assert snapshot["latest_feature_verified_at"] is None


def test_build_catalog_does_not_auto_refresh_stale_junit_by_default(monkeypatch):
    import app.governance.build_feature_catalog as builder

    refresh_calls = {"count": 0}
    captured = {"bundle": None}

    monkeypatch.setattr(
        builder,
        "load_registry",
        lambda: {
            "generated_at": "2026-03-23T00:00:00+00:00",
            "features": [
                {
                    "feature_id": "FR10-HOME-01",
                    "fr_id": "FR-10",
                    "title": "首页概览",
                    "visibility": "public",
                    "primary_api": {"method": "GET", "path": "/api/v1/home"},
                    "required_test_kinds": ["api"],
                    "gaps": [],
                }
            ],
        },
    )
    monkeypatch.setattr(builder, "scan_fastapi_routes", lambda: {"GET /api/v1/home"})
    monkeypatch.setattr(builder, "scan_html_templates", lambda: set())
    monkeypatch.setattr(
        builder,
        "collect_pytest_nodes",
        lambda: {
            "FR10-HOME-01": ["tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields"],
            "__meta__": {
                "total_collected": 1,
                "mapped_by_feature_marker": 1,
                "mapped_by_fr_inference": 0,
                "unmapped": 0,
            },
        },
    )
    monkeypatch.setattr(
        builder,
        "load_latest_junit_results_bundle",
        lambda **_kwargs: (
            {"tests/test_fr10_site_dashboard.py::test_fr10_home_api_fields": "PASS"},
            "output/junit.xml",
            "2026-03-23T08:00:00+00:00",
            "stale",
            99,
            "testcase_count_mismatch:1!=2",
        ),
    )
    monkeypatch.setattr(builder, "load_junit_node_verified_at", lambda _path: {})

    def _fake_build_snapshot_payload(**kwargs):
        captured["bundle"] = kwargs["junit_bundle"]
        return {
            "features": [],
            "status_summary": {},
            "negative_status_summary": {"negative_total": 0},
        }, []

    monkeypatch.setattr(builder, "build_catalog_snapshot_payload", _fake_build_snapshot_payload)
    monkeypatch.setattr(builder, "_atomic_write_json", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(builder, "generate_22_doc", lambda *_args, **_kwargs: None)

    def _fake_refresh_primary_junit_artifact():
        refresh_calls["count"] += 1
        return 0, None

    monkeypatch.setattr(builder, "refresh_primary_junit_artifact", _fake_refresh_primary_junit_artifact)

    build_catalog(write_progress_doc=False)

    assert refresh_calls["count"] == 0
    assert captured["bundle"] is not None
    assert captured["bundle"][3] == "stale"
    assert captured["bundle"][5] == "testcase_count_mismatch:1!=2"
