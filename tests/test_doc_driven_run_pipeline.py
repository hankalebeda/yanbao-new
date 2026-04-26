from __future__ import annotations

import pytest

from scripts.doc_driven.run_pipeline import (
    STAGE3_TEST_TARGETS,
    _build_stage3_feature_marker_index,
    _feature_ids_for_nodeid,
)


pytestmark = [pytest.mark.feature("FR10-FEATURE-01")]


def test_stage3_targets_cover_governance_and_features_surfaces():
    assert STAGE3_TEST_TARGETS == (
        "tests/test_doc_driven_verify.py",
        "tests/test_features_page.py",
        "tests/test_governance_alignment.py",
    )


def test_stage3_feature_mapping_prefers_pytest_feature_markers():
    marker_index = _build_stage3_feature_marker_index()

    assert _feature_ids_for_nodeid(
        "tests/test_features_page.py::TestFeaturesPage::test_features_page_contains_fr_groups",
        marker_index,
    ) == ["FR10-FEATURE-01"]
    assert _feature_ids_for_nodeid(
        "tests/test_doc_driven_verify.py::TestViewerTierAccess::test_FR09_free_advanced_truncated",
        marker_index,
    ) == ["FR10-DETAIL-02"]
    assert _feature_ids_for_nodeid(
        "tests/test_doc_driven_verify.py::TestViewerTierAccess::test_FR09_auth_me_truth_for_admin_pro_free",
        marker_index,
    ) == ["FR09-AUTH-08"]
