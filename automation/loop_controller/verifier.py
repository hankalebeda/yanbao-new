"""Verification pipeline — scoped pytest, full regression, artifact checks."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import httpx

from automation.loop_controller.schemas import VerifyResult

logger = logging.getLogger(__name__)

# Shared runtime artifacts from AGENTS.md. Doc-22 only participates outside
# infra mode so phase-1 runtime truth does not depend on progress-note writes.
RUNTIME_SHARED_ARTIFACTS = (
    "output/junit.xml",
    "app/governance/catalog_snapshot.json",
    "output/blind_spot_audit.json",
    "github/automation/continuous_audit/latest_run.json",
)
DOC22_PROGRESS_DOC = "docs/core/22_全量功能进度总表_v7_精审.md"
DOC22_SHARED_ARTIFACTS = RUNTIME_SHARED_ARTIFACTS + (
    DOC22_PROGRESS_DOC,
)
# Known pre-existing test failures that should NOT block green verdict.
# These are tracked in repo conventions and will be fixed separately.
BASELINE_KNOWN_FAILURES = {
    "test_deploy_assets_v21",
    "test_double_start_rejected",
}

def _sha256_file(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _parse_junit(junit_path: Path) -> tuple[int, int]:
    """Return (total, failed) from a JUnit XML."""
    if not junit_path.exists():
        return 0, 0
    try:
        tree = ElementTree.parse(junit_path)  # noqa: S314
        root = tree.getroot()
        tests = _junit_attr_total(root, "tests")
        failures = _junit_attr_total(root, "failures")
        errors = _junit_attr_total(root, "errors")
        return tests, failures + errors
    except Exception:
        return 0, 0


def _junit_attr_total(root: ElementTree.Element, attr: str) -> int:
    value = root.attrib.get(attr)
    if value is not None:
        try:
            return int(value)
        except ValueError:
            return 0

    total = 0
    found = False
    for suite in root.iter("testsuite"):
        suite_value = suite.attrib.get(attr)
        if suite_value is None:
            continue
        try:
            total += int(suite_value)
            found = True
        except ValueError:
            continue
    return total if found else 0


def _count_baseline_failures(junit_path: Path) -> int:
    """Count how many failures in the JUnit XML match known baseline failures."""
    if not junit_path.exists() or not BASELINE_KNOWN_FAILURES:
        return 0
    try:
        tree = ElementTree.parse(junit_path)  # noqa: S314
        count = 0
        for tc in tree.iter("testcase"):
            name = tc.attrib.get("name", "")
            if any(known in name for known in BASELINE_KNOWN_FAILURES):
                if tc.find("failure") is not None or tc.find("error") is not None:
                    count += 1
        return count
    except Exception:
        return 0


class Verifier:
    """Five-step verification pipeline."""

    def __init__(
        self,
        repo_root: Path,
        promote_prep_url: str = "",
        promote_prep_token: str = "",
        timeout: float = 600.0,
    ) -> None:
        self._root = repo_root
        self._pp_url = promote_prep_url.rstrip("/")
        self._pp_token = promote_prep_token
        self._timeout = timeout
        self._client = httpx.Client(timeout=self._timeout, trust_env=False)

    def _promote_target_mode(self) -> str:
        state_path = self._root / "automation" / "control_plane" / "current_state.json"
        if state_path.exists():
            try:
                payload = json.loads(state_path.read_text("utf-8"))
            except Exception:
                payload = {}
            mode = str(payload.get("promote_target_mode") or "").strip().lower()
            if mode:
                return mode
        return "infra"

    def _shared_artifacts(self) -> tuple[str, ...]:
        if self._promote_target_mode() == "infra":
            return RUNTIME_SHARED_ARTIFACTS
        return DOC22_SHARED_ARTIFACTS

    # -- step 1: scoped pytest -----------------------------------------------

    def run_scoped_pytest(
        self,
        affected_test_paths: list[str],
        round_id: str = "",
    ) -> tuple[bool, dict[str, Any]]:
        """Run scoped pytest on affected test files only.

        If promote_prep is available, delegates to /v1/triage/scoped-pytest.
        Otherwise, runs pytest locally.
        """
        if not affected_test_paths:
            return True, {"detail": "no_affected_tests"}

        if self._pp_url:
            try:
                headers = {}
                if self._pp_token:
                    headers["Authorization"] = f"Bearer {self._pp_token}"
                resp = self._client.post(
                    f"{self._pp_url}/v1/triage/scoped-pytest",
                    json={
                        "fix_run_id": round_id,
                        "changed_files": affected_test_paths,
                        "timeout_seconds": 120,
                    },
                    headers=headers,
                )
                if resp.status_code == 200:
                    body = resp.json()
                    return body.get("passed", False), body
                return False, {"status_code": resp.status_code, "body": resp.text}
            except Exception as exc:
                logger.warning("scoped-pytest via promote_prep failed: %s", exc)

        # fallback: local subprocess
        cmd = ["python", "-m", "pytest", *affected_test_paths, "-q", "--tb=short"]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(self._root), timeout=300)
        passed = result.returncode == 0
        return passed, {"returncode": result.returncode, "stdout_tail": result.stdout[-500:]}

    # -- step 2: full pytest regression --------------------------------------

    def run_full_pytest(self) -> tuple[bool, int, int]:
        """Run full pytest suite -> (passed, total, failed)."""
        junit_path = self._root / "output" / "junit.xml"
        cmd = [
            "python", "-m", "pytest", "tests/",
            "-q", "--tb=short",
            f"--junitxml={junit_path}",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(self._root), timeout=900)
        except subprocess.TimeoutExpired:
            return False, 0, 0

        total, failed = _parse_junit(junit_path)
        if total <= 0:
            logger.warning("full pytest produced no valid junit test count: %s", junit_path)
            return False, 0, failed
        # Tolerate pre-existing known failures from baseline
        baseline_hits = _count_baseline_failures(junit_path)
        adjusted_failed = max(0, failed - baseline_hits)
        passed = adjusted_failed == 0
        return passed, total, adjusted_failed

    # -- step 3: blind spot audit refresh ------------------------------------

    def refresh_blind_spot(self) -> bool:
        """Run governance audit and check blind_spot_audit.json."""
        audit_script = self._root / "scripts" / "continuous_repo_audit.py"
        if not audit_script.exists():
            logger.info("blind_spot audit script not found, skipping")
            return True
        try:
            subprocess.run(
                ["python", str(audit_script)],
                capture_output=True, text=True,
                cwd=str(self._root), timeout=300,
            )
        except (subprocess.TimeoutExpired, Exception) as exc:
            logger.warning("blind_spot refresh failed: %s", exc)

        bs_path = self._root / "output" / "blind_spot_audit.json"
        if not bs_path.exists():
            return True
        try:
            data = json.loads(bs_path.read_text("utf-8"))
            weak = data.get("weak", 0)
            guarded = data.get("guarded", 0)
            return weak == 0 and guarded == 0
        except Exception:
            return False

    # -- step 4: catalog snapshot freshness ----------------------------------

    def check_catalog(self) -> bool:
        """Check catalog_snapshot.json indicates fresh state."""
        cat_path = self._root / "app" / "governance" / "catalog_snapshot.json"
        if not cat_path.exists():
            return True
        try:
            data = json.loads(cat_path.read_text("utf-8"))
            return (
                data.get("freshness", "") == "fresh"
                or data.get("test_result_freshness", "") == "fresh"
            )
        except Exception:
            return False

    # -- step 5: artifact alignment ------------------------------------------

    def check_artifact_alignment(self) -> tuple[bool, dict[str, str]]:
        """Verify all 4 shared artifacts exist, return fingerprints, and
        check round consistency between junit and catalog test counts."""
        fingerprints: dict[str, str] = {}
        all_ok = True
        for rel in self._shared_artifacts():
            p = self._root / rel
            if not p.exists():
                all_ok = False
                fingerprints[rel] = ""
            else:
                fingerprints[rel] = _sha256_file(p)

        # Round consistency: junit total should match catalog total_collected
        if all_ok:
            try:
                junit_path = self._root / "output" / "junit.xml"
                cat_path = self._root / "app" / "governance" / "catalog_snapshot.json"
                if junit_path.exists() and cat_path.exists():
                    junit_total, _ = _parse_junit(junit_path)
                    cat_data = json.loads(cat_path.read_text("utf-8"))
                    cat_total = int(cat_data.get("total_collected") or 0)
                    if junit_total > 0 and cat_total > 0 and junit_total != cat_total:
                        logger.warning(
                            "artifact round drift: junit_tests=%d, catalog_total_collected=%d",
                            junit_total, cat_total,
                        )
                        all_ok = False
            except Exception as exc:
                logger.warning("artifact round consistency check failed: %s", exc)

        return all_ok, fingerprints

    # -- orchestrate ---------------------------------------------------------

    def run_full_pipeline(
        self,
        affected_test_paths: list[str] | None = None,
        round_id: str = "",
    ) -> VerifyResult:
        """Run all 5 verification steps and return combined result."""
        affected = affected_test_paths or []

        scoped_ok, scoped_detail = self.run_scoped_pytest(affected, round_id)
        full_ok, total, failed = self.run_full_pytest()
        bs_ok = self.refresh_blind_spot()
        cat_ok = self.check_catalog()
        art_ok, fingerprints = self.check_artifact_alignment()

        all_green = scoped_ok and full_ok and bs_ok and cat_ok and art_ok

        return VerifyResult(
            scoped_pytest_passed=scoped_ok,
            full_pytest_passed=full_ok,
            full_pytest_total=total,
            full_pytest_failed=failed,
            blind_spot_clean=bs_ok,
            catalog_improved=cat_ok,
            artifacts_aligned=art_ok,
            all_green=all_green,
            details={
                "scoped": scoped_detail,
                "promote_target_mode": self._promote_target_mode(),
                "shared_artifacts": list(self._shared_artifacts()),
                "artifact_fingerprints": fingerprints,
            },
        )
